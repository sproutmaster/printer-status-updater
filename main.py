from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pymongo.errors
from urllib.error import URLError
from urllib.parse import urlparse
import urllib.request
from dotenv import load_dotenv
from os import getenv
import aiohttp
import asyncio
import redis
import json
import sys

load_dotenv()

STATE = {
    "running": False,
    "refresh_interval": 60,
    "session_timeout": 9000
}


async def get_status(session: aiohttp.ClientSession, url: str) -> dict | None:
    """
    Gets printer status from snatcher
    :param session: session
    :param url: query url
    :return: response
    """
    try:
        async with session.get(url, timeout=STATE['session_timeout']) as resp:
            res = json.loads(await resp.text())
            return {'response': res['response']['message'],
                    'printer_ip': res['request']['ip'],
                    'request_status': 'ok' if res['response']['status'] == 'success' else 'error'
                    }

    except:
        return


async def main():
    """
    Read printer IPs from database and use printer info snatcher to get the data. After getting the data push it
    into redis
    """
    snatcher_uri = getenv("SNATCHER_URI")
    redis_uri_parsed = urlparse(getenv("REDIS_URI"))

    mongo_client = MongoClient(getenv("MONGODB_URI"), server_api=ServerApi('1'))
    redis_client = redis.Redis(host=redis_uri_parsed.hostname, port=redis_uri_parsed.port)

    # check if services are online, else throw an error
    async def update_data() -> None:
        """
        Get all IP addresses from database and add data fetch task to queue. After getting the data, push it into redis
        """

        try:
            urllib.request.urlopen(getenv("SNATCHER_URI"))
            redis_client.ping()

            printer_collection = mongo_client["printer-status"]["printers"]
            printers = printer_collection.find()
            printer_ip_list = [p['ip'] for p in printers]

        except URLError:
            print("Cannot connect to Snatcher")
            return

        except pymongo.errors.ConnectionFailure:
            print("Cannot connect to MongoDB")
            return

        except redis.exceptions.ConnectionError:
            print("Cannot connect to Redis")
            return

        try:
            async with aiohttp.ClientSession() as session:
                tasks = []
                for ip in printer_ip_list:
                    url = f'{snatcher_uri}?ip={ip}'
                    tasks.append(asyncio.ensure_future(get_status(session, url)))
                result = await asyncio.gather(*tasks)

                if None not in result:
                    redis_client.set('printer_data', json.dumps(result))
                    print(f"{len(result)} objects pushed to redis")

        except aiohttp.ServerDisconnectedError:
            return

    def update_state():
        """
        Update state settings from database
        """
        settings_collection = mongo_client["settings"]["printer-status"]
        settings = settings_collection.find({"properties": True})
        STATE['running'] = settings[0]['running']
        STATE['refresh_interval'] = settings[0]['refresh_interval']
        redis_client.set('running', str(STATE['running']))
        print(f"Running: {STATE['running']}, Sleeping for: {STATE['refresh_interval']} sec")

    # XXX: execution starts from here
    """
    Update state settings from database. If running state is true, update data and sleep for refresh interval
    """
    while True:
        update_state()
        if STATE['running']:
            await update_data()
        await asyncio.sleep(STATE['refresh_interval'])


if __name__ == '__main__':
    if sys.platform == 'linux':
        asyncio.run(main())
    elif sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
