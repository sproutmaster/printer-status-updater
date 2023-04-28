from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pymongo.errors
from urllib.error import URLError
from urllib.parse import urlparse
import urllib.request
from dotenv import load_dotenv
from sys import exit
from os import getenv
from enum import IntFlag
import aiohttp
import asyncio
import redis
import json

load_dotenv()


class FLAGS(IntFlag):
    state = 1
    count = 1


async def get_status(session, url):
    """
    If printer is online,
    :param session: session
    :param url: query url
    :return: None
    """
    async with session.get(url, timeout=9000) as resp:
        res = json.loads(await resp.text())
        cache_out = {'response': res['response']['message'],
                     'printer_ip': res['request']['ip'],
                     'request_status': 'ok' if res['response']['status'] == 'success' else 'error'
                     }
        return cache_out


async def main():
    """
    Read printer ips from database and use printer info snatcher to get the data. After getting the data push it
    into redis
    """
    snatcher_uri = getenv("SNATCHER_URI")
    redis_uri_parsed = urlparse(getenv("REDIS_URI"))

    mongo_client = MongoClient(getenv("MONGODB_URI"), server_api=ServerApi('1'))
    redis_client = redis.Redis(host=redis_uri_parsed.hostname, port=redis_uri_parsed.port)

    # check if services are online, else throw an error
    async def update_data():
        try:
            mongo_client.admin.command('ping')
            redis_client.ping()
            urllib.request.urlopen(getenv("SNATCHER_URI"))

        except URLError:
            exit("Cannot connect to Snatcher")

        except pymongo.errors.ConnectionFailure:
            exit("Cannot connect to MongoDB")

        except redis.exceptions.ConnectionError:
            exit("Cannot connect to Redis")

        collection = mongo_client["printer-status"]["printers"]

        printers = collection.find()
        printer_ip_list = [p['ip'] for p in printers]

        try:
            async with aiohttp.ClientSession() as session:
                tasks = []
                for ip in printer_ip_list:
                    url = f'{snatcher_uri}?ip={ip}'
                    tasks.append(asyncio.ensure_future(get_status(session, url)))
                done = await asyncio.gather(*tasks)
                for wo in done:
                    print(wo)
        except aiohttp.ServerDisconnectedError:
            await asyncio.sleep(10)

    def update_state():
        """
        Update Flags from database

        """
        pass

    while True:
        update_state()
        await asyncio.sleep(10)
        if FLAGS.state:
            await update_data()


if __name__ == '__main__':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
