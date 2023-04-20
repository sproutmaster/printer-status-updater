from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from sys import exit
from os import getenv
import aiohttp
import asyncio
import redis

load_dotenv()


async def get_status(session, url):
    async with session.get(url) as resp:
        res = await resp.json()
        return res['response']


async def main():
    """
    Read printer ip's from database and use printer info snatcher to get the data. After getting the data push it
    into redis
    """
    mongo_uri = getenv("MONGODB_URI")
    redis_uri = getenv("REDIS_URI")
    snatcher_uri = getenv("SNATCHER_URI")
    mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))

    try:
        mongo_client.admin.command('ping')
    except Exception as e:
        exit(e)

    collection = mongo_client["printer-status"]["printers"]

    printers = collection.find()
    printer_ip_list = [p['ip'] for p in printers]

    async with aiohttp.ClientSession() as session:
        tasks = []
        for ip in printer_ip_list:
            url = f'{snatcher_uri}?ip={ip}'
            tasks.append(asyncio.ensure_future(get_status(session, url)))
        done = await asyncio.gather(*tasks)
        for wo in done:
            print(wo)


if __name__ == '__main__':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
