#!/usr/bin/python3

import asyncio
import aiohttp
import re
from time import time

t0 = time()
s = 0


def write_to_file(data):
    with open('res.txt', 'a', encoding='utf-8') as f:
        f.write(str(data))


async def fetch(url, session):
    async with session.get(url, allow_redirects=True) as response:
        return await response.read()


async def get_content(url, session, depth):

    global s
    s += 1

    response = await fetch(url, session)
    write_to_file(f'depth{depth}:    {url:70}    |   {type(response.decode())}\n')
    links = []
    if depth < 1:
        links = re.findall(r"<a.*?href=[\"\']{1}/wiki/(.*?)[\"\']{1}.*?>", str(response.decode()))
        for i in range(len(links)):
            links[i] = 'https://ru.wikipedia.org/wiki/' + links[i]
    depth += 1
    tasks = []
    for link in links:
        tasks.append(get_content(link, session, depth))
    await asyncio.gather(*tasks)


async def main(url, depth):
    session_timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        await get_content(url, session, depth)


if __name__ == '__main__':
    # url = 'https://ru.wikipedia.org/wiki/Служебная:Случайная_страница'
    url = 'https://ru.wikipedia.org/wiki/Жуковское_(Одесская_область)'
    depth = 0

    asyncio.run(main(url, depth))

    print(f'count_page = {s}')
    print(f' {time() - t0}')