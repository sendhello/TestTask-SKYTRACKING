#!/usr/bin/python3

import asyncio
import aiohttp
import re
import logging

from time import time
from sqlalchemy import create_engine, Column, Integer, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

logging.basicConfig(level='DEBUG')

Base = declarative_base()
t0 = time()


class Page(Base):
    __tablename__ = 'wiki_page'
    id = Column(Integer, primary_key=True)
    url = Column(Text)
    request_depth = Column(Integer)
    parent = Column(Integer)

    def __init__(self, url, request_depth, parent):
        self.url = url
        self.request_depth = request_depth
        self.parent = parent

    def __repr__(self):
        return f'{self.url}, {self.request_depth}, {self.parent}'


def write_to_file(data):
    with open('res.txt', 'a', encoding='utf-8') as f:
        f.write(str(data))


async def fetch(url, session):
    async with session.get(url, allow_redirects=True) as response:
        return await response.read()


async def get_content(url, session, depth, depth_curent, sql_session, parent):
    if parent == 0:
        page = Page(url, depth, parent)
        sql_session.add(page)
        sql_session.commit()
        sql_session.close()

    for id in sql_session.query(Page.id).filter(Page.url == url):
        parent = id[0]
        break

    response = await fetch(url, session)

    links = []
    if depth_curent < depth:
        links = re.findall(r"<a.*?href=[\"\']{1}/wiki/(.*?)[\"\']{1}.*?>", str(response.decode()))
        for i in range(len(links)):
            links[i] = 'https://ru.wikipedia.org/wiki/' + links[i]
    depth_curent += 1
    tasks = []
    for link in links:
        page = Page(link, depth_curent, parent)
        sql_session.add(page)
        if depth_curent < depth:
            tasks.append(get_content(link, session, depth, depth_curent, sql_session, parent))
    sql_session.commit()
    sql_session.close()

    logging.debug(f'{datetime.now()}:    Send to GATHER: {len(tasks)} elements.')
    logging.debug(f'{datetime.now()}:    All tasks: {len(asyncio.tasks.all_tasks())}')
    await asyncio.gather(*tasks)
    logging.debug(f'{datetime.now()}:    After GATHER')


async def main(url, depth):
    engine_string = 'postgresql+psycopg2://wiki:wikipass@localhost:5432/wiki'
    engine = create_engine(engine_string)
    Base.metadata.create_all(engine)
    SQL_Session = sessionmaker(bind=engine)
    sql_session = SQL_Session()

    parent = 0
    depth_curent = 0

    session_timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        await get_content(url, session, depth, depth_curent, sql_session, parent)


if __name__ == '__main__':
    url = 'https://ru.wikipedia.org/wiki/Служебная:Случайная_страница'
    depth = 3

    asyncio.run(main(url, depth))

    logging.debug(f'{datetime.now()}:    Time of run: {time() - t0}')
