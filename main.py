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
from aiohttp.client_exceptions import ServerDisconnectedError, ClientConnectorError, ClientOSError, ClientPayloadError

logging.basicConfig(level='DEBUG', filename='log.log')

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


async def fetch(url, session):
    log_error = ''
    for i in range(1, 4):
        try:
            logging.info(f'{datetime.now()}:    trying ({i}) to run session.get with url: {url}')
            async with session.get(url, allow_redirects=True) as response:
                return await response.read()
        except ServerDisconnectedError as e:
            log_error = f'{datetime.now()}:    Error ServerDisconnectedError: {e}, url: {url}'
            logging.warning(log_error)
            continue
        except ClientConnectorError as e:
            log_error = f'{datetime.now()}:    Error ClientConnectorError: {e}, url: {url}'
            logging.warning(log_error)
            continue
        except ClientOSError as e:
            log_error = f'{datetime.now()}:    Error ClientOSError: {e}, url: {url}'
            logging.warning(log_error)
            continue
        except ClientPayloadError as e:
            log_error = f'{datetime.now()}:    Error ClientPayloadError: {e}, url: {url}'
            logging.warning(log_error)
            continue
    else:
        logging.error(log_error)
        return ''.encode()


async def get_content(url, session, depth, depth_curent, sql_session, parent):
    if parent == 0:
        page = Page(url, depth_curent, parent)
        sql_session.add(page)
        sql_session.commit()

    response = await fetch(url, session)

    links = re.findall(r"<a.*?href=[\"\']{1}/wiki/(.*?)[\"\']{1}.*?>", str(response.decode()))
    for i in range(len(links)):
        links[i] = 'https://ru.wikipedia.org/wiki/' + links[i]

    depth_curent += 1
    tasks = []
    if links:
        parent = sql_session.query(Page).filter(Page.url == url)[-1].id
        for link in links:
            sql_session.add(Page(link, depth_curent, parent))
            if depth_curent < depth:
                tasks.append(get_content(link, session, depth, depth_curent, sql_session, parent))
        sql_session.commit()
        logging.debug(f'{datetime.now()}:    All tasks in eventloop: {len(asyncio.tasks.all_tasks())}')
        if tasks:
            logging.debug(f'{datetime.now()}:    Send to eventloop: {len(tasks)} tasks.')
            await asyncio.gather(*tasks)


async def main(url, depth, engine_string):
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
    engine_string = 'postgresql+psycopg2://wiki:wikipass@localhost:5432/wiki'
    depth = 3

    asyncio.run(main(url, depth, engine_string))
    logging.info(f'{datetime.now()}:    Time of work: {time() - t0}')
