#!/usr/bin/python3

import asyncio
import aiohttp
import re
import logging

from time import time
from sqlalchemy import create_engine, Column, Integer, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from aiohttp.client_exceptions import ServerDisconnectedError, ClientConnectorError, ClientOSError, ClientPayloadError

logging.basicConfig(level='DEBUG', filename='log.log')

Base = declarative_base()
t0 = time()
visited = {}


class Page(Base):
    __tablename__ = 'wiki_page'
    id = Column(Integer, primary_key=True)
    url = Column(Text)
    request_depth = Column(Integer)

    def __init__(self, url, request_depth):
        self.url = url
        self.request_depth = request_depth

    def __repr__(self):
        return f'{self.url}, {self.request_depth}'


class Communication(Base):
    __tablename__ = 'page_communication'
    id = Column(Integer, primary_key=True)
    from_page_id = Column(Integer, ForeignKey('wiki_page.id', ondelete='CASCADE'))
    link_id = Column(Integer, ForeignKey('wiki_page.id', ondelete='CASCADE'))

    def __init__(self, from_page_id, link_id):
        self.from_page_id = from_page_id
        self.link_id = link_id

    def __repr__(self):
        return f'{self.from_page_id}, {self.link_id}'


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


async def get_content(url, session, depth, depth_curent, sql_session, parent_page):
    response = await fetch(url, session)
    links = re.findall(r"<a.*?href=[\"\']{1}/wiki/(.*?)[\"\']{1}.*?>", str(response.decode()))

    global visited
    tasks = []
    depth_curent += 1

    if links:
        for link in links:
            link = 'https://ru.wikipedia.org/wiki/' + link
            if link not in visited:
                page = Page(link, depth_curent)
                sql_session.add(page)
                sql_session.commit()
                sql_session.add(Communication(parent_page.id, page.id))
                visited[link] = page.id
                if depth_curent < depth:
                    tasks.append(get_content(link, session, depth, depth_curent, sql_session, page))
            else:
                sql_session.add(Communication(parent_page.id, visited[link]))
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

    depth_curent = 0

    parent_page = Page(url, depth_curent)
    sql_session.add(parent_page)
    sql_session.commit()

    global visited
    visited[url] = parent_page.id

    session_timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        await get_content(url, session, depth, depth_curent, sql_session, parent_page)


if __name__ == '__main__':
    url = 'https://ru.wikipedia.org/wiki/Плаунок_Мёллендорфа'
    engine_string = 'postgresql+psycopg2://wiki:wikipass@localhost:5432/wiki'
    depth = 3

    asyncio.run(main(url, depth, engine_string))
    logging.info(f'{datetime.now()}:    Time of work: {time() - t0}')
