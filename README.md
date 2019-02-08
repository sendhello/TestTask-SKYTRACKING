

# Тестовое задание
Тестовое задание на тему асинхронности и баз данных

Требуется написать рекурсивный парсер википедии с журналированием на глубину 6 страниц. 

Вся работа должна выполняться в асинхронной функции на python3.6 или старше.

Вот примерный алгоритм работы:

- Инициализировать eventloop
- На вход асинхронной функции подать URL одной страницы из Википедии.
- Подключиться к БД и записать URL текущей страницы
- Скачать страницу по URL и извлечь все ссылки на другие статьи по шаблону <a href="#(*)">, найдете другие способы получить эти ссылки - пожалуйста.
- По каждой найденной ссылке:
    - записать URL ссылки в таблицу страниц, + в таблицу связей записать связанность страниц .
    - закрыть подключение к БД!!! (попробуйте не закрывать, посмотрите что будет, как с этим бороться с точки зрения postgres (/var/lib/pgsql/data/postgresql.conf)?)
    - запустить описанный выше алгоритм для данной ссылки.
    - И так до тех пор, пока глубина вызова не достигнет 6. Для отладки сделать 2 (там очень много вызовов получится, это рекурсия)
- Выйти из функции
 
Примерная структура таблиц:
- таблица страниц

id(PK)

URL

request_depth

- таблица связей

from_page_id(FK) таблица страниц.id

link_id(FK) таблица страниц.id


Для облегчения работы с БД предлагаю освоить ORM SQLAlchemy, но использовать 
psycopg2.cursor и чистый SQLне возбраняется.Использовать Postgres - не обязательно, можно обойтись и sqlite.

 

Литература и примеры:

Асинхронные запросы

https://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html

sqlalchemy

https://www.sqlalchemy.org/

https://docs.sqlalchemy.org/en/latest/orm/extensions/declarative/basic_use.html

https://docs.sqlalchemy.org/en/latest/orm/extensions/declarative/basic_use.html#accessing-the-metadata

Результат разместить в публичном репозитории до 11.02.2019 .

Продление срока исполнения тестового задания обсуждается после оценки предварительных результатов.
