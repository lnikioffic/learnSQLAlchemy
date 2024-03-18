from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import URL, create_engine
from config import settings


# Здесь ещё не будет подключения к БД
engine = create_engine(
    url=settings.DATABASE_URL_psycopg,
    echo=True, #вся работа алхимии показывается в консоле
    # pool_size=5, #кол-во подключений к БД
    # max_overflow=10 #доп подключения
)


engine_async = create_async_engine(
    url=settings.DATABASE_URL_asyncpg,
    # echo=True, #вся работа алхимии показывается в консоле
)


#Session нужна для транзакций
#фабрика
session_factory = sessionmaker(engine)
async_session_factory = async_sessionmaker(engine_async)


