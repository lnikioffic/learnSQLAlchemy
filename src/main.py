import os
import sys

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
sys.path.insert(1, os.path.join(sys.path[0], '..'))

#from hh.service.core import SyncCore, AsyncCore 
from hh.service.orm import SyncORM, AsyncORM
import asyncio
from hh.schemas import WorkersRelDTO

# def sync_db_core():
#     SyncCore.create_table()

#     SyncCore.insert_workers()

#     SyncCore.select_workers()

#     SyncCore.update_worker()

#     SyncCore.insert_resume()

#     SyncCore.select_resumes_avg_compensation()


async def async_db_core():
    pass


def sync_db_ORM():
    SyncORM.create_table()

    SyncORM.insert_workers()

    SyncORM.select_workers()

    SyncORM.update_worker()

    SyncORM.insert_resume()

    SyncORM.insert_additional_resumes()

    SyncORM.select_resumes_avg_compensation()

    SyncORM.join_cte_subquery_window_func()

    SyncORM.select_workers_with_selectin_relationship()

    SyncORM.select_workers_with_condition_relationship()

    SyncORM.select_workers_with_relationship_contains_eager_with_limit()

    SyncORM.select_workers_with_pydent()

    SyncORM.add_vacancies_and_replies()

    SyncORM.select_resumes_with_all_relationships()


async def async_db_ORM():
    pass


def main():
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
    )

    @app.get('/workers')
    async def get_wokers() -> list[WorkersRelDTO]:
        workers = SyncORM.select_workers_with_pydent()
        return workers
    
    @app.get('/resumes')
    async def get_resumes():
        res = await AsyncORM.select_resumes_with_all_relationships()
        return res
    
    return app

#app = main()

if __name__ == '__main__':
    #sync_db_ORM()
    SyncORM.create_table()
    # uvicorn.run(
    #     app='src.main:app',
    #     reload=True,
    # )