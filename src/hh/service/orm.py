from sqlalchemy import and_, text, insert, select, func, cast, Integer
from sqlalchemy.orm import aliased, joinedload, selectinload, contains_eager
from database import engine, engine_async, session_factory, async_session_factory
from hh.models import VacanciesOrm, WorkersOrm, Resume, WorkLoad
from hh.schemas import ResumesRelVacanciesRepliedDTO, ResumesRelVacanciesRepliedWithoutVacancyCompensationDTO, WorkersDTO, WorkersRelDTO
from src.hh.models import Base


class SyncORM():

    @staticmethod
    def create_table():
        Base.metadata.drop_all(engine)
        #Base.metadata.create_all(engine)


    @staticmethod
    def insert_workers():
        with session_factory() as session:
            worke = WorkersOrm(username='Bob')
            w = WorkersOrm(username='Vol')
            session.add_all([worke, w])
            # все изменения которые есть в текушей сессии отправляет в БД, но ещё не завершить запрос 
            session.flush() # можно получить id, ответ от БД по генерируемым полям
            session.commit()
             

    @staticmethod
    def select_workers():
        with session_factory() as session:
            query = select(WorkersOrm)
            result = session.execute(query)
            #scalars первое значение из каждой строки
            worker = result.scalars().all()
            print(f'{worker=}')

    
    @staticmethod
    def update_worker(worker_id: int = 2, new_username: str = 'Mi'):
        with session_factory() as session:
            worker_G = session.get(WorkersOrm, worker_id)
            worker_G.username = new_username
            #session.expire() сброс изменений это синхронный, делает ещё одни запрос в БД
            #session.refresh(worker_G) посделние изменения из БД
            session.commit()

    

    @staticmethod
    def insert_resume():
        with session_factory() as session:
            res_1 = Resume(title='Python Junior Developer', compensation=50000, workload=WorkLoad.fulltime, worker_id=1)
            resume_jack_2 = Resume(
                title="Python Разработчик", compensation=150000, workload=WorkLoad.fulltime, worker_id=1)
            resume_michael_1 = Resume(
                title="Python Data Engineer", compensation=250000, workload=WorkLoad.parttime, worker_id=2)
            resume_michael_2 = Resume(
                title="Data Scientist", compensation=300000, workload=WorkLoad.fulltime, worker_id=2)
            session.add_all(
                    [
                        res_1, 
                        resume_jack_2, 
                        resume_michael_1, resume_michael_2
                    ]
                )
            session.commit()


    @staticmethod
    def select_resumes_avg_compensation(language: str = 'Python'):
        with session_factory() as session:
            query = (
                select(Resume.workload, cast(func.avg(Resume.compensation), Integer).label('avg_compensation'))
                .select_from(Resume)
                .filter(and_(
                    Resume.title.contains(language),
                    Resume.compensation > 40000,
                ))
                .group_by(Resume.workload)
                .having(cast(func.avg(Resume.compensation), Integer) > 70000)
                
            )
            print(query.compile(compile_kwargs={'literal_binds': True}))
            res = session.execute(query)
            result = res.all()
            
            print(result[0].avg_compensation)

    
    @staticmethod
    def insert_additional_resumes():
        with session_factory() as session:
            workers = [
                WorkersOrm(username='Artem'),
                WorkersOrm(username='Roman'),
                WorkersOrm(username='Petr')
            ]
            resumes = [
                Resume(title='Python программист', compensation=60000, workload=WorkLoad.fulltime, worker_id=3),
                Resume(title='Machine Learning Engineer', compensation=70000, workload=WorkLoad.parttime, worker_id=3),
                Resume(title='Python Data Scientist', compensation=80000, workload=WorkLoad.parttime, worker_id=4),
                Resume(title='Python Analyst', compensation=90000, workload=WorkLoad.fulltime, worker_id=4),
                Resume(title='Python Junior Developer', compensation=100000, workload=WorkLoad.fulltime, worker_id=5)
            ]
            session.add_all(workers)
            session.commit()
            session.add_all(resumes)
            session.commit()


    @staticmethod
    def join_cte_subquery_window_func(language: str = 'Python'):
        with session_factory() as session:
            r = aliased(Resume)
            w = aliased(WorkersOrm)
            subq = (
                select(
                    r, w,
                    func.avg(r.compensation).over(partition_by=r.workload).cast(Integer)
                        .label('avg_workload_compensation')
                )
                #.select_from(r)
                # есть только левый join
                .join(r, r.worker_id == w.id).subquery('helper_1')
            )
            cte = (
                select(
                    subq.c.worker_id,
                    subq.c.username,
                    subq.c.compensation,
                    subq.c.workload,
                    subq.c.avg_workload_compensation,
                    (subq.c.compensation - subq.c.avg_workload_compensation).label('compensation_diff'),
                )
                .cte('helper_2')
            )
            query = (
                select(cte)
                .order_by(cte.c.compensation_diff.desc())
            )
            
            print(query.compile(compile_kwargs={'literal_binds': True}))

            res = session.execute(query)
            resalt = res.all()

            print(f'{resalt=}')


    # в асинхронном нельзя использовать ленивую подгрузку 
    @staticmethod
    def select_workers_with_lazy_relationship():
        with session_factory() as session:
            query = (
                select(WorkersOrm)
            )
            res = session.execute(query)
            result = res.scalars().all()

            # проблема N+1 запросов
            w_1 = result[0].resumes
            print(w_1)

            w_2 = result[1].resumes
            print(w_2)


    @staticmethod
    def select_workers_with_joined_relationship():
        with session_factory() as session:
            # joinedload не подходит для один ко многим
            query = (
                select(WorkersOrm)
                .options(joinedload(WorkersOrm.resumes))
            )
            res = session.execute(query)
            #  unique чтобы отсеять первычные ключи 
            result = res.unique().scalars().all()

            w_1 = result[0].resumes
            print(w_1)

            w_2 = result[1].resumes
            print(w_2)

    
    @staticmethod
    def select_workers_with_selectin_relationship():
        with session_factory() as session:
            # подходит к многим ко многи и один ко многим
            # сначало всех работников а потом все резюме тех работников которые подгрузили
            query = (
                select(WorkersOrm)
                .options(selectinload(WorkersOrm.resumes))
            )
            res = session.execute(query)
            #  unique чтобы отсеять первычные ключи 
            result = res.unique().scalars().all()

            w_1 = result[0].resumes
            print(w_1)

            w_2 = result[1].resumes
            print(w_2)


    @staticmethod
    def select_workers_with_condition_relationship():
        with session_factory() as session:
            query = (
                select(WorkersOrm)
                .options(selectinload(WorkersOrm.resumes_parttime))
            )

            res = session.execute(query)
            #  unique чтобы отсеять первычные ключи 
            result = res.scalars().all()

            print(result)


    @staticmethod
    def select_workers_with_relationship_contains_eager():
        with session_factory() as session:
            query = (
                select(WorkersOrm)
                .join(WorkersOrm.resumes)
                .options(contains_eager(WorkersOrm.resumes))
                .filter(Resume.workload == WorkLoad.parttime)
            )

            res = session.execute(query)
            #  unique чтобы отсеять первычные ключи 
            result = res.unique().scalars().all()

            print(result)

    
    @staticmethod
    def select_workers_with_relationship_contains_eager_with_limit():
        with session_factory() as session:
            subq = (
                select(Resume.id.label("parttime_resume_id"))
                .filter(Resume.worker_id == WorkersOrm.id)
                .order_by(WorkersOrm.id.desc())
                .limit(1)
                .scalar_subquery()
                .correlate(WorkersOrm)
            )

            query = (
                select(WorkersOrm)
                .join(Resume, Resume.id.in_(subq))
                .options(contains_eager(WorkersOrm.resumes))
            )

            res = session.execute(query)
            result = res.unique().scalars().all()
            print(result)


    @staticmethod
    def select_workers_with_pydent():
        with session_factory() as session:
            query = (
                select(WorkersOrm)
                .options(selectinload(WorkersOrm.resumes))
                .limit(2)
            )

            res = session.execute(query)
            #  unique чтобы отсеять первычные ключи 
            result_orm = res.scalars().all()
            print(f'{result_orm=}')
            #result = [WorkersDTO.model_validate(row, from_attributes=True) for row in result_orm]
            result = [WorkersRelDTO.model_validate(row, from_attributes=True) for row in result_orm]
            return result


    @staticmethod
    def add_vacancies_and_replies():
        with session_factory() as session:
            new_vacancy = VacanciesOrm(title="Python разработчик", compensation=100000)
            resume_1 = session.get(Resume, 1)
            resume_2 = session.get(Resume, 2)
            resume_1.vacancies_replied.append(new_vacancy)
            resume_2.vacancies_replied.append(new_vacancy)
            session.commit()

    
    @staticmethod
    def select_resumes_with_all_relationships():
        with session_factory() as session:
            query = (
                select(Resume)
                .options(joinedload(Resume.worker))
                .options(selectinload(Resume.vacancies_replied).load_only(VacanciesOrm.title))
            )

            res = session.execute(query)
            result = res.unique().scalars().all()
            print(f'{result=}')

            # Обратите внимание, что созданная в видео модель содержала лишний столбец compensation
            # И так как он есть в схеме ResumesRelVacanciesRepliedDTO, столбец compensation был вызван
            # Алхимией через ленивую загрузку. В асинхронном варианте это приводило к краху программы
            #result_dto = [ResumesRelVacanciesRepliedWithoutVacancyCompensationDTO.model_validate(row, from_attributes=True) for row in result]
            
            result_dto = [ResumesRelVacanciesRepliedDTO.model_validate(row, from_attributes=True) for row in result]

            print(result_dto)
            return result_dto


class AsyncORM():

    @staticmethod
    async def async_create_table():
        async with async_session_factory() as session:
            await session.run_sync(Base.metadata.drop_all)
            await session.run_sync(Base.metadata.create_all)


    @staticmethod        
    async def async_insert_workers():
        async with async_session_factory() as session:
            worke = WorkersOrm(username='Bob')
            w = WorkersOrm(username='Vol')
            session.add_all([worke, w])
            # flush взаимодействует с БД, поэтому пишем await
            await session.flush()
            await session.commit()

    
    @staticmethod
    async def async_select_workers():
        async with async_session_factory() as session:
            query = select(WorkersOrm)
            result = await session.execute(query)
            workers = result.scalars().all()
            print(f"{workers=}")

    @staticmethod
    async def update_worker(worker_id: int = 2, new_username: str = 'Mi'):
        async with async_session_factory() as session:
            worker_G = await session.get(WorkersOrm, worker_id)
            worker_G.username = new_username
            #session.expire() сброс изменений это синхронный, делает ещё одни запрос в БД
            #session.refresh(worker_G) посделние изменения из БД
            session.commit()


    @staticmethod
    async def insert_resume():
        async with async_session_factory() as session:
            res_1 = Resume(title='Python Junior Developer', compensation=50000, workload=WorkLoad.fulltime, worker_id=1)
            resume_jack_2 = Resume(
                title="Python Разработчик", compensation=150000, workload=WorkLoad.fulltime, worker_id=1)
            resume_michael_1 = Resume(
                title="Python Data Engineer", compensation=250000, workload=WorkLoad.parttime, worker_id=2)
            resume_michael_2 = Resume(
                title="Data Scientist", compensation=300000, workload=WorkLoad.fulltime, worker_id=2)
            session.add_all(
                    [
                        res_1, 
                        resume_jack_2, 
                        resume_michael_1, resume_michael_2
                    ]
                )
            await session.commit()


    @staticmethod
    async def select_resumes_avg_compensation(like_language: str = "Python"):
        """
        select workload, avg(compensation)::int as avg_compensation
        from resumes
        where title like '%Python%' and compensation > 40000
        group by workload
        having avg(compensation) > 70000
        """
        async with async_session_factory() as session:
            query = (
                select(
                    Resume.workload,
                    # 1 вариант использования cast
                    # cast(func.avg(ResumesOrm.compensation), Integer).label("avg_compensation"),
                    # 2 вариант использования cast (предпочтительный способ)
                    func.avg(Resume.compensation).cast(Integer).label("avg_compensation"),
                )
                .select_from(Resume)
                .filter(and_(
                    Resume.title.contains(like_language),
                    Resume.compensation > 40000,
                ))
                .group_by(Resume.workload)
                .having(func.avg(Resume.compensation) > 70000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True}))
            res = await session.execute(query)
            result = res.all()
            print(result[0].avg_compensation)


    @staticmethod
    async def insert_additional_resumes():
        async with async_session_factory() as session:
            workers = [
                WorkersOrm(username='Artem'),
                WorkersOrm(username='Roman'),
                WorkersOrm(username='Petr')
            ]
            resumes = [
                Resume(title='Python программист', compensation=60000, workload=WorkLoad.fulltime, worker_id=3),
                Resume(title='Machine Learning Engineer', compensation=70000, workload=WorkLoad.parttime, worker_id=3),
                Resume(title='Python Data Scientist', compensation=80000, workload=WorkLoad.parttime, worker_id=4),
                Resume(title='Python Analyst', compensation=90000, workload=WorkLoad.fulltime, worker_id=4),
                Resume(title='Python Junior Developer', compensation=100000, workload=WorkLoad.fulltime, worker_id=5)
            ]
            await session.add_all(workers)
            await session.commit()
            await session.add_all(resumes)
            await session.commit()

    
    @staticmethod
    async def join_cte_subquery_window_func():
        """
        WITH helper2 AS (
            SELECT *, compensation-avg_workload_compensation AS compensation_diff
            FROM 
            (SELECT
                w.id,
                w.username,
                r.compensation,
                r.workload,
                avg(r.compensation) OVER (PARTITION BY workload)::int AS avg_workload_compensation
            FROM resumes r
            JOIN workers w ON r.worker_id = w.id) helper1
        )
        SELECT * FROM helper2
        ORDER BY compensation_diff DESC;
        """
        async with async_session_factory() as session:
            r = aliased(Resume)
            w = aliased(WorkersOrm)
            subq = (
                select(
                    r,
                    w,
                    func.avg(r.compensation).over(partition_by=r.workload).cast(Integer).label("avg_workload_compensation"),
                )
                # .select_from(r)
                .join(r, r.worker_id == w.id).subquery("helper1")
            )
            cte = (
                select(
                    subq.c.worker_id,
                    subq.c.username,
                    subq.c.compensation,
                    subq.c.workload,
                    subq.c.avg_workload_compensation,
                    (subq.c.compensation - subq.c.avg_workload_compensation).label("compensation_diff"),
                )
                .cte("helper2")
            )
            query = (
                select(cte)
                .order_by(cte.c.compensation_diff.desc())
            )

            res = await session.execute(query)
            result = res.all()
            print(f"{len(result)=}. {result=}")


    @staticmethod
    async def select_workers_with_lazy_relationship():
        async with async_session_factory() as session:
            query = (
                select(WorkersOrm)
            )
            
            res = await session.execute(query)
            result = res.scalars().all()

            # worker_1_resumes = result[0].resumes  # -> Приведет к ошибке
            # Нельзя использовать ленивую подгрузку в асинхронном варианте!

            # Ошибка: sqlalchemy.exc.MissingGreenlet: greenlet_spawn has not been called; can't call await_only() here. 
            # Was IO attempted in an unexpected place? (Background on this error at: https://sqlalche.me/e/20/xd2s)

    

    @staticmethod
    async def select_workers_with_joined_relationship():
        async with async_session_factory() as session:
            # joinedload не подходит для один ко многим
            query = (
                select(WorkersOrm)
                .options(joinedload(WorkersOrm.resumes))
            )
            res = await session.execute(query)
            #  unique чтобы отсеять первычные ключи 
            result = res.unique().scalars().all()

            w_1 = result[0].resumes
            print(w_1)

            w_2 = result[1].resumes
            print(w_2)


    @staticmethod
    async def select_workers_with_selectin_relationship():
        async with async_session_factory() as session:
            # подходит к многим ко многи и один ко многим
            # сначало всех работников а потом все резюме тех работников которые подгрузили
            query = (
                select(WorkersOrm)
                .options(selectinload(WorkersOrm.resumes))
            )
            res = await session.execute(query)
            #  unique чтобы отсеять первычные ключи 
            result = res.unique().scalars().all()

            w_1 = result[0].resumes
            print(w_1)

            w_2 = result[1].resumes
            print(w_2)

    
    @staticmethod
    async def select_workers_with_condition_relationship():
        async with async_session_factory() as session:
            query = (
                select(WorkersOrm)
                .options(selectinload(WorkersOrm.resumes_parttime))
            )

            res = await session.execute(query)
            #  unique чтобы отсеять первычные ключи 
            result = res.scalars().all()

            print(result)


    @staticmethod
    async def select_workers_with_relationship_contains_eager():
        async with async_session_factory() as session:
            query = (
                select(WorkersOrm)
                .join(WorkersOrm.resumes)
                .options(contains_eager(WorkersOrm.resumes))
                .filter(Resume.workload == WorkLoad.parttime)
            )

            res = await session.execute(query)
            #  unique чтобы отсеять первычные ключи 
            result = res.unique().scalars().all()

            print(result)

    
    @staticmethod
    async def select_workers_with_relationship_contains_eager_with_limit():
        async with async_session_factory() as session:
            subq = (
                select(Resume.id.label("parttime_resume_id"))
                .filter(Resume.worker_id == WorkersOrm.id)
                .order_by(WorkersOrm.id.desc())
                .limit(1)
                .scalar_subquery()
                .correlate(WorkersOrm)
            )

            query = (
                select(WorkersOrm)
                .join(Resume, Resume.id.in_(subq))
                .options(contains_eager(WorkersOrm.resumes))
            )

            res = await session.execute(query)
            result = res.unique().scalars().all()
            print(result)


    @staticmethod
    async def select_workers_with_pydent():
        async with async_session_factory() as session:
            query = (
                select(WorkersOrm)
                .options(selectinload(WorkersOrm.resumes))
                .limit(2)
            )

            res = await session.execute(query)
            #  unique чтобы отсеять первычные ключи 
            result_orm = res.scalars().all()
            print(f'{result_orm=}')
            #result = [WorkersDTO.model_validate(row, from_attributes=True) for row in result_orm]
            result = [WorkersRelDTO.model_validate(row, from_attributes=True) for row in result_orm]
            return result
        

    @staticmethod
    async def add_vacancies_and_replies():
        async with async_session_factory() as session:
            new_vacancy = VacanciesOrm(title="Python разработчик", compensation=100000)
            get_resume_1 = select(Resume).options(selectinload(Resume.vacancies_replied)).filter_by(id=1)
            get_resume_2 = select(Resume).options(selectinload(Resume.vacancies_replied)).filter_by(id=2)
            resume_1 = (await session.execute(get_resume_1)).scalar_one()
            resume_2 = (await session.execute(get_resume_2)).scalar_one()
            resume_1.vacancies_replied.append(new_vacancy)
            resume_2.vacancies_replied.append(new_vacancy)
            await session.commit()

    
    @staticmethod
    async def select_resumes_with_all_relationships():
        async with async_session_factory() as session:
            query = (
                select(Resume)
                .options(joinedload(Resume.worker))
                .options(selectinload(Resume.vacancies_replied))
            )
            # .load_only(VacanciesOrm.title)
            res = await session.execute(query)
            result = res.unique().scalars().all()
            print(f'{result=}')

            # Обратите внимание, что созданная в видео модель содержала лишний столбец compensation
            # И так как он есть в схеме ResumesRelVacanciesRepliedDTO, столбец compensation был вызван
            # Алхимией через ленивую загрузку. В асинхронном варианте это приводило к краху программы
            #result_dto = [ResumesRelVacanciesRepliedWithoutVacancyCompensationDTO.model_validate(row, from_attributes=True) for row in result]
            
            result_dto = [ResumesRelVacanciesRepliedDTO.model_validate(row, from_attributes=True) for row in result]
            return result_dto