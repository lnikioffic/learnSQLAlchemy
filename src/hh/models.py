import datetime
from typing import Annotated
from sqlalchemy import Table, Column, Integer, String, ForeignKey, text, CheckConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, relationship
import enum
from src.models import Base, str_200

intpk = Annotated[int, mapped_column(primary_key=True)]


created_at = Annotated[datetime.datetime, mapped_column(server_default=text("TIMEZONE('utc', now())"))]
update_at = Annotated[datetime.datetime, mapped_column(
            server_default=text("TIMEZONE('utc', now())"),
            onupdate=datetime.datetime.utcnow()
        )]


class WorkersOrm(Base):
    __tablename__ = 'workers' 

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str]

    resumes: Mapped[list['Resume']] = relationship(
        # указание на relationship
        back_populates='worker',

    )

    __table_args__ = {'extend_existing': True}
    # resumes_parttime: Mapped[list['Resume']] = relationship(
    #     # указание на re;ationship
    #     back_populates='worker',
    #     primaryjoin="and_(WorkersOrm.id == Resume.worker_id, Resume.workload == 'parttime')",
    #     order_by="Resume.id.desc()",
    #     # тип подгрузки
    #     #lazy="selectin"
    # )



class WorkLoad(enum.Enum):
    parttime = 'parttime'
    fulltime = 'fulltime'



class Resume(Base):
    __tablename__ = 'resumes'

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str_200]
    compensation: Mapped[int | None]
    workload: Mapped[WorkLoad]
    worker_id: Mapped[int] = mapped_column(ForeignKey('workers.id', ondelete='CASCADE'))

    created_at: Mapped[created_at]
    update_at: Mapped[update_at] 


    worker: Mapped['WorkersOrm'] = relationship(
        back_populates='resumes'
    )

    vacancies_replied: Mapped[list['VacanciesOrm']] = relationship(
        back_populates="resumes_replied",
        secondary="vacancies_replies",
    )

    __table_args__ = (
        {'extend_existing': True},
        # Index('title_index', 'title'),
        # CheckConstraint("compensation > 0", name='check')
    )


class VacanciesOrm(Base):
    __tablename__ = 'vacancies'

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str_200]
    compensation: Mapped[int | None]

    resumes_replied: Mapped[list['Resume']] = relationship(
        back_populates='vacancies_replied',
        secondary='vacancies_replies'
    )

    __table_args__ = {'extend_existing': True}


class VacanciesRepliesOrm(Base):
    __tablename__ = 'vacancies_replies'

    resume_id: Mapped[int] = mapped_column(
            ForeignKey('resumes.id', ondelete='CASCADE'),
            primary_key=True
        )
    vacancie_id: Mapped[int] = mapped_column(
            ForeignKey('vacancies.id', ondelete='CASCADE'),
            primary_key=True
        )
    
    cover_letter: Mapped[str | None]

    __table_args__ = {'extend_existing': True}






# metadata = MetaData()

# #императивный
# workers_table = Table(
#     'workers',
#     metadata,
#     Column('id', Integer, primary_key=True),
#     Column('username', String)
# )


# resumes_table = Table(
#     "resumes",
#     metadata,
#     Column("id", Integer, primary_key=True),
#     Column("title", String(256)),
#     Column("compensation", Integer, nullable=True),
#     Column("workload", Enum(WorkLoad)),
#     Column("worker_id", ForeignKey("workers.id", ondelete="CASCADE")),
#     Column("created_at", TIMESTAMP,server_default=text("TIMEZONE('utc', now())")),
#     Column("updated_at", TIMESTAMP,server_default=text("TIMEZONE('utc', now())"), onupdate=datetime.datetime.utcnow),
# )


# vacancies_table = Table(
#     "vacancies",
#     metadata,
#     Column("id", Integer, primary_key=True),
#     Column("title", String),
#     Column("compensation", Integer, nullable=True),
# )

# vacancies_replies_table = Table(
#     "vacancies_replies",
#     metadata,
#     Column("resume_id", ForeignKey("resumes.id", ondelete="CASCADE"), primary_key=True),
#     Column("vacancy_id", ForeignKey("vacancies.id", ondelete="CASCADE"), primary_key=True),
# )