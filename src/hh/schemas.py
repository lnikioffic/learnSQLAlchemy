from datetime import datetime
from pydantic import BaseModel, ConfigDict

from .models import WorkersOrm, Resume, WorkLoad

# для добавления POST  data transfer object
class WorkersAddDTO(BaseModel):
    username: str

# для получения GET
class WorkersDTO(WorkersAddDTO):
    id: int


class ResumeAddDTO(BaseModel):
    title: str
    compensation: int
    workload: WorkLoad
    worker_id: int


class ResumeDTO(ResumeAddDTO):
    id: int
    created_at: datetime
    update_at: datetime



class ResumeRelDTO(ResumeDTO):
    worker: 'WorkersDTO'


class WorkersRelDTO(WorkersDTO):
    # от циклических импортов
    resumes: list['ResumeDTO']



class WorkloadAVG(BaseModel):
    workload: WorkLoad
    avg: int


class VacanciesAddDTO(BaseModel):
    title: str
    compensation: int | None

class VacanciesDTO(VacanciesAddDTO):
    id: int

class ResumesRelVacanciesRepliedDTO(ResumeDTO):
    worker: 'WorkersDTO'
    vacancies_replied: list['VacanciesDTO']


class VacanciesWithoutCompensationDTO(BaseModel):
    id: int
    title: str
    

class ResumesRelVacanciesRepliedWithoutVacancyCompensationDTO(ResumeDTO):
    worker: "WorkersDTO"
    vacancies_replied: list["VacanciesWithoutCompensationDTO"]
