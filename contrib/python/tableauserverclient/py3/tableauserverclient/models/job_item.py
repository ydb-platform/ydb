import datetime
from typing import Optional

from defusedxml.ElementTree import fromstring

from tableauserverclient.datetime_helpers import parse_datetime
from tableauserverclient.models.flow_run_item import FlowRunItem


class JobItem:
    """
    Using the TSC library, you can get information about an asynchronous process
    (or job) on the server. These jobs can be created when Tableau runs certain
    tasks that could be long running, such as importing or synchronizing users
    from Active Directory, or running an extract refresh. For example, the REST
    API methods to create or update groups, to run an extract refresh task, or
    to publish workbooks can take an asJob parameter (asJob-true) that creates a
    background process (the job) to complete the call. Information about the
    asynchronous job is returned from the method.

    If you have the identifier of the job, you can use the TSC library to find
    out the status of the asynchronous job.

    The job properties are defined in the JobItem class. The class corresponds
    to the properties for jobs you can access using the Tableau Server REST API.
    The job methods are based upon the endpoints for jobs in the REST API and
    operate on the JobItem class.

    Parameters
    ----------
    id_ : str
        The identifier of the job.

    job_type : str
        The type of job.

    progress : str
        The progress of the job.

    created_at : datetime.datetime
        The date and time the job was created.

    started_at : Optional[datetime.datetime]
        The date and time the job was started.

    completed_at : Optional[datetime.datetime]
        The date and time the job was completed.

    finish_code : int
        The finish code of the job. 0 for success, 1 for failure, 2 for cancelled.

    notes : Optional[list[str]]
        Contains detailed notes about the job.

    mode : Optional[str]

    workbook_id : Optional[str]
        The identifier of the workbook associated with the job.

    datasource_id : Optional[str]
        The identifier of the datasource associated with the job.

    flow_run : Optional[FlowRunItem]
        The flow run associated with the job.

    updated_at : Optional[datetime.datetime]
        The date and time the job was last updated.

    workbook_name : Optional[str]
        The name of the workbook associated with the job.

    datasource_name : Optional[str]
        The name of the datasource associated with the job.
    """

    class FinishCode:
        """
        Status codes as documented on
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm#query_job
        """

        Success: int = 0
        Failed: int = 1
        Cancelled: int = 2
        Completed: int = 3

    def __init__(
        self,
        id_: str,
        job_type: str,
        progress: str,
        created_at: datetime.datetime,
        started_at: Optional[datetime.datetime] = None,
        completed_at: Optional[datetime.datetime] = None,
        finish_code: int = 0,
        notes: Optional[list[str]] = None,
        mode: Optional[str] = None,
        workbook_id: Optional[str] = None,
        datasource_id: Optional[str] = None,
        flow_run: Optional[FlowRunItem] = None,
        updated_at: Optional[datetime.datetime] = None,
        workbook_name: Optional[str] = None,
        datasource_name: Optional[str] = None,
    ):
        self._id = id_
        self._type = job_type
        self._progress = progress
        self._created_at = created_at
        self._started_at = started_at
        self._completed_at = completed_at
        self._finish_code = finish_code
        self._notes: list[str] = notes or []
        self._mode = mode
        self._workbook_id = workbook_id
        self._datasource_id = datasource_id
        self._flow_run = flow_run
        self._updated_at = updated_at
        self._workbook_name = workbook_name
        self._datasource_name = datasource_name

    @property
    def id(self) -> str:
        return self._id

    @property
    def type(self) -> str:
        return self._type

    @property
    def progress(self) -> str:
        return self._progress

    @property
    def created_at(self) -> datetime.datetime:
        return self._created_at

    @property
    def started_at(self) -> Optional[datetime.datetime]:
        return self._started_at

    @property
    def completed_at(self) -> Optional[datetime.datetime]:
        return self._completed_at

    @property
    def finish_code(self) -> int:
        return self._finish_code

    @property
    def notes(self) -> list[str]:
        return self._notes

    @property
    def mode(self) -> Optional[str]:
        return self._mode

    @mode.setter
    def mode(self, value: str) -> None:
        # check for valid data here
        self._mode = value

    @property
    def workbook_id(self) -> Optional[str]:
        return self._workbook_id

    @workbook_id.setter
    def workbook_id(self, value: Optional[str]) -> None:
        self._workbook_id = value

    @property
    def datasource_id(self) -> Optional[str]:
        return self._datasource_id

    @datasource_id.setter
    def datasource_id(self, value: Optional[str]) -> None:
        self._datasource_id = value

    @property
    def flow_run(self):
        return self._flow_run

    @flow_run.setter
    def flow_run(self, value):
        self._flow_run = value

    @property
    def updated_at(self) -> Optional[datetime.datetime]:
        return self._updated_at

    @property
    def workbook_name(self) -> Optional[str]:
        return self._workbook_name

    @property
    def datasource_name(self) -> Optional[str]:
        return self._datasource_name

    def __str__(self):
        return (
            "<Job#{_id} {_type} created_at({_created_at}) started_at({_started_at}) updated_at({_updated_at}) completed_at({_completed_at})"
            " progress ({_progress}) finish_code({_finish_code})>".format(**self.__dict__)
        )

    def __repr__(self):
        return self.__str__() + "  { " + ", ".join(" % s: % s" % item for item in vars(self).items()) + "}"

    @classmethod
    def from_response(cls, xml, ns) -> list["JobItem"]:
        parsed_response = fromstring(xml)
        all_tasks_xml = parsed_response.findall(".//t:job", namespaces=ns)

        all_tasks = [JobItem._parse_element(x, ns) for x in all_tasks_xml]

        return all_tasks

    @classmethod
    def _parse_element(cls, element, ns):
        id_ = element.get("id", None)
        type_ = element.get("type", None)
        progress = element.get("progress", None)
        created_at = parse_datetime(element.get("createdAt", None))
        started_at = parse_datetime(element.get("startedAt", None))
        completed_at = parse_datetime(element.get("completedAt", None))
        finish_code = int(element.get("finishCode", -1))
        notes = [note.text for note in element.findall(".//t:notes", namespaces=ns)] or None
        mode = element.get("mode", None)
        workbook = element.find(".//t:workbook[@id]", namespaces=ns)
        workbook_id = workbook.get("id") if workbook is not None else None
        workbook_name = workbook.get("name") if workbook is not None else None
        datasource = element.find(".//t:datasource[@id]", namespaces=ns)
        datasource_id = datasource.get("id") if datasource is not None else None
        datasource_name = datasource.get("name") if datasource is not None else None
        flow_run = None
        updated_at = parse_datetime(element.get("updatedAt", None))
        for flow_job in element.findall(".//t:runFlowJobType", namespaces=ns):
            flow_run = FlowRunItem()
            flow_run._id = flow_job.get("flowRunId", None)
            for flow in flow_job.findall(".//t:flow", namespaces=ns):
                flow_run._flow_id = flow.get("id", None)
                flow_run._started_at = created_at or started_at
        return cls(
            id_,
            type_,
            progress,
            created_at,
            started_at,
            completed_at,
            finish_code,
            notes,
            mode,
            workbook_id,
            datasource_id,
            flow_run,
            updated_at,
            workbook_name,
            datasource_name,
        )


class BackgroundJobItem:
    class Status:
        Pending: str = "Pending"
        InProgress: str = "InProgress"
        Success: str = "Success"
        Failed: str = "Failed"
        Cancelled: str = "Cancelled"

    def __init__(
        self,
        id_: str,
        created_at: datetime.datetime,
        priority: int,
        job_type: str,
        status: str,
        title: Optional[str] = None,
        subtitle: Optional[str] = None,
        started_at: Optional[datetime.datetime] = None,
        ended_at: Optional[datetime.datetime] = None,
    ):
        self._id = id_
        self._type = job_type
        self._status = status
        self._created_at = created_at
        self._started_at = started_at
        self._ended_at = ended_at
        self._priority = priority
        self._title = title
        self._subtitle = subtitle

    def __str__(self):
        return f"<{self.__class__.__qualname__} {self._id} {self._type}>"

    def __repr__(self):
        return self.__str__() + "  { " + ", ".join(" % s: % s" % item for item in vars(self).items()) + "}"

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> Optional[str]:
        """For API consistency - all other resource endpoints have a name attribute which is used to display what
        they are.  Alias title as name to allow consistent handling of resources in the list sample."""
        return self._title

    @property
    def status(self) -> str:
        return self._status

    @property
    def type(self) -> str:
        return self._type

    @property
    def created_at(self) -> datetime.datetime:
        return self._created_at

    @property
    def started_at(self) -> Optional[datetime.datetime]:
        return self._started_at

    @property
    def ended_at(self) -> Optional[datetime.datetime]:
        return self._ended_at

    @property
    def title(self) -> Optional[str]:
        return self._title

    @property
    def subtitle(self) -> Optional[str]:
        return self._subtitle

    @property
    def priority(self) -> int:
        return self._priority

    @classmethod
    def from_response(cls, xml, ns) -> list["BackgroundJobItem"]:
        parsed_response = fromstring(xml)
        all_tasks_xml = parsed_response.findall(".//t:backgroundJob", namespaces=ns)
        return [cls._parse_element(x, ns) for x in all_tasks_xml]

    @classmethod
    def _parse_element(cls, element, ns):
        id_ = element.get("id", None)
        type_ = element.get("jobType", None)
        status = element.get("status", None)
        created_at = parse_datetime(element.get("createdAt", None))
        started_at = parse_datetime(element.get("startedAt", None))
        ended_at = parse_datetime(element.get("endedAt", None))
        priority = element.get("priority", None)
        title = element.get("title", None)
        subtitle = element.get("subtitle", None)

        return cls(
            id_,
            created_at,
            priority,
            type_,
            status,
            title,
            subtitle,
            started_at,
            ended_at,
        )
