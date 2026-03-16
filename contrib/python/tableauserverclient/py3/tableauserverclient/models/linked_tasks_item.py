import datetime as dt
from typing import Optional

from defusedxml.ElementTree import fromstring

from tableauserverclient.datetime_helpers import parse_datetime
from tableauserverclient.models.schedule_item import ScheduleItem


class LinkedTaskItem:
    def __init__(self) -> None:
        self.id: Optional[str] = None
        self.num_steps: Optional[int] = None
        self.schedule: Optional[ScheduleItem] = None

    @classmethod
    def from_response(cls, resp: bytes, namespace) -> list["LinkedTaskItem"]:
        parsed_response = fromstring(resp)
        return [
            cls._parse_element(x, namespace)
            for x in parsed_response.findall(".//t:linkedTasks[@id]", namespaces=namespace)
        ]

    @classmethod
    def _parse_element(cls, xml, namespace) -> "LinkedTaskItem":
        task = cls()
        task.id = xml.get("id")
        task.num_steps = int(xml.get("numSteps"))
        task.schedule = ScheduleItem.from_element(xml, namespace)[0]
        return task


class LinkedTaskStepItem:
    def __init__(self) -> None:
        self.id: Optional[str] = None
        self.step_number: Optional[int] = None
        self.stop_downstream_on_failure: Optional[bool] = None
        self.task_details: list[LinkedTaskFlowRunItem] = []

    @classmethod
    def from_task_xml(cls, xml, namespace) -> list["LinkedTaskStepItem"]:
        return [cls._parse_element(x, namespace) for x in xml.findall(".//t:linkedTaskSteps[@id]", namespace)]

    @classmethod
    def _parse_element(cls, xml, namespace) -> "LinkedTaskStepItem":
        step = cls()
        step.id = xml.get("id")
        step.step_number = int(xml.get("stepNumber"))
        step.stop_downstream_on_failure = string_to_bool(xml.get("stopDownstreamTasksOnFailure"))
        step.task_details = LinkedTaskFlowRunItem._parse_element(xml, namespace)
        return step


class LinkedTaskFlowRunItem:
    def __init__(self) -> None:
        self.flow_run_id: Optional[str] = None
        self.flow_run_priority: Optional[int] = None
        self.flow_run_consecutive_failed_count: Optional[int] = None
        self.flow_run_task_type: Optional[str] = None
        self.flow_id: Optional[str] = None
        self.flow_name: Optional[str] = None

    @classmethod
    def _parse_element(cls, xml, namespace) -> list["LinkedTaskFlowRunItem"]:
        all_tasks = []
        for flow_run in xml.findall(".//t:flowRun[@id]", namespace):
            task = cls()
            task.flow_run_id = flow_run.get("id")
            task.flow_run_priority = int(flow_run.get("priority"))
            task.flow_run_consecutive_failed_count = int(flow_run.get("consecutiveFailedCount"))
            task.flow_run_task_type = flow_run.get("type")
            flow = flow_run.find(".//t:flow[@id]", namespace)
            task.flow_id = flow.get("id")
            task.flow_name = flow.get("name")
            all_tasks.append(task)

        return all_tasks


class LinkedTaskJobItem:
    def __init__(self) -> None:
        self.id: Optional[str] = None
        self.linked_task_id: Optional[str] = None
        self.status: Optional[str] = None
        self.created_at: Optional[dt.datetime] = None

    @classmethod
    def from_response(cls, resp: bytes, namespace) -> "LinkedTaskJobItem":
        parsed_response = fromstring(resp)
        job = cls()
        job_xml = parsed_response.find(".//t:linkedTaskJob[@id]", namespaces=namespace)
        if job_xml is None:
            raise ValueError("No linked task job found in response")
        job.id = job_xml.get("id")
        job.linked_task_id = job_xml.get("linkedTaskId")
        job.status = job_xml.get("status")
        job.created_at = parse_datetime(job_xml.get("createdAt"))
        return job


def string_to_bool(s: str) -> bool:
    return s.lower() == "true"
