from collections.abc import Iterable
import copy
import logging
import warnings
from collections import namedtuple
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, Union, overload

from .endpoint import Endpoint, api, parameter_added_in
from .exceptions import MissingRequiredFieldError
from tableauserverclient.server import RequestFactory
from tableauserverclient.models import PaginationItem, ScheduleItem, TaskItem, ExtractItem
from tableauserverclient.models.schedule_item import parse_batch_schedule_state

from tableauserverclient.helpers.logging import logger

AddResponse = namedtuple("AddResponse", ("result", "error", "warnings", "task_created"))
OK = AddResponse(result=True, error=None, warnings=None, task_created=None)

if TYPE_CHECKING:
    from ..request_options import RequestOptions
    from ...models import DatasourceItem, WorkbookItem, FlowItem


class Schedules(Endpoint):
    @property
    def baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/schedules"

    @property
    def siteurl(self) -> str:
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/schedules"

    @api(version="2.3")
    def get(self, req_options: Optional["RequestOptions"] = None) -> tuple[list[ScheduleItem], PaginationItem]:
        """
        Returns a list of flows, extract, and subscription server schedules on
        Tableau Server. For each schedule, the API returns name, frequency,
        priority, and other information.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm#query_schedules

        Parameters
        ----------
        req_options : Optional[RequestOptions]
            Filtering and paginating options for request.

        Returns
        -------
        Tuple[List[ScheduleItem], PaginationItem]
            A tuple of list of ScheduleItem and PaginationItem
        """
        logger.info("Querying all schedules")
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_schedule_items = ScheduleItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_schedule_items, pagination_item

    @api(version="3.8")
    def get_by_id(self, schedule_id: str) -> ScheduleItem:
        """
        Returns detailed information about the specified server schedule.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm#get-schedule

        Parameters
        ----------
        schedule_id : str
            The ID of the schedule to get information for.

        Returns
        -------
        ScheduleItem
            The schedule item that corresponds to the given ID.
        """
        if not schedule_id:
            error = "No Schedule ID provided"
            raise ValueError(error)
        logger.info(f"Querying a single schedule by id ({schedule_id})")
        url = f"{self.baseurl}/{schedule_id}"
        server_response = self.get_request(url)
        return ScheduleItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version="2.3")
    def delete(self, schedule_id: str) -> None:
        """
        Deletes the specified schedule from the server.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm#delete_schedule

        Parameters
        ----------
        schedule_id : str
            The ID of the schedule to delete.

        Returns
        -------
        None
        """
        if not schedule_id:
            error = "Schedule ID undefined"
            raise ValueError(error)
        url = f"{self.baseurl}/{schedule_id}"
        self.delete_request(url)
        logger.info(f"Deleted single schedule (ID: {schedule_id})")

    @api(version="2.3")
    def update(self, schedule_item: ScheduleItem) -> ScheduleItem:
        """
        Modifies settings for the specified server schedule, including the name,
        priority, and frequency details on Tableau Server. For Tableau Cloud,
        see the tasks and subscritpions API.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm#update_schedule

        Parameters
        ----------
        schedule_item : ScheduleItem
            The schedule item to update.

        Returns
        -------
        ScheduleItem
            The updated schedule item.
        """
        if not schedule_item.id:
            error = "Schedule item missing ID."
            raise MissingRequiredFieldError(error)

        url = f"{self.baseurl}/{schedule_item.id}"
        update_req = RequestFactory.Schedule.update_req(schedule_item)
        server_response = self.put_request(url, update_req)
        logger.info(f"Updated schedule item (ID: {schedule_item.id})")
        updated_schedule = copy.copy(schedule_item)
        return updated_schedule._parse_common_tags(server_response.content, self.parent_srv.namespace)

    @api(version="2.3")
    def create(self, schedule_item: ScheduleItem) -> ScheduleItem:
        """
        Creates a new server schedule on Tableau Server. For Tableau Cloud, use
        the tasks and subscriptions API.

        Parameters
        ----------
        schedule_item : ScheduleItem
            The schedule item to create.

        Returns
        -------
        ScheduleItem
            The newly created schedule.
        """
        if schedule_item.interval_item is None:
            error = "Interval item must be defined."
            raise MissingRequiredFieldError(error)

        url = self.baseurl
        create_req = RequestFactory.Schedule.create_req(schedule_item)
        server_response = self.post_request(url, create_req)
        new_schedule = ScheduleItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        logger.info(f"Created new schedule (ID: {new_schedule.id})")
        return new_schedule

    @api(version="2.8")
    @parameter_added_in(flow="3.3")
    def add_to_schedule(
        self,
        schedule_id: str,
        workbook: Optional["WorkbookItem"] = None,
        datasource: Optional["DatasourceItem"] = None,
        flow: Optional["FlowItem"] = None,
        task_type: Optional[str] = None,
    ) -> list[AddResponse]:
        """
        Adds a workbook, datasource, or flow to a schedule on Tableau Server.
        Only one of workbook, datasource, or flow can be passed in at a time.

        The task type is optional and will default to ExtractRefresh if a
        workbook or datasource is passed in, and RunFlow if a flow is passed in.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm#add_workbook_to_schedule
        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm#add_data_source_to_schedule
        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_flow.htm#add_flow_task_to_schedule

        Parameters
        ----------
        schedule_id : str
            The ID of the schedule to add the item to.

        workbook : Optional[WorkbookItem]
            The workbook to add to the schedule.

        datasource : Optional[DatasourceItem]
            The datasource to add to the schedule.

        flow : Optional[FlowItem]
            The flow to add to the schedule.

        task_type : Optional[str]
            The type of task to add to the schedule. If not provided, it will
            default to ExtractRefresh if a workbook or datasource is passed in,
            and RunFlow if a flow is passed in.

        Returns
        -------
        list[AddResponse]
            A list of responses for each item added to the schedule.
        """
        # There doesn't seem to be a good reason to allow one item of each type?
        if workbook and datasource:
            warnings.warn("Passing in multiple items for add_to_schedule will be deprecated", PendingDeprecationWarning)
        items: list[
            tuple[str, Union[WorkbookItem, FlowItem, DatasourceItem], str, Callable[[Optional[str], str], bytes], str]
        ] = []

        if workbook is not None:
            if not task_type:
                task_type = TaskItem.Type.ExtractRefresh
            items.append((schedule_id, workbook, "workbook", RequestFactory.Schedule.add_workbook_req, task_type))
        if datasource is not None:
            if not task_type:
                task_type = TaskItem.Type.ExtractRefresh
            items.append((schedule_id, datasource, "datasource", RequestFactory.Schedule.add_datasource_req, task_type))
        if flow is not None and not (workbook or datasource):  # Cannot pass a flow with any other type
            if not task_type:
                task_type = TaskItem.Type.RunFlow
            items.append(
                (schedule_id, flow, "flow", RequestFactory.Schedule.add_flow_req, task_type)
            )  # type:ignore[arg-type]

        results = (self._add_to(*x) for x in items)
        return [x for x in results if not x.result]

    def _add_to(
        self,
        schedule_id,
        resource: Union["DatasourceItem", "WorkbookItem", "FlowItem"],
        type_: str,
        req_factory: Callable[
            [
                str,
                str,
            ],
            bytes,
        ],
        item_task_type,
    ) -> AddResponse:
        id_ = resource.id
        url = f"{self.siteurl}/{schedule_id}/{type_}s"
        add_req = req_factory(id_, task_type=item_task_type)  # type: ignore[call-arg, arg-type]
        response = self.put_request(url, add_req)

        error, warnings, task_created = ScheduleItem.parse_add_to_schedule_response(response, self.parent_srv.namespace)
        if task_created:
            logger.info(f"Added {type_} to {id_} to schedule {schedule_id}")

        if error is not None or warnings is not None:
            return AddResponse(
                result=False,
                error=error,
                warnings=warnings,
                task_created=task_created,
            )
        else:
            return OK

    @api(version="2.3")
    def get_extract_refresh_tasks(
        self, schedule_id: str, req_options: Optional["RequestOptions"] = None
    ) -> tuple[list["ExtractItem"], "PaginationItem"]:
        """Get all extract refresh tasks for the specified schedule."""
        if not schedule_id:
            error = "Schedule ID undefined"
            raise ValueError(error)

        logger.info(f"Querying extract refresh tasks for schedule (ID: {schedule_id})")
        url = f"{self.siteurl}/{schedule_id}/extracts"
        server_response = self.get_request(url, req_options)

        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        extract_items = ExtractItem.from_response(server_response.content, self.parent_srv.namespace)

        return extract_items, pagination_item

    @overload
    def batch_update_state(
        self,
        schedules: Iterable[ScheduleItem | str],
        state: Literal["active", "suspended"],
        update_all: Literal[False] = False,
    ) -> list[str]: ...

    @overload
    def batch_update_state(
        self, schedules: Any, state: Literal["active", "suspended"], update_all: Literal[True]
    ) -> list[str]: ...

    @api(version="3.27")
    def batch_update_state(self, schedules, state, update_all=False) -> list[str]:
        """
        Batch update the status of one or more scheudles. If update_all is set,
        all schedules on the Tableau Server are affected.

        Parameters
        ----------
        schedules: Iterable[ScheudleItem | str] | Any
            The schedules to be updated. If update_all=True, this is ignored.

        state: Literal["active", "suspended"]
            The state of the schedules, whether active or suspended.

        update_all: bool
            Whether or not to apply the status to all schedules.

        Returns
        -------
        List[str]
            The IDs of the affected schedules.
        """
        params = {"state": state}
        if update_all:
            params["updateAll"] = "true"
            payload = RequestFactory.Empty.empty_req()
        else:
            payload = RequestFactory.Schedule.batch_update_state(schedules)

        response = self.put_request(self.baseurl, payload, parameters={"params": params})
        return parse_batch_schedule_state(response, self.parent_srv.namespace)
