from .endpoint import Endpoint, api
from .exceptions import MissingRequiredFieldError
from .. import RequestFactory, PaginationItem, ScheduleItem, TaskItem
import logging
import copy
from collections import namedtuple

logger = logging.getLogger('tableau.endpoint.schedules')
# Oh to have a first class Result concept in Python...
AddResponse = namedtuple('AddResponse', ('result', 'error', 'warnings', 'task_created'))
OK = AddResponse(result=True, error=None, warnings=None, task_created=None)


class Schedules(Endpoint):
    @property
    def baseurl(self):
        return "{0}/schedules".format(self.parent_srv.baseurl)

    @property
    def siteurl(self):
        return "{0}/sites/{1}/schedules".format(self.parent_srv.baseurl, self.parent_srv.site_id)

    @api(version="2.3")
    def get(self, req_options=None):
        logger.info("Querying all schedules")
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_schedule_items = ScheduleItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_schedule_items, pagination_item

    @api(version="2.3")
    def delete(self, schedule_id):
        if not schedule_id:
            error = "Schedule ID undefined"
            raise ValueError(error)
        url = "{0}/{1}".format(self.baseurl, schedule_id)
        self.delete_request(url)
        logger.info("Deleted single schedule (ID: {0})".format(schedule_id))

    @api(version="2.3")
    def update(self, schedule_item):
        if not schedule_item.id:
            error = "Schedule item missing ID."
            raise MissingRequiredFieldError(error)
        if schedule_item.interval_item is None:
            error = "Interval item must be defined."
            raise MissingRequiredFieldError(error)

        url = "{0}/{1}".format(self.baseurl, schedule_item.id)
        update_req = RequestFactory.Schedule.update_req(schedule_item)
        server_response = self.put_request(url, update_req)
        logger.info("Updated schedule item (ID: {})".format(schedule_item.id))
        updated_schedule = copy.copy(schedule_item)
        return updated_schedule._parse_common_tags(server_response.content, self.parent_srv.namespace)

    @api(version="2.3")
    def create(self, schedule_item):
        if schedule_item.interval_item is None:
            error = "Interval item must be defined."
            raise MissingRequiredFieldError(error)

        url = self.baseurl
        create_req = RequestFactory.Schedule.create_req(schedule_item)
        server_response = self.post_request(url, create_req)
        new_schedule = ScheduleItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        logger.info("Created new schedule (ID: {})".format(new_schedule.id))
        return new_schedule

    @api(version="2.8")
    def add_to_schedule(self, schedule_id, workbook=None, datasource=None,
                        task_type=TaskItem.Type.ExtractRefresh):
        def add_to(resource, type_, req_factory):
            id_ = resource.id
            url = "{0}/{1}/{2}s".format(self.siteurl, schedule_id, type_)
            add_req = req_factory(id_, task_type=task_type)
            response = self.put_request(url, add_req)

            error, warnings, task_created = ScheduleItem.parse_add_to_schedule_response(
                response, self.parent_srv.namespace)
            if task_created:
                logger.info("Added {} to {} to schedule {}".format(type_, id_, schedule_id))

            if error is not None or warnings is not None:
                return AddResponse(result=False, error=error, warnings=warnings, task_created=task_created)
            else:
                return OK

        items = []

        if workbook is not None:
            items.append((workbook, "workbook", RequestFactory.Schedule.add_workbook_req))
        if datasource is not None:
            items.append((datasource, "datasource", RequestFactory.Schedule.add_datasource_req))

        results = (add_to(*x) for x in items)
        # list() is needed for python 3.x compatibility
        return list(filter(lambda x: not x.result, results))
