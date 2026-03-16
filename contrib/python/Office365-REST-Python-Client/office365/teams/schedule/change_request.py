from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.teams.schedule.change_tracked_entity import ChangeTrackedEntity


class ScheduleChangeRequest(ChangeTrackedEntity):
    def approve(self, message):
        """
        Approve an ScheduleChangeRequest object.

        :param str message: A custom approval message.
        """
        qry = ServiceOperationQuery(self, "approve", None, {"message": message})
        self.context.add_query(qry)
        return self

    def decline(self, message):
        """
        :param str message: A custom approval message.
        """
        qry = ServiceOperationQuery(self, "decline", None, {"message": message})
        self.context.add_query(qry)
        return self
