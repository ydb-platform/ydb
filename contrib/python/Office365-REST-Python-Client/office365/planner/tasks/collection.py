from office365.entity_collection import EntityCollection
from office365.planner.plans.plan import PlannerPlan
from office365.planner.tasks.task import PlannerTask
from office365.runtime.queries.create_entity import CreateEntityQuery


class PlannerTaskCollection(EntityCollection[PlannerTask]):
    def __init__(self, context, resource_path=None):
        super(PlannerTaskCollection, self).__init__(context, PlannerTask, resource_path)

    def add(self, title, plan, bucket=None):
        """
        Create a new plannerTask.

        :param str title: Task title
        :param str|PlannerPlan plan: Plan identifier or Plan object
        :param str|PlannerBucket bucket: Bucket identifier or Plan object
        """
        return_type = PlannerTask(self.context)
        self.add_child(return_type)

        def _add(plan_id):
            # type: (str) -> None
            payload = {"title": title, "planId": plan_id, "bucketId": bucket}
            qry = CreateEntityQuery(self, payload, return_type)
            self.context.add_query(qry)

        if isinstance(plan, PlannerPlan):

            def _parent_loaded():
                _add(plan.id)

            plan.ensure_property("id", _parent_loaded)
        else:
            _add(plan)

        return return_type
