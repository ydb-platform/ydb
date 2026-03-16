from office365.directory.groups.group import Group
from office365.entity_collection import EntityCollection
from office365.planner.plans.plan import PlannerPlan
from office365.runtime.queries.create_entity import CreateEntityQuery


class PlannerPlanCollection(EntityCollection[PlannerPlan]):
    def __init__(self, context, resource_path=None):
        super(PlannerPlanCollection, self).__init__(context, PlannerPlan, resource_path)

    def add(self, title, container):
        # type: (str, str|Group) -> PlannerPlan
        """Creates a new plannerPlan.
        :param str title: Plan title
        :param str or Group container: Identifies the container of the plan.
        """
        return_type = PlannerPlan(self.context)
        self.add_child(return_type)

        def _add(owner_id):
            # type: (str) -> None
            payload = {
                "title": title,
                "container": {
                    "url": "https://graph.microsoft.com/v1.0/groups/{0}".format(
                        owner_id
                    )
                },
            }
            qry = CreateEntityQuery(self, payload, return_type)
            self.context.add_query(qry)

        if isinstance(container, Group):

            def _owner_loaded():
                _add(container.id)

            container.ensure_property("id", _owner_loaded)
        else:
            _add(container)

        return return_type
