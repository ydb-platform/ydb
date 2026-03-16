from typing import Callable

import requests
from typing_extensions import Self

from office365.entity_collection import EntityCollection
from office365.runtime.paths.builder import ODataPathBuilder
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.create_entity import CreateEntityQuery
from office365.teams.operations.async_operation import TeamsAsyncOperation
from office365.teams.team import Team


class TeamCollection(EntityCollection[Team]):
    """Team's collection"""

    def __init__(self, context, resource_path=None):
        super(TeamCollection, self).__init__(context, Team, resource_path)

    def get_all(self, page_size=None, page_loaded=None):
        # type: (int, Callable[[Self], None]) -> Self
        """List all teams in Microsoft Teams for an organization"""

        def _init_teams(groups):
            # type: (Self) -> None
            for grp in groups:
                if "Team" in grp.properties["resourceProvisioningOptions"]:
                    team = Team(self.context, ResourcePath(grp.id, self.resource_path))
                    for k, v in grp.properties.items():
                        team.set_property(k, v)
                    self.add_child(team)

        self.context.groups.get_all(page_size, page_loaded=_init_teams)
        return self

    def create(self, display_name, description=None):
        """Create a new team.

        This is async operation.

        :param str display_name: The name of the team.
        :param str or None description: An optional description for the team. Maximum length: 1024 characters.
        """
        return_type = Team(self.context)
        self.add_child(return_type)

        def _process_response(resp):
            # type: (requests.Response) -> None
            content_loc = resp.headers.get("Content-Location", None)
            team_path = ODataPathBuilder.parse_url(content_loc)
            return_type.set_property("id", team_path.segment, False)

            loc = resp.headers.get("Location", None)
            operation_path = ODataPathBuilder.parse_url(loc)
            operation = TeamsAsyncOperation(self.context, operation_path)
            return_type.operations.add_child(operation)

        payload = {
            "displayName": display_name,
            "description": description,
            "template@odata.bind": "https://graph.microsoft.com/v1.0/teamsTemplates('standard')",
        }
        qry = CreateEntityQuery(self, payload, return_type)
        self.context.add_query(qry).after_execute(_process_response)
        return return_type
