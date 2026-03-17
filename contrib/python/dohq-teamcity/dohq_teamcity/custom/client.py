from dohq_teamcity.api_client import ApiClient
from dohq_teamcity.configuration import Configuration
from dohq_teamcity.custom.api import *


class TeamCity(ApiClient):
    def __init__(self, url, auth=None, auth_settings=["Basic"], proxy=None, configuration=None):
        configuration = configuration or Configuration()
        configuration.host = url
        if isinstance(auth, tuple):
            configuration.username, configuration.password = auth
        self.auth_settings = auth_settings
        if proxy is not None:
            configuration.proxy = proxy
        super(TeamCity, self).__init__(configuration=configuration)
        self.default_headers.update({'Content-Type': 'application/json',
                                     'Accept': 'application/json',
                                     'Content-Encoding': 'utf-8'})

        # Add "Managers" or APIs
        self.agents = AgentApi(self)
        self.agent_pools = AgentPoolApi(self)
        self.builds = BuildApi(self)
        self.build_queues = BuildQueueApi(self)
        self.build_types = BuildTypeApi(self)
        self.changes = ChangeApi(self)
        self.debug = DebugApi(self)
        self.default = DefaultApi(self)
        self.federation = FederationApi(self)
        self.groups = GroupApi(self)
        self.investigations = InvestigationApi(self)
        self.problems = ProblemApi(self)
        self.problem_occurrence = ProblemOccurrenceApi(self)
        self.projects = ProjectApi(self)
        self.server = ServerApi(self)
        self.tests = TestApi(self)
        self.test_occurrence = TestOccurrenceApi(self)
        self.users = UserApi(self)
        self.vcs_root = VcsRootApi(self)
        self.vcs_root_instance = VcsRootInstanceApi(self)

        # Add "Managers" with _api - legacy for .md documantation
        self.agent_api = self.agents

        self.agent_pool_api = self.agent_pools
        self.build_api = self.builds
        self.build_queue_api = self.build_queues
        self.build_type_api = self.build_types
        self.change_api = self.changes
        self.debug_api = self.debug
        self.default_api = self.default
        self.federation_api = self.federation
        self.group_api = self.groups
        self.investigation_api = self.investigations
        self.problem_api = self.problems
        self.problem_occurrence_api = self.problem_occurrence
        self.project_api = self.projects
        self.server_api = self.server
        self.test_api = self.tests
        self.test_occurrence_api = self.test_occurrence
        self.user_api = self.users
        self.vcs_root_api = self.vcs_root
        self.vcs_root_instance_api = self.vcs_root_instance

    def call_api(self, *args, **kwargs):
        """
        Quick hack for add Basic auth to swagger-codegen python
        """
        kwargs['auth_settings'] = self.auth_settings
        return super(TeamCity, self).call_api(*args, **kwargs)

    def to_str(self):
        """Returns the string representation of the model"""
        return "{}('{}')".format(
            self.__class__.__name__,
            self.configuration.host)

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()
