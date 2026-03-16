from dohq_teamcity.api import *  # noqa


class AgentApi(AgentApi):
    def get(self, agent_locator, **kwargs):
        """
        :param async_req: bool
        :param str agent_locator: (required)
        :param str fields:
        :return: Agent
        """
        return self.serve_agent(agent_locator, **kwargs)

    def get_agent(self, agent_locator, **kwargs):
        """
        :param async_req: bool
        :param str agent_locator: (required)
        :param str fields:
        :return: Agent
        """
        return self.serve_agent(agent_locator, **kwargs)

    def get_agent_field(self, agent_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str agent_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.serve_agent_field(agent_locator, field, **kwargs)

    def get_agents(self, **kwargs):
        """
        :param async_req: bool
        :param bool include_disconnected:
        :param bool include_unauthorized:
        :param str locator:
        :param str fields:
        :return: Agents
        """
        return self.serve_agents(**kwargs)


class AgentPoolApi(AgentPoolApi):
    def get(self, agent_pool_locator, **kwargs):
        """
        :param async_req: bool
        :param str agent_pool_locator: (required)
        :param str fields:
        :return: AgentPool
        """
        return self.get_pool(agent_pool_locator, **kwargs)


class BuildApi(BuildApi):
    def get(self, build_locator, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str fields:
        :return: Build
        """
        return self.serve_build(build_locator, **kwargs)

    def get_aggregated_build_status(self, build_locator, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :return: str
        """
        return self.serve_aggregated_build_status(build_locator, **kwargs)

    def get_aggregated_build_status_icon(self, build_locator, suffix, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str suffix: (required)
        :return: None
        """
        return self.serve_aggregated_build_status_icon(
            build_locator, suffix, **kwargs)

    def get_all_builds(self, **kwargs):
        """
        :param async_req: bool
        :param str build_type:
        :param str status:
        :param str triggered_by_user:
        :param bool include_personal:
        :param bool include_canceled:
        :param bool only_pinned:
        :param list[str] tag:
        :param str agent_name:
        :param str since_build:
        :param str since_date:
        :param int start:
        :param int count:
        :param str locator:
        :param str fields:
        :return: Builds
        """
        return self.serve_all_builds(**kwargs)

    def get_build(self, build_locator, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str fields:
        :return: Build
        """
        return self.serve_build(build_locator, **kwargs)

    def get_build_actual_parameters(self, build_locator, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str fields:
        :return: Properties
        """
        return self.serve_build_actual_parameters(build_locator, **kwargs)

    def get_build_field_by_build_only(self, build_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.serve_build_field_by_build_only(
            build_locator, field, **kwargs)

    def get_build_related_issues(self, build_locator, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str fields:
        :return: IssuesUsages
        """
        return self.serve_build_related_issues(build_locator, **kwargs)

    def get_build_related_issues_old(self, build_locator, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str fields:
        :return: IssuesUsages
        """
        return self.serve_build_related_issues_old(build_locator, **kwargs)

    def get_build_statistic_value(self, build_locator, name, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str name: (required)
        :return: str
        """
        return self.serve_build_statistic_value(build_locator, name, **kwargs)

    def get_build_statistic_values(self, build_locator, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str fields:
        :return: Properties
        """
        return self.serve_build_statistic_values(build_locator, **kwargs)

    def get_build_status_icon(self, build_locator, suffix, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str suffix: (required)
        :return: None
        """
        return self.serve_build_status_icon(build_locator, suffix, **kwargs)

    def get_source_file(self, build_locator, file_name, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str file_name: (required)
        :return: None
        """
        return self.serve_source_file(build_locator, file_name, **kwargs)

    def get_tags(self, build_locator, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str locator:
        :param str fields:
        :return: Tags
        """
        return self.serve_tags(build_locator, **kwargs)


class BuildQueueApi(BuildQueueApi):
    def get_build_field_by_build_only(self, build_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.serve_build_field_by_build_only(
            build_locator, field, **kwargs)

    def get_compatible_agents(self, queued_build_locator, **kwargs):
        """
        :param async_req: bool
        :param str queued_build_locator: (required)
        :param str fields:
        :return: Agents
        """
        return self.serve_compatible_agents(queued_build_locator, **kwargs)

    def get_tags(self, build_locator, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str locator:
        :param str fields:
        :return: Tags
        """
        return self.serve_tags(build_locator, **kwargs)


class BuildTypeApi(BuildTypeApi):
    def get(self, bt_locator, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str fields:
        :return: BuildType
        """
        return self.serve_build_type_xml(bt_locator, **kwargs)

    def get_branches(self, bt_locator, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str locator:
        :param str fields:
        :return: Branches
        """
        return self.serve_branches(bt_locator, **kwargs)

    def get_build_field(self, bt_locator, build_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str build_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.serve_build_field(
            bt_locator, build_locator, field, **kwargs)

    def get_build_type_builds_tags(self, bt_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str field: (required)
        :return: Tags
        """
        return self.serve_build_type_builds_tags(bt_locator, field, **kwargs)

    def get_build_type_field(self, bt_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.serve_build_type_field(bt_locator, field, **kwargs)

    def get_build_type_template(self, bt_locator, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str fields:
        :return: BuildType
        """
        return self.serve_build_type_template(bt_locator, **kwargs)

    def get_build_type_xml(self, bt_locator, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str fields:
        :return: BuildType
        """
        return self.serve_build_type_xml(bt_locator, **kwargs)

    def get_build_with_project(self, bt_locator, build_locator, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str build_locator: (required)
        :param str fields:
        :return: Build
        """
        return self.serve_build_with_project(
            bt_locator, build_locator, **kwargs)

    def get_builds(self, bt_locator, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str status:
        :param str triggered_by_user:
        :param bool include_personal:
        :param bool include_canceled:
        :param bool only_pinned:
        :param list[str] tag:
        :param str agent_name:
        :param str since_build:
        :param str since_date:
        :param int start:
        :param int count:
        :param str locator:
        :param str fields:
        :return: Builds
        """
        return self.serve_builds(bt_locator, **kwargs)


class ChangeApi(ChangeApi):
    def get(self, change_locator, **kwargs):
        """
        :param async_req: bool
        :param str change_locator: (required)
        :param str fields:
        :return: Change
        """
        return self.serve_change(change_locator, **kwargs)

    def get_change(self, change_locator, **kwargs):
        """
        :param async_req: bool
        :param str change_locator: (required)
        :param str fields:
        :return: Change
        """
        return self.serve_change(change_locator, **kwargs)

    def get_changes(self, **kwargs):
        """
        :param async_req: bool
        :param str project:
        :param str build_type:
        :param str build:
        :param str vcs_root:
        :param str since_change:
        :param int start:
        :param int count:
        :param str locator:
        :param str fields:
        :return: Changes
        """
        return self.serve_changes(**kwargs)


class DefaultApi(DefaultApi):
    def get_api_version(self, **kwargs):
        """
        :param async_req: bool
        :return: str
        """
        return self.serve_api_version(**kwargs)

    def get_build_field_short(
            self,
            project_locator,
            bt_locator,
            build_locator,
            field,
            **kwargs):
        """
        :param async_req: bool
        :param str project_locator: (required)
        :param str bt_locator: (required)
        :param str build_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.serve_build_field_short(
            project_locator, bt_locator, build_locator, field, **kwargs)

    def get_plugin_info(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Plugin
        """
        return self.serve_plugin_info(**kwargs)

    def get_root(self, **kwargs):
        """
        :param async_req: bool
        :return: str
        """
        return self.serve_root(**kwargs)

    def get_version(self, **kwargs):
        """
        :param async_req: bool
        :return: str
        """
        return self.serve_version(**kwargs)


class GroupApi(GroupApi):
    def get(self, group_locator, **kwargs):
        """
        :param async_req: bool
        :param str group_locator: (required)
        :param str fields:
        :return: Group
        """
        return self.serve_group(group_locator, **kwargs)

    def get_group(self, group_locator, **kwargs):
        """
        :param async_req: bool
        :param str group_locator: (required)
        :param str fields:
        :return: Group
        """
        return self.serve_group(group_locator, **kwargs)

    def get_groups(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Groups
        """
        return self.serve_groups(**kwargs)

    def get_user_properties(self, group_locator, name, **kwargs):
        """
        :param async_req: bool
        :param str group_locator: (required)
        :param str name: (required)
        :return: str
        """
        return self.serve_user_properties(group_locator, name, **kwargs)


class InvestigationApi(InvestigationApi):
    def get(self, investigation_locator, **kwargs):
        """
        :param async_req: bool
        :param str investigation_locator: (required)
        :param str fields:
        :return: Investigation
        """
        return self.serve_instance(investigation_locator, **kwargs)

    def get_instance(self, investigation_locator, **kwargs):
        """
        :param async_req: bool
        :param str investigation_locator: (required)
        :param str fields:
        :return: Investigation
        """
        return self.serve_instance(investigation_locator, **kwargs)


class ProblemApi(ProblemApi):
    def get(self, problem_locator, **kwargs):
        """
        :param async_req: bool
        :param str problem_locator: (required)
        :param str fields:
        :return: Problem
        """
        return self.serve_instance(problem_locator, **kwargs)

    def get_instance(self, problem_locator, **kwargs):
        """
        :param async_req: bool
        :param str problem_locator: (required)
        :param str fields:
        :return: Problem
        """
        return self.serve_instance(problem_locator, **kwargs)


class ProblemOccurrenceApi(ProblemOccurrenceApi):
    def get(self, problem_locator, **kwargs):
        """
        :param async_req: bool
        :param str problem_locator: (required)
        :param str fields:
        :return: ProblemOccurrence
        """
        return self.serve_instance(problem_locator, **kwargs)

    def get_instance(self, problem_locator, **kwargs):
        """
        :param async_req: bool
        :param str problem_locator: (required)
        :param str fields:
        :return: ProblemOccurrence
        """
        return self.serve_instance(problem_locator, **kwargs)


class ProjectApi(ProjectApi):
    def get(self, project_locator, **kwargs):
        """
        :param async_req: bool
        :param str project_locator: (required)
        :param str fields:
        :return: Project
        """
        return self.serve_project(project_locator, **kwargs)

    def get_build_field_with_project(
            self,
            project_locator,
            bt_locator,
            build_locator,
            field,
            **kwargs):
        """
        :param async_req: bool
        :param str project_locator: (required)
        :param str bt_locator: (required)
        :param str build_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.serve_build_field_with_project(
            project_locator, bt_locator, build_locator, field, **kwargs)

    def get_build_type(self, project_locator, bt_locator, **kwargs):
        """
        :param async_req: bool
        :param str project_locator: (required)
        :param str bt_locator: (required)
        :param str fields:
        :return: BuildType
        """
        return self.serve_build_type(project_locator, bt_locator, **kwargs)

    def get_build_type_field_with_project(
            self, project_locator, bt_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str project_locator: (required)
        :param str bt_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.serve_build_type_field_with_project(
            project_locator, bt_locator, field, **kwargs)

    def get_build_type_templates(self, project_locator, bt_locator, **kwargs):
        """
        :param async_req: bool
        :param str project_locator: (required)
        :param str bt_locator: (required)
        :param str fields:
        :return: BuildType
        """
        return self.serve_build_type_templates(
            project_locator, bt_locator, **kwargs)

    def get_build_types_in_project(self, project_locator, **kwargs):
        """
        :param async_req: bool
        :param str project_locator: (required)
        :param str fields:
        :return: BuildTypes
        """
        return self.serve_build_types_in_project(project_locator, **kwargs)

    def get_build_with_project(
            self,
            project_locator,
            bt_locator,
            build_locator,
            **kwargs):
        """
        :param async_req: bool
        :param str project_locator: (required)
        :param str bt_locator: (required)
        :param str build_locator: (required)
        :param str fields:
        :return: Build
        """
        return self.serve_build_with_project(
            project_locator, bt_locator, build_locator, **kwargs)

    def get_builds(self, project_locator, bt_locator, **kwargs):
        """
        :param async_req: bool
        :param str project_locator: (required)
        :param str bt_locator: (required)
        :param str status:
        :param str triggered_by_user:
        :param bool include_personal:
        :param bool include_canceled:
        :param bool only_pinned:
        :param list[str] tag:
        :param str agent_name:
        :param str since_build:
        :param str since_date:
        :param int start:
        :param int count:
        :param str locator:
        :param str fields:
        :return: Builds
        """
        return self.serve_builds(project_locator, bt_locator, **kwargs)

    def get_project(self, project_locator, **kwargs):
        """
        :param async_req: bool
        :param str project_locator: (required)
        :param str fields:
        :return: Project
        """
        return self.serve_project(project_locator, **kwargs)

    def get_project_field(self, project_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str project_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.serve_project_field(project_locator, field, **kwargs)

    def get_projects(self, **kwargs):
        """
        :param async_req: bool
        :param str locator:
        :param str fields:
        :return: Projects
        """
        return self.serve_projects(**kwargs)

    def get_templates_in_project(self, project_locator, **kwargs):
        """
        :param async_req: bool
        :param str project_locator: (required)
        :param str fields:
        :return: BuildTypes
        """
        return self.serve_templates_in_project(project_locator, **kwargs)


class ServerApi(ServerApi):
    def get_plugins(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Plugins
        """
        return self.serve_plugins(**kwargs)

    def get_server_info(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Server
        """
        return self.serve_server_info(**kwargs)

    def get_server_version(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: str
        """
        return self.serve_server_version(field, **kwargs)


class TestApi(TestApi):
    def get(self, test_locator, **kwargs):
        """
        :param async_req: bool
        :param str test_locator: (required)
        :param str fields:
        :return: Test
        """
        return self.serve_instance(test_locator, **kwargs)

    def get_instance(self, test_locator, **kwargs):
        """
        :param async_req: bool
        :param str test_locator: (required)
        :param str fields:
        :return: Test
        """
        return self.serve_instance(test_locator, **kwargs)


class TestOccurrenceApi(TestOccurrenceApi):
    def get(self, test_locator, **kwargs):
        """
        :param async_req: bool
        :param str test_locator: (required)
        :param str fields:
        :return: TestOccurrence
        """
        return self.serve_instance(test_locator, **kwargs)

    def get_instance(self, test_locator, **kwargs):
        """
        :param async_req: bool
        :param str test_locator: (required)
        :param str fields:
        :return: TestOccurrence
        """
        return self.serve_instance(test_locator, **kwargs)


class UserApi(UserApi):
    def get(self, user_locator, **kwargs):
        """
        :param async_req: bool
        :param str user_locator: (required)
        :param str fields:
        :return: User
        """
        return self.serve_user(user_locator, **kwargs)

    def get_user(self, user_locator, **kwargs):
        """
        :param async_req: bool
        :param str user_locator: (required)
        :param str fields:
        :return: User
        """
        return self.serve_user(user_locator, **kwargs)

    def get_user_field(self, user_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str user_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.serve_user_field(user_locator, field, **kwargs)

    def get_user_properties(self, user_locator, **kwargs):
        """
        :param async_req: bool
        :param str user_locator: (required)
        :param str fields:
        :return: Properties
        """
        return self.serve_user_properties(user_locator, **kwargs)

    def get_user_property(self, user_locator, name, **kwargs):
        """
        :param async_req: bool
        :param str user_locator: (required)
        :param str name: (required)
        :return: str
        """
        return self.serve_user_property(user_locator, name, **kwargs)

    def get_users(self, **kwargs):
        """
        :param async_req: bool
        :param str locator:
        :param str fields:
        :return: Users
        """
        return self.serve_users(**kwargs)


class VcsRootApi(VcsRootApi):
    def get(self, vcs_root_locator, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_locator: (required)
        :param str fields:
        :return: VcsRoot
        """
        return self.serve_root(vcs_root_locator, **kwargs)

    def get_field(self, vcs_root_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.serve_field(vcs_root_locator, field, **kwargs)

    def get_instance_field(
            self,
            vcs_root_locator,
            vcs_root_instance_locator,
            field,
            **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_locator: (required)
        :param str vcs_root_instance_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.serve_instance_field(
            vcs_root_locator, vcs_root_instance_locator, field, **kwargs)

    def get_properties(self, vcs_root_locator, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_locator: (required)
        :param str fields:
        :return: Properties
        """
        return self.serve_properties(vcs_root_locator, **kwargs)

    def get_property(self, vcs_root_locator, name, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_locator: (required)
        :param str name: (required)
        :return: str
        """
        return self.serve_property(vcs_root_locator, name, **kwargs)

    def get_root(self, vcs_root_locator, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_locator: (required)
        :param str fields:
        :return: VcsRoot
        """
        return self.serve_root(vcs_root_locator, **kwargs)

    def get_root_instance(
            self,
            vcs_root_locator,
            vcs_root_instance_locator,
            **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_locator: (required)
        :param str vcs_root_instance_locator: (required)
        :param str fields:
        :return: VcsRootInstance
        """
        return self.serve_root_instance(
            vcs_root_locator, vcs_root_instance_locator, **kwargs)

    def get_root_instance_properties(
            self,
            vcs_root_locator,
            vcs_root_instance_locator,
            **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_locator: (required)
        :param str vcs_root_instance_locator: (required)
        :param str fields:
        :return: Properties
        """
        return self.serve_root_instance_properties(
            vcs_root_locator, vcs_root_instance_locator, **kwargs)

    def get_root_instances(self, vcs_root_locator, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_locator: (required)
        :param str fields:
        :return: VcsRootInstances
        """
        return self.serve_root_instances(vcs_root_locator, **kwargs)

    def get_roots(self, **kwargs):
        """
        :param async_req: bool
        :param str locator:
        :param str fields:
        :return: VcsRoots
        """
        return self.serve_roots(**kwargs)


class VcsRootInstanceApi(VcsRootInstanceApi):
    def get(self, vcs_root_instance_locator, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_instance_locator: (required)
        :param str fields:
        :return: VcsRootInstance
        """
        return self.serve_instance(vcs_root_instance_locator, **kwargs)

    def get_instance(self, vcs_root_instance_locator, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_instance_locator: (required)
        :param str fields:
        :return: VcsRootInstance
        """
        return self.serve_instance(vcs_root_instance_locator, **kwargs)

    def get_instance_field(self, vcs_root_instance_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_instance_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.serve_instance_field(
            vcs_root_instance_locator, field, **kwargs)

    def get_instances(self, **kwargs):
        """
        :param async_req: bool
        :param str locator:
        :param str fields:
        :return: VcsRootInstances
        """
        return self.serve_instances(**kwargs)

    def get_root_instance_properties(self, vcs_root_instance_locator, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_instance_locator: (required)
        :param str fields:
        :return: Properties
        """
        return self.serve_root_instance_properties(
            vcs_root_instance_locator, **kwargs)
