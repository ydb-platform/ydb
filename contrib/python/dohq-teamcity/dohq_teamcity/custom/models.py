from dohq_teamcity.custom.base_model import ReadMixin, DeleteMixin, ContainerMixin
from dohq_teamcity.models import *


class Agent(Agent, ReadMixin, DeleteMixin):
    @property
    def api(self):
        return self.teamcity.agents

    def _read(self):
        return self.api.get

    def _delete(self):
        return self.api.delete_agent

    def delete_agent(self, **kwargs):
        """

        :param async_req: bool
        :return: None

        """
        return self.api.delete_agent(agent_locator=self, **kwargs)

    def get(self, **kwargs):
        """ Test msg

        :param async_req: bool
        :param str fields:
        :return: Agent
        """
        return self.api.get(agent_locator=self, **kwargs)

    def get_agent(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Agent
        """
        return self.api.get_agent(agent_locator=self, **kwargs)

    def get_agent_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: str
        """
        return self.api.get_agent_field(field, agent_locator=self, **kwargs)

    def get_agent_pool(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: AgentPool
        """
        return self.api.get_agent_pool(agent_locator=self, **kwargs)

    def get_authorized_info(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: AuthorizedInfo
        """
        return self.api.get_authorized_info(agent_locator=self, **kwargs)

    def get_compatible_build_types(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: BuildTypes
        """
        return self.api.get_compatible_build_types(agent_locator=self, **kwargs)

    def get_enabled_info(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: EnabledInfo
        """
        return self.api.get_enabled_info(agent_locator=self, **kwargs)

    def get_incompatible_build_types(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Compatibilities
        """
        return self.api.get_incompatible_build_types(
            agent_locator=self, **kwargs)

    def serve_agent(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Agent
        """
        return self.api.serve_agent(agent_locator=self, **kwargs)

    def serve_agent_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: str
        """
        return self.api.serve_agent_field(field, agent_locator=self, **kwargs)

    def set_agent_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :param str body:
        :return: str
        """
        return self.api.set_agent_field(field, agent_locator=self, **kwargs)

    def set_agent_pool(self, **kwargs):
        """
        :param async_req: bool
        :param AgentPool body:
        :param str fields:
        :return: AgentPool
        """
        return self.api.set_agent_pool(agent_locator=self, **kwargs)

    def set_authorized_info(self, **kwargs):
        """
        :param async_req: bool
        :param AuthorizedInfo body:
        :param str fields:
        :return: AuthorizedInfo
        """
        return self.api.set_authorized_info(agent_locator=self, **kwargs)

    def set_enabled_info(self, **kwargs):
        """
        :param async_req: bool
        :param EnabledInfo body:
        :param str fields:
        :return: EnabledInfo
        """
        return self.api.set_enabled_info(agent_locator=self, **kwargs)


class AgentPool(AgentPool, ReadMixin, DeleteMixin):
    @property
    def api(self):
        return self.teamcity.agent_pools

    def _read(self):
        return self.api.get

    def _delete(self):
        return self.api.delete_pool

    def add_agent(self, **kwargs):
        """
        :param async_req: bool
        :param Agent body:
        :param str fields:
        :return: Agent
        """
        return self.api.add_agent(agent_pool_locator=self, **kwargs)

    def add_project(self, **kwargs):
        """
        :param async_req: bool
        :param Project body:
        :return: Project
        """
        return self.api.add_project(agent_pool_locator=self, **kwargs)

    def delete_pool(self, **kwargs):
        """
        :param async_req: bool
        :return: None
        """
        return self.api.delete_pool(agent_pool_locator=self, **kwargs)

    def delete_pool_project(self, project_locator, **kwargs):
        """
        :param async_req: bool
        :param str project_locator: (required)
        :return: None
        """
        return self.api.delete_pool_project(
            project_locator, agent_pool_locator=self, **kwargs)

    def delete_projects(self, **kwargs):
        """
        :param async_req: bool
        :return: None
        """
        return self.api.delete_projects(agent_pool_locator=self, **kwargs)

    def get(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: AgentPool
        """
        return self.api.get(agent_pool_locator=self, **kwargs)

    def get_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: str
        """
        return self.api.get_field(field, agent_pool_locator=self, **kwargs)

    def get_pool(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: AgentPool
        """
        return self.api.get_pool(agent_pool_locator=self, **kwargs)

    def get_pool_agents(self, **kwargs):
        """
        :param async_req: bool
        :param str locator:
        :param str fields:
        :return: Agents
        """
        return self.api.get_pool_agents(agent_pool_locator=self, **kwargs)

    def get_pool_project(self, project_locator, **kwargs):
        """
        :param async_req: bool
        :param str project_locator: (required)
        :param str fields:
        :return: Project
        """
        return self.api.get_pool_project(
            project_locator, agent_pool_locator=self, **kwargs)

    def get_pool_projects(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Projects
        """
        return self.api.get_pool_projects(agent_pool_locator=self, **kwargs)

    def replace_projects(self, **kwargs):
        """
        :param async_req: bool
        :param Projects body:
        :return: Projects
        """
        return self.api.replace_projects(agent_pool_locator=self, **kwargs)

    def set_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :param str body:
        :return: str
        """
        return self.api.set_field(field, agent_pool_locator=self, **kwargs)


class Build(Build, ReadMixin, DeleteMixin):
    @property
    def api(self):
        return self.teamcity.builds

    def _read(self):
        return self.api.get

    def _delete(self):
        return self.api.delete_build

    def add_tags(self, **kwargs):
        """
        :param async_req: bool
        :param Tags body:
        :param str fields:
        :return: Tags
        """
        return self.api.add_tags(build_locator=self, **kwargs)

    def cancel_build(self, **kwargs):
        """
        :param async_req: bool
        :param BuildCancelRequest body:
        :param str fields:
        :return: Build
        """
        return self.api.cancel_build(build_locator=self, **kwargs)

    def cancel_build_0(self, **kwargs):
        """
        :param async_req: bool
        :return: BuildCancelRequest
        """
        return self.api.cancel_build_0(build_locator=self, **kwargs)

    def delete_build(self, **kwargs):
        """
        :param async_req: bool
        :return: None
        """
        return self.api.delete_build(build_locator=self, **kwargs)

    def delete_comment(self, **kwargs):
        """
        :param async_req: bool
        :return: None
        """
        return self.api.delete_comment(build_locator=self, **kwargs)

    def get(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Build
        """
        return self.api.get(build_locator=self, **kwargs)

    def get_aggregated_build_status(self, **kwargs):
        """
        :param async_req: bool
        :return: str
        """
        return self.api.get_aggregated_build_status(
            build_locator=self, **kwargs)

    def get_aggregated_build_status_icon(self, suffix, **kwargs):
        """
        :param async_req: bool
        :param str suffix: (required)
        :return: None
        """
        return self.api.get_aggregated_build_status_icon(
            suffix, build_locator=self, **kwargs)

    def get_artifacts_directory(self, **kwargs):
        """
        :param async_req: bool
        :return: str
        """
        return self.api.get_artifacts_directory(build_locator=self, **kwargs)

    def get_build(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Build
        """
        return self.api.get_build(build_locator=self, **kwargs)

    def get_build_actual_parameters(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Properties
        """
        return self.api.get_build_actual_parameters(
            build_locator=self, **kwargs)

    def get_build_field_by_build_only(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: str
        """
        return self.api.get_build_field_by_build_only(
            field, build_locator=self, **kwargs)

    def get_build_related_issues(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: IssuesUsages
        """
        return self.api.get_build_related_issues(build_locator=self, **kwargs)

    def get_build_related_issues_old(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: IssuesUsages
        """
        return self.api.get_build_related_issues_old(
            build_locator=self, **kwargs)

    def get_build_statistic_value(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: str
        """
        return self.api.get_build_statistic_value(
            name, build_locator=self, **kwargs)

    def get_build_statistic_values(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Properties
        """
        return self.api.get_build_statistic_values(build_locator=self, **kwargs)

    def get_build_status_icon(self, suffix, **kwargs):
        """
        :param async_req: bool
        :param str suffix: (required)
        :return: None
        """
        return self.api.get_build_status_icon(
            suffix, build_locator=self, **kwargs)

    def get_canceled_info(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Comment
        """
        return self.api.get_canceled_info(build_locator=self, **kwargs)

    def get_children(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :param str base_path:
        :param str locator:
        :param str fields:
        :param bool resolve_parameters:
        :param bool log_build_usage:
        :return: Files
        """
        return self.api.get_children(path, build_locator=self, **kwargs)

    def get_children_alias(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :param str base_path:
        :param str locator:
        :param str fields:
        :param bool resolve_parameters:
        :param bool log_build_usage:
        :return: Files
        """
        return self.api.get_children_alias(path, build_locator=self, **kwargs)

    def get_content(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :param bool resolve_parameters:
        :param bool log_build_usage:
        :return: None
        """
        return self.api.get_content(path, build_locator=self, **kwargs)

    def get_content_alias(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :param bool resolve_parameters:
        :param bool log_build_usage:
        :return: None
        """
        return self.api.get_content_alias(path, build_locator=self, **kwargs)

    def get_metadata(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :param str fields:
        :param bool resolve_parameters:
        :param bool log_build_usage:
        :return: file
        """
        return self.api.get_metadata(path, build_locator=self, **kwargs)

    def get_parameter(self, property_name, **kwargs):
        """
        :param async_req: bool
        :param str property_name: (required)
        :return: str
        """
        return self.api.get_parameter(
            property_name, build_locator=self, **kwargs)

    def get_pinned(self, **kwargs):
        """
        :param async_req: bool
        :return: str
        """
        return self.api.get_pinned(build_locator=self, **kwargs)

    def get_problems(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: ProblemOccurrences
        """
        return self.api.get_problems(build_locator=self, **kwargs)

    def get_root(self, **kwargs):
        """
        :param async_req: bool
        :param str base_path:
        :param str locator:
        :param str fields:
        :param bool resolve_parameters:
        :param bool log_build_usage:
        :return: Files
        """
        return self.api.get_root(build_locator=self, **kwargs)

    def get_source_file(self, file_name, **kwargs):
        """
        :param async_req: bool
        :param str file_name: (required)
        :return: None
        """
        return self.api.get_source_file(file_name, build_locator=self, **kwargs)

    def get_tags(self, **kwargs):
        """
        :param async_req: bool
        :param str locator:
        :param str fields:
        :return: Tags
        """
        return self.api.get_tags(build_locator=self, **kwargs)

    def get_tests(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: TestOccurrences
        """
        return self.api.get_tests(build_locator=self, **kwargs)

    def get_zipped(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :param str base_path:
        :param str locator:
        :param str name:
        :param bool resolve_parameters:
        :param bool log_build_usage:
        :return: None
        """
        return self.api.get_zipped(path, build_locator=self, **kwargs)

    def pin_build(self, **kwargs):
        """
        :param async_req: bool
        :param str body:
        :return: None
        """
        return self.api.pin_build(build_locator=self, **kwargs)

    def replace_comment(self, **kwargs):
        """
        :param async_req: bool
        :param str body:
        :return: None
        """
        return self.api.replace_comment(build_locator=self, **kwargs)

    def replace_tags(self, **kwargs):
        """
        :param async_req: bool
        :param str locator:
        :param Tags body:
        :param str fields:
        :return: Tags
        """
        return self.api.replace_tags(build_locator=self, **kwargs)

    def serve_aggregated_build_status(self, **kwargs):
        """
        :param async_req: bool
        :return: str
        """
        return self.api.serve_aggregated_build_status(
            build_locator=self, **kwargs)

    def serve_aggregated_build_status_icon(self, suffix, **kwargs):
        """
        :param async_req: bool
        :param str suffix: (required)
        :return: None
        """
        return self.api.serve_aggregated_build_status_icon(
            suffix, build_locator=self, **kwargs)

    def serve_build(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Build
        """
        return self.api.serve_build(build_locator=self, **kwargs)

    def serve_build_actual_parameters(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Properties
        """
        return self.api.serve_build_actual_parameters(
            build_locator=self, **kwargs)

    def serve_build_field_by_build_only(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: str
        """
        return self.api.serve_build_field_by_build_only(
            field, build_locator=self, **kwargs)

    def serve_build_related_issues(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: IssuesUsages
        """
        return self.api.serve_build_related_issues(build_locator=self, **kwargs)

    def serve_build_related_issues_old(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: IssuesUsages
        """
        return self.api.serve_build_related_issues_old(
            build_locator=self, **kwargs)

    def serve_build_statistic_value(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: str
        """
        return self.api.serve_build_statistic_value(
            name, build_locator=self, **kwargs)

    def serve_build_statistic_values(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Properties
        """
        return self.api.serve_build_statistic_values(
            build_locator=self, **kwargs)

    def serve_build_status_icon(self, suffix, **kwargs):
        """
        :param async_req: bool
        :param str suffix: (required)
        :return: None
        """
        return self.api.serve_build_status_icon(
            suffix, build_locator=self, **kwargs)

    def serve_source_file(self, file_name, **kwargs):
        """
        :param async_req: bool
        :param str file_name: (required)
        :return: None
        """
        return self.api.serve_source_file(
            file_name, build_locator=self, **kwargs)

    def serve_tags(self, **kwargs):
        """
        :param async_req: bool
        :param str locator:
        :param str fields:
        :return: Tags
        """
        return self.api.serve_tags(build_locator=self, **kwargs)

    def unpin_build(self, **kwargs):
        """
        :param async_req: bool
        :param str body:
        :return: None
        """
        return self.api.unpin_build(build_locator=self, **kwargs)


class BuildType(BuildType, ReadMixin, DeleteMixin):
    @property
    def api(self):
        return self.teamcity.build_types

    def _read(self):
        return self.api.get

    def _delete(self):
        return self.api.delete_build_type

    def add_agent_requirement(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :param AgentRequirement body:
        :return: AgentRequirement
        """
        return self.api.add_agent_requirement(bt_locator=self, **kwargs)

    def add_artifact_dep(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :param ArtifactDependency body:
        :return: ArtifactDependency
        """
        return self.api.add_artifact_dep(bt_locator=self, **kwargs)

    def add_feature(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :param Feature body:
        :return: Feature
        """
        return self.api.add_feature(bt_locator=self, **kwargs)

    def add_feature_parameter(self, feature_id, parameter_name, **kwargs):
        """
        :param async_req: bool
        :param str feature_id: (required)
        :param str parameter_name: (required)
        :param str body:
        :return: str
        """
        return self.api.add_feature_parameter(
            feature_id, parameter_name, bt_locator=self, **kwargs)

    def add_snapshot_dep(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :param SnapshotDependency body:
        :return: SnapshotDependency
        """
        return self.api.add_snapshot_dep(bt_locator=self, **kwargs)

    def add_step(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :param Step body:
        :return: Step
        """
        return self.api.add_step(bt_locator=self, **kwargs)

    def add_step_parameter(self, step_id, parameter_name, **kwargs):
        """
        :param async_req: bool
        :param str step_id: (required)
        :param str parameter_name: (required)
        :param str body:
        :return: str
        """
        return self.api.add_step_parameter(
            step_id, parameter_name, bt_locator=self, **kwargs)

    def add_trigger(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :param Trigger body:
        :return: Trigger
        """
        return self.api.add_trigger(bt_locator=self, **kwargs)

    def add_vcs_root_entry(self, **kwargs):
        """
        :param async_req: bool
        :param VcsRootEntry body:
        :param str fields:
        :return: VcsRootEntry
        """
        return self.api.add_vcs_root_entry(bt_locator=self, **kwargs)

    def change_artifact_dep_setting(
            self,
            artifact_dep_locator,
            field_name,
            **kwargs):
        """
        :param async_req: bool
        :param str artifact_dep_locator: (required)
        :param str field_name: (required)
        :param str body:
        :return: str
        """
        return self.api.change_artifact_dep_setting(
            artifact_dep_locator, field_name, bt_locator=self, **kwargs)

    def change_feature_setting(self, feature_id, name, **kwargs):
        """
        :param async_req: bool
        :param str feature_id: (required)
        :param str name: (required)
        :param str body:
        :return: str
        """
        return self.api.change_feature_setting(
            feature_id, name, bt_locator=self, **kwargs)

    def change_requirement_setting(
            self,
            agent_requirement_locator,
            field_name,
            **kwargs):
        """
        :param async_req: bool
        :param str agent_requirement_locator: (required)
        :param str field_name: (required)
        :param str body:
        :return: str
        """
        return self.api.change_requirement_setting(
            agent_requirement_locator, field_name, bt_locator=self, **kwargs)

    def change_step_setting(self, step_id, field_name, **kwargs):
        """
        :param async_req: bool
        :param str step_id: (required)
        :param str field_name: (required)
        :param str body:
        :return: str
        """
        return self.api.change_step_setting(
            step_id, field_name, bt_locator=self, **kwargs)

    def change_trigger_setting(self, trigger_locator, field_name, **kwargs):
        """
        :param async_req: bool
        :param str trigger_locator: (required)
        :param str field_name: (required)
        :param str body:
        :return: str
        """
        return self.api.change_trigger_setting(
            trigger_locator, field_name, bt_locator=self, **kwargs)

    def delete_agent_requirement(self, agent_requirement_locator, **kwargs):
        """
        :param async_req: bool
        :param str agent_requirement_locator: (required)
        :return: None
        """
        return self.api.delete_agent_requirement(
            agent_requirement_locator, bt_locator=self, **kwargs)

    def delete_all_parameters(self, **kwargs):
        """
        :param async_req: bool
        :return: None
        """
        return self.api.delete_all_parameters(bt_locator=self, **kwargs)

    def delete_all_parameters_0(self, **kwargs):
        """
        :param async_req: bool
        :return: None
        """
        return self.api.delete_all_parameters_0(bt_locator=self, **kwargs)

    def delete_artifact_dep(self, artifact_dep_locator, **kwargs):
        """
        :param async_req: bool
        :param str artifact_dep_locator: (required)
        :return: None
        """
        return self.api.delete_artifact_dep(
            artifact_dep_locator, bt_locator=self, **kwargs)

    def delete_build_type(self, **kwargs):
        """
        :param async_req: bool
        :return: None
        """
        return self.api.delete_build_type(bt_locator=self, **kwargs)

    def delete_feature(self, feature_id, **kwargs):
        """
        :param async_req: bool
        :param str feature_id: (required)
        :return: None
        """
        return self.api.delete_feature(feature_id, bt_locator=self, **kwargs)

    def delete_parameter(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: None
        """
        return self.api.delete_parameter(name, bt_locator=self, **kwargs)

    def delete_parameter_0(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: None
        """
        return self.api.delete_parameter_0(name, bt_locator=self, **kwargs)

    def delete_snapshot_dep(self, snapshot_dep_locator, **kwargs):
        """
        :param async_req: bool
        :param str snapshot_dep_locator: (required)
        :return: None
        """
        return self.api.delete_snapshot_dep(
            snapshot_dep_locator, bt_locator=self, **kwargs)

    def delete_step(self, step_id, **kwargs):
        """
        :param async_req: bool
        :param str step_id: (required)
        :return: None
        """
        return self.api.delete_step(step_id, bt_locator=self, **kwargs)

    def delete_template_association(self, **kwargs):
        """
        :param async_req: bool
        :return: None
        """
        return self.api.delete_template_association(bt_locator=self, **kwargs)

    def delete_trigger(self, trigger_locator, **kwargs):
        """
        :param async_req: bool
        :param str trigger_locator: (required)
        :return: None
        """
        return self.api.delete_trigger(
            trigger_locator, bt_locator=self, **kwargs)

    def delete_vcs_root_entry(self, vcs_root_locator, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_locator: (required)
        :return: None
        """
        return self.api.delete_vcs_root_entry(
            vcs_root_locator, bt_locator=self, **kwargs)

    def get(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: BuildType
        """
        return self.api.get(bt_locator=self, **kwargs)

    def get_agent_requirement(self, agent_requirement_locator, **kwargs):
        """
        :param async_req: bool
        :param str agent_requirement_locator: (required)
        :param str fields:
        :return: AgentRequirement
        """
        return self.api.get_agent_requirement(
            agent_requirement_locator, bt_locator=self, **kwargs)

    def get_agent_requirements(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: AgentRequirements
        """
        return self.api.get_agent_requirements(bt_locator=self, **kwargs)

    def get_aliases(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: Items
        """
        return self.api.get_aliases(field, bt_locator=self, **kwargs)

    def get_artifact_dep(self, artifact_dep_locator, **kwargs):
        """
        :param async_req: bool
        :param str artifact_dep_locator: (required)
        :param str fields:
        :return: ArtifactDependency
        """
        return self.api.get_artifact_dep(
            artifact_dep_locator, bt_locator=self, **kwargs)

    def get_artifact_dep_setting(
            self,
            artifact_dep_locator,
            field_name,
            **kwargs):
        """
        :param async_req: bool
        :param str artifact_dep_locator: (required)
        :param str field_name: (required)
        :return: str
        """
        return self.api.get_artifact_dep_setting(
            artifact_dep_locator, field_name, bt_locator=self, **kwargs)

    def get_artifact_deps(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: ArtifactDependencies
        """
        return self.api.get_artifact_deps(bt_locator=self, **kwargs)

    def get_branches(self, **kwargs):
        """
        :param async_req: bool
        :param str locator:
        :param str fields:
        :return: Branches
        """
        return self.api.get_branches(bt_locator=self, **kwargs)

    def get_build_field(self, build_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.api.get_build_field(
            build_locator, field, bt_locator=self, **kwargs)

    def get_build_type_builds_tags(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: Tags
        """
        return self.api.get_build_type_builds_tags(
            field, bt_locator=self, **kwargs)

    def get_build_type_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: str
        """
        return self.api.get_build_type_field(field, bt_locator=self, **kwargs)

    def get_build_type_template(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: BuildType
        """
        return self.api.get_build_type_template(bt_locator=self, **kwargs)

    def get_build_type_xml(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: BuildType
        """
        return self.api.get_build_type_xml(bt_locator=self, **kwargs)

    def get_build_with_project(self, build_locator, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str fields:
        :return: Build
        """
        return self.api.get_build_with_project(
            build_locator, bt_locator=self, **kwargs)

    def get_builds(self, **kwargs):
        """
        :param async_req: bool
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
        return self.api.get_builds(bt_locator=self, **kwargs)

    def get_children(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :param str base_path:
        :param str locator:
        :param str fields:
        :param bool resolve_parameters:
        :return: Files
        """
        return self.api.get_children(path, bt_locator=self, **kwargs)

    def get_children_alias(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :param str base_path:
        :param str locator:
        :param str fields:
        :param bool resolve_parameters:
        :return: Files
        """
        return self.api.get_children_alias(path, bt_locator=self, **kwargs)

    def get_content(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :param bool resolve_parameters:
        :return: None
        """
        return self.api.get_content(path, bt_locator=self, **kwargs)

    def get_content_alias(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :param bool resolve_parameters:
        :return: None
        """
        return self.api.get_content_alias(path, bt_locator=self, **kwargs)

    def get_current_vcs_instances(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: VcsRootInstances
        """
        return self.api.get_current_vcs_instances(bt_locator=self, **kwargs)

    def get_example_new_project_description(self, **kwargs):
        """
        :param async_req: bool
        :return: NewBuildTypeDescription
        """
        return self.api.get_example_new_project_description(
            bt_locator=self, **kwargs)

    def get_example_new_project_description_compatibility_version1(
            self, **kwargs):
        """
        :param async_req: bool
        :return: NewBuildTypeDescription
        """
        return self.api.get_example_new_project_description_compatibility_version1(
            bt_locator=self, **kwargs)

    def get_feature(self, feature_id, **kwargs):
        """
        :param async_req: bool
        :param str feature_id: (required)
        :param str fields:
        :return: Feature
        """
        return self.api.get_feature(feature_id, bt_locator=self, **kwargs)

    def get_feature_parameter(self, feature_id, parameter_name, **kwargs):
        """
        :param async_req: bool
        :param str feature_id: (required)
        :param str parameter_name: (required)
        :return: str
        """
        return self.api.get_feature_parameter(
            feature_id, parameter_name, bt_locator=self, **kwargs)

    def get_feature_parameters(self, feature_id, **kwargs):
        """
        :param async_req: bool
        :param str feature_id: (required)
        :param str fields:
        :return: Properties
        """
        return self.api.get_feature_parameters(
            feature_id, bt_locator=self, **kwargs)

    def get_feature_setting(self, feature_id, name, **kwargs):
        """
        :param async_req: bool
        :param str feature_id: (required)
        :param str name: (required)
        :return: str
        """
        return self.api.get_feature_setting(
            feature_id, name, bt_locator=self, **kwargs)

    def get_features(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Features
        """
        return self.api.get_features(bt_locator=self, **kwargs)

    def get_investigations(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Investigations
        """
        return self.api.get_investigations(bt_locator=self, **kwargs)

    def get_metadata(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :param str fields:
        :param bool resolve_parameters:
        :return: file
        """
        return self.api.get_metadata(path, bt_locator=self, **kwargs)

    def get_parameter(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param str fields:
        :return: ModelProperty
        """
        return self.api.get_parameter(name, bt_locator=self, **kwargs)

    def get_parameter_0(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param str fields:
        :return: ModelProperty
        """
        return self.api.get_parameter_0(name, bt_locator=self, **kwargs)

    def get_parameter_type(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: Type
        """
        return self.api.get_parameter_type(name, bt_locator=self, **kwargs)

    def get_parameter_type_raw_value(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: str
        """
        return self.api.get_parameter_type_raw_value(
            name, bt_locator=self, **kwargs)

    def get_parameter_value_long(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: str
        """
        return self.api.get_parameter_value_long(
            name, bt_locator=self, **kwargs)

    def get_parameter_value_long_0(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: str
        """
        return self.api.get_parameter_value_long_0(
            name, bt_locator=self, **kwargs)

    def get_parameters(self, **kwargs):
        """
        :param async_req: bool
        :param str locator:
        :param str fields:
        :return: Properties
        """
        return self.api.get_parameters(bt_locator=self, **kwargs)

    def get_parameters_0(self, **kwargs):
        """
        :param async_req: bool
        :param str locator:
        :param str fields:
        :return: Properties
        """
        return self.api.get_parameters_0(bt_locator=self, **kwargs)

    def get_requirement_setting(
            self,
            agent_requirement_locator,
            field_name,
            **kwargs):
        """
        :param async_req: bool
        :param str agent_requirement_locator: (required)
        :param str field_name: (required)
        :return: str
        """
        return self.api.get_requirement_setting(
            agent_requirement_locator, field_name, bt_locator=self, **kwargs)

    def get_root(self, **kwargs):
        """
        :param async_req: bool
        :param str base_path:
        :param str locator:
        :param str fields:
        :param bool resolve_parameters:
        :return: Files
        """
        return self.api.get_root(bt_locator=self, **kwargs)

    def get_settings_file(self, **kwargs):
        """
        :param async_req: bool
        :return: str
        """
        return self.api.get_settings_file(bt_locator=self, **kwargs)

    def get_snapshot_dep(self, snapshot_dep_locator, **kwargs):
        """
        :param async_req: bool
        :param str snapshot_dep_locator: (required)
        :param str fields:
        :return: SnapshotDependency
        """
        return self.api.get_snapshot_dep(
            snapshot_dep_locator=snapshot_dep_locator, bt_locator=self, **kwargs)

    def get_snapshot_deps(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: SnapshotDependencies
        """
        return self.api.get_snapshot_deps(bt_locator=self, **kwargs)

    def get_step(self, step_id, **kwargs):
        """
        :param async_req: bool
        :param str step_id: (required)
        :param str fields:
        :return: Step
        """
        return self.api.get_step(step_id, bt_locator=self, **kwargs)

    def get_step_parameter(self, step_id, parameter_name, **kwargs):
        """
        :param async_req: bool
        :param str step_id: (required)
        :param str parameter_name: (required)
        :return: str
        """
        return self.api.get_step_parameter(
            step_id, parameter_name, bt_locator=self, **kwargs)

    def get_step_parameters(self, step_id, **kwargs):
        """
        :param async_req: bool
        :param str step_id: (required)
        :param str fields:
        :return: Properties
        """
        return self.api.get_step_parameters(step_id, bt_locator=self, **kwargs)

    def get_step_setting(self, step_id, field_name, **kwargs):
        """
        :param async_req: bool
        :param str step_id: (required)
        :param str field_name: (required)
        :return: str
        """
        return self.api.get_step_setting(
            step_id, field_name, bt_locator=self, **kwargs)

    def get_steps(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Steps
        """
        return self.api.get_steps(bt_locator=self, **kwargs)

    def get_templates(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: BuildTypes
        """
        return self.api.get_templates(bt_locator=self, **kwargs)

    def add_template(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: BuildType
        """
        return self.api.add_template(bt_locator=self, **kwargs)

    def get_trigger(self, trigger_locator, **kwargs):
        """
        :param async_req: bool
        :param str trigger_locator: (required)
        :param str fields:
        :return: Trigger
        """
        return self.api.get_trigger(trigger_locator, bt_locator=self, **kwargs)

    def get_trigger_setting(self, trigger_locator, field_name, **kwargs):
        """
        :param async_req: bool
        :param str trigger_locator: (required)
        :param str field_name: (required)
        :return: str
        """
        return self.api.get_trigger_setting(
            trigger_locator, field_name, bt_locator=self, **kwargs)

    def get_triggers(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Triggers
        """
        return self.api.get_triggers(bt_locator=self, **kwargs)

    def get_vcs_labeling_options(self, **kwargs):
        """
        :param async_req: bool
        :return: VcsLabeling
        """
        return self.api.get_vcs_labeling_options(bt_locator=self, **kwargs)

    def get_vcs_root_entries(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: VcsRootEntries
        """
        return self.api.get_vcs_root_entries(bt_locator=self, **kwargs)

    def get_vcs_root_entry(self, vcs_root_locator, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_locator: (required)
        :param str fields:
        :return: VcsRootEntry
        """
        return self.api.get_vcs_root_entry(
            vcs_root_locator, bt_locator=self, **kwargs)

    def get_vcs_root_entry_checkout_rules(self, vcs_root_locator, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_locator: (required)
        :return: str
        """
        return self.api.get_vcs_root_entry_checkout_rules(
            vcs_root_locator, bt_locator=self, **kwargs)

    def get_zipped(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :param str base_path:
        :param str locator:
        :param str name:
        :param bool resolve_parameters:
        :return: None
        """
        return self.api.get_zipped(path, bt_locator=self, **kwargs)

    def remove_all_templates(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: BuildType
        """
        return self.api.remove_all_templates(bt_locator=self, **kwargs)

    def remove_template(self, template_locator, **kwargs):
        """
        :param async_req: bool
        :param template_locator: str
        :param str fields:
        :return: BuildType
        """
        return self.api.remove_template(bt_locator=self, template_locator=template_locator,
                                        **kwargs)

    def replace_agent_requirement(self, agent_requirement_locator, **kwargs):
        """
        :param async_req: bool
        :param str agent_requirement_locator: (required)
        :param str fields:
        :param AgentRequirement body:
        :return: AgentRequirement
        """
        return self.api.replace_agent_requirement(
            agent_requirement_locator, bt_locator=self, **kwargs)

    def replace_agent_requirements(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :param AgentRequirements body:
        :return: AgentRequirements
        """
        return self.api.replace_agent_requirements(bt_locator=self, **kwargs)

    def replace_artifact_dep(self, artifact_dep_locator, **kwargs):
        """
        :param async_req: bool
        :param str artifact_dep_locator: (required)
        :param str fields:
        :param ArtifactDependency body:
        :return: ArtifactDependency
        """
        return self.api.replace_artifact_dep(
            artifact_dep_locator, bt_locator=self, **kwargs)

    def replace_artifact_deps(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :param ArtifactDependencies body:
        :return: ArtifactDependencies
        """
        return self.api.replace_artifact_deps(bt_locator=self, **kwargs)

    def replace_feature(self, feature_id, **kwargs):
        """
        :param async_req: bool
        :param str feature_id: (required)
        :param str fields:
        :param Feature body:
        :return: Feature
        """
        return self.api.replace_feature(feature_id, bt_locator=self, **kwargs)

    def replace_feature_parameters(self, feature_id, **kwargs):
        """
        :param async_req: bool
        :param str feature_id: (required)
        :param Properties body:
        :param str fields:
        :return: Properties
        """
        return self.api.replace_feature_parameters(
            feature_id, bt_locator=self, **kwargs)

    def replace_features(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :param Features body:
        :return: Features
        """
        return self.api.replace_features(bt_locator=self, **kwargs)

    def replace_snapshot_dep(self, snapshot_dep_locator, **kwargs):
        """
        :param async_req: bool
        :param str snapshot_dep_locator: (required)
        :param str fields:
        :param SnapshotDependency body:
        :return: SnapshotDependency
        """
        return self.api.replace_snapshot_dep(
            snapshot_dep_locator, bt_locator=self, **kwargs)

    def replace_snapshot_deps(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :param SnapshotDependencies body:
        :return: SnapshotDependencies
        """
        return self.api.replace_snapshot_deps(bt_locator=self, **kwargs)

    def replace_step(self, step_id, **kwargs):
        """
        :param async_req: bool
        :param str step_id: (required)
        :param str fields:
        :param Step body:
        :return: Step
        """
        return self.api.replace_step(step_id, bt_locator=self, **kwargs)

    def replace_step_parameters(self, step_id, **kwargs):
        """
        :param async_req: bool
        :param str step_id: (required)
        :param Properties body:
        :param str fields:
        :return: Properties
        """
        return self.api.replace_step_parameters(
            step_id, bt_locator=self, **kwargs)

    def replace_steps(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :param Steps body:
        :return: Steps
        """
        return self.api.replace_steps(bt_locator=self, **kwargs)

    def replace_trigger(self, trigger_locator, **kwargs):
        """
        :param async_req: bool
        :param str trigger_locator: (required)
        :param str fields:
        :param Trigger body:
        :return: Trigger
        """
        return self.api.replace_trigger(
            trigger_locator, bt_locator=self, **kwargs)

    def replace_triggers(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :param Triggers body:
        :return: Triggers
        """
        return self.api.replace_triggers(bt_locator=self, **kwargs)

    def replace_vcs_root_entries(self, **kwargs):
        """
        :param async_req: bool
        :param VcsRootEntries body:
        :param str fields:
        :return: VcsRootEntries
        """
        return self.api.replace_vcs_root_entries(bt_locator=self, **kwargs)

    def serve_branches(self, **kwargs):
        """
        :param async_req: bool
        :param str locator:
        :param str fields:
        :return: Branches
        """
        return self.api.serve_branches(bt_locator=self, **kwargs)

    def serve_build_field(self, build_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.api.serve_build_field(
            build_locator, field, bt_locator=self, **kwargs)

    def serve_build_type_builds_tags(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: Tags
        """
        return self.api.serve_build_type_builds_tags(
            field, bt_locator=self, **kwargs)

    def serve_build_type_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: str
        """
        return self.api.serve_build_type_field(field, bt_locator=self, **kwargs)

    def serve_build_type_template(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: BuildType
        """
        return self.api.serve_build_type_template(bt_locator=self, **kwargs)

    def serve_build_type_xml(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: BuildType
        """
        return self.api.serve_build_type_xml(bt_locator=self, **kwargs)

    def serve_build_with_project(self, build_locator, **kwargs):
        """
        :param async_req: bool
        :param str build_locator: (required)
        :param str fields:
        :return: Build
        """
        return self.api.serve_build_with_project(
            build_locator, bt_locator=self, **kwargs)

    def serve_builds(self, **kwargs):
        """
        :param async_req: bool
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
        return self.api.serve_builds(bt_locator=self, **kwargs)

    def set_build_type_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :param str body:
        :return: str
        """
        return self.api.set_build_type_field(field, bt_locator=self, **kwargs)

    def set_parameter(self, **kwargs):
        """
        :param async_req: bool
        :param ModelProperty body:
        :param str fields:
        :return: ModelProperty
        """
        return self.api.set_parameter(bt_locator=self, **kwargs)

    def set_parameter_0(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param ModelProperty body:
        :param str fields:
        :return: ModelProperty
        """
        return self.api.set_parameter_0(name, bt_locator=self, **kwargs)

    def set_parameter_1(self, **kwargs):
        """
        :param async_req: bool
        :param ModelProperty body:
        :param str fields:
        :return: ModelProperty
        """
        return self.api.set_parameter_1(bt_locator=self, **kwargs)

    def set_parameter_2(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param ModelProperty body:
        :param str fields:
        :return: ModelProperty
        """
        return self.api.set_parameter_2(name, bt_locator=self, **kwargs)

    def set_parameter_type(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param Type body:
        :return: Type
        """
        return self.api.set_parameter_type(name, bt_locator=self, **kwargs)

    def set_parameter_type_raw_value(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param str body:
        :return: str
        """
        return self.api.set_parameter_type_raw_value(
            name, bt_locator=self, **kwargs)

    def set_parameter_value_long(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param str body:
        :return: str
        """
        return self.api.set_parameter_value_long(
            name, bt_locator=self, **kwargs)

    def set_parameter_value_long_0(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param str body:
        :return: str
        """
        return self.api.set_parameter_value_long_0(
            name, bt_locator=self, **kwargs)

    def set_parameters(self, **kwargs):
        """
        :param async_req: bool
        :param Properties body:
        :param str fields:
        :return: Properties
        """
        return self.api.set_parameters(bt_locator=self, **kwargs)

    def set_parameters_0(self, **kwargs):
        """
        :param async_req: bool
        :param Properties body:
        :param str fields:
        :return: Properties
        """
        return self.api.set_parameters_0(bt_locator=self, **kwargs)

    def set_templates(self, **kwargs):
        """
        :param async_req: bool
        :param BuildTypes body:
        :param bool optimize_settings:
        :param str fields:
        :return: BuildTypes
        """
        return self.api.set_templates(bt_locator=self, **kwargs)

    def set_vcs_labeling_options(self, **kwargs):
        """
        :param async_req: bool
        :param VcsLabeling body:
        :return: VcsLabeling
        """
        return self.api.set_vcs_labeling_options(bt_locator=self, **kwargs)

    def update_vcs_root_entry(self, vcs_root_locator, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_locator: (required)
        :param VcsRootEntry body:
        :param str fields:
        :return: VcsRootEntry
        """
        return self.api.update_vcs_root_entry(
            vcs_root_locator, bt_locator=self, **kwargs)

    def update_vcs_root_entry_checkout_rules(self, vcs_root_locator, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_locator: (required)
        :param str body:
        :return: str
        """
        return self.api.update_vcs_root_entry_checkout_rules(
            vcs_root_locator, bt_locator=self, **kwargs)


class Group(Group, ReadMixin, DeleteMixin):
    @property
    def api(self):
        return self.teamcity.groups

    def _read(self):
        return self.api.get

    def _delete(self):
        return self.api.delete_group

    def add_role(self, **kwargs):
        """
        :param async_req: bool
        :param Role body:
        :return: Role
        """
        return self.api.add_role(group_locator=self, **kwargs)

    def add_role_put(self, **kwargs):
        """
        :param async_req: bool
        :param Roles body:
        :return: Roles
        """
        return self.api.add_role_put(group_locator=self, **kwargs)

    def add_role_simple(self, role_id, scope, **kwargs):
        """
        :param async_req: bool
        :param str role_id: (required)
        :param str scope: (required)
        :return: Role
        """
        return self.api.add_role_simple(
            role_id, scope, group_locator=self, **kwargs)

    def delete_group(self, **kwargs):
        """
        :param async_req: bool
        :return: None
        """
        return self.api.delete_group(group_locator=self, **kwargs)

    def delete_role(self, role_id, scope, **kwargs):
        """
        :param async_req: bool
        :param str role_id: (required)
        :param str scope: (required)
        :return: None
        """
        return self.api.delete_role(
            role_id, scope, group_locator=self, **kwargs)

    def get(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Group
        """
        return self.api.get(group_locator=self, **kwargs)

    def get_group(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Group
        """
        return self.api.get_group(group_locator=self, **kwargs)

    def get_permissions(self, **kwargs):
        """
        :param async_req: bool
        :return: str
        """
        return self.api.get_permissions(group_locator=self, **kwargs)

    def get_properties(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Properties
        """
        return self.api.get_properties(group_locator=self, **kwargs)

    def get_user_properties(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: str
        """
        return self.api.get_user_properties(name, group_locator=self, **kwargs)

    def list_role(self, role_id, scope, **kwargs):
        """
        :param async_req: bool
        :param str role_id: (required)
        :param str scope: (required)
        :return: Role
        """
        return self.api.list_role(role_id, scope, group_locator=self, **kwargs)

    def list_roles(self, **kwargs):
        """
        :param async_req: bool
        :return: Roles
        """
        return self.api.list_roles(group_locator=self, **kwargs)

    def put_user_property(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param str body:
        :return: str
        """
        return self.api.put_user_property(name, group_locator=self, **kwargs)

    def remove_user_property(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: None
        """
        return self.api.remove_user_property(name, group_locator=self, **kwargs)

    def serve_group(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Group
        """
        return self.api.serve_group(group_locator=self, **kwargs)

    def serve_user_properties(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: str
        """
        return self.api.serve_user_properties(
            name, group_locator=self, **kwargs)


class User(User, ReadMixin, DeleteMixin):
    @property
    def api(self):
        return self.teamcity.users

    def _read(self):
        return self.api.get

    def _delete(self):
        return self.api.delete_user

    def add_group(self, **kwargs):
        """
        :param async_req: bool
        :param Group body:
        :param str fields:
        :return: Group
        """
        return self.api.add_group(user_locator=self, **kwargs)

    def add_role(self, **kwargs):
        """
        :param async_req: bool
        :param Role body:
        :return: Role
        """
        return self.api.add_role(user_locator=self, **kwargs)

    def add_role_simple(self, role_id, scope, **kwargs):
        """
        :param async_req: bool
        :param str role_id: (required)
        :param str scope: (required)
        :return: Role
        """
        return self.api.add_role_simple(
            role_id, scope, user_locator=self, **kwargs)

    def add_role_simple_post(self, role_id, scope, **kwargs):
        """
        :param async_req: bool
        :param str role_id: (required)
        :param str scope: (required)
        :return: None
        """
        return self.api.add_role_simple_post(
            role_id, scope, user_locator=self, **kwargs)

    def delete_remember_me(self, **kwargs):
        """
        :param async_req: bool
        :return: None
        """
        return self.api.delete_remember_me(user_locator=self, **kwargs)

    def delete_role(self, role_id, scope, **kwargs):
        """
        :param async_req: bool
        :param str role_id: (required)
        :param str scope: (required)
        :return: None
        """
        return self.api.delete_role(role_id, scope, user_locator=self, **kwargs)

    def delete_user(self, **kwargs):
        """
        :param async_req: bool
        :return: None
        """
        return self.api.delete_user(user_locator=self, **kwargs)

    def delete_user_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: None
        """
        return self.api.delete_user_field(field, user_locator=self, **kwargs)

    def get(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: User
        """
        return self.api.get(user_locator=self, **kwargs)

    def get_groups(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Groups
        """
        return self.api.get_groups(user_locator=self, **kwargs)

    def get_permissions(self, **kwargs):
        """
        :param async_req: bool
        :return: str
        """
        return self.api.get_permissions(user_locator=self, **kwargs)

    def get_user(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: User
        """
        return self.api.get_user(user_locator=self, **kwargs)

    def get_user_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: str
        """
        return self.api.get_user_field(field, user_locator=self, **kwargs)

    def get_user_properties(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Properties
        """
        return self.api.get_user_properties(user_locator=self, **kwargs)

    def get_user_property(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: str
        """
        return self.api.get_user_property(name, user_locator=self, **kwargs)

    def list_role(self, role_id, scope, **kwargs):
        """
        :param async_req: bool
        :param str role_id: (required)
        :param str scope: (required)
        :return: Role
        """
        return self.api.list_role(role_id, scope, user_locator=self, **kwargs)

    def list_roles(self, **kwargs):
        """
        :param async_req: bool
        :return: Roles
        """
        return self.api.list_roles(user_locator=self, **kwargs)

    def put_user_property(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param str body:
        :return: str
        """
        return self.api.put_user_property(name, user_locator=self, **kwargs)

    def remove_user_property(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: None
        """
        return self.api.remove_user_property(name, user_locator=self, **kwargs)

    def replace_groups(self, **kwargs):
        """
        :param async_req: bool
        :param Groups body:
        :param str fields:
        :return: Groups
        """
        return self.api.replace_groups(user_locator=self, **kwargs)

    def replace_roles(self, **kwargs):
        """
        :param async_req: bool
        :param Roles body:
        :return: Roles
        """
        return self.api.replace_roles(user_locator=self, **kwargs)

    def serve_user(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: User
        """
        return self.api.serve_user(user_locator=self, **kwargs)

    def serve_user_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: str
        """
        return self.api.serve_user_field(field, user_locator=self, **kwargs)

    def serve_user_properties(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Properties
        """
        return self.api.serve_user_properties(user_locator=self, **kwargs)

    def serve_user_property(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: str
        """
        return self.api.serve_user_property(name, user_locator=self, **kwargs)

    def set_user_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :param str body:
        :return: str
        """
        return self.api.set_user_field(field, user_locator=self, **kwargs)

    def update_user(self, **kwargs):
        """
        :param async_req: bool
        :param User body:
        :param str fields:
        :return: User
        """
        return self.api.update_user(user_locator=self, **kwargs)


class Project(Project, ReadMixin, DeleteMixin):
    @property
    def api(self):
        return self.teamcity.projects

    def _read(self):
        return self.api.get

    def _delete(self):
        return self.api.delete_project

    def add(self, **kwargs):
        """
        :param async_req: bool
        :param ProjectFeature body:
        :param str fields:
        :return: object
        """
        return self.api.add(project_locator=self, **kwargs)

    def create_build_type(self, **kwargs):
        """
        :param async_req: bool
        :param NewBuildTypeDescription body:
        :param str fields:
        :return: BuildType
        """
        return self.api.create_build_type(project_locator=self, **kwargs)

    def create_build_type_template(self, **kwargs):
        """
        :param async_req: bool
        :param NewBuildTypeDescription body:
        :param str fields:
        :return: BuildType
        """
        return self.api.create_build_type_template(
            project_locator=self, **kwargs)

    def delete(self, feature_locator, **kwargs):
        """
        :param async_req: bool
        :param str feature_locator: (required)
        :return: None
        """
        return self.api.delete(feature_locator, project_locator=self, **kwargs)

    def delete_all_parameters(self, **kwargs):
        """
        :param async_req: bool
        :return: None
        """
        return self.api.delete_all_parameters(project_locator=self, **kwargs)

    def delete_all_parameters_0(self, feature_locator, **kwargs):
        """
        :param async_req: bool
        :param str feature_locator: (required)
        :param str fields:
        :return: None
        """
        return self.api.delete_all_parameters_0(
            feature_locator, project_locator=self, **kwargs)

    def delete_parameter(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: None
        """
        return self.api.delete_parameter(name, project_locator=self, **kwargs)

    def delete_parameter_0(self, name, feature_locator, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param str feature_locator: (required)
        :param str fields:
        :return: None
        """
        return self.api.delete_parameter_0(
            name, feature_locator, project_locator=self, **kwargs)

    def delete_project(self, **kwargs):
        """
        :param async_req: bool
        :return: None
        """
        return self.api.delete_project(project_locator=self, **kwargs)

    def delete_project_agent_pools(self, agent_pool_locator, **kwargs):
        """
        :param async_req: bool
        :param str agent_pool_locator: (required)
        :return: None
        """
        return self.api.delete_project_agent_pools(
            agent_pool_locator, project_locator=self, **kwargs)

    def get(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Project
        """
        return self.api.get(project_locator=self, **kwargs)

    def get_build_field_with_project(
            self,
            bt_locator,
            build_locator,
            field,
            **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str build_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.api.get_build_field_with_project(
            bt_locator, build_locator, field, project_locator=self, **kwargs)

    def get_build_type(self, bt_locator, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str fields:
        :return: BuildType
        """
        return self.api.get_build_type(
            bt_locator, project_locator=self, **kwargs)

    def get_build_type_field_with_project(self, bt_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.api.get_build_type_field_with_project(
            bt_locator, field, project_locator=self, **kwargs)

    def get_build_type_templates(self, bt_locator, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str fields:
        :return: BuildType
        """
        return self.api.get_build_type_templates(
            bt_locator, project_locator=self, **kwargs)

    def get_build_types_in_project(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: BuildTypes
        """
        return self.api.get_build_types_in_project(
            project_locator=self, **kwargs)

    def get_build_types_order(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: BuildTypes
        """
        return self.api.get_build_types_order(
            field, project_locator=self, **kwargs)

    def get_build_with_project(self, bt_locator, build_locator, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str build_locator: (required)
        :param str fields:
        :return: Build
        """
        return self.api.get_build_with_project(
            bt_locator, build_locator, project_locator=self, **kwargs)

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
        return self.api.get_builds(bt_locator, project_locator=self, **kwargs)

    def get_example_new_project_description(self, **kwargs):
        """
        :param async_req: bool
        :param str id:
        :return: NewProjectDescription
        """
        return self.api.get_example_new_project_description(
            project_locator=self, **kwargs)

    def get_example_new_project_description_compatibility_version1(
            self, **kwargs):
        """
        :param async_req: bool
        :param str id:
        :return: NewProjectDescription
        """
        return self.api.get_example_new_project_description_compatibility_version1(
            project_locator=self, **kwargs)

    def get_parameter(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param str fields:
        :return: ModelProperty
        """
        return self.api.get_parameter(name, project_locator=self, **kwargs)

    def get_parameter_0(self, name, feature_locator, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param str feature_locator: (required)
        :param str fields:
        :param str fields2:
        :return: ModelProperty
        """
        return self.api.get_parameter_0(
            name, feature_locator, project_locator=self, **kwargs)

    def get_parameter_type(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: Type
        """
        return self.api.get_parameter_type(name, project_locator=self, **kwargs)

    def get_parameter_type_raw_value(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: str
        """
        return self.api.get_parameter_type_raw_value(
            name, project_locator=self, **kwargs)

    def get_parameter_value_long(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: str
        """
        return self.api.get_parameter_value_long(
            name, project_locator=self, **kwargs)

    def get_parameter_value_long_0(self, name, feature_locator, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param str feature_locator: (required)
        :param str fields:
        :return: str
        """
        return self.api.get_parameter_value_long_0(
            name, feature_locator, project_locator=self, **kwargs)

    def get_parameters(self, **kwargs):
        """
        :param async_req: bool
        :param str locator:
        :param str fields:
        :return: Properties
        """
        return self.api.get_parameters(project_locator=self, **kwargs)

    def get_parameters_0(self, feature_locator, **kwargs):
        """
        :param async_req: bool
        :param str feature_locator: (required)
        :param str locator:
        :param str fields:
        :param str fields2:
        :return: Properties
        """
        return self.api.get_parameters_0(
            feature_locator, project_locator=self, **kwargs)

    def get_parent_project(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Project
        """
        return self.api.get_parent_project(project_locator=self, **kwargs)

    def get_project(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Project
        """
        return self.api.get_project(project_locator=self, **kwargs)

    def get_project_agent_pools(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: AgentPools
        """
        return self.api.get_project_agent_pools(project_locator=self, **kwargs)

    def get_project_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: str
        """
        return self.api.get_project_field(field, project_locator=self, **kwargs)

    def get_projects_order(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: Projects
        """
        return self.api.get_projects_order(
            field, project_locator=self, **kwargs)

    def get_settings_file(self, **kwargs):
        """
        :param async_req: bool
        :return: str
        """
        return self.api.get_settings_file(project_locator=self, **kwargs)

    def get_single(self, feature_locator, **kwargs):
        """
        :param async_req: bool
        :param str feature_locator: (required)
        :param str fields:
        :return: object
        """
        return self.api.get_single(
            feature_locator, project_locator=self, **kwargs)

    def get_templates_in_project(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: BuildTypes
        """
        return self.api.get_templates_in_project(project_locator=self, **kwargs)

    def reload_settings_file(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Project
        """
        return self.api.reload_settings_file(project_locator=self, **kwargs)

    def replace(self, feature_locator, **kwargs):
        """
        :param async_req: bool
        :param str feature_locator: (required)
        :param ProjectFeature body:
        :param str fields:
        :return: object
        """
        return self.api.replace(feature_locator, project_locator=self, **kwargs)

    def replace_all(self, **kwargs):
        """
        :param async_req: bool
        :param ProjectFeatures body:
        :param str fields:
        :return: object
        """
        return self.api.replace_all(project_locator=self, **kwargs)

    def serve_build_field_with_project(
            self,
            bt_locator,
            build_locator,
            field,
            **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str build_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.api.serve_build_field_with_project(
            bt_locator, build_locator, field, project_locator=self, **kwargs)

    def serve_build_type(self, bt_locator, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str fields:
        :return: BuildType
        """
        return self.api.serve_build_type(
            bt_locator, project_locator=self, **kwargs)

    def serve_build_type_field_with_project(self, bt_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.api.serve_build_type_field_with_project(
            bt_locator, field, project_locator=self, **kwargs)

    def serve_build_type_templates(self, bt_locator, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str fields:
        :return: BuildType
        """
        return self.api.serve_build_type_templates(
            bt_locator, project_locator=self, **kwargs)

    def serve_build_types_in_project(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: BuildTypes
        """
        return self.api.serve_build_types_in_project(
            project_locator=self, **kwargs)

    def serve_build_with_project(self, bt_locator, build_locator, **kwargs):
        """
        :param async_req: bool
        :param str bt_locator: (required)
        :param str build_locator: (required)
        :param str fields:
        :return: Build
        """
        return self.api.serve_build_with_project(
            bt_locator, build_locator, project_locator=self, **kwargs)

    def serve_builds(self, bt_locator, **kwargs):
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
        return self.api.serve_builds(bt_locator, project_locator=self, **kwargs)

    def serve_project(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Project
        """
        return self.api.serve_project(project_locator=self, **kwargs)

    def serve_project_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: str
        """
        return self.api.serve_project_field(
            field, project_locator=self, **kwargs)

    def serve_templates_in_project(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: BuildTypes
        """
        return self.api.serve_templates_in_project(
            project_locator=self, **kwargs)

    def set_build_types_order(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :param BuildTypes body:
        :return: BuildTypes
        """
        return self.api.set_build_types_order(
            field, project_locator=self, **kwargs)

    def set_parameter(self, **kwargs):
        """
        :param async_req: bool
        :param ModelProperty body:
        :param str fields:
        :return: ModelProperty
        """
        return self.api.set_parameter(project_locator=self, **kwargs)

    def set_parameter_0(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param ModelProperty body:
        :param str fields:
        :return: ModelProperty
        """
        return self.api.set_parameter_0(name, project_locator=self, **kwargs)

    def set_parameter_1(self, feature_locator, **kwargs):
        """
        :param async_req: bool
        :param str feature_locator: (required)
        :param ModelProperty body:
        :param str fields:
        :param str fields2:
        :return: ModelProperty
        """
        return self.api.set_parameter_1(
            feature_locator, project_locator=self, **kwargs)

    def set_parameter_2(self, name, feature_locator, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param str feature_locator: (required)
        :param ModelProperty body:
        :param str fields:
        :param str fields2:
        :return: ModelProperty
        """
        return self.api.set_parameter_2(
            name, feature_locator, project_locator=self, **kwargs)

    def set_parameter_type(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param Type body:
        :return: Type
        """
        return self.api.set_parameter_type(name, project_locator=self, **kwargs)

    def set_parameter_type_raw_value(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param str body:
        :return: str
        """
        return self.api.set_parameter_type_raw_value(
            name, project_locator=self, **kwargs)

    def set_parameter_value_long(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param str body:
        :return: str
        """
        return self.api.set_parameter_value_long(
            name, project_locator=self, **kwargs)

    def set_parameter_value_long_0(self, name, feature_locator, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param str feature_locator: (required)
        :param str body:
        :param str fields:
        :return: str
        """
        return self.api.set_parameter_value_long_0(
            name, feature_locator, project_locator=self, **kwargs)

    def set_parameters(self, **kwargs):
        """
        :param async_req: bool
        :param Properties body:
        :param str fields:
        :return: Properties
        """
        return self.api.set_parameters(project_locator=self, **kwargs)

    def set_parameters_0(self, feature_locator, **kwargs):
        """
        :param async_req: bool
        :param str feature_locator: (required)
        :param Properties body:
        :param str fields:
        :param str fields2:
        :return: Properties
        """
        return self.api.set_parameters_0(
            feature_locator, project_locator=self, **kwargs)

    def set_parent_project(self, **kwargs):
        """
        :param async_req: bool
        :param Project body:
        :return: Project
        """
        return self.api.set_parent_project(project_locator=self, **kwargs)

    def set_project_agent_pools(self, **kwargs):
        """
        :param async_req: bool
        :param AgentPools body:
        :param str fields:
        :return: AgentPools
        """
        return self.api.set_project_agent_pools(project_locator=self, **kwargs)

    def set_project_agent_pools_0(self, **kwargs):
        """
        :param async_req: bool
        :param AgentPool body:
        :return: AgentPool
        """
        return self.api.set_project_agent_pools_0(
            project_locator=self, **kwargs)

    def set_project_filed(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :param str body:
        :return: str
        """
        return self.api.set_project_filed(field, project_locator=self, **kwargs)

    def set_projects_order(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :param Projects body:
        :return: Projects
        """
        return self.api.set_projects_order(
            field, project_locator=self, **kwargs)


class VcsRoot(VcsRoot, ReadMixin, DeleteMixin):
    @property
    def api(self):
        return self.teamcity.vcs_root

    def _read(self):
        return self.api.get

    def _delete(self):
        return self.api.delete_root

    def change_properties(self, **kwargs):
        """
        :param async_req: bool
        :param Properties body:
        :param str fields:
        :return: Properties
        """
        return self.api.change_properties(vcs_root_locator=self, **kwargs)

    def delete_all_properties(self, **kwargs):
        """
        :param async_req: bool
        :return: None
        """
        return self.api.delete_all_properties(vcs_root_locator=self, **kwargs)

    def delete_parameter(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: None
        """
        return self.api.delete_parameter(name, vcs_root_locator=self, **kwargs)

    def delete_root(self, **kwargs):
        """
        :param async_req: bool
        :return: None
        """
        return self.api.delete_root(vcs_root_locator=self, **kwargs)

    def get(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: VcsRoot
        """
        return self.api.get(vcs_root_locator=self, **kwargs)

    def get_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: str
        """
        return self.api.get_field(field, vcs_root_locator=self, **kwargs)

    def get_instance_field(self, vcs_root_instance_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_instance_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.api.get_instance_field(
            vcs_root_instance_locator,
            field,
            vcs_root_locator=self,
            **kwargs)

    def get_properties(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Properties
        """
        return self.api.get_properties(vcs_root_locator=self, **kwargs)

    def get_property(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: str
        """
        return self.api.get_property(name, vcs_root_locator=self, **kwargs)

    def get_root(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: VcsRoot
        """
        return self.api.get_root(vcs_root_locator=self, **kwargs)

    def get_root_instance(self, vcs_root_instance_locator, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_instance_locator: (required)
        :param str fields:
        :return: VcsRootInstance
        """
        return self.api.get_root_instance(
            vcs_root_instance_locator, vcs_root_locator=self, **kwargs)

    def get_root_instance_properties(self, vcs_root_instance_locator, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_instance_locator: (required)
        :param str fields:
        :return: Properties
        """
        return self.api.get_root_instance_properties(
            vcs_root_instance_locator, vcs_root_locator=self, **kwargs)

    def get_root_instances(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: VcsRootInstances
        """
        return self.api.get_root_instances(vcs_root_locator=self, **kwargs)

    def get_settings_file(self, **kwargs):
        """
        :param async_req: bool
        :return: str
        """
        return self.api.get_settings_file(vcs_root_locator=self, **kwargs)

    def put_parameter(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :param str body:
        :return: str
        """
        return self.api.put_parameter(name, vcs_root_locator=self, **kwargs)

    def serve_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: str
        """
        return self.api.serve_field(field, vcs_root_locator=self, **kwargs)

    def serve_instance_field(self, vcs_root_instance_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_instance_locator: (required)
        :param str field: (required)
        :return: str
        """
        return self.api.serve_instance_field(
            vcs_root_instance_locator, field, vcs_root_locator=self, **kwargs)

    def serve_properties(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Properties
        """
        return self.api.serve_properties(vcs_root_locator=self, **kwargs)

    def serve_property(self, name, **kwargs):
        """
        :param async_req: bool
        :param str name: (required)
        :return: str
        """
        return self.api.serve_property(name, vcs_root_locator=self, **kwargs)

    def serve_root(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: VcsRoot
        """
        return self.api.serve_root(vcs_root_locator=self, **kwargs)

    def serve_root_instance(self, vcs_root_instance_locator, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_instance_locator: (required)
        :param str fields:
        :return: VcsRootInstance
        """
        return self.api.serve_root_instance(
            vcs_root_instance_locator, vcs_root_locator=self, **kwargs)

    def serve_root_instance_properties(
            self, vcs_root_instance_locator, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_instance_locator: (required)
        :param str fields:
        :return: Properties
        """
        return self.api.serve_root_instance_properties(
            vcs_root_instance_locator, vcs_root_locator=self, **kwargs)

    def serve_root_instances(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: VcsRootInstances
        """
        return self.api.serve_root_instances(vcs_root_locator=self, **kwargs)

    def set_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :param str body:
        :return: str
        """
        return self.api.set_field(field, vcs_root_locator=self, **kwargs)

    def set_instance_field(self, vcs_root_instance_locator, field, **kwargs):
        """
        :param async_req: bool
        :param str vcs_root_instance_locator: (required)
        :param str field: (required)
        :param str body:
        :return: str
        """
        return self.api.set_instance_field(
            vcs_root_instance_locator,
            field,
            vcs_root_locator=self,
            **kwargs)


class VcsRootInstance(VcsRootInstance, ReadMixin):
    @property
    def api(self):
        return self.teamcity.vcs_root_instance

    def _read(self):
        return self.api.get

    def delete_instance_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: None
        """
        return self.api.delete_instance_field(
            field, vcs_root_instance_locator=self, **kwargs)

    def delete_repository_state(self, **kwargs):
        """
        :param async_req: bool
        :return: None
        """
        return self.api.delete_repository_state(
            vcs_root_instance_locator=self, **kwargs)

    def get(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: VcsRootInstance
        """
        return self.api.get(vcs_root_instance_locator=self, **kwargs)

    def get_children(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :param str base_path:
        :param str locator:
        :param str fields:
        :return: Files
        """
        return self.api.get_children(
            path, vcs_root_instance_locator=self, **kwargs)

    def get_children_alias(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :param str base_path:
        :param str locator:
        :param str fields:
        :return: Files
        """
        return self.api.get_children_alias(
            path, vcs_root_instance_locator=self, **kwargs)

    def get_content(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :return: None
        """
        return self.api.get_content(
            path, vcs_root_instance_locator=self, **kwargs)

    def get_content_alias(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :return: None
        """
        return self.api.get_content_alias(
            path, vcs_root_instance_locator=self, **kwargs)

    def get_instance(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: VcsRootInstance
        """
        return self.api.get_instance(vcs_root_instance_locator=self, **kwargs)

    def get_instance_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: str
        """
        return self.api.get_instance_field(
            field, vcs_root_instance_locator=self, **kwargs)

    def get_metadata(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :param str fields:
        :return: file
        """
        return self.api.get_metadata(
            path, vcs_root_instance_locator=self, **kwargs)

    def get_repository_state(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Entries
        """
        return self.api.get_repository_state(
            vcs_root_instance_locator=self, **kwargs)

    def get_repository_state_creation_date(self, **kwargs):
        """
        :param async_req: bool
        :return: str
        """
        return self.api.get_repository_state_creation_date(
            vcs_root_instance_locator=self, **kwargs)

    def get_root(self, **kwargs):
        """
        :param async_req: bool
        :param str base_path:
        :param str locator:
        :param str fields:
        :return: Files
        """
        return self.api.get_root(vcs_root_instance_locator=self, **kwargs)

    def get_root_instance_properties(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Properties
        """
        return self.api.get_root_instance_properties(
            vcs_root_instance_locator=self, **kwargs)

    def get_zipped(self, path, **kwargs):
        """
        :param async_req: bool
        :param str path: (required)
        :param str base_path:
        :param str locator:
        :param str name:
        :return: None
        """
        return self.api.get_zipped(
            path, vcs_root_instance_locator=self, **kwargs)

    def serve_instance(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: VcsRootInstance
        """
        return self.api.serve_instance(vcs_root_instance_locator=self, **kwargs)

    def serve_instance_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :return: str
        """
        return self.api.serve_instance_field(
            field, vcs_root_instance_locator=self, **kwargs)

    def serve_root_instance_properties(self, **kwargs):
        """
        :param async_req: bool
        :param str fields:
        :return: Properties
        """
        return self.api.serve_root_instance_properties(
            vcs_root_instance_locator=self, **kwargs)

    def set_instance_field(self, field, **kwargs):
        """
        :param async_req: bool
        :param str field: (required)
        :param str body:
        :return: str
        """
        return self.api.set_instance_field(
            field, vcs_root_instance_locator=self, **kwargs)

    def set_repository_state(self, **kwargs):
        """
        :param async_req: bool
        :param Entries body:
        :param str fields:
        :return: Entries
        """
        return self.api.set_repository_state(
            vcs_root_instance_locator=self, **kwargs)


class Sessions(Sessions, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.session


class Groups(Groups, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.group


class Tests(Tests, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.test


class Roles(Roles, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.role


class Compatibilities(Compatibilities, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.compatibility


class Tags(Tags, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.tag


class Changes(Changes, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.change


class LicenseKeys(LicenseKeys, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.license_key


class VcsRoots(VcsRoots, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.vcs_root


class Problems(Problems, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.problem


class Mutes(Mutes, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.mute


class ArtifactDependencies(ArtifactDependencies, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.artifact_dependency


class Features(Features, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.feature


class AgentPools(AgentPools, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.agent_pool


class Users(Users, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.user


class Projects(Projects, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.project


class ProjectFeatures(ProjectFeatures, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.project_feature


class BuildTypes(BuildTypes, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.build_type


class Issues(Issues, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.issues


class FileChanges(FileChanges, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.file


class ProblemOccurrences(ProblemOccurrences, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.problem_occurrence


class Investigations(Investigations, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.investigation


class Revisions(Revisions, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.revision


class Properties(Properties, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self._property

    def get(self, name, value=None):
        """
        Get Property by name or value
        :param name:
        :return:
        """
        return next((x for x in self.data if x.name == name), None)


class VcsRootEntries(VcsRootEntries, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.vcs_root_entry


class Triggers(Triggers, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.trigger


class VcsRootInstances(VcsRootInstances, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.vcs_root_instance


class Agents(Agents, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.agent


class Steps(Steps, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.step


class AgentRequirements(AgentRequirements, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.agent_requirement


class Builds(Builds, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.build


class Plugins(Plugins, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.plugin


class Branches(Branches, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.branch


class TestOccurrences(TestOccurrences, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.test_occurrence


class Links(Links, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.link


class Entries(Entries, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.entry


class SnapshotDependencies(SnapshotDependencies, ContainerMixin):
    @property
    def _container_mixin_data(self):
        return self.snapshot_dependency
