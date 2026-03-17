#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
import logging
import requests
from gerrit.utils.gerritbase import GerritBase
from gerrit.projects.commit import GerritProjectCommit
from gerrit.projects.branches import GerritProjectBranches
from gerrit.projects.tags import GerritProjectTags
from gerrit.projects.dashboards import GerritProjectDashboards
from gerrit.projects.labels import GerritProjectLabels
from gerrit.projects.webhooks import GerritProjectWebHooks
from gerrit.utils.exceptions import CommitNotFoundError, GerritAPIException

logger = logging.getLogger(__name__)


class GerritProject(GerritBase):
    def __init__(self, project_id: str, gerrit):
        self.id = project_id
        self.gerrit = gerrit
        self.endpoint = f"/projects/{self.id}"
        super().__init__(self)

    def __str__(self):
        return self.id

    def get_description(self):
        """
        Retrieves the description of a project.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/description")

    def set_description(self, input_):
        """
        Sets the description of a project.

        .. code-block:: python

            input_ = {
                "description": "Plugin for Gerrit that handles the replication.",
                "commit_message": "Update the project description"
            }
            project = client.projects.get('myproject')
            result = project.set_description(input_)

        :param input_: the ProjectDescriptionInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#project-description-input
        :return:
        """
        return self.gerrit.put(
            self.endpoint + "/description",
            json=input_,
            headers=self.gerrit.default_headers,
        )

    def delete_description(self):
        """
        Deletes the description of a project.

        :return:
        """
        self.gerrit.delete(self.endpoint + "/description")

    def delete(self):
        """
        Delete the project, requires delete-project plugin

        :return:
        """
        self.gerrit.post(self.endpoint + "/delete-project~delete")

    def get_parent(self):
        """
        Retrieves the name of a project’s parent project. For the All-Projects root project an empty
        string is returned.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/parent")

    def set_parent(self, input_):
        """
        Sets the parent project for a project.

        .. code-block:: python

            input_ = {
                "parent": "Public-Plugins",
                "commit_message": "Update the project parent"
            }
            project = client.projects.get('myproject')
            result = project.set_parent(input_)

        :param input_: The ProjectParentInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#project-parent-input
        :return:
        """
        return self.gerrit.put(
            self.endpoint + "/parent", json=input_, headers=self.gerrit.default_headers
        )

    def get_head(self):
        """
        Retrieves for a project the name of the branch to which HEAD points.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/HEAD")

    def set_head(self, input_):
        """
        Sets HEAD for a project.

        .. code-block:: python

            input_ = {
                "ref": "refs/heads/stable"
            }
            project = client.projects.get('myproject')
            result = project.set_HEAD(input_)

        :param input_: The HeadInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#head-input
        :return:
        """
        return self.gerrit.put(
            self.endpoint + "/HEAD", json=input_, headers=self.gerrit.default_headers
        )

    def get_config(self):
        """
        Gets some configuration information about a project.
        Note that this config info is not simply the contents of project.config; it generally
        contains fields that may
        have been inherited from parent projects.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/config")

    def set_config(self, input_):
        """
        Sets the configuration of a project.

        .. code-block:: python

            input_ = {
                "description": "demo project",
                "use_contributor_agreements": "FALSE",
                "use_content_merge": "INHERIT",
                "use_signed_off_by": "INHERIT",
                "create_new_change_for_all_not_in_target": "INHERIT",
                "enable_signed_push": "INHERIT",
                "require_signed_push": "INHERIT",
                "reject_implicit_merges": "INHERIT",
                "require_change_id": "TRUE",
                "max_object_size_limit": "10m",
                "submit_type": "REBASE_IF_NECESSARY",
                "state": "ACTIVE"
            }
            project = client.projects.get('myproject')
            result = project.set_config(input_)

        :param input_: the ConfigInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#config-info
        :return:
        """
        return self.gerrit.put(
            self.endpoint + "/config", json=input_, headers=self.gerrit.default_headers
        )

    def get_statistics(self):
        """
        Return statistics for the repository of a project.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/statistics.git")

    def run_garbage_collection(self, input_):
        """
        Run the Git garbage collection for the repository of a project.

        .. code-block:: python

            input_ = {
                "show_progress": "true"
            }
            project = client.projects.get('myproject')
            result = project.run_garbage_collection(input_)

        :param input_: the GCInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#gc-input
        :return:
        """
        return self.gerrit.post(
            self.endpoint + "/gc", json=input_, headers=self.gerrit.default_headers
        )

    def ban_commits(self, input_):
        """
        Marks commits as banned for the project.

        .. code-block:: python

            input_ = {
                "commits": [
                  "a8a477efffbbf3b44169bb9a1d3a334cbbd9aa96",
                  "cf5b56541f84b8b57e16810b18daca9c3adc377b"
                ],
                "reason": "Violates IP"
            }
            project = client.projects.get('myproject')
            result = project.ban_commits(input_)

        :param input_: the BanInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#ban-input
        :return:
        """
        return self.gerrit.put(
            self.endpoint + "/ban", json=input_, headers=self.gerrit.default_headers
        )

    def get_access_rights(self):
        """
        Lists the access rights for a single project.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/access")

    def set_access_rights(self, input_):
        """
        Sets access rights for the project using the diff schema provided by ProjectAccessInput.
        https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#set-access

        :param input_: the ProjectAccessInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#project-access-input
        :return:
        """
        return self.gerrit.post(
            self.endpoint + "/access", json=input_, headers=self.gerrit.default_headers
        )

    def create_change(self, input_):
        """
        Create Change for review. This endpoint is functionally equivalent to create change in the
        change API, but it has the project name in the URL, which is easier to route in sharded
        deployments.
        support this method since v3.3.0

        .. code-block:: python

            input_ = {
                "subject": "Let's support 100% Gerrit workflow direct in browser",
                "branch": "stable",
                "topic": "create-change-in-browser",
                "status": "NEW"
            }

            project = client.projects.get('myproject')
            result = project.create_change(input_)

        :param input_:
        :return:
        """
        result = self.gerrit.post(
            self.endpoint + "/create.change",
            json=input_,
            headers=self.gerrit.default_headers,
        )
        return result

    def create_access_rights_change(self, input_):
        """
        Sets access rights for the project using the diff schema provided by ProjectAccessInput
        This takes the same input as Update Access Rights, but creates a pending change for review.
        Like Create Change, it returns a ChangeInfo entity describing the resulting change.
        https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#create-access-change

        :param input_: the ProjectAccessInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#project-access-input
        :return:
        """
        result = self.gerrit.put(
            self.endpoint + "/access:review",
            json=input_,
            headers=self.gerrit.default_headers,
        )
        return result

    def check_access(self, options):
        """
        runs access checks for other users.

        :param options:
          Check Access Options
            * Account(account): The account for which to check access. Mandatory.
            * Permission(perm): The ref permission for which to check access.
              If not specified, read access to at least branch is checked.
            * Ref(ref): The branch for which to check access. This must be given if perm is
            specified.

        :return:
        """
        return self.gerrit.get(self.endpoint + f"/check.access?{options}")

    def index(self, input_):
        """
        Adds or updates the current project (and children, if specified) in the secondary index.
        The indexing task is executed asynchronously in background and this command returns
        immediately if async is specified in the input.

        .. code-block:: python

            input_ = {
                "index_children": "true"
                "async": "true"
            }
            project = client.projects.get('myproject')
            result = project.index(input_)

        :param input_: the IndexProjectInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#index-project-input
        :return:
        """
        self.gerrit.post(
            self.endpoint + "/index", json=input_, headers=self.gerrit.default_headers
        )

    def index_all_changes(self):
        """
        Adds or updates the current project (and children, if specified) in the secondary index.
        The indexing task is executed asynchronously in background and this command returns
        immediately if async is specified in the input.

        :return:
        """
        self.gerrit.post(self.endpoint + "/index.changes")

    def check_consistency(self, input_):
        """
        Performs consistency checks on the project.

        .. code-block:: python

            input_ = {
                "auto_closeable_changes_check": {
                    "fix": 'true',
                    "branch": "refs/heads/master",
                    "max_commits": 100
                }
            }

            project = client.projects.get('myproject')
            result = project.check_consistency(input_)

        :param input_: the CheckProjectInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#check-project-input
        :return:
        """
        return self.gerrit.post(
            self.endpoint + "/check", json=input_, headers=self.gerrit.default_headers
        )

    @property
    def branches(self):
        """
        List the branches of a project. except the refs/meta/config

        :return:
        """
        return GerritProjectBranches(self.id, self.gerrit)

    @property
    def child_projects(self):
        """
        List the direct child projects of a project.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/children/")

    @property
    def tags(self):
        """
        List the tags of a project.

        :return:
        """
        return GerritProjectTags(self.id, self.gerrit)

    def get_commit(self, commit):
        """
        Retrieves a commit of a project.

        :return:
        """
        try:
            result = self.gerrit.get(self.endpoint + f"/commits/{commit}")

            commit = result.get("commit")
            return GerritProjectCommit(
                commit=commit, project=self.id, gerrit=self.gerrit
            )
        except requests.exceptions.HTTPError as error:
            if error.response.status_code == 404:
                message = f"Commit {commit} does not exist"
                logger.error(message)
                raise CommitNotFoundError(message)
            raise GerritAPIException from error

    @property
    def dashboards(self):
        """
        gerrit dashboards operations

        :return:
        """
        return GerritProjectDashboards(project=self.id, gerrit=self.gerrit)

    @property
    def labels(self):
        """
        gerrit labels or gerrit labels operations

        :return:
        """
        return GerritProjectLabels(project=self.id, gerrit=self.gerrit)

    @property
    def webhooks(self):
        """
        gerrit webhooks operations, requires webhooks plugin

        :return:
        """
        return GerritProjectWebHooks(project=self.id, gerrit=self.gerrit)
