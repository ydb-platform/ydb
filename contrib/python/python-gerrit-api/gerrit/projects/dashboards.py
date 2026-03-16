#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
from gerrit.utils.gerritbase import GerritBase


class GerritProjectDashboard(GerritBase):
    def __init__(self, id: str, project: str, gerrit):
        self.id = id
        self.project = project
        self.gerrit = gerrit
        self.endpoint = f"/projects/{self.project}/dashboards/{self.id}"
        super().__init__(self)

    def __str__(self):
        return str(self.id)

    def delete(self):
        """
        Deletes a project dashboard.

        :return:
        """
        self.gerrit.delete(self.endpoint)


class GerritProjectDashboards:
    def __init__(self, project, gerrit):
        self.project = project
        self.gerrit = gerrit
        self.endpoint = f"/projects/{self.project}/dashboards"

    def list(self):
        """
        List custom dashboards for a project.

        :return:
        """
        result = self.gerrit.get(self.endpoint + "/")
        return result

    def create(self, id_, input_):
        """
        Creates a project dashboard, if a project dashboard with the given dashboard ID doesn't
        exist yet.

        .. code-block:: python

            input_ = {
                "id": "master:closed",
                "commit_message": "Define the default dashboard"
            }
            new_dashboard = project.dashboards.create('master:closed', input_)


        :param id_: the dashboard id
        :param input_: the DashboardInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#dashboard-input
        :return:
        """
        result = self.gerrit.put(
            self.endpoint + f"/{id_}", json=input_, headers=self.gerrit.default_headers
        )
        return result

    def get(self, id_):
        """
        Retrieves a project dashboard. The dashboard can be defined on that project or be inherited
        from a parent project.

        :param id_: the dashboard id
        :return:
        """
        result = self.gerrit.get(self.endpoint + f"/{id_}")

        dashboard_id = result.get("id")
        return GerritProjectDashboard(
            id=dashboard_id, project=self.project, gerrit=self.gerrit
        )

    def delete(self, id_):
        """
        Deletes a project dashboard.

        :param id_: the dashboard id
        :return:
        """
        self.gerrit.delete(self.endpoint + f"/{id_}")
