#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi


class GerritGroupSubGroups:
    def __init__(self, group_id, gerrit):
        self.id = group_id
        self.gerrit = gerrit
        self.endpoint = f"/groups/{self.id}/groups/"

    def list(self):
        """
        Lists the direct subgroups of a group.
        This endpoint is only allowed for Gerrit internal groups;
        attempting to call on a non-internal group will return 405 Method Not Allowed.

        :return:
        """
        result = self.gerrit.get(self.endpoint)
        subgroups = []
        for item in result:
            group_id = item.get("id")
            subgroups.append(self.gerrit.groups.get(group_id))

        return subgroups

    def get(self, subgroup):
        """
        Retrieves a subgroup.
        This endpoint is only allowed for Gerrit internal groups;
        attempting to call on a non-internal group will return 405 Method Not Allowed.

        :param subgroup: subgroup id or name
        :return:
        """
        result = self.gerrit.get(self.endpoint + f"/{subgroup}")

        subgroup_id = result.get("id")
        return self.gerrit.groups.get(subgroup_id)

    def add(self, subgroup):
        """
        Adds an internal or external group as subgroup to a Gerrit internal group.
        This endpoint is only allowed for Gerrit internal groups;
        attempting to call on a non-internal group will return 405 Method Not Allowed.

        :param subgroup: subgroup id or name
        :return:
        """
        result = self.gerrit.put(self.endpoint + f"/{subgroup}")

        subgroup_id = result.get("id")
        return self.gerrit.groups.get(subgroup_id)

    def remove(self, subgroup):
        """
        Removes a subgroup from a Gerrit internal group.
        This endpoint is only allowed for Gerrit internal groups;
        attempting to call on a non-internal group will return 405 Method Not Allowed.

        :param subgroup: subgroup id or name
        :return:
        """
        self.gerrit.delete(self.endpoint + f"/{subgroup}")
