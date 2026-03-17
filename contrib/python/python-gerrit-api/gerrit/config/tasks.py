#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
class Task:
    def __init__(self, task_id: str, gerrit):
        self.id = task_id
        self.gerrit = gerrit
        self.endpoint = f"/config/server/tasks/{self.id}"

    def delete(self):
        """
        Kills a task from the background work queue that the Gerrit daemon is currently performing,
        or will perform in the near future.

        :return:
        """
        self.gerrit.delete(self.endpoint)


class Tasks:
    def __init__(self, gerrit):
        self.gerrit = gerrit
        self.endpoint = "/config/server/tasks"

    def list(self):
        """
        Lists the tasks from the background work queues that the Gerrit daemon is currently
        performing, or will perform in the near future.

        :return:
        """
        result = self.gerrit.get(self.endpoint)
        return result

    def get(self, id_):
        """
        Retrieves a task from the background work queue that the Gerrit daemon is currently
        performing, or will perform in the near future.

        :param id_: task id
        :return:
        """

        result = self.gerrit.get(self.endpoint + f"/{id_}")

        task_id = result.get("id")
        return Task(task_id=task_id, gerrit=self.gerrit)

    def delete(self, id_):
        """
        Kills a task from the background work queue that the Gerrit daemon is currently performing,
        or will perform in the near future.

        :param id_: task id
        :return:
        """
        self.gerrit.delete(self.endpoint + f"/{id_}")
