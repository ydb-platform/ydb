from __future__ import annotations


class MissingTask(Exception):
    def __init__(self, task_id: str | int) -> None:
        super().__init__(task_id)
        self.task_id = task_id
