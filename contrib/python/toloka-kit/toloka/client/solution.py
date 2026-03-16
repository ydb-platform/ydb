__all__ = ['Solution']
from typing import Dict, Any

import attr


@attr.attrs(auto_attribs=True)
class Solution:
    """A Toloker's response to a single task.

    A solution contains values for output fields specified in a [TaskSpec](toloka.client.project.task_spec.TaskSpec.md) when a project was created.

    Solutions can be accessed via the [Assignment](toloka.client.assignment.Assignment.md) class.

    Attributes:
        output_values: A dictionary with keys named as output fields.
    """

    output_values: Dict[str, Any]
