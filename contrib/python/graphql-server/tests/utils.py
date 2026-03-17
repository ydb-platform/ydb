from typing import List

from graphql import ExecutionResult
from graphql.execution import ExecutionContext


def as_dicts(results: List[ExecutionResult]):
    """Convert execution results to a list of tuples of dicts for better comparison."""
    return [
        {
            "data": result.data,
            "errors": [error.formatted for error in result.errors]
            if result.errors
            else result.errors,
        }
        for result in results
    ]


class RepeatExecutionContext(ExecutionContext):
    def execute_field(self, parent_type, source, field_nodes, path):
        result = super().execute_field(parent_type, source, field_nodes, path)
        return result * 2
