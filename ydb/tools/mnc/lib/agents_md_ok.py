from ydb.tools.mnc.lib.exceptions import CliError
from ydb.tools.mnc.lib.output import get_console
from ydb.tools.mnc.lib.progress import TaskResult, TaskResultLevel


def describe_probe(name):
    if not name:
        raise CliError("probe name is required")
    get_console().print(name)
    return TaskResult(
        level=TaskResultLevel.OK,
        step_title="Describe probe",
        message=name,
    )
