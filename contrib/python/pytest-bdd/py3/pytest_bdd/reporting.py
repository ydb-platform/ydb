"""Reporting functionality.

Collection of the scenario excecution statuses, timing and other information
that enriches the pytest test reporting.
"""

import time

from .utils import get_parametrize_markers_args


class StepReport:
    """Step excecution report."""

    failed = False
    stopped = None

    def __init__(self, step):
        """Step report constructor.

        :param pytest_bdd.parser.Step step: Step.
        """
        self.step = step
        self.started = time.time()

    def serialize(self):
        """Serialize the step excecution report.

        :return: Serialized step excecution report.
        :rtype: dict
        """
        return {
            "name": self.step.name,
            "type": self.step.type,
            "keyword": self.step.keyword,
            "line_number": self.step.line_number,
            "failed": self.failed,
            "duration": self.duration,
        }

    def finalize(self, failed):
        """Stop collecting information and finalize the report.

        :param bool failed: Wheither the step excecution is failed.
        """
        self.stopped = time.time()
        self.failed = failed

    @property
    def duration(self):
        """Step excecution duration.

        :return: Step excecution duration.
        :rtype: float
        """
        if self.stopped is None:
            return 0

        return self.stopped - self.started


class ScenarioReport:
    """Scenario execution report."""

    def __init__(self, scenario, node):
        """Scenario report constructor.

        :param pytest_bdd.parser.Scenario scenario: Scenario.
        :param node: pytest test node object
        """
        self.scenario = scenario
        self.step_reports = []
        self.param_index = None
        parametrize_args = get_parametrize_markers_args(node)
        if parametrize_args and scenario.examples:
            param_names = (
                parametrize_args[0] if isinstance(parametrize_args[0], (tuple, list)) else [parametrize_args[0]]
            )
            param_values = parametrize_args[1]
            node_param_values = [node.funcargs[param_name] for param_name in param_names]
            if node_param_values in param_values:
                self.param_index = param_values.index(node_param_values)
            elif tuple(node_param_values) in param_values:
                self.param_index = param_values.index(tuple(node_param_values))
        self.example_kwargs = {
            example_param: str(node.funcargs[example_param]) for example_param in scenario.get_example_params()
        }

    @property
    def current_step_report(self):
        """Get current step report.

        :return: Last or current step report.
        :rtype: pytest_bdd.reporting.StepReport
        """
        return self.step_reports[-1]

    def add_step_report(self, step_report):
        """Add new step report.

        :param step_report: New current step report.
        :type step_report: pytest_bdd.reporting.StepReport
        """
        self.step_reports.append(step_report)

    def serialize(self):
        """Serialize scenario excecution report in order to transfer reportin from nodes in the distributed mode.

        :return: Serialized report.
        :rtype: dict
        """
        scenario = self.scenario
        feature = scenario.feature

        params = sum(scenario.get_params(builtin=True), []) if scenario.examples else None
        return {
            "steps": [step_report.serialize() for step_report in self.step_reports],
            "name": scenario.name,
            "line_number": scenario.line_number,
            "tags": sorted(scenario.tags),
            "feature": {
                "name": feature.name,
                "filename": feature.filename,
                "rel_filename": feature.rel_filename,
                "line_number": feature.line_number,
                "description": feature.description,
                "tags": sorted(feature.tags),
            },
            "examples": [
                {
                    "name": scenario.examples.name,
                    "line_number": scenario.examples.line_number,
                    "rows": params,
                    "row_index": self.param_index,
                }
            ]
            if scenario.examples
            else [],
            "example_kwargs": self.example_kwargs,
        }

    def fail(self):
        """Stop collecting information and finalize the report as failed."""
        self.current_step_report.finalize(failed=True)
        remaining_steps = self.scenario.steps[len(self.step_reports) :]

        # Fail the rest of the steps and make reports.
        for step in remaining_steps:
            report = StepReport(step=step)
            report.finalize(failed=True)
            self.add_step_report(report)


def runtest_makereport(item, call, rep):
    """Store item in the report object."""
    try:
        scenario_report = item.__scenario_report__
    except AttributeError:
        pass
    else:
        rep.scenario = scenario_report.serialize()
        rep.item = {"name": item.name}


def before_scenario(request, feature, scenario):
    """Create scenario report for the item."""
    request.node.__scenario_report__ = ScenarioReport(scenario=scenario, node=request.node)


def step_error(request, feature, scenario, step, step_func, step_func_args, exception):
    """Finalize the step report as failed."""
    request.node.__scenario_report__.fail()


def before_step(request, feature, scenario, step, step_func):
    """Store step start time."""
    request.node.__scenario_report__.add_step_report(StepReport(step=step))


def after_step(request, feature, scenario, step, step_func, step_func_args):
    """Finalize the step report as successful."""
    request.node.__scenario_report__.current_step_report.finalize(failed=False)
