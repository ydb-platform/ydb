"""Cucumber json output formatter."""

import codecs
import json
import math
import os
import sys
import time

from .feature import force_unicode


def add_options(parser):
    """Add pytest-bdd options."""
    group = parser.getgroup("bdd", "Cucumber JSON")
    group.addoption(
        "--cucumberjson",
        "--cucumber-json",
        action="store",
        dest="cucumber_json_path",
        metavar="path",
        default=None,
        help="create cucumber json style report file at given path.",
    )

    group._addoption(
        "--cucumberjson-expanded",
        "--cucumber-json-expanded",
        action="store_true",
        dest="expand",
        default=False,
        help="expand scenario outlines into scenarios and fill in the step names",
    )


def configure(config):
    cucumber_json_path = config.option.cucumber_json_path
    # prevent opening json log on worker nodes (xdist)
    if cucumber_json_path and not hasattr(config, "workerinput"):
        config._bddcucumberjson = LogBDDCucumberJSON(cucumber_json_path, expand=config.option.expand)
        config.pluginmanager.register(config._bddcucumberjson)


def unconfigure(config):
    xml = getattr(config, "_bddcucumberjson", None)
    if xml is not None:
        del config._bddcucumberjson
        config.pluginmanager.unregister(xml)


class LogBDDCucumberJSON(object):

    """Logging plugin for cucumber like json output."""

    def __init__(self, logfile, expand=False):
        logfile = os.path.expanduser(os.path.expandvars(logfile))
        self.logfile = os.path.normpath(os.path.abspath(logfile))
        self.features = {}
        self.expand = expand

    def append(self, obj):
        self.features[-1].append(obj)

    def _get_result(self, step, report, error_message=False):
        """Get scenario test run result.

        :param step: `Step` step we get result for
        :param report: pytest `Report` object
        :return: `dict` in form {"status": "<passed|failed|skipped>", ["error_message": "<error_message>"]}
        """
        result = {}
        if report.passed or not step["failed"]:  # ignore setup/teardown
            result = {"status": "passed"}
        elif report.failed and step["failed"]:
            result = {"status": "failed", "error_message": force_unicode(report.longrepr) if error_message else ""}
        elif report.skipped:
            result = {"status": "skipped"}
        result["duration"] = int(math.floor((10 ** 9) * step["duration"]))  # nanosec
        return result

    def _serialize_tags(self, item):
        """Serialize item's tags.

        :param item: json-serialized `Scenario` or `Feature`.
        :return: `list` of `dict` in the form of:
            [
                {
                    "name": "<tag>",
                    "line": 2,
                }
            ]
        """
        return [{"name": tag, "line": item["line_number"] - 1} for tag in item["tags"]]

    def _format_name(self, name, keys, values):
        for param, value in zip(keys, values):
            name = name.replace("<{}>".format(param), value)
        return name

    def _format_step_name(self, report, step):
        examples = report.scenario["examples"]
        if len(examples) == 0:
            return step["name"]

        # we take the keys from the first "examples", but in each table, the keys should
        # be the same anyway since all the variables need to be filled in.
        keys, values = examples[0]["rows"]
        row_index = examples[0]["row_index"]

        return self._format_name(step["name"], keys, values[row_index])

    def pytest_runtest_logreport(self, report):
        try:
            scenario = report.scenario
        except AttributeError:
            # skip reporting for non-bdd tests
            return

        if not scenario["steps"] or report.when != "call":
            # skip if there isn't a result or scenario has no steps
            return

        def stepmap(step):
            error_message = False
            if step["failed"] and not scenario.setdefault("failed", False):
                scenario["failed"] = True
                error_message = True

            if self.expand:
                # XXX The format is already 'expanded' (scenario oultines -> scenarios),
                # but the step names were not filled in with parameters. To be backwards
                # compatible, do not fill in the step names unless explicitly asked for.
                step_name = self._format_step_name(report, step)
            else:
                step_name = step["name"]

            return {
                "keyword": step["keyword"],
                "name": step_name,
                "line": step["line_number"],
                "match": {"location": ""},
                "result": self._get_result(step, report, error_message),
            }

        if scenario["feature"]["filename"] not in self.features:
            self.features[scenario["feature"]["filename"]] = {
                "keyword": "Feature",
                "uri": scenario["feature"]["rel_filename"],
                "name": scenario["feature"]["name"] or scenario["feature"]["rel_filename"],
                "id": scenario["feature"]["rel_filename"].lower().replace(" ", "-"),
                "line": scenario["feature"]["line_number"],
                "description": scenario["feature"]["description"],
                "tags": self._serialize_tags(scenario["feature"]),
                "elements": [],
            }

        self.features[scenario["feature"]["filename"]]["elements"].append(
            {
                "keyword": "Scenario",
                "id": report.item["name"],
                "name": scenario["name"],
                "line": scenario["line_number"],
                "description": "",
                "tags": self._serialize_tags(scenario),
                "type": "scenario",
                "steps": [stepmap(step) for step in scenario["steps"]],
            }
        )

    def pytest_sessionstart(self):
        self.suite_start_time = time.time()

    def pytest_sessionfinish(self):
        if sys.version_info[0] < 3:
            logfile_open = codecs.open
        else:
            logfile_open = open
        with logfile_open(self.logfile, "w", encoding="utf-8") as logfile:
            logfile.write(json.dumps(list(self.features.values())))

    def pytest_terminal_summary(self, terminalreporter):
        terminalreporter.write_sep("-", "generated json file: %s" % (self.logfile))
