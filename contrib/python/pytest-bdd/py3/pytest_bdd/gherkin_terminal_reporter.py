import re

from _pytest.terminal import TerminalReporter

from .parser import STEP_PARAM_RE


def add_options(parser):
    group = parser.getgroup("terminal reporting", "reporting", after="general")
    group._addoption(
        "--gherkin-terminal-reporter",
        action="store_true",
        dest="gherkin_terminal_reporter",
        default=False,
        help=("enable gherkin output"),
    )
    group._addoption(
        "--gherkin-terminal-reporter-expanded",
        action="store_true",
        dest="expand",
        default=False,
        help="expand scenario outlines into scenarios and fill in the step names",
    )


def configure(config):
    if config.option.gherkin_terminal_reporter:
        # Get the standard terminal reporter plugin and replace it with our
        current_reporter = config.pluginmanager.getplugin("terminalreporter")
        if current_reporter.__class__ != TerminalReporter:
            raise Exception(
                "gherkin-terminal-reporter is not compatible with any other terminal reporter."
                "You can use only one terminal reporter."
                "Currently '{0}' is used."
                "Please decide to use one by deactivating {0} or gherkin-terminal-reporter.".format(
                    current_reporter.__class__
                )
            )
        gherkin_reporter = GherkinTerminalReporter(config)
        config.pluginmanager.unregister(current_reporter)
        config.pluginmanager.register(gherkin_reporter, "terminalreporter")
        if config.pluginmanager.getplugin("dsession"):
            raise Exception("gherkin-terminal-reporter is not compatible with 'xdist' plugin.")


class GherkinTerminalReporter(TerminalReporter):
    def __init__(self, config):
        TerminalReporter.__init__(self, config)

    def pytest_runtest_logreport(self, report):
        rep = report
        res = self.config.hook.pytest_report_teststatus(report=rep, config=self.config)
        cat, letter, word = res

        if not letter and not word:
            # probably passed setup/teardown
            return

        if isinstance(word, tuple):
            word, word_markup = word
        else:
            if rep.passed:
                word_markup = {"green": True}
            elif rep.failed:
                word_markup = {"red": True}
            elif rep.skipped:
                word_markup = {"yellow": True}
        feature_markup = {"blue": True}
        scenario_markup = word_markup

        if self.verbosity <= 0:
            return TerminalReporter.pytest_runtest_logreport(self, rep)
        elif self.verbosity == 1:
            if hasattr(report, "scenario"):
                self.ensure_newline()
                self._tw.write("Feature: ", **feature_markup)
                self._tw.write(report.scenario["feature"]["name"], **feature_markup)
                self._tw.write("\n")
                self._tw.write("    Scenario: ", **scenario_markup)
                self._tw.write(report.scenario["name"], **scenario_markup)
                self._tw.write(" ")
                self._tw.write(word, **word_markup)
                self._tw.write("\n")
            else:
                return TerminalReporter.pytest_runtest_logreport(self, rep)
        elif self.verbosity > 1:
            if hasattr(report, "scenario"):
                self.ensure_newline()
                self._tw.write("Feature: ", **feature_markup)
                self._tw.write(report.scenario["feature"]["name"], **feature_markup)
                self._tw.write("\n")
                self._tw.write("    Scenario: ", **scenario_markup)
                self._tw.write(report.scenario["name"], **scenario_markup)
                self._tw.write("\n")
                for step in report.scenario["steps"]:
                    if self.config.option.expand:
                        step_name = self._format_step_name(step["name"], **report.scenario["example_kwargs"])
                    else:
                        step_name = step["name"]
                    self._tw.write("        {} {}\n".format(step["keyword"], step_name), **scenario_markup)
                self._tw.write("    " + word, **word_markup)
                self._tw.write("\n\n")
            else:
                return TerminalReporter.pytest_runtest_logreport(self, rep)
        self.stats.setdefault(cat, []).append(rep)

    def _format_step_name(self, step_name, **example_kwargs):
        while True:
            param_match = re.search(STEP_PARAM_RE, step_name)
            if not param_match:
                break
            param_token = param_match.group(0)
            param_name = param_match.group(1)
            param_value = example_kwargs[param_name]
            step_name = step_name.replace(param_token, param_value)
        return step_name
