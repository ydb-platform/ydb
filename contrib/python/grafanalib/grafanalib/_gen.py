"""Generate JSON Grafana dashboards."""

import argparse
import json
import os
import sys


DASHBOARD_SUFFIX = '.dashboard.py'
ALERTGROUP_SUFFIX = '.alertgroup.py'

"""
Common generation functionality
"""


class DashboardEncoder(json.JSONEncoder):
    """Encode dashboard objects."""

    def default(self, obj):
        to_json_data = getattr(obj, 'to_json_data', None)
        if to_json_data:
            return to_json_data()
        return json.JSONEncoder.default(self, obj)


class DashboardError(Exception):
    """Raised when there is something wrong with a dashboard."""


class AlertGroupError(Exception):
    """Raised when there is something wrong with an alertgroup."""


def write_dashboard(dashboard, stream):
    json.dump(
        dashboard.to_json_data(), stream, sort_keys=True, indent=2,
        cls=DashboardEncoder)
    stream.write('\n')


write_alertgroup = write_dashboard


class DefinitionError(Exception):
    """Raised when there is a problem loading a Grafanalib type from a python definition."""


def loader(path):
    """Load a grafanalib type from a Python definition.

    :param str path: Path to a *.<type>.py file that defines a variable called <type>.
    """
    gtype = path.split(".")[-2]

    if sys.version_info[0] == 3 and sys.version_info[1] >= 5:
        import importlib.util
        spec = importlib.util.spec_from_file_location(gtype, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    elif sys.version_info[0] == 3 and (sys.version_info[1] >= 3 or sys.version_info[1] <= 4):
        from importlib.machinery import SourceFileLoader
        module = SourceFileLoader(gtype, path).load_module()
    elif sys.version_info[0] == 2:
        import imp
        module = imp.load_source(gtype, path)
    else:
        import importlib
        module = importlib.load_source(gtype, path)

    marker = object()
    grafanalibtype = getattr(module, gtype, marker)
    if grafanalibtype is marker:
        raise DefinitionError(
            "Definition {} does not define a variable '{}'".format(path, gtype))
    return grafanalibtype


def run_script(f):
    sys.exit(f(sys.argv[1:]))


"""
AlertGroup generation
"""


def print_alertgroup(dashboard):
    write_dashboard(dashboard, stream=sys.stdout)


def write_alertgroups(paths):
    for path in paths:
        assert path.endswith(ALERTGROUP_SUFFIX)
        dashboard = loader(path)
        with open(get_alertgroup_json_path(path), 'w') as json_file:
            write_dashboard(dashboard, json_file)


def get_alertgroup_json_path(path):
    assert path.endswith(ALERTGROUP_SUFFIX)
    return '{}.json'.format(path[:-len(ALERTGROUP_SUFFIX)])


def alertgroup_path(path):
    abspath = os.path.abspath(path)
    if not abspath.endswith(ALERTGROUP_SUFFIX):
        raise argparse.ArgumentTypeError(
            'AlertGroup file {} does not end with {}'.format(
                path, ALERTGROUP_SUFFIX))
    return abspath


def generate_alertgroups(args):
    """Script for generating multiple alertgroups at a time"""
    parser = argparse.ArgumentParser(prog='generate-alertgroups')
    parser.add_argument(
        'alertgroups', metavar='ALERT', type=os.path.abspath,
        nargs='+', help='Path to alertgroup definition',
    )
    opts = parser.parse_args(args)
    try:
        write_alertgroups(opts.alertgroups)
    except AlertGroupError as e:
        sys.stderr.write('ERROR: {}\n'.format(e))
        return 1
    return 0


def generate_alertgroup(args):
    parser = argparse.ArgumentParser(prog='generate-alertgroup')
    parser.add_argument(
        '--output', '-o', type=os.path.abspath,
        help='Where to write the alertgroup JSON'
    )
    parser.add_argument(
        'alertgroup', metavar='ALERT', type=os.path.abspath,
        help='Path to alertgroup definition',
    )
    opts = parser.parse_args(args)
    try:
        alertgroup = loader(opts.alertgroup)
        if not opts.output:
            print_alertgroup(alertgroup)
        else:
            with open(opts.output, 'w') as output:
                write_alertgroup(alertgroup, output)
    except AlertGroupError as e:
        sys.stderr.write('ERROR: {}\n'.format(e))
        return 1
    return 0


def generate_alertgroups_script():
    """Entry point for generate-alertgroups."""
    run_script(generate_alertgroups)


def generate_alertgroup_script():
    """Entry point for generate-alertgroup."""
    run_script(generate_alertgroup)


"""
Dashboard generation
"""


def print_dashboard(dashboard):
    write_dashboard(dashboard, stream=sys.stdout)


def write_dashboards(paths):
    for path in paths:
        assert path.endswith(DASHBOARD_SUFFIX)
        dashboard = loader(path)
        with open(get_dashboard_json_path(path), 'w') as json_file:
            write_dashboard(dashboard, json_file)


def get_dashboard_json_path(path):
    assert path.endswith(DASHBOARD_SUFFIX)
    return '{}.json'.format(path[:-len(DASHBOARD_SUFFIX)])


def dashboard_path(path):
    abspath = os.path.abspath(path)
    if not abspath.endswith(DASHBOARD_SUFFIX):
        raise argparse.ArgumentTypeError(
            'Dashboard file {} does not end with {}'.format(
                path, DASHBOARD_SUFFIX))
    return abspath


def generate_dashboards(args):
    """Script for generating multiple dashboards at a time."""
    parser = argparse.ArgumentParser(prog='generate-dashboards')
    parser.add_argument(
        'dashboards', metavar='DASHBOARD', type=os.path.abspath,
        nargs='+', help='Path to dashboard definition',
    )
    opts = parser.parse_args(args)
    try:
        write_dashboards(opts.dashboards)
    except DashboardError as e:
        sys.stderr.write('ERROR: {}\n'.format(e))
        return 1
    return 0


def generate_dashboard(args):
    parser = argparse.ArgumentParser(prog='generate-dashboard')
    parser.add_argument(
        '--output', '-o', type=os.path.abspath,
        help='Where to write the dashboard JSON'
    )
    parser.add_argument(
        'dashboard', metavar='DASHBOARD', type=os.path.abspath,
        help='Path to dashboard definition',
    )
    opts = parser.parse_args(args)
    try:
        dashboard = loader(opts.dashboard)
        if not opts.output:
            print_dashboard(dashboard)
        else:
            with open(opts.output, 'w') as output:
                write_dashboard(dashboard, output)
    except DashboardError as e:
        sys.stderr.write('ERROR: {}\n'.format(e))
        return 1
    return 0


def generate_dashboards_script():
    """Entry point for generate-dashboards."""
    run_script(generate_dashboards)


def generate_dashboard_script():
    """Entry point for generate-dashboard."""
    run_script(generate_dashboard)
