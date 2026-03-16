import sys
from contextlib import contextmanager

import click

from uwsgiconf import VERSION
from uwsgiconf.exceptions import ConfigurationError
from uwsgiconf.sysinit import TYPES, TYPE_SYSTEMD, get_config
from uwsgiconf.utils import ConfModule, UwsgiRunner


@contextmanager
def errorprint():
    """Print out descriptions from ConfigurationError."""
    try:
        yield

    except ConfigurationError as e:
        click.secho(f'{e}', err=True, fg='red')
        sys.exit(1)


@click.group()
@click.version_option(version='.'.join(map(str, VERSION)))
def base():
    """uwsgiconf command line utility.

    Tools to facilitate uWSGI configuration.

    """

arg_conf = click.argument(
    'conf', type=click.Path(exists=True, dir_okay=False), default=ConfModule.default_name)


@base.command()
@arg_conf
@click.option('--only', help='Configuration alias from module to run uWSGI with.')
def run(conf, only):
    """Runs uWSGI passing to it using the default or another `uwsgiconf` configuration module.

    """
    with errorprint():
        config = ConfModule(conf)
        spawned = config.spawn_uwsgi(only=only)

        for alias, pid in spawned:
            click.secho(f"Spawned uWSGI for configuration aliased '{alias}'. PID {pid}", fg='green')


@base.command()
@arg_conf
def compile(conf):
    """Compiles classic uWSGI configuration file using the default
    or given `uwsgiconf` configuration module.

    """
    with errorprint():
        config = ConfModule(conf)
        for conf in config.configurations:
            conf.format(do_print=True)


@base.command()
@click.argument('systype', type=click.Choice(TYPES), default=TYPE_SYSTEMD)
@arg_conf
@click.option('--project', help='Project name to use as service name.')
def sysinit(systype, conf, project):
    """Outputs configuration for system initialization subsystem."""

    click.secho(get_config(
        systype,
        conf=ConfModule(conf).configurations[0],
        conf_path=conf,
        project_name=project,
    ))


@base.command()
def probe_plugins():
    """Runs uWSGI to determine what plugins are available and prints them out.

    Generic plugins come first then after blank line follow request plugins.

    """
    plugins = UwsgiRunner().get_plugins()

    for plugin in sorted(plugins.generic):
        click.secho(plugin)

    click.secho('')

    for plugin in sorted(plugins.request):
        click.secho(plugin)


def main():
    """
    CLI entry point
    """
    base(obj={})


if __name__ == '__main__':
    main()
