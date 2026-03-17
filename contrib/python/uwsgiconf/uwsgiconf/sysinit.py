from os.path import dirname, basename, abspath
from textwrap import dedent
from typing import List, Dict, Callable, Union

from .config import Configuration, Section
from .utils import Finder, UwsgiRunner


TYPE_UPSTART = 'upstart'
TYPE_SYSTEMD = 'systemd'

TYPES: List[str] = [
    TYPE_UPSTART,
    TYPE_SYSTEMD,
]


def get_tpl_systemd(conf: 'Section') -> str:
    """

    Some Systemd hints:

        * uwsgiconf sysinit > my.service
        * sudo cp my.service /etc/systemd/system/

        * sudo sh -c "systemctl daemon-reload; systemctl start my.service"

        * journalctl -fu my.service

    :param conf: Section object.

    """
    tpl = '''
        # Place into:   /etc/systemd/system/{project}.service
        # Setup:        sudo systemctl enable --now $PWD/{project}.service
        # Start:        sudo systemctl start {project}.service
        # Stop:         sudo systemctl stop {project}.service
        # Restart:      sudo systemctl restart {project}.service
        # Status:       systemctl status {project}.service
        # Journal:      journalctl -fu {project}.service

        [Unit]
        Description={project} uWSGI Service
        Wants=network-online.target
        After=network-online.target

        [Service]
        Environment="PATH=%(path)s"
        ExecStartPre=-/usr/bin/install -d -m 0755 -o %(user)s -g %(group)s %(runtime_dir)s
        ExecStart={command}
        Restart=on-failure
        KillSignal=SIGTERM
        Type=notify
        StandardError=syslog
        NotifyAccess=all
        # Bind to priviledged ports.
        AmbientCapabilities=CAP_NET_BIND_SERVICE

        [Install]
        WantedBy=multi-user.target
    '''
    # We do not use 'RuntimeDirectory' systemd directive since we need to chown.

    uid, gid = conf.main_process.get_owner()

    tpl = tpl % {
        'runtime_dir': conf.replace_placeholders('{project_runtime_dir}'),
        'path': UwsgiRunner.get_env_path(),
        'user':  uid,
        'group': gid,
    }

    return tpl


def get_tpl_upstart(conf: 'Section') -> str:
    """

    :param conf: Section object.

    """
    tpl = '''
        # Place into: /etc/init/{project}.conf
        # Verify:     initctl check-config {project}
        # Start:      initctl start {project}
        # Stop:       initctl stop {project}
        # Restart:    initctl restart {project}
        
        description "{project} uWSGI Service"
        start on runlevel [2345]
        stop on runlevel [06]
        
        respawn
        
        env PATH=%(path)s
        pre-start exec -/usr/bin/install -d -m 0755 -o %(user)s -g %(group)s %(runtime_dir)s
        
        exec {command}
    '''

    uid, gid = conf.main_process.get_owner()

    tpl = tpl % {
        'path': UwsgiRunner.get_env_path(),
        'runtime_dir': conf.replace_placeholders('{project_runtime_dir}'),
        'user': uid,
        'group': gid,
    }

    return tpl


TEMPLATES: Dict[str, Callable] = {
    TYPE_SYSTEMD: get_tpl_systemd,
    TYPE_UPSTART: get_tpl_upstart,
}


def get_config(
        systype: str,
        *,
        conf: Union[Configuration, Section],
        conf_path: str,
        runner: str = None,
        project_name: str = None
) -> str:
    """Returns init system configuration file contents.

    :param systype: System type alias, e.g. systemd, upstart
    :param conf: Configuration/Section object.
    :param conf_path: File path to a configuration file or a command producing such a configuration.
    :param runner: Runner command to execute conf_path. Defaults to ``uwsgiconf`` runner.
    :param project_name: Project name to override.

    """
    runner = runner or f'{Finder.uwsgiconf()} run'
    conf_path = abspath(conf_path)

    if isinstance(conf, Configuration):
        conf = conf.sections[0]  # todo Maybe something more intelligent.

    tpl = dedent(TEMPLATES.get(systype)(conf=conf))

    formatted = tpl.strip().format(
        project=project_name or conf.project_name or basename(dirname(conf_path)),
        command=f'{runner} {conf_path}',
    )

    return formatted
