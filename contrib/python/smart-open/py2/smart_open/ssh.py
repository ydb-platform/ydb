# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#

"""Implements I/O streams over SSH.

Examples
--------

>>> with open('/proc/version_signature', host='1.2.3.4') as conn:
...     print(conn.read())
b'Ubuntu 4.4.0-1061.70-aws 4.4.131'

Similarly, from a command line::

    $ python -c "from smart_open import ssh;print(ssh.open('/proc/version_signature', host='1.2.3.4').read())"
    b'Ubuntu 4.4.0-1061.70-aws 4.4.131'

"""

import getpass
import logging
import warnings

logger = logging.getLogger(__name__)

#
# Global storage for SSH connections.
#
_SSH = {}

SCHEMES = ("ssh", "scp", "sftp")
"""Supported URL schemes."""

DEFAULT_PORT = 22


def _connect(hostname, username, port, password, transport_params):
    try:
        import paramiko
    except ImportError:
        warnings.warn(
            'paramiko missing, opening SSH/SCP/SFTP paths will be disabled. '
            '`pip install paramiko` to suppress'
        )
        raise

    key = (hostname, username)
    ssh = _SSH.get(key)
    if ssh is None:
        ssh = _SSH[key] = paramiko.client.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        kwargs = transport_params.get('connect_kwargs', {}).copy()
        kwargs.setdefault('password', password)
        kwargs.setdefault('username', username)
        ssh.connect(hostname, port, **kwargs)
    return ssh


def open(path, mode='r', host=None, user=None, password=None, port=DEFAULT_PORT, transport_params=None):
    """Open a file on a remote machine over SSH.

    Expects authentication to be already set up via existing keys on the local machine.

    Parameters
    ----------
    path: str
        The path to the file to open on the remote machine.
    mode: str, optional
        The mode to use for opening the file.
    host: str, optional
        The hostname of the remote machine.  May not be None.
    user: str, optional
        The username to use to login to the remote machine.
        If None, defaults to the name of the current user.
    password: str, optional
        The password to use to login to the remote machine.
    port: int, optional
        The port to connect to.
    transport_params: dict, optional
        Any additional settings to be passed to paramiko.SSHClient.connect

    Returns
    -------
    A file-like object.

    Important
    ---------
    If you specify a previously unseen host, then its host key will be added to
    the local ~/.ssh/known_hosts *automatically*.

    If ``username`` or ``password`` are specified in *both* the uri and
    ``transport_params``, ``transport_params`` will take precedence
    """
    if not host:
        raise ValueError('you must specify the host to connect to')
    if not user:
        user = getpass.getuser()
    if not transport_params:
        transport_params = {}

    conn = _connect(host, user, port, password, transport_params)
    sftp_client = conn.get_transport().open_sftp_client()
    return sftp_client.open(path, mode)
