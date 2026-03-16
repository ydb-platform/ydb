|CircleCI| |AppVeyor| |readthedocs| |coveralls| |version|

|pyversions| |license|

**Author**: `Pahaz`_

**Repo**: https://github.com/pahaz/sshtunnel/

Inspired by https://github.com/jmagnusson/bgtunnel, which doesn't work on
Windows.

See also: https://github.com/paramiko/paramiko/blob/master/demos/forward.py

Requirements
-------------

* `paramiko`_

Installation
============

`sshtunnel`_ is on PyPI, so simply run:

::

    pip install sshtunnel

or ::

    easy_install sshtunnel

or ::

    conda install -c conda-forge sshtunnel

to have it installed in your environment.

For installing from source, clone the
`repo <https://github.com/pahaz/sshtunnel>`_ and run::

    python setup.py install

Testing the package
-------------------

In order to run the tests you first need
`tox <https://testrun.org/tox/latest/>`_ and run::

    python setup.py test

Usage scenarios
===============

One of the typical scenarios where ``sshtunnel`` is helpful is depicted in the
figure below. User may need to connect a port of a remote server (i.e. 8080)
where only SSH port (usually port 22) is reachable. ::

    ----------------------------------------------------------------------

                                |
    -------------+              |    +----------+
        LOCAL    |              |    |  REMOTE  | :22 SSH
        CLIENT   | <== SSH ========> |  SERVER  | :8080 web service
    -------------+              |    +----------+
                                |
                             FIREWALL (only port 22 is open)

    ----------------------------------------------------------------------

**Fig1**: How to connect to a service blocked by a firewall through SSH tunnel.


If allowed by the SSH server, it is also possible to reach a private server
(from the perspective of ``REMOTE SERVER``) not directly visible from the
outside (``LOCAL CLIENT``'s perspective). ::

    ----------------------------------------------------------------------

                                |
    -------------+              |    +----------+               +---------
        LOCAL    |              |    |  REMOTE  |               | PRIVATE
        CLIENT   | <== SSH ========> |  SERVER  | <== local ==> | SERVER
    -------------+              |    +----------+               +---------
                                |
                             FIREWALL (only port 443 is open)

    ----------------------------------------------------------------------

**Fig2**: How to connect to ``PRIVATE SERVER`` through SSH tunnel.


Usage examples
==============

API allows either initializing the tunnel and starting it or using a ``with``
context, which will take care of starting **and stopping** the tunnel:

Example 1
---------

Code corresponding to **Fig1** above follows, given remote server's address is
``pahaz.urfuclub.ru``, password authentication and randomly assigned local bind
port.

.. code-block:: python

    from sshtunnel import SSHTunnelForwarder

    server = SSHTunnelForwarder(
        'alfa.8iq.dev',
        ssh_username="pahaz",
        ssh_password="secret",
        remote_bind_address=('127.0.0.1', 8080)
    )

    server.start()

    print(server.local_bind_port)  # show assigned local port
    # work with `SECRET SERVICE` through `server.local_bind_port`.

    server.stop()

Example 2
---------

Example of a port forwarding to a private server not directly reachable,
assuming password protected pkey authentication, remote server's SSH service is
listening on port 443 and that port is open in the firewall (**Fig2**):

.. code-block:: python

    import paramiko
    import sshtunnel

    with sshtunnel.open_tunnel(
        (REMOTE_SERVER_IP, 443),
        ssh_username="",
        ssh_pkey="/var/ssh/rsa_key",
        ssh_private_key_password="secret",
        remote_bind_address=(PRIVATE_SERVER_IP, 22),
        local_bind_address=('0.0.0.0', 10022)
    ) as tunnel:
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect('127.0.0.1', 10022)
        # do some operations with client session
        client.close()

    print('FINISH!')

Example 3
---------

Example of a port forwarding for the Vagrant MySQL local port:

.. code-block:: python

    from sshtunnel import open_tunnel
    from time import sleep

    with open_tunnel(
        ('localhost', 2222),
        ssh_username="vagrant",
        ssh_password="vagrant",
        remote_bind_address=('127.0.0.1', 3306)
    ) as server:

        print(server.local_bind_port)
        while True:
            # press Ctrl-C for stopping
            sleep(1)

    print('FINISH!')

Or simply using the CLI:

.. code-block:: console

    (bash)$ python -m sshtunnel -U vagrant -P vagrant -L :3306 -R 127.0.0.1:3306 -p 2222 localhost

Example 4
---------

Opening an SSH session jumping over two tunnels. SSH transport and tunnels
will be daemonised, which will not wait for the connections to stop at close
time.

.. code-block:: python

    import sshtunnel
    from paramiko import SSHClient


    with sshtunnel.open_tunnel(
        ssh_address_or_host=('GW1_ip', 20022),
        remote_bind_address=('GW2_ip', 22),
    ) as tunnel1:
        print('Connection to tunnel1 (GW1_ip:GW1_port) OK...')
        with sshtunnel.open_tunnel(
            ssh_address_or_host=('localhost', tunnel1.local_bind_port),
            remote_bind_address=('target_ip', 22),
            ssh_username='GW2_user',
            ssh_password='GW2_pwd',
        ) as tunnel2:
            print('Connection to tunnel2 (GW2_ip:GW2_port) OK...')
            with SSHClient() as ssh:
                ssh.connect('localhost',
                    port=tunnel2.local_bind_port,
                    username='target_user',
                    password='target_pwd',
                )
                ssh.exec_command(...)


CLI usage
=========

::

    $ sshtunnel --help
    usage: sshtunnel [-h] [-U SSH_USERNAME] [-p SSH_PORT] [-P SSH_PASSWORD] -R
                     IP:PORT [IP:PORT ...] [-L [IP:PORT [IP:PORT ...]]]
                     [-k SSH_HOST_KEY] [-K KEY_FILE] [-S KEY_PASSWORD] [-t] [-v]
                     [-V] [-x IP:PORT] [-c SSH_CONFIG_FILE] [-z] [-n]
                     [-d [FOLDER [FOLDER ...]]]
                     ssh_address

    Pure python ssh tunnel utils
    Version 0.4.0

    positional arguments:
      ssh_address           SSH server IP address (GW for SSH tunnels)
                            set with "-- ssh_address" if immediately after -R or -L

    optional arguments:
      -h, --help            show this help message and exit
      -U SSH_USERNAME, --username SSH_USERNAME
                            SSH server account username
      -p SSH_PORT, --server_port SSH_PORT
                            SSH server TCP port (default: 22)
      -P SSH_PASSWORD, --password SSH_PASSWORD
                            SSH server account password
      -R IP:PORT [IP:PORT ...], --remote_bind_address IP:PORT [IP:PORT ...]
                            Remote bind address sequence: ip_1:port_1 ip_2:port_2 ... ip_n:port_n
                            Equivalent to ssh -Lxxxx:IP_ADDRESS:PORT
                            If port is omitted, defaults to 22.
                            Example: -R 10.10.10.10: 10.10.10.10:5900
      -L [IP:PORT [IP:PORT ...]], --local_bind_address [IP:PORT [IP:PORT ...]]
                            Local bind address sequence: ip_1:port_1 ip_2:port_2 ... ip_n:port_n
                            Elements may also be valid UNIX socket domains:
                            /tmp/foo.sock /tmp/bar.sock ... /tmp/baz.sock
                            Equivalent to ssh -LPORT:xxxxxxxxx:xxxx, being the local IP address optional.
                            By default it will listen in all interfaces (0.0.0.0) and choose a random port.
                            Example: -L :40000
      -k SSH_HOST_KEY, --ssh_host_key SSH_HOST_KEY
                            Gateway's host key
      -K KEY_FILE, --private_key_file KEY_FILE
                            RSA/DSS/ECDSA private key file
      -S KEY_PASSWORD, --private_key_password KEY_PASSWORD
                            RSA/DSS/ECDSA private key password
      -t, --threaded        Allow concurrent connections to each tunnel
      -v, --verbose         Increase output verbosity (default: ERROR)
      -V, --version         Show version number and quit
      -x IP:PORT, --proxy IP:PORT
                            IP and port of SSH proxy to destination
      -c SSH_CONFIG_FILE, --config SSH_CONFIG_FILE
                            SSH configuration file, defaults to ~/.ssh/config
      -z, --compress        Request server for compression over SSH transport
      -n, --noagent         Disable looking for keys from an SSH agent
      -d [FOLDER [FOLDER ...]], --host_pkey_directories [FOLDER [FOLDER ...]]
                            List of directories where SSH pkeys (in the format `id_*`) may be found

.. _Pahaz: https://github.com/pahaz
.. _sshtunnel: https://pypi.python.org/pypi/sshtunnel
.. _paramiko: http://www.paramiko.org/
.. |CircleCI| image:: https://circleci.com/gh/pahaz/sshtunnel.svg?style=svg
   :target: https://circleci.com/gh/pahaz/sshtunnel
.. |AppVeyor| image:: https://ci.appveyor.com/api/projects/status/oxg1vx2ycmnw3xr9?svg=true&passingText=Windows%20-%20OK&failingText=Windows%20-%20Fail
   :target: https://ci.appveyor.com/project/pahaz/sshtunnel
.. |readthedocs| image:: https://readthedocs.org/projects/sshtunnel/badge/?version=latest
   :target: http://sshtunnel.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status
.. |coveralls| image:: https://coveralls.io/repos/github/pahaz/sshtunnel/badge.svg?branch=master
   :target: https://coveralls.io/github/pahaz/sshtunnel?branch=master
.. |pyversions| image:: https://img.shields.io/pypi/pyversions/sshtunnel.svg
.. |version| image:: https://img.shields.io/pypi/v/sshtunnel.svg
   :target: `sshtunnel`_
.. |license| image::  https://img.shields.io/pypi/l/sshtunnel.svg
   :target: https://github.com/pahaz/sshtunnel/blob/master/LICENSE
