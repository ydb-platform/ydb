# JupyterHub Traefik Proxy

[![Documentation build status](https://img.shields.io/readthedocs/jupyterhub-traefik-proxy?logo=read-the-docs)](https://jupyterhub-traefik-proxy.readthedocs.org/en/latest/)
[![GitHub Workflow Status](https://github.com/jupyterhub/traefik-proxy/actions/workflows/test.yml/badge.svg)](https://github.com/jupyterhub/traefik-proxy/actions/workflows/test.yml)
[![Latest PyPI version](https://img.shields.io/pypi/v/jupyterhub-traefik-proxy?logo=pypi)](https://pypi.python.org/pypi/jupyterhub-traefik-proxy)
[![GitHub](https://img.shields.io/badge/issue_tracking-github-blue?logo=github)](https://github.com/jupyterhub/traefik-proxy/issues)
[![Discourse](https://img.shields.io/badge/help_forum-discourse-blue?logo=discourse)](https://discourse.jupyter.org/c/jupyterhub)
[![Gitter](https://img.shields.io/badge/social_chat-gitter-blue?logo=gitter)](https://gitter.im/jupyterhub/jupyterhub)

When JupyterHub starts a server for a user, it will _dynamically configure a
proxy server_ so that accessing `jupyterhub.example.com/user/<user>` routes to
the individual Jupyter server.
This project enables JupyterHub to dynamically configure the routes of a [traefik](https://traefik.io) proxy server!

There are two main implementations of the [JupyterHub proxy
API](https://jupyterhub.readthedocs.io/en/stable/reference/proxy.html),
depending on how traefik stores its routing configuration.

For **smaller**, single-node deployments:

- TraefikFileProviderProxy

For **distributed** setups:

- TraefikRedisProxy

Other implementations are maintained on a best-effort basis due to a lack of well-maintained
Python clients:

- TraefikEtcdProxy
- TraefikConsulProxy

## Installation

The [documentation](https://jupyterhub-traefik-proxy.readthedocs.io) contains a
[complete installation
guide](https://jupyterhub-traefik-proxy.readthedocs.io/en/latest/install.html)
with examples for all implementations, including the recommended
[TraefikRedisProxy](https://jupyterhub-traefik-proxy.readthedocs.io/en/latest/redis.html#example-setup).

## Running tests

You can then run the all the test suite from the _traefik-proxy_ directory with:

```
$ pytest
```

Or you can run a specific test file with:

```
$ pytest tests/<test-file-name>
```

There are some tests that use _etcdctl_ command line client for etcd. Make sure
to set environment variable `ETCDCTL_API=3` before running the tests if etcd
version 3.3 or older is used, so that the v3 API to be used, e.g.:

```
$ export ETCDCTL_API=3
```
