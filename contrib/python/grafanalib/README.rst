===============================
Getting Started with grafanalib
===============================

.. image:: https://readthedocs.org/projects/grafanalib/badge/?version=main
    :alt: Documentation Status
    :scale: 100%
    :target: https://grafanalib.readthedocs.io/en/main

Do you like `Grafana <http://grafana.org/>`_ but wish you could version your
dashboard configuration? Do you find yourself repeating common patterns? If
so, grafanalib is for you.

grafanalib lets you generate Grafana dashboards from simple Python scripts.

How it works
============

Take a look at `the examples directory
<https://github.com/weaveworks/grafanalib/blob/main/grafanalib/tests/examples/>`_,
e.g. `this dashboard
<https://github.com/weaveworks/grafanalib/blob/main/grafanalib/tests/examples/example.dashboard.py>`_
will configure a dashboard with a single row, with one QPS graph broken down
by status code and another latency graph showing median and 99th percentile
latency.

In the code is a fair bit of repetition here, but once you figure out what
works for your needs, you can factor that out.
See `our Weave-specific customizations
<https://github.com/weaveworks/grafanalib/blob/main/grafanalib/weave.py>`_
for inspiration.

You can read the entire grafanlib documentation on `readthedocs.io
<https://grafanalib.readthedocs.io/>`_.

Getting started
===============

grafanalib is just a Python package, so:

.. code-block:: console

  $ pip install grafanalib


Generate the JSON dashboard like so:

.. code-block:: console

  $ curl -o example.dashboard.py https://raw.githubusercontent.com/weaveworks/grafanalib/main/grafanalib/tests/examples/example.dashboard.py
  $ generate-dashboard -o frontend.json example.dashboard.py


Support
=======

This library is in its very early stages. We'll probably make changes that
break backwards compatibility, although we'll try hard not to.

grafanalib works with Python 3.6 through 3.11.

Developing
==========
If you're working on the project, and need to build from source, it's done as follows:

.. code-block:: console

  $ virtualenv .env
  $ . ./.env/bin/activate
  $ pip install -e .

Configuring Grafana Datasources
===============================

This repo used to contain a program ``gfdatasource`` for configuring
Grafana data sources, but it has been retired since Grafana now has a
built-in way to do it.  See https://grafana.com/docs/administration/provisioning/#datasources

Community
=========

We currently don't follow a roadmap for ``grafanalib`` and both `maintainers
<https://github.com/weaveworks/grafanalib/blob/main/MAINTAINERS>` have recently
become somewhat occupied otherwise.

We'd like you to join the ``grafanalib`` community! If you would like to
help out maintaining ``grafanalib`` that would be great. It's a fairly laid-back
and straight-forward project. Please talk to us on Slack (see the links below).

We follow the `CNCF Code of Conduct </docs/CODE_OF_CONDUCT.rst>`_.

Getting Help
------------

If you have any questions about, feedback for or problems with ``grafanalib``:

- Read the documentation at https://grafanalib.readthedocs.io
- Invite yourself to the `Weave Users Slack <https://slack.weave.works/>`_.
- Ask a question on the `#grafanalib <https://weave-community.slack.com/messages/grafanalib/>`_ slack channel.
- `File an issue <https://github.com/weaveworks/grafanalib/issues/new>`_.

Your feedback is always welcome!
