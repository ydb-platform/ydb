==========
WhiteNoise
==========

.. image:: https://img.shields.io/readthedocs/whitenoise?style=for-the-badge
   :target: https://whitenoise.readthedocs.io/en/latest/

.. image:: https://img.shields.io/github/actions/workflow/status/evansd/whitenoise/main.yml.svg?branch=master&style=for-the-badge
   :target: https://github.com/evansd/whitenoise/actions?workflow=CI

.. image:: https://img.shields.io/badge/Coverage-96%25-success?style=for-the-badge
   :target: https://github.com/evansd/whitenoise/actions?workflow=CI

.. image:: https://img.shields.io/pypi/v/whitenoise.svg?style=for-the-badge
   :target: https://pypi.org/project/whitenoise/

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg?style=for-the-badge
   :target: https://github.com/psf/black

.. image:: https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white&style=for-the-badge
   :target: https://github.com/pre-commit/pre-commit
   :alt: pre-commit

**Radically simplified static file serving for Python web apps**

With a couple of lines of config WhiteNoise allows your web app to serve its
own static files, making it a self-contained unit that can be deployed anywhere
without relying on nginx, Amazon S3 or any other external service. (Especially
useful on Heroku, OpenShift and other PaaS providers.)

It's designed to work nicely with a CDN for high-traffic sites so you don't have to
sacrifice performance to benefit from simplicity.

WhiteNoise works with any WSGI-compatible app but has some special auto-configuration
features for Django.

WhiteNoise takes care of best-practices for you, for instance:

* Serving compressed content (gzip and Brotli formats, handling Accept-Encoding
  and Vary headers correctly)
* Setting far-future cache headers on content which won't change

Worried that serving static files with Python is horribly inefficient?
Still think you should be using Amazon S3? Have a look at the `Infrequently
Asked Questions`_.

To get started, see the documentation_.

.. _Infrequently Asked Questions: https://whitenoise.readthedocs.io/en/stable/#infrequently-asked-questions
.. _documentation: https://whitenoise.readthedocs.io/en/stable/
