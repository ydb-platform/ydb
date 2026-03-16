===============
django-tastypie
===============

.. image:: https://readthedocs.org/projects/django-tastypie/badge/
    :target: https://django-tastypie.readthedocs.io/
    :alt: Docs

.. image:: https://github.com/django-tastypie/django-tastypie/actions/workflows/python-package.yml/badge.svg
    :target: https://github.com/django-tastypie/django-tastypie/actions
    :alt: CI

.. image:: https://coveralls.io/repos/django-tastypie/django-tastypie/badge.svg?service=github
    :target: https://coveralls.io/github/django-tastypie/django-tastypie
    :alt: Code Coverage

.. image:: https://img.shields.io/pypi/v/django-tastypie.svg
    :target: https://pypi.python.org/pypi/django-tastypie
    :alt: Version

.. image:: https://pypi-badges.global.ssl.fastly.net/svg?package=django-tastypie&timeframe=monthly
    :target: https://pypi.python.org/pypi/django-tastypie
    :alt: Downloads

Creating delicious APIs for Django apps since 2010.

Currently in beta but being used actively in production on several
sites.


Requirements
============

Core
----

* Python 3.8+ (Whatever is supported by your version of Django)
* Django 4.2 (LTS releases) or Django 4.0+ (intermediate releases, including 5.0 and 5.1)
* dateutil (http://labix.org/python-dateutil) >= 2.1

Format Support
--------------

* XML: lxml 3 (http://lxml.de/) and defusedxml (https://pypi.python.org/pypi/defusedxml)
* YAML: pyyaml (http://pyyaml.org/)
* binary plist: biplist (https://bitbucket.org/wooster/biplist)

Optional
--------

* HTTP Digest authentication: python3-digest (https://bitbucket.org/akoha/python-digest/)


What's It Look Like?
====================

A basic example looks like:

.. code:: python

    # myapp/api.py
    # ============
    from tastypie.resources import ModelResource
    from myapp.models import Entry


    class EntryResource(ModelResource):
        class Meta:
            queryset = Entry.objects.all()


    # urls.py
    # =======
    from django.urls.conf import re_path, include
    from tastypie.api import Api
    from myapp.api import EntryResource

    v1_api = Api(api_name='v1')
    v1_api.register(EntryResource())

    urlpatterns = [
        # The normal jazz here then...
        re_path(r'^api/', include(v1_api.urls)),
    ]

That gets you a fully working, read-write API for the ``Entry`` model that
supports all CRUD operations in a RESTful way. JSON/XML/YAML support is already
there, and it's easy to add related data/authentication/caching.

You can find more in the documentation at
https://django-tastypie.readthedocs.io/.


Why Tastypie?
=============

There are other API frameworks out there for Django. You need to
assess the options available and decide for yourself. That said, here are some
common reasons for tastypie.

* You need an API that is RESTful and uses HTTP well.
* You want to support deep relations.
* You DON'T want to have to write your own serializer to make the output right.
* You want an API framework that has little magic, very flexible and maps well to
  the problem domain.
* You want/need XML serialization that is treated equally to JSON (and YAML is
  there too).


Reference Material
==================

* https://django-tastypie.readthedocs.io/en/latest/
* https://github.com/django-tastypie/django-tastypie/tree/master/tests/basic shows
  basic usage of tastypie
* http://en.wikipedia.org/wiki/REST
* http://en.wikipedia.org/wiki/List_of_HTTP_status_codes
* http://www.ietf.org/rfc/rfc2616.txt
* http://jacobian.org/writing/rest-worst-practices/


Getting Help
============

There are two primary ways of getting help.

1. Go to `StackOverflow`_ and post a question with the ``tastypie`` tag.
2. We have an IRC channel (`#tastypie on irc.freenode.net`_) to get help,
   bounce an idea by us, or generally shoot the breeze.

.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/tastypie
.. _#tastypie on irc.freenode.net: irc://irc.freenode.net/tastypie


Security
========

Tastypie is committed to providing a flexible and secure API, and was designed
with many security features and options in mind. Due to the complex nature of
APIs and the constant discovery of new attack vectors and vulnerabilities,
no software is immune to security holes. We rely on our community to report
and help us investigate security issues.

If you come across a security hole **please do not open a Github issue**.
Instead, **drop us an email** at ``tastypie-security@googlegroups.com``

We'll then work together to investigate and resolve the problem so we can
announce a solution along with the vulnerability.
