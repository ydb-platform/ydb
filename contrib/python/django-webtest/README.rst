==============
django-webtest
==============

.. image:: https://img.shields.io/pypi/v/django-webtest.svg
   :target: https://pypi.python.org/pypi/django-webtest
   :alt: PyPI Version

.. image:: https://img.shields.io/github/license/kmike/django-webtest.svg
   :target: https://github.com/django-webtest/django-webtest/blob/master/LICENSE.txt
   :alt: License

.. image:: https://img.shields.io/travis/django-webtest/django-webtest/master.svg
   :target: http://travis-ci.org/django-webtest/django-webtest
   :alt: Build Status

django-webtest is an app for instant integration of Ian Bicking's
WebTest (http://docs.pylonsproject.org/projects/webtest/) with Django's
testing framework.

Installation
============

.. code-block:: console

    $ pip install django-webtest

Usage
=====

.. code-block:: python

    from django_webtest import WebTest

    class MyTestCase(WebTest):

        # optional: we want some initial data to be able to login
        fixtures = ['users', 'blog_posts']

        # optional: default extra_environ for this TestCase
        extra_environ = {'HTTP_ACCEPT_LANGUAGE': 'ru'}

        def testBlog(self):
            # pretend to be logged in as user `kmike` and go to the index page
            index = self.app.get('/', user='kmike')

            # All the webtest API is available. For example, we click
            # on a <a href='/tech-blog/'>Blog</a> link, check that it
            # works (result page doesn't raise exceptions and returns 200 http
            # code) and test if result page have 'My Article' text in
            # its body.
            assert 'My Article' in index.click('Blog')

django-webtest provides a django.test.TestCase subclass
(``django_webtest.WebTest``) that creates ``webtest.TestApp`` around
django wsgi interface and makes it available in tests as ``self.app``.

It also features an optional ``user`` argument for ``self.app.get``,
``self.app.post``, etc. to help making authorized requests. This argument
should be a django.contrib.auth.models.User instance or a string with user's
``username`` for the user who is supposed to be logged in. To log out again,
call ``self.app.reset``, clearing all cookies.  To make a bunch of calls
with the same user, call ``app.set_user(user)`` before your requests; if
you want to disable that user, call ``app.get(..., user=None)`` for one
request or ``app.set_user(None)`` to unset the user for all following calls.

For 500 errors original traceback is shown instead of usual html result
from handler500.

You also get the ``response.templates`` and ``response.context`` goodness that
is usually only available if you use Django's native test client. These
attributes contain a list of templates that were used to render the response
and the context used to render these templates. All of Django's native asserts (
``assertFormError``,  ``assertTemplateUsed``, ``assertTemplateNotUsed``,
``assertContains``, ``assertNotContains``, ``assertRedirects``) are
also supported for WebTest responses.

The session dictionary is available via ``self.app.session``, and has the
same content than Django's native test client.

Unlike Django's native test client CSRF checks are not suppressed
by default so missing CSRF tokens will cause test fails (and that's good).

If forms are submitted via WebTest forms API then all form fields (including
CSRF token) are submitted automagically:

.. code-block:: python

    class AuthTest(WebTest):
        fixtures = ['users.json']

        def test_login(self):
            form = self.app.get(reverse('auth_login')).form
            form['username'] = 'foo'
            form['password'] = 'bar'
            response = form.submit().follow()
            self.assertEqual(response.context['user'].username, 'foo')

However if forms are submitted via raw POST requests using ``app.post`` then
csrf tokens become hard to construct. CSRF checks can be disabled by setting
``csrf_checks`` attribute to False in this case:

.. code-block:: python

    class MyTestCase(WebTest):
        csrf_checks = False

        def test_post(self):
            self.app.post('/')

When a subclass of Django's ``TransactionTestCase`` is desired,
use ``django_webtest.TransactionWebTest``.

For disabling CSRF checks in a ``pytest-django`` fixture, see
`Usage with PyTest`_.

All of these features can be easily set up manually (thanks to WebTest
architecture) and they are even not neccessary for using WebTest with Django but
it is nice to have some sort of integration instantly.

See http://docs.pylonsproject.org/projects/webtest/ for API help. Webtest can
follow links, submit forms, parse html, xml and json responses with different
parsing libraries, upload files and more.

Integration with django-rest-framework
======================================

If your project uses django-rest-framework__, the setting
``REST_FRAMEWORK['AUTHENTICATION_CLASSES']`` will be patched
automatically to include a class that links the rest-framework
authentication system with ``app.get(user=user)``.

.. __: https://www.django-rest-framework.org/

Usage with PyTest
=================

You need to install `pytest-django <https://pytest-django.readthedocs.io>`_:

.. code-block:: console

    $ pip install pytest-django

Then you can use ``django-webtest``'s fixtures:

.. code-block:: python

    def test_1(django_app):
        resp = django_app.get('/')
        assert resp.status_code == 200, 'Should return a 200 status code'

We have a ``django_app_factory`` fixture we can use to create custom fixtures.
For example, one that doesn't do CSRF checks:

.. code-block:: python

    # conftest.py

    @pytest.fixture
    def csrf_exempt_django_app(django_app_factory):
        return django_app_factory(csrf_checks=False)

``csrf_checks`` and ``extra_environ`` are the only arguments to
``django_app_factory``.


Why?
====

While django.test.client.Client is fine for its purposes, it is not
well-suited for functional or integration testing. From Django's test client
docstring:

    This is not intended as a replacement for Twill/Selenium or
    the like - it is here to allow testing against the
    contexts and templates produced by a view, rather than the
    HTML rendered to the end-user.

WebTest plays on the same field as twill. WebTest has a nice API,
is fast, small, talks to the django application via WSGI instead of HTTP
and is an easy way to write functional/integration/acceptance tests.
django-webtest is able to provide access to the names of rendered templates
and template context just like native Django TestClient.

Contributing
============

Development happens at github: https://github.com/django-webtest/django-webtest
Issue tracker: https://github.com/django-webtest/django-webtest/issues

Feel free to submit ideas, bugs or pull requests.

Running tests
-------------

Make sure `tox`_ is installed and run:

.. code-block:: console

    $ tox

from the source checkout.

.. _tox: http://tox.testrun.org
