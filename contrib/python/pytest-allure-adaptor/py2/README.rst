Allure Pytest Adaptor
=====================

.. image:: https://badges.gitter.im/Join%20Chat.svg
   :alt: Join the chat at https://gitter.im/allure-framework/allure-core
   :target: https://gitter.im/allure-framework/allure-core?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

.. image:: https://travis-ci.org/allure-framework/allure-pytest.svg?branch=master
        :alt: Build Status
        :target: https://travis-ci.org/allure-framework/allure-pytest/
.. image:: https://pypip.in/v/pytest-allure-adaptor/badge.png
        :alt: Release Status
        :target: https://pypi.python.org/pypi/pytest-allure-adaptor
.. image:: https://pypip.in/d/pytest-allure-adaptor/badge.png
        :alt: Downloads
        :target: https://pypi.python.org/pypi/pytest-allure-adaptor

This repository contains a plugin for ``py.test`` which automatically prepares input data used to generate ``Allure Report``.
**Note:** this plugin currently supports only Allure 1.4.x series.

Installation and Usage
======================
.. code:: python

 py.test --alluredir [path_to_report_dir]
 # WARNING [path_to_report_dir] will be purged at first run


This plugin gets automatically connected to ``py.test`` via ``entry point`` if installed.

Connecting to IDE:

.. code:: python

 pytest_plugins = 'allure.pytest_plugin',\


Allure Features
===============

Attachments
===========

To attach some content to test report:

.. code:: python

 import allure

 def test_foo():
     allure.attach('my attach', 'Hello, World')


Steps
=====

To divide a test into steps:

.. code:: python

 import pytest

 def test_foo():
     with pytest.allure.step('step one'):
         # do stuff

     with pytest.allure.step('step two'):
         # do more stuff


Can also be used as decorators. By default step name is generated from method name:

.. code:: python

 import pytest

 @pytest.allure.step
 def make_test_data_foo():
     # do stuff

 def test_foo():
     assert make_some_data_foo() is not None

 @pytest.allure.step('make_some_data_foo')
 def make_some_data_bar():
     # do another stuff

 def test_bar():
     assert make_some_data_bar() is not None


Steps can also be used without pytest. In that case instead of ``pytest.allure.step`` simply use ``allure.step``:

.. code:: python

 import allure

 @allure.step('some operation')
 def do_operation():
     # do stuff


``allure.step`` decorator supports step name formatting with function parameters:

.. code:: python

 import allure

 @allure.step('some operation for bar={0}')
 def do_operation(bar):
     # do stuff
     
 def test_foo():
     assert do_operation('abcdef')

The step in the latter case will have name ``some operation for bar=abcdef``. 
Formatting is done via python's built-in ``string.format`` and supports it's options. 
Arguments are passed to ``format`` method in the same way they are passed to the decorated function.

Steps support is limited when used with fixtures.


Severity
========

Any test, class or module can be marked with different severity:

.. code:: python

 import pytest

 @pytest.allure.severity(pytest.allure.severity_level.MINOR)
 def test_minor():
     assert False


 @pytest.allure.severity(pytest.allure.severity_level.CRITICAL)
 class TestBar:

     # will have CRITICAL priority
     def test_bar(self):
         pass

     # will have BLOCKER priority via a short-cut decorator
     @pytest.allure.BLOCKER
     def test_bar(self):
         pass


To run tests with concrete priority:

.. code:: rest

 py.test my_tests/ --allure_severities=critical,blocker


Issues
======
Issues can be set for test.

.. code:: python

 import pytest

 @pytest.allure.issue('http://jira.lan/browse/ISSUE-1')
 def test_foo():
     assert False


 import allure

 @allure.issue('http://jira.lan/browse/ISSUE-2')
 class TestBar:

     # test will have ISSUE-2, ISSUE-3 and ISSUE-4 label
     @allure.issue('http://jira.lan/browse/ISSUE-3')
     def test_bar1(self):
         # You can use this feature like a function inside the test
         allure.dynamic_issue('http://jira.lan/browse/ISSUE-4')
         pass

     # test will have only ISSUE-2 label
     def test_bar2(self):
         pass


Test cases
==========
Test cases links can be set for test also.

.. code:: python

 import pytest

 @pytest.allure.testcase('http://my.tms.org/TESTCASE-1')
 def test_foo():
     assert False


 import allure

 @allure.testcase('http://my.tms.org/browse/TESTCASE-2')
 class TestBar:

     # test will have TESTCASE-2 and TESTCASE-3 label
     @allure.testcase('TESTCASE-3')
     def test_bar1(self):
         pass

     # test will have only TESTCASE-2 label
     def test_bar2(self):
         pass


Features & Stories
==================

Feature and Story can be set for test.

.. code:: python

 import allure


 @allure.feature('Feature1')
 @allure.story('Story1')
 def test_minor():
     assert False


 @allure.feature('Feature2')
 @allure.story('Story2', 'Story3')
 @allure.story('Story4')
 class TestBar:

     # will have 'Feature2 and Story2 and Story3 and Story4'
     def test_bar(self):
         pass


To run tests by Feature or Story:

.. code:: rest

 py.test my_tests/ --allure_features=feature1,feature2
 py.test my_tests/ --allure_features=feature1,feature2 --allure_stories=story1,story2


Environment Parameters
======================

You can provide test environment parameters such as report name, browser or test server address to allure test report.

.. code:: python

 import allure
 import pytest


 def pytest_configure(config):
     allure.environment(report='Allure report', browser=u'Я.Браузер')


 @pytest.fixture(scope="session")
 def app_host_name():
     host_name = "my.host.local"
     allure.environment(hostname=host_name)
     return host_name


 @pytest.mark.parametrize('country', ('USA', 'Germany', u'Россия', u'Япония'))
 def test_minor(country):
     allure.environment(country=country)
     assert country


More details about allure environment you can know from documentation_. 

.. _documentation: https://github.com/allure-framework/allure-core/wiki/Environment


Development
===========

Use ``allure.common.AllureImpl`` class to bind your logic to this adapter.
