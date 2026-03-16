pytest-responsemock
===================
https://github.com/idlesign/pytest-responsemock

|release| |lic| |coverage|

.. |release| image:: https://img.shields.io/pypi/v/pytest-responsemock.svg
    :target: https://pypi.python.org/pypi/pytest-responsemock

.. |lic| image:: https://img.shields.io/pypi/l/pytest-responsemock.svg
    :target: https://pypi.python.org/pypi/pytest-responsemock

.. |coverage| image:: https://img.shields.io/coveralls/idlesign/pytest-responsemock/master.svg
    :target: https://coveralls.io/r/idlesign/pytest-responsemock


Description
-----------

*Simplified requests calls mocking for pytest*

Provides ``response_mock`` fixture, exposing simple context manager.

Any request under that manager will be intercepted and mocked according
to one or more ``rules`` passed to the manager. If actual request won't fall
under any of given rules then an exception is raised (by default).

Rules are simple strings, of the pattern: ``HTTP_METHOD URL -> STATUS_CODE :BODY``.


Requirements
------------

* Python 3.7+


Usage
-----

When this package is installed ``response_mock`` is available for ``pytest`` test functions.

.. code-block:: python

    def for_test():
        return requests.get('http://some.domain')


    def test_me(response_mock):

        # Pass response rule as a string,
        # or many rules (to mock consequent requests) as a list of strings/bytes.
        # Use optional `bypass` argument to disable mock conditionally.

        with response_mock('GET http://some.domain -> 200 :Nice', bypass=False):

            result = for_test()

            assert result.ok
            assert result.content == b'Nice'
            
        # mock consequent requests
        with response_mock([
            'GET http://some.domain -> 200 :Nice',
            'GET http://other.domain -> 200 :Sweet',
        ]):
            for_test()
            requests.get('http://other.domain')


Use with ``pytest-datafixtures``:

.. code-block:: python

    def test_me(response_mock, datafix_read):

        with response_mock(f"GET http://some.domain -> 200 :{datafix_read('myresponse.html')}"):
            ...


Describe response header fields using multiline strings:

.. code-block:: python

    with response_mock(
        '''
        GET http://some.domain

        Allow: GET, HEAD
        Content-Language: ru

        -> 200 :OK
        '''
    ):
        ...

Test json response:

.. code-block:: python

    response = json.dumps({'key': 'value', 'another': 'yes'})

    with response_mock(f'POST http://some.domain -> 400 :{response}'):
        ...

To test binary response pass rule as bytes:

.. code-block:: python

    with response_mock(b'GET http://some.domain -> 200 :' + my_bytes):
        ...

Access underlying RequestsMock (from ``responses`` package) as ``mock``:

.. code-block:: python

    with response_mock('HEAD http://some.domain -> 200 :Nope') as mock:

        mock.add_passthru('http://other.domain')

