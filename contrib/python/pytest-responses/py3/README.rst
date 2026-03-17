pytest-responses
================

.. image:: https://img.shields.io/pypi/v/pytest-responses.svg
    :target: https://pypi.python.org/pypi/pytest-responses/
    
.. image:: https://github.com/getsentry/pytest-responses/workflows/Test/badge.svg
    :target: https://github.com/getsentry/pytest-responses/actions/workflows/test.yml

Automatically activate responses across your py.test-powered test suite (thus preventing HTTP requests).

.. sourcecode:: shell

    $ pip install pytest-responses

If particular tests need access to external domains, you can use the ``withoutresponses`` marker:

.. sourcecode:: python

    @pytest.mark.withoutresponses
    def test_disabled():
        with pytest.raises(ConnectionError):
            requests.get('http://responses.invalid')

        assert len(responses.calls) == 0


Additionally, you can use the responses fixture:

.. sourcecode:: python

    def test_enabled(responses):
        with pytest.raises(ConnectionError):
            requests.get('http://responses.invalid')

        assert len(responses.calls) == 1
