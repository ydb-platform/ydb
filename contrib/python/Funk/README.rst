Funk: A mocking framework for Python
====================================

Funk is a mocking framework for Python, influenced heavily by `JMock <http://www.jmock.org/>`_.
Funk helps to test modules in isolation by allowing mock objects to be used in place of "real" objects.
Funk is licensed under the 2-clause BSD licence.

Installation
------------

.. code-block:: sh

    $ pip install funk

Example
-------

Suppose we have an API for a file storage service.
We want to list the names of all files,
but the API limits the number of names it will return at a time.
Therefore, we need to write some code that will keep making requests to the API
until all names have been retrieved.

.. code-block:: python

    def fetch_names(file_storage):
        has_more = True
        token = None
        names = []
        
        while has_more:
            response = file_storage.names(token=token)
            names += response.names
            token = response.next_token
            has_more = token is not None
        
        return names    
        

    import funk

    @funk.with_mocks
    def test_request_for_names_until_all_names_are_fetched(mocks):
        file_storage = mocks.mock(FileStorage)
        
        mocks.allows(file_storage).names(token=None).returns(mocks.data(
            next_token="<token 1>",
            names=["a", "b"],
        ))
        mocks.allows(file_storage).names(token="<token 1>").returns(mocks.data(
            next_token="<token 2>",
            names=["c", "d"],
        ))
        mocks.allows(file_storage).names(token="<token 2>").returns(mocks.data(
            next_token=None,
            names=["e"],
        ))
        
        assert fetch_names(file_storage) == ["a", "b", "c", "d", "e"]

By using a mock object instead of a real instance of ``FileStorage``,
we can run our tests without a running instance of the file storage system.
We also avoid relying on the implementation of ``FileStorage``,
making our tests more focused and less brittle.

If you're using pytest,
the easiest way to use Funk is as a fixture:

.. code-block:: python

    import funk
    import pytest
    
    @pytest.yield_fixture
    def mocks():
        mocks = funk.Mocks()
        yield mocks
        mocks.verify()
    
    def test_request_for_names_until_all_names_are_fetched(mocks):
        file_storage = mocks.mock(FileStorage)
        ...

Usage
-----

Creating a mock context
^^^^^^^^^^^^^^^^^^^^^^^

Create an instance of ``Mocks`` to allow mock objects to be created.
Call ``Mocks.verify()`` to assert that all expectations have been met.

.. code-block:: python

    import funk
    
    def test_case():
        mocks = funk.Mocks()
        ...
        mocks.verify()

Use the decorator ``funk.with_mocks`` to inject a ``mocks`` argument to a function.
``verify()`` will be automatically invoked at the end of the function.

.. code-block:: python

    import funk
    
    @funk.with_mocks
    def test_case(mocks):
        ...

If using pytest, a fixture is the simplest way to use Funk:

.. code-block:: python

    import funk
    import pytest
    
    @pytest.yield_fixture
    def mocks():
        mocks = funk.Mocks()
        yield mocks
        mocks.verify()
    
    def test_case(mocks):
        ...

Creating mock objects
^^^^^^^^^^^^^^^^^^^^^

Call ``Mocks.mock()`` to create a mock object.

.. code-block:: python

    file_storage = mocks.mock()

If the ``base`` argument is passed,
only methods on that type can be mocked:

.. code-block:: python

    file_storage = mocks.mock(FileStorage)

This can be useful to ensure that only existing methods are mocked,
but should be avoided if generating methods dynamically, such as by using ``__getattr__``.

Set the ``name`` argument to set the name that should be used in assertion failure messages for the mock:

.. code-block:: python

    file_storage = mocks.mock(name="file_storage")

Setting expectations
^^^^^^^^^^^^^^^^^^^^

To set up an expectation, use ``funk.allows()`` or ``funk.expects()``.
For convenience, these functions are also available on ``Mocks``.
``funk.allows()`` will let the method be called any number of times, including none.
``funk.expects()`` will ensure that the method is called exactly once.
For instance:

.. code-block:: python

    allows(file_storage).names

This allows the method ``file_storage.names`` to be called with any arguments
any number of times.
To only allow calls with specific arguments, you can invoke ``.names`` as a method:

.. code-block:: python

    allows(file_storage).names(token="<token 1>")

This will only allow calls with a matching ``token`` keyword argument,
and no other arguments.

You can also use matchers from Precisely_ to match arguments:

.. code-block:: python

    from precisely import instance_of

    allows(file_storage).names(token=instance_of(str))

.. _Precisely: https://pypi.python.org/pypi/precisely

If more than one expectation is set up on the same method,
the first matching expectation is used.
If you need to enforce methods being called in a particular order,
use sequences.

Actions
~~~~~~~

By default, a mocked method returns ``None``.
Use ``returns()`` to return a different value:

.. code-block:: python

    allows(file_storage).names().returns([])

Use ``raises()`` to raise an exception:

.. code-block:: python

    allows(file_storage).names().raises(Exception("Could not connect"))

Sequences
^^^^^^^^^

A sequence object can be created using ``Mocks.sequence``.
The sequencing on objects can then be defined using ``in_sequence(sequence)`` when setting expectations.
For instance:

.. code-block:: python

    file_storage = mocks.mock(FileStorage)
    file_ordering = mocks.sequence()

    expects(file_storage).save(NAME_1, CONTENTS_1).in_sequence(file_ordering)
    expects(file_storage).save(NAME_2, CONTENTS_2).in_sequence(file_ordering)
