If you're a library or framework creator then it is nice to be able to create
APIs that can be used *either* as decorators or context managers.

The contextdecorator module is a backport of new features added to the
`contextlib module <http://docs.python.org/library/contextlib.html>`_ in 
Python 3.2. contextdecorator works with Python 2.4+ including Python 3.

Context managers inheriting from ``ContextDecorator`` have to implement 
``__enter__`` and ``__exit__`` as normal. 
`__exit__ <http://docs.python.org/reference/datamodel.html#object.__exit__>`_ 
retains its optional exception handling even when used as a decorator.

Example::

   from contextdecorator import ContextDecorator

   class mycontext(ContextDecorator):
      def __enter__(self):
         print 'Starting'
         return self

      def __exit__(self, *exc):
         print 'Finishing'
         return False

   @mycontext()
   def function():
      print 'The bit in the middle'
   
   with mycontext():
      print 'The bit in the middle'

Existing context managers that already have a base class can be extended by
using ``ContextDecorator`` as a mixin class::

   from contextdecorator import ContextDecorator

   class mycontext(ContextBaseClass, ContextDecorator):
      def __enter__(self):
         return self

      def __exit__(self, *exc):
         return False

contextdecorator also contains an implementation of `contextlib.contextmanager
<http://docs.python.org/library/contextlib.html#contextlib.contextmanager>`_
that uses ``ContextDecorator``. The context managers it creates can be used as
decorators as well as in statements. ::

   from contextdecorator import contextmanager
   
   @contextmanager
   def mycontext(*args):
      print 'started'
      try:
         yield
      finally:
         print 'finished!'
   
   @mycontext('some', 'args')
   def function():
      print 'In the middle'
      
   with mycontext('some', 'args'):
      print 'In the middle'


Repository and issue tracker:

* `contextdecorator on google code <http://code.google.com/p/contextdecorator/>`_

The project is available for download from `PyPI <http://pypi.python.org/pypi/contextdecorator>`_
so it can be easily installed:

    | ``pip install -U contextdecorator``
    | ``easy_install -U contextdecorator``

The tests require `unittest2 <http://pypi.python.org/pypi/unittest2>`_
to run.

