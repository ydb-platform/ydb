Cobble
======

Cobble is a Python library that allows easy creation of data objects,
including implementations of common methods such as ``__eq__`` and ``__repr__``.

Examples
--------

.. code-block:: python

    import cobble

    @cobble.data
    class Song(object):
        name = cobble.field()
        artist = cobble.field()
        album = cobble.field(default=None)


    song = Song("MFEO", artist="Jack's Mannequin")

    print(song) # Prints "Song(name='MFEO', artist="Jack's Mannequin", album=None)"

.. code-block:: python

    class Expression(object):
        pass

    @cobble.data
    class Literal(Expression):
        value = cobble.field()

    @cobble.data
    class Add(Expression):
        left = cobble.field()
        right = cobble.field()
    
    class Evaluator(cobble.visitor(Expression)):
        def visit_literal(self, literal):
            return literal.value
        
        def visit_add(self, add):
            return self.visit(add.left) + self.visit(add.right)

    Evaluator().visit(Add(Literal(2), Literal(4))) # 6

.. code-block:: python

    class Expression(object):
        pass

    @cobble.visitable
    class Literal(Expression):
        def __init__(self, value):
            self.value = value

    @cobble.visitable
    class Add(Expression):
        def __init__(self, left, right):
            self.left = left
            self.right = right
    
    class Evaluator(cobble.visitor(Expression)):
        def visit_literal(self, literal):
            return literal.value
        
        def visit_add(self, add):
            return self.visit(add.left) + self.visit(add.right)

    Evaluator().visit(Add(Literal(2), Literal(4))) # 6

License
-------

`2-Clause BSD <http://opensource.org/licenses/BSD-2-Clause>`_
