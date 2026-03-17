# 🌱 Turn any object into a module 🌱

Callable modules!  Indexable modules!?

Ever wanted to call a module directly, or index it?  Or just sick of seeing
`from foo import foo` in your examples?

Give your module the awesome power of an object, or maybe just save a
little typing, with `xmod`.

`xmod` is a tiny library that lets a module to do things that normally
only a class could do - handy for modules that "just do one thing".

## Example: Make a module callable like a function!

    # In your_module.py
    import xmod

    @xmod
    def a_function():
        return 'HERE!!'


    # Test at the command line
    >>> import your_module
    >>> your_module()
    HERE!!

## Example: Make a module look like a list!?!

    # In your_module.py
    import xmod

    xmod(list(), __name__)

    # Test at the command line
    >>> import your_module
    >>> assert your_module == []
    >>> your_module.extend(range(3))
    >>> print(your_module)
    [0, 1, 2]


### [API Documentation](https://rec.github.io/xmod#xmod--api-documentation)
