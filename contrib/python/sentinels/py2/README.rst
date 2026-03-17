Overview
--------

The sentinels module is a small utility providing the Sentinel class, along with useful instances.

What are Sentinels?
-------------------

Sentinels are objects with special meanings. They can be thought of as singletons, but they service the need of having 'special' values in your code, that have special meanings (see example below).

Why Do I Need Sentinels?
------------------------

Let's take *NOTHING* for example. This sentinel is automatically provided with the sentinels import::

  >>> from sentinels import NOTHING

Let's say you're writing a wrapper around a Python dictionary, which supports a special kind of method, *get_default_or_raise*. This method behaves like *get*, but when it does not receive a default and the key does not exist, it raises a *KeyError*. How would you implement such a thing? The naive method is this::

  >>> class MyDict(dict):
  ...     def get_default_or_raise(self, key, default=None):
  ...         if key not in self and default is None:
  ...             raise KeyError(key)
  ...         return self.get(key, default)

Or even this::

  >>> class MyDict(dict):
  ...     def get_default_or_raise(self, key, default=None):
  ...         returned = self.get(key, default)
  ...         if returned is None:
  ...             raise KeyError(key)
  ...         return returned

But the problem with the above two pieces of code is the same -- when writing a general utility class, we don't know how it will be used later on. More importantly, **None might be a perfectly valid dictionary value!**

This is where NOTHING comes in handy::


  >>> class MyDict(dict):
  ...     def get_default_or_raise(self, key, default=NOTHING):
  ...         returned = self.get(key, default)
  ...         if returned is NOTHING:
  ...             raise KeyError(key)
  ...         return returned

And Tada!

Semantics
---------

Sentinels are always equal to themselves::

  >>> NOTHING == NOTHING
  True

But never to another object::

  >>> from sentinels import Sentinel
  >>> NOTHING == 2
  False
  >>> NOTHING == "NOTHING"
  False

Copying sentinels returns the same object::

  >>> import copy
  >>> copy.deepcopy(NOTHING) is NOTHING
  True

And of course also pickling/unpickling::

  >>> import pickle
  >>> NOTHING is pickle.loads(pickle.dumps(NOTHING))
  True
