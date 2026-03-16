====
lazy
====
----------------------------------
Lazy attributes for Python objects
----------------------------------

Package Contents
================

@lazy
    A decorator to create lazy attributes.

Overview
========

Lazy attributes are computed attributes that are evaluated only
once, the first time they are used.  Subsequent uses return the
results of the first call. They come handy when code should run

- *late*, i.e. just before it is needed, and
- *once*, i.e. not twice, in the lifetime of an object.

You can think of it as *deferred initialization*.
The possibilities are endless.

Typing
======

The decorator is fully typed. Type checkers can infer the type of
a lazy attribute from the return value of the decorated method.

Examples
========

The class below creates its ``store`` resource lazily:

.. code-block:: python

    from lazy import lazy

    class FileUploadTmpStore(object):

        @lazy
        def store(self):
            location = settings.get('fs.filestore')
            return FileSystemStore(location)

        def put(self, uid, fp):
            self.store.put(uid, fp)
            fp.seek(0)

        def get(self, uid, default=None):
            return self.store.get(uid, default)

        def close(self):
            if 'store' in self.__dict__:
                self.store.close()

Another application area is caching:

.. code-block:: python

    class PersonView(View):

        @lazy
        def person_id(self):
            return self.request.get('person_id', -1)

        @lazy
        def person_data(self):
            return self.session.query(Person).get(self.person_id)

Documentation
=============

For further details please refer to the `API Documentation`_.

.. _`API Documentation`: https://lazy.readthedocs.io/en/stable/

