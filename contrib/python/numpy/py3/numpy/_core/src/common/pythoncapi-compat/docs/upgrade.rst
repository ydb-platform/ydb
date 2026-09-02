++++++++++++++++++++++++++++
upgrade_pythoncapi.py script
++++++++++++++++++++++++++++

``upgrade_pythoncapi.py`` requires Python 3.6 or newer.

Usage
=====

Run the with no arguments to display command line options and list available
operations::

    python3 upgrade_pythoncapi.py

Select files and directories
----------------------------

To upgrade ``module.c`` file, type::

    python3 upgrade_pythoncapi.py module.c

To upgrade all C and C++ files (``.c``, ``.h``, ``.cc``, ``.cpp``, ``.cxx`` and
``.hpp`` files) in the ``directory/`` directory, type::

    python3 upgrade_pythoncapi.py directory/

Multiple filenames an directories can be specified on the command line.

Files are modified in-place! If a file is modified, a copy of the original file
is created with the ``.old`` suffix.

Select operations
-----------------

To only replace ``op->ob_type`` with ``Py_TYPE(op)``, select the ``Py_TYPE``
operation with::

    python3 upgrade_pythoncapi.py -o Py_TYPE module.c

Or the opposite, to apply all operations but leave ``op->ob_type`` unchanged,
deselect the ``Py_TYPE`` operation with::

    python3 upgrade_pythoncapi.py -o all,-Py_TYPE module.c

Download pythoncapi_compat.h
----------------------------

Most ``upgrade_pythoncapi.py`` operations add ``#include
"pythoncapi_compat.h"``. You may have to download the ``pythoncapi_compat.h``
header file to your project. It can be downloaded with::

    python3 upgrade_pythoncapi.py --download PATH


Upgrade Operations
==================

``upgrade_pythoncapi.py`` implements the following operations:

Py_TYPE
-------

* Replace ``op->ob_type`` with ``Py_TYPE(op)``.

Py_SIZE
-------

* Replace ``op->ob_size`` with ``Py_SIZE(op)``.

Py_REFCNT
---------

* Replace ``op->ob_refcnt`` with ``Py_REFCNT(op)``.

Py_SET_TYPE
-----------

* Replace ``obj->ob_type = type;`` with ``Py_SET_TYPE(obj, type);``.
* Replace ``Py_TYPE(obj) = type;`` with ``Py_SET_TYPE(obj, type);``.

Py_SET_SIZE
-----------

* Replace ``obj->ob_size = size;`` with ``Py_SET_SIZE(obj, size);``.
* Replace ``Py_SIZE(obj) = size;`` with ``Py_SET_SIZE(obj, size);``.

Py_SET_REFCNT
-------------

* Replace ``obj->ob_refcnt = refcnt;`` with ``Py_SET_REFCNT(obj, refcnt);``.
* Replace ``Py_REFCNT(obj) = refcnt;`` with ``Py_SET_REFCNT(obj, refcnt);``.

Py_Is
-----

* Replace ``x == Py_None`` with ``Py_IsNone(x)``.
* Replace ``x == Py_True`` with ``Py_IsTrue(x)``.
* Replace ``x == Py_False`` with ``Py_IsFalse(x)``.
* Replace ``x != Py_None`` with ``!Py_IsNone(x)``.
* Replace ``x != Py_True`` with ``!Py_IsTrue(x)``.
* Replace ``x != Py_False`` with ``!Py_IsFalse(x)``.

PyObject_NEW
------------

* Replace ``PyObject_NEW(...)`` with ``PyObject_New(...)``.
* Replace ``PyObject_NEW_VAR(...)`` with ``PyObject_NewVar(...)``.

PyMem_MALLOC
------------

* Replace ``PyMem_MALLOC(n)`` with ``PyMem_Malloc(n)``.
* Replace ``PyMem_REALLOC(ptr, n)`` with ``PyMem_Realloc(ptr, n)``.
* Replace ``PyMem_FREE(ptr)``, ``PyMem_DEL(ptr)`` and ``PyMem_Del(ptr)`` .
  with ``PyMem_Free(n)``.

PyObject_MALLOC
---------------

* Replace ``PyObject_MALLOC(n)`` with ``PyObject_Malloc(n)``.
* Replace ``PyObject_REALLOC(ptr, n)`` with ``PyObject_Realloc(ptr, n)``.
* Replace ``PyObject_FREE(ptr)``, ``PyObject_DEL(ptr)``
  and ``PyObject_Del(ptr)`` .  with ``PyObject_Free(n)``.

PyFrame_GetBack
---------------

* Replace ``frame->f_back`` with ``_PyFrame_GetBackBorrow(frame)``.

PyFrame_GetCode
---------------

* Replace ``frame->f_code`` with ``_PyFrame_GetCodeBorrow(frame)``.

PyThreadState_GetInterpreter
----------------------------

* Replace ``tstate->interp`` with ``PyThreadState_GetInterpreter(tstate)``.

PyThreadState_GetFrame
----------------------

* Replace ``tstate->frame`` with ``_PyThreadState_GetFrameBorrow(tstate)``.

Experimental operations
-----------------------

The following operations are experimental (ex: can introduce compiler warnings)
and so not included in the ``all`` group, they have to be selected explicitly.
Example: ``-o all,Py_SETREF``.

Experimental operations:

* ``Py_NewRef``:

  * Replace ``Py_INCREF(res); return res;`` with ``return Py_NewRef(res);``
  * Replace ``x = y; Py_INCREF(x);`` with ``x = Py_NewRef(y);``
  * Replace ``x = y; Py_INCREF(y);`` with ``x = Py_NewRef(y);``
  * Replace ``Py_INCREF(y); x = y;`` with ``x = Py_NewRef(y);``

* ``Py_CLEAR``:

  * Replace ``Py_XDECREF(var); var = NULL;`` with ``Py_CLEAR(var);``

* ``Py_SETREF``:

  * Replace ``Py_DECREF(x); x = y;`` with ``Py_SETREF(x, y);``
  * Replace ``Py_XDECREF(x); x = y;`` with ``Py_XSETREF(x, y);``
