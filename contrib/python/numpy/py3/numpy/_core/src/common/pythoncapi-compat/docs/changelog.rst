Changelog
=========

* 2024-07-18: Add functions:

  * ``PyUnicodeWriter_Create()``
  * ``PyUnicodeWriter_Discard()``
  * ``PyUnicodeWriter_Finish()``
  * ``PyUnicodeWriter_WriteChar()``
  * ``PyUnicodeWriter_WriteUTF8()``
  * ``PyUnicodeWriter_WriteStr()``
  * ``PyUnicodeWriter_WriteRepr()``
  * ``PyUnicodeWriter_WriteSubstring()``
  * ``PyUnicodeWriter_WriteWideChar()``
  * ``PyUnicodeWriter_Format()``

* 2024-06-03: Add ``PyLong_GetSign()``.
* 2024-04-23: Drop Python 3.5 support. It cannot be tested anymore (pip fails).
* 2024-04-02: Add ``PyDict_SetDefaultRef()`` function.
* 2024-03-29: Add ``PyList_GetItemRef()`` function.
* 2024-03-21: Add functions:

  * ``Py_GetConstant()``
  * ``Py_GetConstantBorrowed()``

* 2024-03-09: Add hash constants:

  * ``PyHASH_BITS``
  * ``PyHASH_IMAG``
  * ``PyHASH_INF``
  * ``PyHASH_MODULUS``

* 2024-02-20: Add PyTime API:

  * ``PyTime_t`` type
  * ``PyTime_MIN`` and ``PyTime_MAX`` constants
  * ``PyTime_AsSecondsDouble()``
  * ``PyTime_Monotonic()``
  * ``PyTime_PerfCounter()``
  * ``PyTime_Time()``

* 2023-12-15: Add function ``Py_HashPointer()``.
* 2023-11-14: Add functions:

  * ``PyDict_Pop()``
  * ``PyDict_PopString()``

* 2023-11-13: Add functions:

  * ``PyList_Extend()``
  * ``PyList_Clear()``

* 2023-10-04: Add functions:

  * ``PyUnicode_EqualToUTF8()``
  * ``PyUnicode_EqualToUTF8AndSize()``

* 2023-10-03: Add functions:

  * ``PyObject_VisitManagedDict()``
  * ``PyObject_ClearManagedDict()``
  * ``PyThreadState_GetUnchecked()``

* 2023-09-29: Add functions:

  * ``PyMapping_HasKeyWithError()``
  * ``PyMapping_HasKeyStringWithError()``
  * ``PyObject_HasAttrWithError()``
  * ``PyObject_HasAttrStringWithError()``

* 2023-08-25: Add ``PyDict_ContainsString()`` and ``PyLong_AsInt()`` functions.
* 2023-08-21: Remove support for Python 2.7, Python 3.4 and older.
* 2023-08-16: Add ``Py_IsFinalizing()`` function.
* 2023-07-21: Add ``PyDict_GetItemRef()`` function.
* 2023-07-18: Add ``PyModule_Add()`` function.
* 2023-07-12: Add functions:

  * ``PyObject_GetOptionalAttr()``
  * ``PyObject_GetOptionalAttrString()``
  * ``PyMapping_GetOptionalItem()``
  * ``PyMapping_GetOptionalItemString()``

* 2023-07-05: Add ``PyObject_Vectorcall()`` function.
* 2023-06-21: Add ``PyWeakref_GetRef()`` function.
* 2023-06-20: Add ``PyImport_AddModuleRef()`` function.
* 2022-11-15: Add experimental operations to the ``upgrade_pythoncapi.py``
  script: ``Py_NewRef``, ``Py_CLEAR`` and ``Py_SETREF``.
* 2022-11-09: Fix ``Py_SETREF()`` and ``Py_XSETREF()`` macros
  for `gh-98724 <https://github.com/python/cpython/issues/98724>`_.
* 2022-11-04: Add ``PyFrame_GetVar()`` and ``PyFrame_GetVarString()``
  functions.
* 2022-08-04: Add ``PyCode_GetVarnames()``, ``PyCode_GetFreevars()``
  and ``PyCode_GetCellvars()`` functions.
* 2022-06-14: Fix compatibility with C++ older than C++11.
* 2022-05-03: Add ``PyCode_GetCode()`` function.
* 2022-04-26: Rename the project from ``pythoncapi_compat`` to
  ``pythoncapi-compat``: replace the underscore separator with a dash.
* 2022-04-08: Add functions ``PyFrame_GetLocals()``, ``PyFrame_GetGlobals()``
  ``PyFrame_GetBuiltins()``, and ``PyFrame_GetLasti()``.
* 2022-03-12: Add functions ``PyFloat_Pack2()``, ``PyFloat_Pack4()``,
  ``PyFloat_Pack8()``, ``PyFloat_Unpack2()``, ``PyFloat_Unpack4()`` and
  ``PyFloat_Unpack8()``.
* 2022-03-03: The project moved to https://github.com/python/pythoncapi-compat
* 2022-02-11: The project license changes from the MIT license to the Zero
  Clause BSD (0BSD) license. Projects copying ``pythoncapi_compat.h`` no longer
  have to include the MIT license and the copyright notice.
* 2022-02-08: Add documentation.
* 2022-02-09: ``pythoncapi_compat.h`` now supports C++ on Python 3.6 and newer:
  use ``nullptr`` and ``reinterpret_cast<type>`` cast on C++, and use ``NULL``
  and ``(type)`` cast on C.
* 2021-10-15: Add ``PyThreadState_EnterTracing()`` and
  ``PyThreadState_LeaveTracing()``.
* 2021-04-09: Add ``Py_Is()``, ``Py_IsNone()``, ``Py_IsTrue()``,
  ``Py_IsFalse()`` functions.
* 2021-04-01:

  * Add ``Py_SETREF()``, ``Py_XSETREF()`` and ``Py_UNUSED()``.
  * Add PyPy support.

* 2021-01-27: Fix compatibility with Visual Studio 2008 for Python 2.7.
* 2020-11-30: Creation of the ``upgrade_pythoncapi.py`` script.
* 2020-06-04: Creation of the ``pythoncapi_compat.h`` header file.

