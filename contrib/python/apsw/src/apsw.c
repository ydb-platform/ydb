/*
  Another Python Sqlite Wrapper

  This wrapper aims to be the minimum necessary layer over SQLite 3
  itself.

  It assumes we are running as 32 bit int with a 64 bit long long type
  available.

  See the accompanying LICENSE file.
*/

/**

.. module:: apsw
  :synopsis: Python access to SQLite database library

APSW Module
***********

The module is the main interface to SQLite.  Methods and data on the
module have process wide effects.

Type Annotations
================

Comprehensive `type annotations
<https://docs.python.org/3/library/typing.html>`__ are included, and
your code using apsw can be checked using tools like `mypy
<http://mypy-lang.org/>`__.  You can refer to the types below for
your annotations (eg as :class:`apsw.SQLiteValue`)

.. include:: ../doc/typing.rstgen

API Reference
=============
*/

/* Fight with setuptools over ndebug */
#ifdef APSW_NO_NDEBUG
#ifdef NDEBUG
#undef NDEBUG
#endif
#endif

#ifdef APSW_USE_SQLITE_CONFIG
#error #include "sqlite3config.h"
#endif

/* SQLite amalgamation */
#ifdef APSW_USE_SQLITE_AMALGAMATION
#ifndef SQLITE_MAX_ATTACHED
#define SQLITE_MAX_ATTACHED 125
#endif
#ifndef APSW_NO_NDEBUG
/* See SQLite ticket 2554 */
#define SQLITE_API static
#define SQLITE_EXTERN static
#endif
#define SQLITE_ENABLE_API_ARMOR 1
#include "sqlite3.c"
#undef small

/* Fight with SQLite over ndebug */
#ifdef APSW_NO_NDEBUG
#ifdef NDEBUG
#undef NDEBUG
#endif
#endif

#else
/* SQLite 3 headers */
#include "sqlite3.h"
#endif

#if SQLITE_VERSION_NUMBER < 3039000
#endif

/* system headers */
#include <assert.h>
#include <stdarg.h>

/* Get the version number */
#include "apswversion.h"

#include "apsw.docstrings"

/* Python headers */
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <pythread.h>
#include "structmember.h"

#ifdef APSW_TESTFIXTURES
/* Fault injection */
#define APSW_FAULT_INJECT(faultName, good, bad) \
  do                                            \
  {                                             \
    if (APSW_Should_Fault(#faultName))          \
    {                                           \
      do                                        \
      {                                         \
        bad;                                    \
      } while (0);                              \
    }                                           \
    else                                        \
    {                                           \
      do                                        \
      {                                         \
        good;                                   \
      } while (0);                              \
    }                                           \
  } while (0)

static int APSW_Should_Fault(const char *);

/* Are we doing 64 bit? - _LP64 is best way I can find as sizeof isn't valid in cpp #if */
#if defined(_LP64) && _LP64
#define APSW_TEST_LARGE_OBJECTS
#endif

#else /* APSW_TESTFIXTURES */
#define APSW_FAULT_INJECT(faultName, good, bad) \
  do                                            \
  {                                             \
    good;                                       \
  } while (0)

#endif

/* The module object */
static PyObject *apswmodule;

/* root exception class */
static PyObject *APSWException;

typedef struct
{
  PyObject_HEAD long long blobsize;
} ZeroBlobBind;

/* Argument parsing helpers */
#include "argparse.c"

/* Augment tracebacks */
#include "traceback.c"

/* Make various versions of Python code compatible with each other */
#include "pyutil.c"

/* various utility functions and macros */
#include "util.c"

/* Exceptions we can raise */
#include "exceptions.c"

/* The statement cache */
#include "statementcache.c"

/* connections */
#include "connection.c"

/* backup */
#include "backup.c"

/* Zeroblob and blob */
#include "blob.c"

/* cursors */
#include "cursor.c"

/* virtual tables */
#include "vtable.c"

/* virtual file system */
#include "vfs.c"

/* MODULE METHODS */

/** .. method:: sqlitelibversion() -> str

  Returns the version of the SQLite library.  This value is queried at
  run time from the library so if you use shared libraries it will be
  the version in the shared library.

  -* sqlite3_libversion
*/

static PyObject *
getsqliteversion(void)
{
  return PyUnicode_FromString(sqlite3_libversion());
}

/** .. method:: sqlite3_sourceid() -> str

    Returns the exact checkin information for the SQLite 3 source
    being used.

    -* sqlite3_sourceid
*/

static PyObject *
get_sqlite3_sourceid(void)
{
  return PyUnicode_FromString(sqlite3_sourceid());
}

/** .. method:: apswversion() -> str

  Returns the APSW version.
*/
static PyObject *
getapswversion(void)
{
  return PyUnicode_FromString(APSW_VERSION);
}

/** .. method:: enablesharedcache(enable: bool) -> None

  If you use the same :class:`Connection` across threads or use
  multiple :class:`connections <Connection>` accessing the same file,
  then SQLite can `share the cache between them
  <https://sqlite.org/sharedcache.html>`_.  It is :ref:`not
  recommended <sharedcache>` that you use this.

  -* sqlite3_enable_shared_cache
*/
static PyObject *
enablesharedcache(PyObject *Py_UNUSED(self), PyObject *args, PyObject *kwds)
{
  int enable = 0, res;
  {
    static char *kwlist[] = {"enable", NULL};
    Apsw_enablesharedcache_CHECK;
    argcheck_bool_param enable_param = {&enable, Apsw_enablesharedcache_enable_MSG};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O&:" Apsw_enablesharedcache_USAGE, kwlist, argcheck_bool, &enable_param))
      return NULL;
  }
  APSW_FAULT_INJECT(EnableSharedCacheFail, res = sqlite3_enable_shared_cache(enable), res = SQLITE_NOMEM);
  SET_EXC(res, NULL);

  if (res != SQLITE_OK)
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: initialize() -> None

  It is unlikely you will want to call this method as SQLite automatically initializes.

  -* sqlite3_initialize
*/

static PyObject *
initialize(void)
{
  int res;

  res = sqlite3_initialize();
  APSW_FAULT_INJECT(InitializeFail, , res = SQLITE_NOMEM);
  SET_EXC(res, NULL);

  if (res != SQLITE_OK)
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: shutdown() -> None

  It is unlikely you will want to call this method and there is no
  need to do so.  It is a **really** bad idea to call it unless you
  are absolutely sure all :class:`connections <Connection>`,
  :class:`blobs <Blob>`, :class:`cursors <Cursor>`, :class:`vfs <VFS>`
  etc have been closed, deleted and garbage collected.

  -* sqlite3_shutdown
*/
#ifdef APSW_FORK_CHECKER
static void free_fork_checker(void);
#endif

static PyObject *
sqliteshutdown(void)
{
  int res;

  APSW_FAULT_INJECT(ShutdownFail, res = sqlite3_shutdown(), res = SQLITE_NOMEM);
  SET_EXC(res, NULL);

  if (res != SQLITE_OK)
    return NULL;

#ifdef APSW_FORK_CHECKER
  free_fork_checker();
#endif

  Py_RETURN_NONE;
}

/** .. method:: config(op: int, *args: Any) -> None

  :param op: A `configuration operation <https://sqlite.org/c3ref/c_config_chunkalloc.html>`_
  :param args: Zero or more arguments as appropriate for *op*

  Many operations don't make sense from a Python program.  The
  following configuration operations are supported: SQLITE_CONFIG_LOG,
  SQLITE_CONFIG_SINGLETHREAD, SQLITE_CONFIG_MULTITHREAD,
  SQLITE_CONFIG_SERIALIZED, SQLITE_CONFIG_URI, SQLITE_CONFIG_MEMSTATUS,
  SQLITE_CONFIG_COVERING_INDEX_SCAN, SQLITE_CONFIG_PCACHE_HDRSZ,
  SQLITE_CONFIG_PMASZ, and SQLITE_CONFIG_STMTJRNL_SPILL.

  See :ref:`tips <diagnostics_tips>` for an example of how to receive
  log messages (SQLITE_CONFIG_LOG)

  -* sqlite3_config
*/

static PyObject *logger_cb = NULL;

static void
apsw_logger(void *arg, int errcode, const char *message)
{
  PyGILState_STATE gilstate;
  PyObject *etype = NULL, *evalue = NULL, *etraceback = NULL;
  PyObject *res = NULL;
  PyObject *msgaspystring = NULL;

  gilstate = PyGILState_Ensure();
  assert(arg == logger_cb);
  assert(arg);
  PyErr_Fetch(&etype, &evalue, &etraceback);

  msgaspystring = convertutf8string(message);
  if (msgaspystring)
    res = PyObject_CallFunction(arg, "iO", errcode, msgaspystring);
  if (!res)
  {
    AddTraceBackHere(__FILE__, __LINE__, "apsw_sqlite3_log_receiver",
                     "{s: O, s: i, s: s}",
                     "logger", OBJ(arg),
                     "errcode", errcode,
                     "message", message);
    apsw_write_unraisable(NULL);
  }
  else
    Py_DECREF(res);

  Py_XDECREF(msgaspystring);
  if (etype || evalue || etraceback)
    PyErr_Restore(etype, evalue, etraceback);
  PyGILState_Release(gilstate);
}

static PyObject *
config(PyObject *Py_UNUSED(self), PyObject *args)
{
  int res, optdup;
  long opt;

  if (PyTuple_GET_SIZE(args) < 1 || !PyLong_Check(PyTuple_GET_ITEM(args, 0)))
    return PyErr_Format(PyExc_TypeError, "There should be at least one argument with the first being a number");

  opt = PyLong_AsLong(PyTuple_GET_ITEM(args, 0));
  if (PyErr_Occurred())
    return NULL;

  switch (opt)
  {
  case SQLITE_CONFIG_SINGLETHREAD:
  case SQLITE_CONFIG_MULTITHREAD:
  case SQLITE_CONFIG_SERIALIZED:
  case SQLITE_CONFIG_URI:
    if (!PyArg_ParseTuple(args, "i", &optdup))
      return NULL;
    assert(opt == optdup);
    res = sqlite3_config((int)opt);
    break;

  case SQLITE_CONFIG_PCACHE_HDRSZ:
  {
    int outval = -1;
    if (!PyArg_ParseTuple(args, "i", &optdup))
      return NULL;
    assert(opt == optdup);
    APSW_FAULT_INJECT(SCPHConfigFails, res = sqlite3_config((int)opt, &outval), res = SQLITE_FULL);
    if (res)
    {
      SET_EXC(res, NULL);
      return NULL;
    }
    return PyLong_FromLong(outval);
  }

  case SQLITE_CONFIG_MEMSTATUS:
  case SQLITE_CONFIG_COVERING_INDEX_SCAN:
  case SQLITE_CONFIG_PMASZ:
  case SQLITE_CONFIG_STMTJRNL_SPILL:
  {
    int intval;
    if (!PyArg_ParseTuple(args, "ii", &optdup, &intval))
      return NULL;
    assert(opt == optdup);
    res = sqlite3_config((int)opt, intval);
    break;
  }

  case SQLITE_CONFIG_LOG:
  {
    PyObject *logger;
    if (!PyArg_ParseTuple(args, "iO", &optdup, &logger))
      return NULL;
    if (logger == Py_None)
    {
      res = sqlite3_config((int)opt, NULL);
      if (res == SQLITE_OK)
        Py_CLEAR(logger_cb);
    }
    else if (!PyCallable_Check(logger))
    {
      return PyErr_Format(PyExc_TypeError, "Logger should be None or a callable");
    }
    else
    {
      res = sqlite3_config((int)opt, apsw_logger, logger);
      if (res == SQLITE_OK)
      {
        Py_CLEAR(logger_cb);
        logger_cb = logger;
        Py_INCREF(logger);
      }
    }
    break;
  }

  default:
    return PyErr_Format(PyExc_TypeError, "Unknown config type %d", (int)opt);
  }

  SET_EXC(res, NULL);

  if (res != SQLITE_OK)
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: memoryused() -> int

  Returns the amount of memory SQLite is currently using.

  .. seealso::
    :meth:`status`


  -* sqlite3_memory_used
*/
static PyObject *
memoryused(void)
{
  return PyLong_FromLongLong(sqlite3_memory_used());
}

/** .. method:: memoryhighwater(reset: bool = False) -> int

  Returns the maximum amount of memory SQLite has used.  If *reset* is
  True then the high water mark is reset to the current value.

  .. seealso::

    :meth:`status`

  -* sqlite3_memory_highwater
*/
static PyObject *
memoryhighwater(PyObject *Py_UNUSED(self), PyObject *args, PyObject *kwds)
{
  int reset = 0;

  {
    static char *kwlist[] = {"reset", NULL};
    Apsw_memoryhighwater_CHECK;
    argcheck_bool_param reset_param = {&reset, Apsw_memoryhighwater_reset_MSG};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|O&:" Apsw_memoryhighwater_USAGE, kwlist, argcheck_bool, &reset_param))
      return NULL;
  }
  return PyLong_FromLongLong(sqlite3_memory_highwater(reset));
}

/** .. method:: softheaplimit(limit: int) -> int

  Requests SQLite try to keep memory usage below *limit* bytes and
  returns the previous limit.

  .. seealso::

      :meth:`hard_heap_limit`

  -* sqlite3_soft_heap_limit64
*/
static PyObject *
softheaplimit(PyObject *Py_UNUSED(self), PyObject *args, PyObject *kwds)
{
  sqlite3_int64 limit, oldlimit;
  {
    static char *kwlist[] = {"limit", NULL};
    Apsw_softheaplimit_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "L:" Apsw_softheaplimit_USAGE, kwlist, &limit))
      return NULL;
  }
  oldlimit = sqlite3_soft_heap_limit64(limit);

  return PyLong_FromLongLong(oldlimit);
}

/** .. method:: hard_heap_limit(limit: int) -> int

  Enforces SQLite keeping memory usage below *limit* bytes and
  returns the previous limit.

  .. seealso::

      :meth:`softheaplimit`

  -* sqlite3_hard_heap_limit64
*/
static PyObject *
apsw_hard_heap_limit(PyObject *Py_UNUSED(self), PyObject *args, PyObject *kwds)
{
  sqlite3_int64 limit, oldlimit;
  {
    static char *kwlist[] = {"limit", NULL};
    Apsw_hard_heap_limit_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "L:" Apsw_hard_heap_limit_USAGE, kwlist, &limit))
      return NULL;
  }
  oldlimit = sqlite3_hard_heap_limit64(limit);

  return PyLong_FromLongLong(oldlimit);
}

/** .. method:: randomness(amount: int)  -> bytes

  Gets random data from SQLite's random number generator.

  :param amount: How many bytes to return

  -* sqlite3_randomness
*/
static PyObject *
randomness(PyObject *Py_UNUSED(self), PyObject *args, PyObject *kwds)
{
  int amount;
  PyObject *bytes;

  {
    static char *kwlist[] = {"amount", NULL};
    Apsw_randomness_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "i:" Apsw_randomness_USAGE, kwlist, &amount))
      return NULL;
  }
  if (amount < 0)
    return PyErr_Format(PyExc_ValueError, "Can't have negative number of bytes");

  bytes = PyBytes_FromStringAndSize(NULL, amount);
  if (!bytes)
    return bytes;
  sqlite3_randomness(amount, PyBytes_AS_STRING(bytes));
  return bytes;
}

/** .. method:: releasememory(amount: int) -> int

  Requests SQLite try to free *amount* bytes of memory.  Returns how
  many bytes were freed.

  -* sqlite3_release_memory
*/

static PyObject *
releasememory(PyObject *Py_UNUSED(self), PyObject *args, PyObject *kwds)
{
  int amount;

  {
    static char *kwlist[] = {"amount", NULL};
    Apsw_releasememory_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "i:" Apsw_releasememory_USAGE, kwlist, &amount))
      return NULL;
  }
  return PyLong_FromLong(sqlite3_release_memory(amount));
}

/** .. method:: status(op: int, reset: bool = False) -> Tuple[int, int]

  Returns current and highwater measurements.

  :param op: A `status parameter <https://sqlite.org/c3ref/c_status_malloc_size.html>`_
  :param reset: If *True* then the highwater is set to the current value
  :returns: A tuple of current value and highwater value

  .. seealso::

    * :ref:`Status example <example_status>`

  -* sqlite3_status64

*/
static PyObject *
status(PyObject *Py_UNUSED(self), PyObject *args, PyObject *kwds)
{
  int res, op, reset = 0;
  sqlite3_int64 current = 0, highwater = 0;

  {
    static char *kwlist[] = {"op", "reset", NULL};
    Apsw_status_CHECK;
    argcheck_bool_param reset_param = {&reset, Apsw_status_reset_MSG};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "i|O&:" Apsw_status_USAGE, kwlist, &op, argcheck_bool, &reset_param))
      return NULL;
  }

  res = sqlite3_status64(op, &current, &highwater, reset);
  SET_EXC(res, NULL);

  if (res != SQLITE_OK)
    return NULL;

  return Py_BuildValue("(LL)", current, highwater);
}

/** .. method:: vfsnames() -> List[str]

  Returns a list of the currently installed :ref:`vfs <vfs>`.  The first
  item in the list is the default vfs.
*/
static PyObject *
vfsnames(PyObject *Py_UNUSED(self))
{
  PyObject *result = NULL, *str = NULL;
  int res;
  sqlite3_vfs *vfs = sqlite3_vfs_find(0);

  APSW_FAULT_INJECT(vfsnamesallocfail, result = PyList_New(0), result = PyErr_NoMemory());
  if (!result)
    goto error;

  while (vfs)
  {
    APSW_FAULT_INJECT(vfsnamesfails,
                      str = convertutf8string(vfs->zName),
                      str = PyErr_NoMemory());
    if (!str)
      goto error;
    APSW_FAULT_INJECT(vfsnamesappendfails, res = PyList_Append(result, str), (res = -1, PyErr_NoMemory()));
    if (res)
      goto error;
    Py_DECREF(str);
    vfs = vfs->pNext;
  }
  return result;

error:
  Py_XDECREF(str);
  Py_XDECREF(result);
  return NULL;
}

/** .. method:: exceptionfor(code: int) -> Exception

  If you would like to raise an exception that corresponds to a
  particular SQLite `error code
  <https://sqlite.org/c3ref/c_abort.html>`_ then call this function.
  It also understands `extended error codes
  <https://sqlite.org/c3ref/c_ioerr_access.html>`_.

  For example to raise `SQLITE_IOERR_ACCESS <https://sqlite.org/c3ref/c_ioerr_access.html>`_::

    raise apsw.exceptionfor(apsw.SQLITE_IOERR_ACCESS)

*/
static PyObject *
getapswexceptionfor(PyObject *Py_UNUSED(self), PyObject *args, PyObject *kwds)
{
  int code = 0, i;
  PyObject *result = NULL;

  {
    static char *kwlist[] = {"code", NULL};
    Apsw_exceptionfor_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "i:" Apsw_exceptionfor_USAGE, kwlist, &code))
      return NULL;
  }

  for (i = 0; exc_descriptors[i].name; i++)
    if (exc_descriptors[i].code == (code & 0xff))
    {
      result = PyObject_CallObject(exc_descriptors[i].cls, NULL);
      if (!result)
        return result;
      break;
    }
  if (!result)
    return PyErr_Format(PyExc_ValueError, "%d is not a known error code", code);

  PyObject_SetAttrString(result, "extendedresult", PyLong_FromLong(code));
  PyObject_SetAttrString(result, "result", PyLong_FromLong(code & 0xff));
  return result;
}

/** .. method:: complete(statement: str) -> bool

  Returns True if the input string comprises one or more complete SQL
  statements by looking for an unquoted trailing semi-colon.

  An example use would be if you were prompting the user for SQL
  statements and needed to know if you had a whole statement, or
  needed to ask for another line::

    statement = input("SQL> ")
    while not apsw.complete(statement):
       more = input("  .. ")
       statement = statement + "\\n" + more

  -* sqlite3_complete
*/
static PyObject *
apswcomplete(PyObject *Py_UNUSED(self), PyObject *args, PyObject *kwds)
{
  const char *statement = NULL;
  int res;

  {
    static char *kwlist[] = {"statement", NULL};
    Apsw_complete_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s:" Apsw_complete_USAGE, kwlist, &statement))
      return NULL;
  }

  res = sqlite3_complete(statement);

  if (res)
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

#ifdef APSW_TESTFIXTURES
static PyObject *
apsw_fini(PyObject *Py_UNUSED(self))
{
  Py_XDECREF(tls_errmsg);
  statementcache_fini();
  Py_RETURN_NONE;
}
#endif

#ifdef APSW_FORK_CHECKER

/*
   We want to verify that SQLite objects are not used across forks.
   One way is to modify all calls to SQLite to do the checking but
   this is a pain as well as a performance hit.  Instead we use the
   approach of providing an alternative mutex implementation since
   pretty much every SQLite API call takes and releases a mutex.

   Our diverted functions check the process id on calls and set the
   process id on allocating a mutex.  We have to avoid the checks for
   the static mutexes.

   This code also doesn't bother with some things like checking malloc
   results.  It is intended to only be used to verify correctness with
   test suites.  The code that sets Python exceptions is also very
   brute force and is likely to cause problems.  That however is a
   good thing - you will really be sure there is a problem!
 */

typedef struct
{
  pid_t pid;
  sqlite3_mutex *underlying_mutex;
} apsw_mutex;

static apsw_mutex *apsw_mutexes[] =
    {
        NULL, /* not used - fast */
        NULL, /* not used - recursive */
        NULL, /* from this point on corresponds to the various static mutexes */
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL};

static sqlite3_mutex_methods apsw_orig_mutex_methods;

static int
apsw_xMutexInit(void)
{
  return apsw_orig_mutex_methods.xMutexInit();
}

static int
apsw_xMutexEnd(void)
{
  return apsw_orig_mutex_methods.xMutexEnd();
}

#define MUTEX_MAX_ALLOC 20
static apsw_mutex *fork_checker_mutexes[MUTEX_MAX_ALLOC];
static int current_apsw_fork_mutex = 0;

static sqlite3_mutex *
apsw_xMutexAlloc(int which)
{
  switch (which)
  {
  case SQLITE_MUTEX_FAST:
  case SQLITE_MUTEX_RECURSIVE:
  {
    apsw_mutex *am;
    sqlite3_mutex *m = apsw_orig_mutex_methods.xMutexAlloc(which);

    if (!m)
      return m;
    assert(current_apsw_fork_mutex < MUTEX_MAX_ALLOC);
    fork_checker_mutexes[current_apsw_fork_mutex++] = am = malloc(sizeof(apsw_mutex));
    am->pid = getpid();
    am->underlying_mutex = m;
    return (sqlite3_mutex *)am;
  }
  default:
    /* verify we have space */
    assert((unsigned)which < sizeof(apsw_mutexes) / sizeof(apsw_mutexes[0]));
    /* fill in if missing */
    if (!apsw_mutexes[which])
    {
      apsw_mutexes[which] = malloc(sizeof(apsw_mutex));
      apsw_mutexes[which]->pid = 0;
      apsw_mutexes[which]->underlying_mutex = apsw_orig_mutex_methods.xMutexAlloc(which);
    }
    return (sqlite3_mutex *)apsw_mutexes[which];
  }
}

static void
free_fork_checker(void)
{
  unsigned i;
  for (i = 0; i < sizeof(apsw_mutexes) / sizeof(apsw_mutexes[0]); i++)
  {
    free(apsw_mutexes[i]);
    apsw_mutexes[i] = NULL;
  }
  for (i = 0; i < MUTEX_MAX_ALLOC; i++)
  {
    free(fork_checker_mutexes[i]);
    fork_checker_mutexes[i] = 0;
  }
  current_apsw_fork_mutex = 0;
}

static int
apsw_check_mutex(apsw_mutex *am)
{
  if (am->pid && am->pid != getpid())
  {
    PyGILState_STATE gilstate;
    gilstate = PyGILState_Ensure();
    PyErr_Format(ExcForkingViolation, "SQLite object allocated in one process is being used in another (across a fork)");
    apsw_write_unraisable(NULL);
    PyErr_Format(ExcForkingViolation, "SQLite object allocated in one process is being used in another (across a fork)");
    PyGILState_Release(gilstate);
    return SQLITE_MISUSE;
  }
  return SQLITE_OK;
}

static void
apsw_xMutexFree(sqlite3_mutex *mutex)
{
  apsw_mutex *am = (apsw_mutex *)mutex;
  apsw_check_mutex(am);
  apsw_orig_mutex_methods.xMutexFree(am->underlying_mutex);
}

static void
apsw_xMutexEnter(sqlite3_mutex *mutex)
{
  apsw_mutex *am = (apsw_mutex *)mutex;
  apsw_check_mutex(am);
  apsw_orig_mutex_methods.xMutexEnter(am->underlying_mutex);
}

static int
apsw_xMutexTry(sqlite3_mutex *mutex)
{
  apsw_mutex *am = (apsw_mutex *)mutex;
  if (apsw_check_mutex(am))
    return SQLITE_MISUSE;
  return apsw_orig_mutex_methods.xMutexTry(am->underlying_mutex);
}

static void
apsw_xMutexLeave(sqlite3_mutex *mutex)
{
  apsw_mutex *am = (apsw_mutex *)mutex;
  apsw_check_mutex(am);
  apsw_orig_mutex_methods.xMutexLeave(am->underlying_mutex);
}

#ifdef SQLITE_DEBUG
static int
apsw_xMutexHeld(sqlite3_mutex *mutex)
{
  apsw_mutex *am = (apsw_mutex *)mutex;
  apsw_check_mutex(am);
  return apsw_orig_mutex_methods.xMutexHeld(am->underlying_mutex);
}

static int
apsw_xMutexNotheld(sqlite3_mutex *mutex)
{
  apsw_mutex *am = (apsw_mutex *)mutex;
  apsw_check_mutex(am);
  return apsw_orig_mutex_methods.xMutexNotheld(am->underlying_mutex);
}
#endif

static sqlite3_mutex_methods apsw_mutex_methods =
    {
        apsw_xMutexInit,
        apsw_xMutexEnd,
        apsw_xMutexAlloc,
        apsw_xMutexFree,
        apsw_xMutexEnter,
        apsw_xMutexTry,
        apsw_xMutexLeave,
#ifdef SQLITE_DEBUG
        apsw_xMutexHeld,
        apsw_xMutexNotheld
#else
        0,
        0
#endif
};

/** .. method:: fork_checker() -> None

  **Note** This method is not available on Windows as it does not
  support the fork system call.

  SQLite does not allow the use of database connections across `forked
  <http://en.wikipedia.org/wiki/Fork_(operating_system)>`__ processes
  (see the `SQLite FAQ Q6 <https://sqlite.org/faq.html#q6>`__).
  (Forking creates a child process that is a duplicate of the parent
  including the state of all data structures in the program.  If you
  do this to SQLite then parent and child would both consider
  themselves owners of open databases and silently corrupt each
  other's work and interfere with each other's locks.)

  One example of how you may end up using fork is if you use the
  `multiprocessing module
  <http://docs.python.org/library/multiprocessing.html>`__ which uses
  fork to make child processes.

  If you do use fork or multiprocessing on a platform that supports
  fork then you **must** ensure database connections and their objects
  (cursors, backup, blobs etc) are not used in the parent process, or
  are all closed before calling fork or starting a `Process
  <http://docs.python.org/library/multiprocessing.html#process-and-exceptions>`__.
  (Note you must call close to ensure the underlying SQLite objects
  are closed.  It is also a good idea to call `gc.collect(2)
  <http://docs.python.org/library/gc.html#gc.collect>`__ to ensure
  anything you may have missed is also deallocated.)

  Once you run this method, extra checking code is inserted into
  SQLite's mutex operations (at a very small performance penalty) that
  verifies objects are not used across processes.  You will get a
  :exc:`ForkingViolationError` if you do so.  Note that due to the way
  Python's internals work, the exception will be delivered to
  `sys.excepthook` in addition to the normal exception mechanisms and
  may be reported by Python after the line where the issue actually
  arose.  (Destructors of objects you didn't close also run between
  lines.)

  You should only call this method as the first line after importing
  APSW, as it has to shutdown and re-initialize SQLite.  If you have
  any SQLite objects already allocated when calling the method then
  the program will later crash.  The recommended use is to use the fork
  checking as part of your test suite.
*/
static PyObject *
apsw_fork_checker(PyObject *Py_UNUSED(self))
{
  int rc;

  /* ignore multiple attempts to use this routine */
  if (apsw_orig_mutex_methods.xMutexInit)
    goto ok;

  /* Ensure mutex methods available and installed */
  rc = sqlite3_initialize();
  if (rc)
    goto fail;

  /* then do a shutdown as we can't get or change mutex while sqlite is running */
  rc = sqlite3_shutdown();
  if (rc)
    goto fail;

  rc = sqlite3_config(SQLITE_CONFIG_GETMUTEX, &apsw_orig_mutex_methods);
  if (rc)
    goto fail;

  rc = sqlite3_config(SQLITE_CONFIG_MUTEX, &apsw_mutex_methods);
  if (rc)
    goto fail;

  /* start back up again */
  rc = sqlite3_initialize();
  if (rc)
    goto fail;

ok:
  Py_RETURN_NONE;

fail:
  assert(rc != SQLITE_OK);
  SET_EXC(rc, NULL);
  return NULL;
}
#endif

/** .. attribute:: compile_options
    :type: Tuple[str, ...]

    A tuple of the options used to compile SQLite.  For example it
    will be something like this::

        ('ENABLE_LOCKING_STYLE=0', 'TEMP_STORE=1', 'THREADSAFE=1')

    -* sqlite3_compileoption_get
*/
static PyObject *
get_compile_options(void)
{
  int i, count = 0;
  const char *opt;
  PyObject *tmpstring;
  PyObject *res = 0;

  /* this method is only called once at startup */

  for (i = 0;; i++)
  {
    opt = sqlite3_compileoption_get(i); /* No PYSQLITE_CALL needed */
    if (!opt)
      break;
  }
  count = i;

  res = PyTuple_New(count);
  if (!res)
    goto fail;
  for (i = 0; i < count; i++)
  {
    opt = sqlite3_compileoption_get(i); /* No PYSQLITE_CALL needed */
    assert(opt);
    tmpstring = PyUnicode_FromString(opt);
    if (!tmpstring)
      goto fail;
    PyTuple_SET_ITEM(res, i, tmpstring);
  }

  return res;
fail:
  Py_XDECREF(res);
  return NULL;
}

/** .. attribute:: keywords
    :type: Set[str]

    A set containing every SQLite keyword

    -* sqlite3_keyword_count sqlite3_keyword_name

*/
static PyObject *
get_keywords(void)
{
  int i, j, count, size;
  PyObject *res = NULL, *tmpstring;
  const char *name;

  res = PySet_New(0);
  if (!res)
    goto fail;

  count = sqlite3_keyword_count(); /* No PYSQLITE_CALL needed */
  for (i = 0; i < count; i++)
  {
    j = sqlite3_keyword_name(i, &name, &size); /* No PYSQLITE_CALL needed */
    assert(j == SQLITE_OK);
    tmpstring = PyUnicode_FromStringAndSize(name, size);
    if (!tmpstring)
      goto fail;
    j = PySet_Add(res, tmpstring);
    Py_DECREF(tmpstring);
    if (j)
      goto fail;
  }

  return res;
fail:
  Py_XDECREF(res);
  return NULL;
}

/** .. method:: format_sql_value(value: SQLiteValue) -> str

  Returns a Python string representing the supplied value in SQL syntax.

*/
static PyObject *
formatsqlvalue(PyObject *Py_UNUSED(self), PyObject *value)
{
  /* NULL/None */
  if (value == Py_None)
    return PyUnicode_FromString("NULL");

  /* Integer/Float */
  if (PyLong_Check(value) || PyFloat_Check(value))
    return PyObject_Str(value);

  /* Unicode */
  if (PyUnicode_Check(value))
  {
    Py_ssize_t needed_chars = 2; /* leading and trailing quote */
    unsigned int input_kind = PyUnicode_KIND(value), output_kind;
    void *input_data = PyUnicode_DATA(value);
    Py_ssize_t input_length = PyUnicode_GET_LENGTH(value);
    Py_ssize_t pos, outpos;
    int simple = 1;
    Py_UCS4 ch;

    PyObject *strres;
    void *output_data;

    for (pos = 0; pos < input_length; pos++)
    {
      switch (PyUnicode_READ(input_kind, input_data, pos))
      {
      case '\'':
        needed_chars += 2;
        simple = 0;
        break;
      case 0:
        /* To output an embedded null we have to concatenate a blob
           containing only a null to a string and sqlite does the
           necessary co-ercion and gets things right irrespective of
           the underlying string being utf8 or utf16.  It takes 11
           characters to do that. */
        needed_chars += 11;
        simple = 0;
        break;
      default:
        needed_chars += 1;
      }
    }

    STRING_NEW(formatsqlStrFail, strres, needed_chars, PyUnicode_MAX_CHAR_VALUE(value));
    if (!strres)
      return NULL;
    output_kind = PyUnicode_KIND(strres);
    output_data = PyUnicode_DATA(strres);

    PyUnicode_WRITE(output_kind, output_data, 0, '\'');
    PyUnicode_WRITE(output_kind, output_data, needed_chars - 1, '\'');

    if (simple)
    {
#ifdef PYPY_VERSION
      PyErr_Format(PyExc_NotImplementedError, "PyPy has not implemented PyUnicode_CopyCharacters");
      return NULL;
#else
      PyUnicode_CopyCharacters(strres, 1, value, 0, input_length);
      return strres;
#endif
    }

    outpos = 1;

    for (pos = 0; pos < input_length; pos++)
    {
      switch (ch = PyUnicode_READ(input_kind, input_data, pos))
      {
      case 0:
      {
        int i;
        for (i = 0; i < 11; i++)
          PyUnicode_WRITE(output_kind, output_data, outpos++, "'||X'00'||'"[i]);
      }
      break;
      case '\'':
        PyUnicode_WRITE(output_kind, output_data, outpos++, ch);
        /* fall through */
      default:
        PyUnicode_WRITE(output_kind, output_data, outpos++, ch);
      }
    }
    return strres;
  }
  /* Blob */
  if (PyBytes_Check(value))
  {
    int asrb;
    PyObject *strres;
    void *unidata;
    Py_ssize_t unipos = 0;
    Py_buffer buffer;
    Py_ssize_t buflen;
    const unsigned char *bufferc;

    GET_BUFFER(formatsqlHexBufFail, asrb, value, &buffer);
    if (asrb == -1)
      return NULL;

    STRING_NEW(formatsqlHexStrFail, strres, buffer.len * 2 + 3, 127);
    if (!strres)
      goto bytesfinally;

    bufferc = buffer.buf;
    buflen = buffer.len;
    unidata = PyUnicode_DATA(strres);
    PyUnicode_WRITE(PyUnicode_1BYTE_KIND, unidata, unipos++, 'X');
    PyUnicode_WRITE(PyUnicode_1BYTE_KIND, unidata, unipos++, '\'');
    /* About the billionth time I have written a hex conversion routine */
    for (; buflen; buflen--)
    {
      PyUnicode_WRITE(PyUnicode_1BYTE_KIND, unidata, unipos++, "0123456789ABCDEF"[(*bufferc) >> 4]);
      PyUnicode_WRITE(PyUnicode_1BYTE_KIND, unidata, unipos++, "0123456789ABCDEF"[(*bufferc++) & 0x0f]);
    }
    PyUnicode_WRITE(PyUnicode_1BYTE_KIND, unidata, unipos++, '\'');

  bytesfinally:
    PyBuffer_Release(&buffer);
    return strres;
  }

  return PyErr_Format(PyExc_TypeError, "Unsupported type");
}

/** .. method:: log(errorcode: int, message: str) -> None

    Calls the SQLite logging interface.  Note that you must format the
    message before passing it to this method::

        apsw.log(apsw.SQLITE_NOMEM, f"Need { needed } bytes of memory")

    See :ref:`tips <diagnostics_tips>` for an example of how to
    receive log messages.

    -* sqlite3_log
 */
static PyObject *
apsw_log(PyObject *Py_UNUSED(self), PyObject *args, PyObject *kwds)
{
  int errorcode;
  const char *message;
  {
    static char *kwlist[] = {"errorcode", "message", NULL};
    Apsw_log_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "is:" Apsw_log_USAGE, kwlist, &errorcode, &message))
      return NULL;
  }
  sqlite3_log(errorcode, "%s", message); /* PYSQLITE_CALL not needed */

  Py_RETURN_NONE;
}

/** .. method:: strlike(glob: str, string: str, escape: int = 0) -> int

  Does string LIKE matching.  Note that zero is returned on on a match.

  -* sqlite3_strlike
*/
static PyObject *
apsw_strlike(PyObject *Py_UNUSED(self), PyObject *args, PyObject *kwds)
{
  const char *glob = NULL, *string = NULL;
  int escape = 0;
  int res;

  {
    static char *kwlist[] = {"glob", "string", "escape", NULL};
    Apsw_strlike_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "ss|i:" Apsw_strlike_USAGE, kwlist, &glob, &string, &escape))
      return NULL;
  }

  res = sqlite3_strlike(glob, string, escape); /* PYSQLITE_CALL not needed */

  return PyLong_FromLong(res);
}

/** .. method:: strglob(glob: str, string: str) -> int

  Does string GLOB matching.  Note that zero is returned on on a match.

  -* sqlite3_strglob
*/
static PyObject *
apsw_strglob(PyObject *Py_UNUSED(self), PyObject *args, PyObject *kwds)
{
  const char *glob = NULL, *string = NULL;
  int res;

  {
    static char *kwlist[] = {"glob", "string", NULL};
    Apsw_strglob_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "ss:" Apsw_strglob_USAGE, kwlist, &glob, &string))
      return NULL;
  }

  res = sqlite3_strglob(glob, string); /* PYSQLITE_CALL not needed */

  return PyLong_FromLong(res);
}

/** .. method:: stricmp(string1: str, string2: str) -> int

  Does string case-insensitive comparison.  Note that zero is returned
  on on a match.

  -* sqlite3_stricmp
*/
static PyObject *
apsw_stricmp(PyObject *Py_UNUSED(self), PyObject *args, PyObject *kwds)
{
  const char *string1 = NULL, *string2 = NULL;
  int res;

  {
    static char *kwlist[] = {"string1", "string2", NULL};
    Apsw_stricmp_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "ss:" Apsw_stricmp_USAGE, kwlist, &string1, &string2))
      return NULL;
  }

  res = sqlite3_stricmp(string1, string2); /* PYSQLITE_CALL not needed */

  return PyLong_FromLong(res);
}

/** .. method:: strnicmp(string1: str, string2: str, count: int) -> int

  Does string case-insensitive comparison.  Note that zero is returned
  on on a match.

  -* sqlite3_strnicmp
*/
static PyObject *
apsw_strnicmp(PyObject *Py_UNUSED(self), PyObject *args, PyObject *kwds)
{
  const char *string1 = NULL, *string2 = NULL;
  int count, res;

  {
    static char *kwlist[] = {"string1", "string2", "count", NULL};
    Apsw_strnicmp_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "ssi:" Apsw_strnicmp_USAGE, kwlist, &string1, &string2, &count))
      return NULL;
  }

  res = sqlite3_strnicmp(string1, string2, count); /* PYSQLITE_CALL not needed */

  return PyLong_FromLong(res);
}

/** .. method:: set_default_vfs(name: str) -> None

 Sets the default vfs to *name* which must be an existing vfs.
 See :meth:`vfsnames`.

 -* sqlite3_vfs_register sqlite3_vfs_find
*/
static PyObject *
apsw_set_default_vfs(PyObject *Py_UNUSED(module), PyObject *args, PyObject *kwds)
{
  const char *name;
  sqlite3_vfs *vfs;
  int res;

  {
    static char *kwlist[] = {"name", NULL};
    Apsw_set_default_vfs_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s:" Apsw_set_default_vfs_USAGE, kwlist, &name))
      return NULL;
  }

  vfs = sqlite3_vfs_find(name);
  if (!vfs)
    return PyErr_Format(PyExc_ValueError, "vfs named \"%s\" not known", name);
  res = sqlite3_vfs_register(vfs, 1);
  SET_EXC(res, NULL);
  if (res)
    return NULL;
  Py_RETURN_NONE;
}

/** .. method:: unregister_vfs(name: str) -> None

 Unregisters the named vfs.  See :meth:`vfsnames`.

 -* sqlite3_vfs_unregister sqlite3_vfs_find
*/
static PyObject *
apsw_unregister_vfs(PyObject *Py_UNUSED(module), PyObject *args, PyObject *kwds)
{
  const char *name;
  sqlite3_vfs *vfs;
  int res;

  {
    static char *kwlist[] = {"name", NULL};
    Apsw_unregister_vfs_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s:" Apsw_unregister_vfs_USAGE, kwlist, &name))
      return NULL;
  }

  vfs = sqlite3_vfs_find(name);
  if (!vfs)
    return PyErr_Format(PyExc_ValueError, "vfs named \"%s\" not known", name);
  res = sqlite3_vfs_unregister(vfs);
  SET_EXC(res, NULL);
  if (res)
    return NULL;
  Py_RETURN_NONE;
}

static PyObject *
apsw_getattr(PyObject *module, PyObject *name)
{
  PyObject *shellmodule = NULL, *res = NULL;
  const char *cname = PyUnicode_AsUTF8(name);

  if (strcmp(cname, "Shell") && strcmp(cname, "main"))
    return PyErr_Format(PyExc_AttributeError, "Unknown apsw attribute %R", name);

  shellmodule = PyImport_ImportModule("apsw.shell");
  if (shellmodule)
    res = PyObject_GetAttrString(shellmodule, cname);
  Py_XDECREF(shellmodule);
  return res;
}

static PyMethodDef module_methods[] = {
    {"sqlite3_sourceid", (PyCFunction)get_sqlite3_sourceid, METH_NOARGS,
     Apsw_sqlite3_sourceid_DOC},
    {"sqlitelibversion", (PyCFunction)getsqliteversion, METH_NOARGS,
     Apsw_sqlitelibversion_DOC},
    {"apswversion", (PyCFunction)getapswversion, METH_NOARGS,
     Apsw_apswversion_DOC},
    {"vfsnames", (PyCFunction)vfsnames, METH_NOARGS,
     Apsw_vfsnames_DOC},
    {"enablesharedcache", (PyCFunction)enablesharedcache, METH_VARARGS | METH_KEYWORDS,
     Apsw_enablesharedcache_DOC},
    {"initialize", (PyCFunction)initialize, METH_NOARGS,
     Apsw_initialize_DOC},
    {"shutdown", (PyCFunction)sqliteshutdown, METH_NOARGS,
     Apsw_shutdown_DOC},
    {"format_sql_value", (PyCFunction)formatsqlvalue, METH_O,
     Apsw_format_sql_value_DOC},
    {"config", (PyCFunction)config, METH_VARARGS,
     Apsw_config_DOC},
    {"log", (PyCFunction)apsw_log, METH_VARARGS | METH_KEYWORDS,
     Apsw_log_DOC},
    {"memoryused", (PyCFunction)memoryused, METH_NOARGS,
     Apsw_memoryused_DOC},
    {"memoryhighwater", (PyCFunction)memoryhighwater, METH_VARARGS | METH_KEYWORDS,
     Apsw_memoryhighwater_DOC},
    {"status", (PyCFunction)status, METH_VARARGS | METH_KEYWORDS,
     Apsw_status_DOC},
    {"softheaplimit", (PyCFunction)softheaplimit, METH_VARARGS | METH_KEYWORDS,
     Apsw_softheaplimit_DOC},
    {"hard_heap_limit", (PyCFunction)apsw_hard_heap_limit, METH_VARARGS | METH_KEYWORDS,
     Apsw_hard_heap_limit_DOC},
    {"releasememory", (PyCFunction)releasememory, METH_VARARGS | METH_KEYWORDS,
     Apsw_releasememory_DOC},
    {"randomness", (PyCFunction)randomness, METH_VARARGS | METH_KEYWORDS,
     Apsw_randomness_DOC},
    {"exceptionfor", (PyCFunction)getapswexceptionfor, METH_VARARGS | METH_KEYWORDS,
     Apsw_exceptionfor_DOC},
    {"complete", (PyCFunction)apswcomplete, METH_VARARGS | METH_KEYWORDS,
     Apsw_complete_DOC},
    {"strlike", (PyCFunction)apsw_strlike, METH_VARARGS | METH_KEYWORDS, Apsw_strlike_DOC},
    {"strglob", (PyCFunction)apsw_strglob, METH_VARARGS | METH_KEYWORDS, Apsw_strglob_DOC},
    {"stricmp", (PyCFunction)apsw_stricmp, METH_VARARGS | METH_KEYWORDS, Apsw_stricmp_DOC},
    {"strnicmp", (PyCFunction)apsw_strnicmp, METH_VARARGS | METH_KEYWORDS, Apsw_strnicmp_DOC},
    {"set_default_vfs", (PyCFunction)apsw_set_default_vfs, METH_VARARGS | METH_KEYWORDS, Apsw_set_default_vfs_DOC},
    {"unregister_vfs", (PyCFunction)apsw_unregister_vfs, METH_VARARGS | METH_KEYWORDS, Apsw_unregister_vfs_DOC},
#ifdef APSW_TESTFIXTURES
    {"_fini", (PyCFunction)apsw_fini, METH_NOARGS,
     "Frees all caches and recycle lists"},
#endif
#ifdef APSW_FORK_CHECKER
    {"fork_checker", (PyCFunction)apsw_fork_checker, METH_NOARGS,
     Apsw_fork_checker_DOC},
#endif
    {"__getattr__", (PyCFunction)apsw_getattr, METH_O, "module getattr"},
    {0, 0, 0, 0} /* Sentinel */
};

static struct PyModuleDef apswmoduledef = {
    PyModuleDef_HEAD_INIT,
    "apsw",
    NULL,
    -1,
    module_methods,
    0,
    0,
    0,
    0};

PyMODINIT_FUNC
PyInit_apsw(void)
{
  PyObject *m = NULL;
  PyObject *thedict = NULL;
  const char *mapping_name = NULL;
  PyObject *hooks;
  unsigned int i;

  assert(sizeof(int) == 4);       /* we expect 32 bit ints */
  assert(sizeof(long long) == 8); /* we expect 64 bit long long */

  /* Check SQLite was compiled with thread safety */
  if (!sqlite3_threadsafe())
  {
    PyErr_Format(PyExc_EnvironmentError, "SQLite was compiled without thread safety and cannot be used.");
    goto fail;
  }

  if (PyType_Ready(&ConnectionType) < 0 || PyType_Ready(&APSWCursorType) < 0 || PyType_Ready(&ZeroBlobBindType) < 0 || PyType_Ready(&APSWBlobType) < 0 || PyType_Ready(&APSWVFSType) < 0 || PyType_Ready(&APSWVFSFileType) < 0 || PyType_Ready(&APSWURIFilenameType) < 0 || PyType_Ready(&FunctionCBInfoType) < 0 || PyType_Ready(&APSWBackupType) < 0)
    goto fail;

  m = apswmodule = PyModule_Create(&apswmoduledef);

  if (m == NULL)
    goto fail;

  Py_INCREF(m);

  if (init_exceptions(m))
    goto fail;

  Py_INCREF(&ConnectionType);
  PyModule_AddObject(m, "Connection", (PyObject *)&ConnectionType);

  Py_INCREF(&APSWCursorType);
  PyModule_AddObject(m, "Cursor", (PyObject *)&APSWCursorType);

  Py_INCREF(&APSWBlobType);
  PyModule_AddObject(m, "Blob", (PyObject *)&APSWBlobType);

  Py_INCREF(&APSWBackupType);
  PyModule_AddObject(m, "Backup", (PyObject *)&APSWBackupType);

  Py_INCREF(&ZeroBlobBindType);
  PyModule_AddObject(m, "zeroblob", (PyObject *)&ZeroBlobBindType);

  Py_INCREF(&APSWVFSType);
  PyModule_AddObject(m, "VFS", (PyObject *)&APSWVFSType);
  Py_INCREF(&APSWVFSFileType);
  PyModule_AddObject(m, "VFSFile", (PyObject *)&APSWVFSFileType);
  Py_INCREF(&APSWURIFilenameType);
  PyModule_AddObject(m, "URIFilename", (PyObject *)&APSWURIFilenameType);

  /** .. attribute:: connection_hooks
       :type: List[Callable[[Connection], None]]

       The purpose of the hooks is to allow the easy registration of
       :meth:`functions <Connection.createscalarfunction>`,
       :ref:`virtual tables <virtualtables>` or similar items with
       each :class:`Connection` as it is created. The default value is an empty
       list. Whenever a Connection is created, each item in
       apsw.connection_hooks is invoked with a single parameter being
       the new Connection object. If the hook raises an exception then
       the creation of the Connection fails.

       If you wanted to store your own defined functions in the
       database then you could define a hook that looked in the
       relevant tables, got the Python text and turned it into the
       functions.
    */
  hooks = PyList_New(0);
  if (!hooks)
    goto fail;
  PyModule_AddObject(m, "connection_hooks", hooks);

  /** .. attribute:: SQLITE_VERSION_NUMBER
    :type: int

    The integer version number of SQLite that APSW was compiled
    against.  For example SQLite 3.6.4 will have the value *3006004*.
    This number may be different than the actual library in use if the
    library is shared and has been updated.  Call
    :meth:`sqlitelibversion` to get the actual library version.

    */
  PyModule_AddIntConstant(m, "SQLITE_VERSION_NUMBER", SQLITE_VERSION_NUMBER);

  /** .. attribute:: using_amalgamation
    :type: bool

    If True then `SQLite amalgamation
    <https://sqlite.org/cvstrac/wiki?p=TheAmalgamation>`__ is in
    use (statically compiled into APSW).  Using the amalgamation means
    that SQLite shared libraries are not used and will not affect your
    code.

    */

#ifdef APSW_USE_SQLITE_AMALGAMATION
  Py_INCREF(Py_True);
  PyModule_AddObject(m, "using_amalgamation", Py_True);
#else
  Py_INCREF(Py_False);
  PyModule_AddObject(m, "using_amalgamation", Py_False);
#endif

  /**

.. _sqliteconstants:

SQLite constants
================

SQLite has `many constants
<https://sqlite.org/c3ref/constlist.html>`_ used in various
interfaces.  To use a constant such as *SQLITE_OK*, just
use ``apsw.SQLITE_OK``.

The same values can be used in different contexts. For example
*SQLITE_OK* and *SQLITE_CREATE_INDEX* both have a value
of zero. For each group of constants there is also a mapping (dict)
available that you can supply a string to and get the corresponding
numeric value, or supply a numeric value and get the corresponding
string. These can help improve diagnostics/logging, calling other
modules etc. For example::

      apsw.mapping_authorizer_function["SQLITE_READ"] == 20
      apsw.mapping_authorizer_function[20] == "SQLITE_READ"


    */

  /* add in some constants and also put them in a corresponding mapping dictionary */

  {

    /* sentinel should be a number that doesn't exist */
#define SENTINEL -786343
#define DICT(n) \
  {             \
    n, SENTINEL \
  }
#define END \
  {         \
    NULL, 0 \
  }
#define ADDINT(n) \
  {               \
#n, n         \
  }

    static const struct
    {
      const char *name;
      const int value;
    } integers[] = {
        DICT("mapping_authorizer_return"),
        ADDINT(SQLITE_DENY),
        ADDINT(SQLITE_IGNORE),
        ADDINT(SQLITE_OK),
        END,

        DICT("mapping_authorizer_function"),
        ADDINT(SQLITE_CREATE_INDEX),
        ADDINT(SQLITE_CREATE_TABLE),
        ADDINT(SQLITE_CREATE_TEMP_INDEX),
        ADDINT(SQLITE_CREATE_TEMP_TABLE),
        ADDINT(SQLITE_CREATE_TEMP_TRIGGER),
        ADDINT(SQLITE_CREATE_TEMP_VIEW),
        ADDINT(SQLITE_CREATE_TRIGGER),
        ADDINT(SQLITE_CREATE_VIEW),
        ADDINT(SQLITE_DELETE),
        ADDINT(SQLITE_DROP_INDEX),
        ADDINT(SQLITE_DROP_TABLE),
        ADDINT(SQLITE_DROP_TEMP_INDEX),
        ADDINT(SQLITE_DROP_TEMP_TABLE),
        ADDINT(SQLITE_DROP_TEMP_TRIGGER),
        ADDINT(SQLITE_DROP_TEMP_VIEW),
        ADDINT(SQLITE_DROP_TRIGGER),
        ADDINT(SQLITE_DROP_VIEW),
        ADDINT(SQLITE_INSERT),
        ADDINT(SQLITE_PRAGMA),
        ADDINT(SQLITE_READ),
        ADDINT(SQLITE_SELECT),
        ADDINT(SQLITE_TRANSACTION),
        ADDINT(SQLITE_UPDATE),
        ADDINT(SQLITE_ATTACH),
        ADDINT(SQLITE_DETACH),
        ADDINT(SQLITE_ALTER_TABLE),
        ADDINT(SQLITE_REINDEX),
        ADDINT(SQLITE_COPY),
        ADDINT(SQLITE_ANALYZE),
        ADDINT(SQLITE_CREATE_VTABLE),
        ADDINT(SQLITE_DROP_VTABLE),
        ADDINT(SQLITE_FUNCTION),
        ADDINT(SQLITE_SAVEPOINT),
        ADDINT(SQLITE_RECURSIVE),
        END,

        /* vtable best index constraints */
        DICT("mapping_bestindex_constraints"),
        ADDINT(SQLITE_INDEX_CONSTRAINT_EQ),
        ADDINT(SQLITE_INDEX_CONSTRAINT_GT),
        ADDINT(SQLITE_INDEX_CONSTRAINT_LE),
        ADDINT(SQLITE_INDEX_CONSTRAINT_LT),
        ADDINT(SQLITE_INDEX_CONSTRAINT_GE),
        ADDINT(SQLITE_INDEX_CONSTRAINT_MATCH),
        ADDINT(SQLITE_INDEX_CONSTRAINT_LIKE),
        ADDINT(SQLITE_INDEX_CONSTRAINT_REGEXP),
        ADDINT(SQLITE_INDEX_CONSTRAINT_GLOB),
        ADDINT(SQLITE_INDEX_CONSTRAINT_ISNULL),
        ADDINT(SQLITE_INDEX_CONSTRAINT_ISNOT),
        ADDINT(SQLITE_INDEX_CONSTRAINT_ISNOTNULL),
        ADDINT(SQLITE_INDEX_CONSTRAINT_IS),
        ADDINT(SQLITE_INDEX_CONSTRAINT_NE),
        ADDINT(SQLITE_INDEX_CONSTRAINT_FUNCTION),
        ADDINT(SQLITE_INDEX_CONSTRAINT_OFFSET),
        ADDINT(SQLITE_INDEX_CONSTRAINT_LIMIT),
        END,

        /* extended result codes */
        DICT("mapping_extended_result_codes"),
        ADDINT(SQLITE_IOERR_READ),
        ADDINT(SQLITE_IOERR_SHORT_READ),
        ADDINT(SQLITE_IOERR_WRITE),
        ADDINT(SQLITE_IOERR_FSYNC),
        ADDINT(SQLITE_IOERR_DIR_FSYNC),
        ADDINT(SQLITE_IOERR_TRUNCATE),
        ADDINT(SQLITE_IOERR_FSTAT),
        ADDINT(SQLITE_IOERR_UNLOCK),
        ADDINT(SQLITE_IOERR_RDLOCK),
        ADDINT(SQLITE_IOERR_DELETE),
        ADDINT(SQLITE_IOERR_BLOCKED),
        ADDINT(SQLITE_IOERR_NOMEM),
        ADDINT(SQLITE_IOERR_ACCESS),
        ADDINT(SQLITE_IOERR_CHECKRESERVEDLOCK),
        ADDINT(SQLITE_IOERR_LOCK),
        ADDINT(SQLITE_IOERR_CLOSE),
        ADDINT(SQLITE_IOERR_DIR_CLOSE),
        ADDINT(SQLITE_LOCKED_SHAREDCACHE),
        ADDINT(SQLITE_BUSY_RECOVERY),
        ADDINT(SQLITE_CANTOPEN_NOTEMPDIR),
        ADDINT(SQLITE_IOERR_SHMOPEN),
        ADDINT(SQLITE_IOERR_SHMSIZE),
        ADDINT(SQLITE_IOERR_SHMLOCK),
        ADDINT(SQLITE_CORRUPT_VTAB),
        ADDINT(SQLITE_IOERR_SEEK),
        ADDINT(SQLITE_IOERR_SHMMAP),
        ADDINT(SQLITE_READONLY_CANTLOCK),
        ADDINT(SQLITE_READONLY_RECOVERY),
        ADDINT(SQLITE_ABORT_ROLLBACK),
        ADDINT(SQLITE_CANTOPEN_ISDIR),
        ADDINT(SQLITE_CANTOPEN_FULLPATH),
        ADDINT(SQLITE_IOERR_DELETE_NOENT),
        ADDINT(SQLITE_CONSTRAINT_CHECK),
        ADDINT(SQLITE_CONSTRAINT_COMMITHOOK),
        ADDINT(SQLITE_CONSTRAINT_FOREIGNKEY),
        ADDINT(SQLITE_CONSTRAINT_FUNCTION),
        ADDINT(SQLITE_CONSTRAINT_NOTNULL),
        ADDINT(SQLITE_CONSTRAINT_PRIMARYKEY),
        ADDINT(SQLITE_CONSTRAINT_TRIGGER),
        ADDINT(SQLITE_CONSTRAINT_UNIQUE),
        ADDINT(SQLITE_CONSTRAINT_VTAB),
        ADDINT(SQLITE_READONLY_ROLLBACK),
        ADDINT(SQLITE_IOERR_MMAP),
        ADDINT(SQLITE_NOTICE_RECOVER_ROLLBACK),
        ADDINT(SQLITE_NOTICE_RECOVER_WAL),
        ADDINT(SQLITE_BUSY_SNAPSHOT),
        ADDINT(SQLITE_IOERR_GETTEMPPATH),
        ADDINT(SQLITE_WARNING_AUTOINDEX),
        ADDINT(SQLITE_CANTOPEN_CONVPATH),
        ADDINT(SQLITE_IOERR_CONVPATH),
        ADDINT(SQLITE_CONSTRAINT_ROWID),
        ADDINT(SQLITE_READONLY_DBMOVED),
        ADDINT(SQLITE_AUTH_USER),
        ADDINT(SQLITE_IOERR_VNODE),
        ADDINT(SQLITE_IOERR_AUTH),
        ADDINT(SQLITE_OK_LOAD_PERMANENTLY),
        ADDINT(SQLITE_IOERR_ROLLBACK_ATOMIC),
        ADDINT(SQLITE_IOERR_COMMIT_ATOMIC),
        ADDINT(SQLITE_IOERR_BEGIN_ATOMIC),
        ADDINT(SQLITE_READONLY_CANTINIT),
        ADDINT(SQLITE_ERROR_RETRY),
        ADDINT(SQLITE_ERROR_MISSING_COLLSEQ),
        ADDINT(SQLITE_READONLY_DIRECTORY),
        ADDINT(SQLITE_LOCKED_VTAB),
        ADDINT(SQLITE_CORRUPT_SEQUENCE),
        ADDINT(SQLITE_CANTOPEN_DIRTYWAL),
        ADDINT(SQLITE_ERROR_SNAPSHOT),
        ADDINT(SQLITE_CONSTRAINT_PINNED),
        ADDINT(SQLITE_OK_SYMLINK),
        ADDINT(SQLITE_CANTOPEN_SYMLINK),
        ADDINT(SQLITE_IOERR_DATA),
        ADDINT(SQLITE_CORRUPT_INDEX),
        ADDINT(SQLITE_BUSY_TIMEOUT),
        ADDINT(SQLITE_IOERR_CORRUPTFS),
        ADDINT(SQLITE_CONSTRAINT_DATATYPE),
        END,

        /* error codes */
        DICT("mapping_result_codes"),
        ADDINT(SQLITE_OK),
        ADDINT(SQLITE_ERROR),
        ADDINT(SQLITE_INTERNAL),
        ADDINT(SQLITE_PERM),
        ADDINT(SQLITE_ABORT),
        ADDINT(SQLITE_BUSY),
        ADDINT(SQLITE_LOCKED),
        ADDINT(SQLITE_NOMEM),
        ADDINT(SQLITE_READONLY),
        ADDINT(SQLITE_INTERRUPT),
        ADDINT(SQLITE_IOERR),
        ADDINT(SQLITE_CORRUPT),
        ADDINT(SQLITE_FULL),
        ADDINT(SQLITE_CANTOPEN),
        ADDINT(SQLITE_PROTOCOL),
        ADDINT(SQLITE_EMPTY),
        ADDINT(SQLITE_SCHEMA),
        ADDINT(SQLITE_CONSTRAINT),
        ADDINT(SQLITE_MISMATCH),
        ADDINT(SQLITE_MISUSE),
        ADDINT(SQLITE_NOLFS),
        ADDINT(SQLITE_AUTH),
        ADDINT(SQLITE_FORMAT),
        ADDINT(SQLITE_RANGE),
        ADDINT(SQLITE_NOTADB),
        ADDINT(SQLITE_NOTFOUND),
        ADDINT(SQLITE_TOOBIG),
        ADDINT(SQLITE_NOTICE),
        ADDINT(SQLITE_WARNING),
        /* you can't get these from apsw code but present for completeness */
        ADDINT(SQLITE_DONE),
        ADDINT(SQLITE_ROW),
        END,

        /* open flags */
        DICT("mapping_open_flags"),
        ADDINT(SQLITE_OPEN_READONLY),
        ADDINT(SQLITE_OPEN_READWRITE),
        ADDINT(SQLITE_OPEN_CREATE),
        ADDINT(SQLITE_OPEN_DELETEONCLOSE),
        ADDINT(SQLITE_OPEN_EXCLUSIVE),
        ADDINT(SQLITE_OPEN_MAIN_DB),
        ADDINT(SQLITE_OPEN_TEMP_DB),
        ADDINT(SQLITE_OPEN_TRANSIENT_DB),
        ADDINT(SQLITE_OPEN_MAIN_JOURNAL),
        ADDINT(SQLITE_OPEN_TEMP_JOURNAL),
        ADDINT(SQLITE_OPEN_SUBJOURNAL),
        ADDINT(SQLITE_OPEN_NOMUTEX),
        ADDINT(SQLITE_OPEN_FULLMUTEX),
        ADDINT(SQLITE_OPEN_PRIVATECACHE),
        ADDINT(SQLITE_OPEN_SHAREDCACHE),
        ADDINT(SQLITE_OPEN_AUTOPROXY),
        ADDINT(SQLITE_OPEN_WAL),
        ADDINT(SQLITE_OPEN_URI),
        ADDINT(SQLITE_OPEN_MEMORY),
        ADDINT(SQLITE_OPEN_NOFOLLOW),
        ADDINT(SQLITE_OPEN_SUPER_JOURNAL),
        ADDINT(SQLITE_OPEN_EXRESCODE),
        END,

        /* limits */
        DICT("mapping_limits"),
        ADDINT(SQLITE_LIMIT_LENGTH),
        ADDINT(SQLITE_LIMIT_SQL_LENGTH),
        ADDINT(SQLITE_LIMIT_COLUMN),
        ADDINT(SQLITE_LIMIT_EXPR_DEPTH),
        ADDINT(SQLITE_LIMIT_COMPOUND_SELECT),
        ADDINT(SQLITE_LIMIT_VDBE_OP),
        ADDINT(SQLITE_LIMIT_FUNCTION_ARG),
        ADDINT(SQLITE_LIMIT_ATTACHED),
        ADDINT(SQLITE_LIMIT_LIKE_PATTERN_LENGTH),
        ADDINT(SQLITE_LIMIT_VARIABLE_NUMBER),
        ADDINT(SQLITE_LIMIT_TRIGGER_DEPTH),
        ADDINT(SQLITE_LIMIT_WORKER_THREADS),
        /* We don't include the MAX limits - see https://github.com/rogerbinns/apsw/issues/17 */
        END,

        DICT("mapping_config"),
        ADDINT(SQLITE_CONFIG_SINGLETHREAD),
        ADDINT(SQLITE_CONFIG_MULTITHREAD),
        ADDINT(SQLITE_CONFIG_SERIALIZED),
        ADDINT(SQLITE_CONFIG_MALLOC),
        ADDINT(SQLITE_CONFIG_GETMALLOC),
        ADDINT(SQLITE_CONFIG_SCRATCH),
        ADDINT(SQLITE_CONFIG_PAGECACHE),
        ADDINT(SQLITE_CONFIG_HEAP),
        ADDINT(SQLITE_CONFIG_MEMSTATUS),
        ADDINT(SQLITE_CONFIG_MUTEX),
        ADDINT(SQLITE_CONFIG_GETMUTEX),
        ADDINT(SQLITE_CONFIG_LOOKASIDE),
        ADDINT(SQLITE_CONFIG_LOG),
        ADDINT(SQLITE_CONFIG_GETPCACHE),
        ADDINT(SQLITE_CONFIG_PCACHE),
        ADDINT(SQLITE_CONFIG_URI),
        ADDINT(SQLITE_CONFIG_PCACHE2),
        ADDINT(SQLITE_CONFIG_GETPCACHE2),
        ADDINT(SQLITE_CONFIG_COVERING_INDEX_SCAN),
        ADDINT(SQLITE_CONFIG_SQLLOG),
        ADDINT(SQLITE_CONFIG_MMAP_SIZE),
        ADDINT(SQLITE_CONFIG_WIN32_HEAPSIZE),
        ADDINT(SQLITE_CONFIG_PCACHE_HDRSZ),
        ADDINT(SQLITE_CONFIG_PMASZ),
        ADDINT(SQLITE_CONFIG_STMTJRNL_SPILL),
        ADDINT(SQLITE_CONFIG_SMALL_MALLOC),
        ADDINT(SQLITE_CONFIG_SORTERREF_SIZE),
        ADDINT(SQLITE_CONFIG_MEMDB_MAXSIZE),
        END,

        DICT("mapping_db_config"),
        ADDINT(SQLITE_DBCONFIG_LOOKASIDE),
        ADDINT(SQLITE_DBCONFIG_ENABLE_FKEY),
        ADDINT(SQLITE_DBCONFIG_ENABLE_TRIGGER),
        ADDINT(SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER),
        ADDINT(SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION),
        ADDINT(SQLITE_DBCONFIG_MAINDBNAME),
        ADDINT(SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE),
        ADDINT(SQLITE_DBCONFIG_ENABLE_QPSG),
        ADDINT(SQLITE_DBCONFIG_TRIGGER_EQP),
#ifdef SQLITE_DBCONFIG_MAX
        /* hopefully this constant will be removed */
        ADDINT(SQLITE_DBCONFIG_MAX),
#endif
        ADDINT(SQLITE_DBCONFIG_RESET_DATABASE),
        ADDINT(SQLITE_DBCONFIG_DEFENSIVE),
        ADDINT(SQLITE_DBCONFIG_WRITABLE_SCHEMA),
        ADDINT(SQLITE_DBCONFIG_DQS_DML),
        ADDINT(SQLITE_DBCONFIG_DQS_DDL),
        ADDINT(SQLITE_DBCONFIG_LEGACY_ALTER_TABLE),
        ADDINT(SQLITE_DBCONFIG_ENABLE_VIEW),
        ADDINT(SQLITE_DBCONFIG_TRUSTED_SCHEMA),
        ADDINT(SQLITE_DBCONFIG_LEGACY_FILE_FORMAT),
        END,

        DICT("mapping_status"),
        ADDINT(SQLITE_STATUS_MEMORY_USED),
        ADDINT(SQLITE_STATUS_PAGECACHE_USED),
        ADDINT(SQLITE_STATUS_PAGECACHE_OVERFLOW),
        ADDINT(SQLITE_STATUS_SCRATCH_USED),
        ADDINT(SQLITE_STATUS_SCRATCH_OVERFLOW),
        ADDINT(SQLITE_STATUS_MALLOC_SIZE),
        ADDINT(SQLITE_STATUS_PARSER_STACK),
        ADDINT(SQLITE_STATUS_PAGECACHE_SIZE),
        ADDINT(SQLITE_STATUS_SCRATCH_SIZE),
        ADDINT(SQLITE_STATUS_MALLOC_COUNT),
        END,

        DICT("mapping_db_status"),
        ADDINT(SQLITE_DBSTATUS_LOOKASIDE_USED),
        ADDINT(SQLITE_DBSTATUS_CACHE_USED),
        ADDINT(SQLITE_DBSTATUS_MAX),
        ADDINT(SQLITE_DBSTATUS_SCHEMA_USED),
        ADDINT(SQLITE_DBSTATUS_STMT_USED),
        ADDINT(SQLITE_DBSTATUS_LOOKASIDE_HIT),
        ADDINT(SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL),
        ADDINT(SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE),
        ADDINT(SQLITE_DBSTATUS_CACHE_HIT),
        ADDINT(SQLITE_DBSTATUS_CACHE_MISS),
        ADDINT(SQLITE_DBSTATUS_CACHE_WRITE),
        ADDINT(SQLITE_DBSTATUS_DEFERRED_FKS),
        ADDINT(SQLITE_DBSTATUS_CACHE_USED_SHARED),
        ADDINT(SQLITE_DBSTATUS_CACHE_SPILL),
        END,

        DICT("mapping_locking_level"),
        ADDINT(SQLITE_LOCK_NONE),
        ADDINT(SQLITE_LOCK_SHARED),
        ADDINT(SQLITE_LOCK_RESERVED),
        ADDINT(SQLITE_LOCK_PENDING),
        ADDINT(SQLITE_LOCK_EXCLUSIVE),
        END,

        DICT("mapping_access"),
        ADDINT(SQLITE_ACCESS_EXISTS),
        ADDINT(SQLITE_ACCESS_READWRITE),
        ADDINT(SQLITE_ACCESS_READ),
        END,

        DICT("mapping_device_characteristics"),
        ADDINT(SQLITE_IOCAP_ATOMIC),
        ADDINT(SQLITE_IOCAP_ATOMIC512),
        ADDINT(SQLITE_IOCAP_ATOMIC1K),
        ADDINT(SQLITE_IOCAP_ATOMIC2K),
        ADDINT(SQLITE_IOCAP_ATOMIC4K),
        ADDINT(SQLITE_IOCAP_ATOMIC8K),
        ADDINT(SQLITE_IOCAP_ATOMIC16K),
        ADDINT(SQLITE_IOCAP_ATOMIC32K),
        ADDINT(SQLITE_IOCAP_ATOMIC64K),
        ADDINT(SQLITE_IOCAP_SAFE_APPEND),
        ADDINT(SQLITE_IOCAP_SEQUENTIAL),
        ADDINT(SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN),
        ADDINT(SQLITE_IOCAP_POWERSAFE_OVERWRITE),
        ADDINT(SQLITE_IOCAP_IMMUTABLE),
        ADDINT(SQLITE_IOCAP_BATCH_ATOMIC),
        END,

        DICT("mapping_sync"),
        ADDINT(SQLITE_SYNC_NORMAL),
        ADDINT(SQLITE_SYNC_FULL),
        ADDINT(SQLITE_SYNC_DATAONLY),
        END,

        DICT("mapping_wal_checkpoint"),
        ADDINT(SQLITE_CHECKPOINT_PASSIVE),
        ADDINT(SQLITE_CHECKPOINT_FULL),
        ADDINT(SQLITE_CHECKPOINT_RESTART),
        ADDINT(SQLITE_CHECKPOINT_TRUNCATE),
        END,

        DICT("mapping_file_control"),
        ADDINT(SQLITE_FCNTL_LOCKSTATE),
        ADDINT(SQLITE_FCNTL_SIZE_HINT),
        ADDINT(SQLITE_FCNTL_CHUNK_SIZE),
        ADDINT(SQLITE_FCNTL_FILE_POINTER),
        ADDINT(SQLITE_FCNTL_SYNC_OMITTED),
        ADDINT(SQLITE_FCNTL_PERSIST_WAL),
        ADDINT(SQLITE_FCNTL_WIN32_AV_RETRY),
        ADDINT(SQLITE_FCNTL_OVERWRITE),
        ADDINT(SQLITE_FCNTL_POWERSAFE_OVERWRITE),
        ADDINT(SQLITE_FCNTL_VFSNAME),
        ADDINT(SQLITE_FCNTL_PRAGMA),
        ADDINT(SQLITE_FCNTL_BUSYHANDLER),
        ADDINT(SQLITE_FCNTL_TEMPFILENAME),
        ADDINT(SQLITE_FCNTL_MMAP_SIZE),
        ADDINT(SQLITE_FCNTL_TRACE),
        ADDINT(SQLITE_FCNTL_COMMIT_PHASETWO),
        ADDINT(SQLITE_FCNTL_HAS_MOVED),
        ADDINT(SQLITE_FCNTL_SYNC),
        ADDINT(SQLITE_FCNTL_WIN32_SET_HANDLE),
        ADDINT(SQLITE_FCNTL_LAST_ERRNO),
        ADDINT(SQLITE_FCNTL_WAL_BLOCK),
        ADDINT(SQLITE_FCNTL_GET_LOCKPROXYFILE),
        ADDINT(SQLITE_FCNTL_SET_LOCKPROXYFILE),
        ADDINT(SQLITE_FCNTL_RBU),
        ADDINT(SQLITE_FCNTL_ZIPVFS),
        ADDINT(SQLITE_FCNTL_JOURNAL_POINTER),
        ADDINT(SQLITE_FCNTL_VFS_POINTER),
        ADDINT(SQLITE_FCNTL_WIN32_GET_HANDLE),
        ADDINT(SQLITE_FCNTL_PDB),
        ADDINT(SQLITE_FCNTL_COMMIT_ATOMIC_WRITE),
        ADDINT(SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE),
        ADDINT(SQLITE_FCNTL_BEGIN_ATOMIC_WRITE),
        ADDINT(SQLITE_FCNTL_LOCK_TIMEOUT),
        ADDINT(SQLITE_FCNTL_DATA_VERSION),
        ADDINT(SQLITE_FCNTL_SIZE_LIMIT),
        ADDINT(SQLITE_FCNTL_CKPT_DONE),
        ADDINT(SQLITE_FCNTL_CKPT_START),
        ADDINT(SQLITE_FCNTL_RESERVE_BYTES),
        ADDINT(SQLITE_FCNTL_EXTERNAL_READER),
        ADDINT(SQLITE_FCNTL_CKSM_FILE),
#ifdef SQLITE_FCNTL_RESET_CACHE
        ADDINT(SQLITE_FCNTL_RESET_CACHE),
#endif
        END,

        DICT("mapping_conflict_resolution_modes"),
        ADDINT(SQLITE_ROLLBACK),
        ADDINT(SQLITE_IGNORE),
        ADDINT(SQLITE_FAIL),
        ADDINT(SQLITE_ABORT),
        ADDINT(SQLITE_REPLACE),
        END,

        DICT("mapping_virtual_table_configuration_options"),
        ADDINT(SQLITE_VTAB_CONSTRAINT_SUPPORT),
        ADDINT(SQLITE_VTAB_DIRECTONLY),
        ADDINT(SQLITE_VTAB_INNOCUOUS),
        END,

        DICT("mapping_xshmlock_flags"),
        ADDINT(SQLITE_SHM_EXCLUSIVE),
        ADDINT(SQLITE_SHM_LOCK),
        ADDINT(SQLITE_SHM_SHARED),
        ADDINT(SQLITE_SHM_UNLOCK),
        END,

        DICT("mapping_virtual_table_scan_flags"),
        ADDINT(SQLITE_INDEX_SCAN_UNIQUE),
        END,

        DICT("mapping_txn_state"),
        ADDINT(SQLITE_TXN_NONE),
        ADDINT(SQLITE_TXN_READ),
        ADDINT(SQLITE_TXN_WRITE),
        END,

        DICT("mapping_prepare_flags"),
        ADDINT(SQLITE_PREPARE_PERSISTENT),
        ADDINT(SQLITE_PREPARE_NORMALIZE),
        ADDINT(SQLITE_PREPARE_NO_VTAB),
        END,

        DICT("mapping_trace_codes"),
        ADDINT(SQLITE_TRACE_STMT),
        ADDINT(SQLITE_TRACE_PROFILE),
        ADDINT(SQLITE_TRACE_ROW),
        ADDINT(SQLITE_TRACE_CLOSE),
        END,

        DICT("mapping_statement_status"),
        ADDINT(SQLITE_STMTSTATUS_FULLSCAN_STEP),
        ADDINT(SQLITE_STMTSTATUS_SORT),
        ADDINT(SQLITE_STMTSTATUS_AUTOINDEX),
        ADDINT(SQLITE_STMTSTATUS_VM_STEP),
        ADDINT(SQLITE_STMTSTATUS_REPREPARE),
        ADDINT(SQLITE_STMTSTATUS_RUN),
        ADDINT(SQLITE_STMTSTATUS_FILTER_MISS),
        ADDINT(SQLITE_STMTSTATUS_FILTER_HIT),
        ADDINT(SQLITE_STMTSTATUS_MEMUSED),
        END,

        DICT("mapping_function_flags"),
        ADDINT(SQLITE_DETERMINISTIC),
        ADDINT(SQLITE_DIRECTONLY),
        ADDINT(SQLITE_SUBTYPE),
        ADDINT(SQLITE_INNOCUOUS),
        END

    };

    for (i = 0; i < sizeof(integers) / sizeof(integers[0]); i++)
    {
      const char *name = integers[i].name;
      int value = integers[i].value;
      PyObject *pyname;
      PyObject *pyvalue;

      /* should be at dict */
      if (!thedict)
      {
        assert(value == SENTINEL);
        assert(mapping_name == NULL);
        mapping_name = name;
        thedict = PyDict_New();
        continue;
      }
      /* at END? */
      if (!name)
      {
        assert(thedict);
        PyModule_AddObject(m, mapping_name, thedict);
        thedict = NULL;
        mapping_name = NULL;
        continue;
      }
      /* regular ADDINT */
      PyModule_AddIntConstant(m, name, value);
      pyname = PyUnicode_FromString(name);
      pyvalue = PyLong_FromLong(value);
      if (!pyname || !pyvalue)
        goto fail;
      PyDict_SetItem(thedict, pyname, pyvalue);
      PyDict_SetItem(thedict, pyvalue, pyname);
      Py_DECREF(pyname);
      Py_DECREF(pyvalue);
    }
    /* should have ended with END so thedict should be NULL */
    assert(thedict == NULL);
  }

  PyModule_AddObject(m, "compile_options", get_compile_options());
  PyModule_AddObject(m, "keywords", get_keywords());

  {
    PyObject *mod = PyImport_ImportModule("collections.abc");
    if (mod)
    {
      collections_abc_Mapping = PyObject_GetAttrString(mod, "Mapping");
      Py_DECREF(mod);
    }
    assert(collections_abc_Mapping);
  }

  if (!PyErr_Occurred())
  {
    return m;
  }

fail:
  Py_XDECREF(m);
  return NULL;
}

#ifdef _WIN32
/* This exists because of issue #327 with the Windows compiler
   looking to export this.  It isn't called in my testing */
PyMODINIT_FUNC
PyInit___init__(void)
{
  return PyInit_apsw();
}
#endif

#ifdef APSW_TESTFIXTURES
static int
APSW_Should_Fault(const char *name)
{
  PyGILState_STATE gilstate;
  PyObject *faultdict = NULL, *truthval = NULL, *value = NULL;
  PyObject *errsave1 = NULL, *errsave2 = NULL, *errsave3 = NULL;
  int res = 0;

  gilstate = PyGILState_Ensure();

  PyErr_Fetch(&errsave1, &errsave2, &errsave3);

  if (!PyObject_HasAttrString(apswmodule, "faultdict"))
  {
    PyObject *dict = PyDict_New();
    if (dict)
      PyModule_AddObject(apswmodule, "faultdict", dict);
  }

  value = PyUnicode_FromString(name);

  faultdict = PyObject_GetAttrString(apswmodule, "faultdict");

  truthval = PyDict_GetItem(faultdict, value);
  if (!truthval)
    goto finally;

  /* set false if present - one shot firing */
  PyDict_SetItem(faultdict, value, Py_False);
  res = PyObject_IsTrue(truthval);

finally:
  Py_XDECREF(value);
  Py_XDECREF(faultdict);

  PyErr_Restore(errsave1, errsave2, errsave3);

  PyGILState_Release(gilstate);
  return res;
}
#endif
