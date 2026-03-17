/*
  Cursor handling code

 See the accompanying LICENSE file.
*/

/**

.. _cursors:

Cursors (executing SQL)
***********************

A cursor encapsulates a SQL query and returning results.  You only need an
explicit cursor if you want more information or control over execution.  Using
:meth:`Connection.execute` or :meth:`Connection.executemany` will automatically
obtain a cursor behind the scenes.

If you need a cursor you should call :meth:`~Connection.cursor` on your
database::

  db=apsw.Connection("databasefilename")
  cursor=db.cursor()

A cursor executes SQL::

  cursor.execute("create table example(title, isbn)")

You can also read data back.  The row is returned as a tuple of the
column values::

  for row in cursor.execute("select * from example"):
     print(row)

There are two ways of supplying data to a query.  The **really bad** way is to compose a string::

  sql="insert into example values('%s', %d)" % ("string", 8390823904)
  cursor.execute(sql)

If there were any single quotes in string then you would have invalid
syntax.  Additionally this is how `SQL injection attacks
<http://en.wikipedia.org/wiki/SQL_injection>`_ happen. Instead you should use bindings::

  sql="insert into example values(?, ?)"
  cursor.execute(sql, ("string", 8390823904))

  # You can also use dictionaries (with colon, $, or @ before names)
  sql="insert into example values(:title, :isbn)"
  cursor.execute(sql, {"title": "string", "isbn": 8390823904})

  # You can use local variables as the dictionary
  title="..."
  isbn="...."
  cursor.execute(sql, locals())

Cursors are cheap.  Use as many as you need.  It is safe to use them
across threads, such as calling :meth:`~Cursor.execute` in one thread,
passing the cursor to another thread that then calls
`next <https://docs.python.org/3/library/functions.html?highlight=next#next>`__.  The only thing you can't do is call methods at
exactly the same time on the same cursor in two different threads - eg
trying to call :meth:`~Cursor.execute` in both at the same time, or
:meth:`~Cursor.execute` in one and `next <https://docs.python.org/3/library/functions.html?highlight=next#next>`__ in another.
(If you do attempt this, it will be detected and
:exc:`ThreadingViolationError` will be raised.)

Behind the scenes a :class:`Cursor` maps to a `SQLite statement
<https://sqlite.org/c3ref/stmt.html>`_.  APSW maintains a
:ref:`cache <statementcache>` so that the mapping is very fast, and the
SQLite objects are reused when possible.

A unique feature of APSW is that your query can be multiple semi-colon
separated statements.  For example::

  cursor.execute("select ... ; insert into ... ; update ... ; select ...")

.. note::

  SQLite fetches data as it is needed.  If table *example* had 10
  million rows it would only get the next row as requested.  This
  code would not work as expected::

    for row in cursor.execute("select * from example"):
       cursor.execute("insert .....")

  The nested :meth:`~Cursor.execute` would start a new query
  abandoning any remaining results from the ``SELECT`` cursor.  There are two
  ways to work around this.  Use a different cursor::

    for row in cursor1.execute("select * from example"):
       cursor2.execute("insert ...")

  You can also get all the rows immediately by filling in a list::

    rows=list( cursor.execute("select * from example") )
    for row in rows:
       cursor.execute("insert ...")

  This last approach is recommended since you don't have to worry
  about the database changing while doing the ``select``.  You should
  also understand transactions and where to put the transaction
  boundaries.

.. note::

  Cursors on the same :ref:`Connection <connections>` are not isolated
  from each other.  Anything done on one cursor is immediately visible
  to all other Cursors on the same connection.  This still applies if
  you start transactions.  Connections are isolated from each other
  with cursors on other connections not seeing changes until they are
  committed.

.. seealso::

  * `SQLite transactions <https://sqlite.org/lang_transaction.html>`_
  * `Atomic commit <https://sqlite.org/atomiccommit.html>`_
  * `Example of changing the database while running a query problem <http://www.mail-archive.com/sqlite-users@sqlite.org/msg42660.html>`_
  * :ref:`Benchmarking`

*/

/** .. class:: Cursor

  You obtain cursors by calling :meth:`Connection.cursor`.
*/

/* CURSOR TYPE */

struct APSWCursor
{
  PyObject_HEAD
      Connection *connection; /* pointer to parent connection */

  unsigned inuse;                  /* track if we are in use preventing concurrent thread mangling */
  struct APSWStatement *statement; /* statement we are currently using */

  /* what state we are in */
  enum
  {
    C_BEGIN,
    C_ROW,
    C_DONE
  } status;

  /* bindings for query */
  PyObject *bindings;        /* dict or sequence */
  Py_ssize_t bindingsoffset; /* for sequence tracks how far along we are when dealing with multiple statements */

  /* iterator for executemany, original query string, prepare options */
  PyObject *emiter;
  PyObject *emoriginalquery;
  APSWStatementOptions emoptions;

  /* tracing functions */
  PyObject *exectrace;
  PyObject *rowtrace;

  /* weak reference support */
  PyObject *weakreflist;

  PyObject *description_cache[3];
};

typedef struct APSWCursor APSWCursor;
static PyTypeObject APSWCursorType;

static PyObject *collections_abc_Mapping;

/* CURSOR CODE */

/* Macro for getting a tracer.  If our tracer is NULL then return connection tracer */

#define ROWTRACE (self->rowtrace ? self->rowtrace : self->connection->rowtrace)

#define EXECTRACE (self->exectrace ? self->exectrace : self->connection->exectrace)

/* Do finalization and free resources.  Returns the SQLITE error code.  If force is 2 then don't raise any exceptions */
static int
resetcursor(APSWCursor *self, int force)
{
  int res = SQLITE_OK;
  PyObject *etype, *eval, *etb;
  int hasmore = statementcache_hasmore(self->statement);

  Py_CLEAR(self->description_cache[0]);
  Py_CLEAR(self->description_cache[1]);
  Py_CLEAR(self->description_cache[2]);

  if (force)
    PyErr_Fetch(&etype, &eval, &etb);

  if (self->statement)
  {
    INUSE_CALL(res = statementcache_finalize(self->connection->stmtcache, self->statement));
    if (res)
    {
      if (force)
        PyErr_Clear();
      else
        SET_EXC(res, self->connection->db);
    }
    self->statement = 0;
  }

  Py_CLEAR(self->bindings);
  self->bindingsoffset = -1;

  if (!force && self->status != C_DONE && hasmore)
  {
    if (res == SQLITE_OK)
    {
      /* We still have more, so this is actually an abort. */
      res = SQLITE_ERROR;
      if (!PyErr_Occurred())
      {
        PyErr_Format(ExcIncomplete, "Error: there are still remaining sql statements to execute");
      }
    }
  }

  if (!force && self->status != C_DONE && self->emiter)
  {
    PyObject *next;
    INUSE_CALL(next = PyIter_Next(self->emiter));
    if (next)
    {
      Py_DECREF(next);
      res = SQLITE_ERROR;
      assert(PyErr_Occurred());
    }
  }

  Py_CLEAR(self->emiter);
  Py_CLEAR(self->emoriginalquery);

  self->status = C_DONE;

  if (PyErr_Occurred())
  {
    assert(res);
    AddTraceBackHere(__FILE__, __LINE__, "resetcursor", "{s: i}", "res", res);
  }

  if (force)
    PyErr_Restore(etype, eval, etb);

  return res;
}

static int
APSWCursor_close_internal(APSWCursor *self, int force)
{
  PyObject *err_type, *err_value, *err_traceback;
  int res;

  if (force == 2)
    PyErr_Fetch(&err_type, &err_value, &err_traceback);

  res = resetcursor(self, force);

  if (force == 2)
    PyErr_Restore(err_type, err_value, err_traceback);
  else
  {
    if (res)
    {
      assert(PyErr_Occurred());
      return 1;
    }
    assert(!PyErr_Occurred());
  }

  /* Remove from connection dependents list.  Has to be done before we decref self->connection
     otherwise connection could dealloc and we'd still be in list */
  if (self->connection)
    Connection_remove_dependent(self->connection, (PyObject *)self);

  /* executemany iterator */
  Py_CLEAR(self->emiter);

  /* no need for tracing */
  Py_CLEAR(self->exectrace);
  Py_CLEAR(self->rowtrace);

  /* we no longer need connection */
  Py_CLEAR(self->connection);

  Py_CLEAR(self->description_cache[0]);
  Py_CLEAR(self->description_cache[1]);
  Py_CLEAR(self->description_cache[2]);

  return 0;
}

static void
APSWCursor_dealloc(APSWCursor *self)
{
  APSW_CLEAR_WEAKREFS;

  APSWCursor_close_internal(self, 2);

  Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *
APSWCursor_new(PyTypeObject *type, PyObject *Py_UNUSED(args), PyObject *Py_UNUSED(kwds))
{
  APSWCursor *self;

  self = (APSWCursor *)type->tp_alloc(type, 0);
  if (self != NULL)
  {
    self->connection = NULL;
    self->statement = 0;
    self->status = C_DONE;
    self->bindings = 0;
    self->bindingsoffset = 0;
    self->emiter = 0;
    self->emoriginalquery = 0;
    self->exectrace = 0;
    self->rowtrace = 0;
    self->inuse = 0;
    self->weakreflist = NULL;
    self->description_cache[0] = 0;
    self->description_cache[1] = 0;
    self->description_cache[2] = 0;
  }

  return (PyObject *)self;
}

static int
APSWCursor_init(APSWCursor *self, PyObject *args, PyObject *kwargs)
{
  static char *kwlist[] = {"connection", NULL};
  PyObject *connection = NULL;

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O:Cursor(Connection)", kwlist, &connection))
    return -1;

  if (!PyObject_TypeCheck(connection, &ConnectionType))
  {
    PyErr_Format(PyExc_TypeError, "Cursor parameter must be a Connection instance");
    return -1;
  }

  Py_INCREF(connection);
  self->connection = (Connection *)connection;

  return 0;
}

static int
APSWCursor_tp_traverse(APSWCursor *self, visitproc visit, void *arg)
{
  Py_VISIT(self->connection);
  Py_VISIT(self->exectrace);
  Py_VISIT(self->rowtrace);
  return 0;
}

static const char *description_formats[] = {
    "(O&O&)",
    "(O&O&OOOOO)",
    "(O&O&O&O&O&)"};

static PyObject *
APSWCursor_internal_getdescription(APSWCursor *self, int fmtnum)
{
  int ncols, i;
  PyObject *result = NULL;
  PyObject *column = NULL;

  assert(sizeof(description_formats) == sizeof(self->description_cache));

  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  if (!self->statement)
  {
    assert(self->description_cache[0] == 0);
    assert(self->description_cache[1] == 0);
    assert(self->description_cache[2] == 0);
    return PyErr_Format(ExcComplete, "Can't get description for statements that have completed execution");
  }

  if (self->description_cache[fmtnum])
  {
    Py_INCREF(self->description_cache[fmtnum]);
    return self->description_cache[fmtnum];
  }

  ncols = sqlite3_column_count(self->statement->vdbestatement);
  result = PyTuple_New(ncols);
  if (!result)
    goto error;

  for (i = 0; i < ncols; i++)
  {
#define INDEX self->statement->vdbestatement, i
/* this is needed because msvc chokes with the ifdef inside */
#ifdef SQLITE_ENABLE_COLUMN_METADATA
#define DESCFMT2 Py_BuildValue(description_formats[fmtnum],                            \
                               convertutf8string, sqlite3_column_name(INDEX),          \
                               convertutf8string, sqlite3_column_decltype(INDEX),      \
                               convertutf8string, sqlite3_column_database_name(INDEX), \
                               convertutf8string, sqlite3_column_table_name(INDEX),    \
                               convertutf8string, sqlite3_column_origin_name(INDEX))
#else
#define DESCFMT2 NULL
#endif
    INUSE_CALL(
        APSW_FAULT_INJECT(GetDescriptionFail,
                          column = (fmtnum < 2) ? Py_BuildValue(description_formats[fmtnum],
                                                                convertutf8string, sqlite3_column_name(INDEX),
                                                                convertutf8string, sqlite3_column_decltype(INDEX),
                                                                Py_None,
                                                                Py_None,
                                                                Py_None,
                                                                Py_None,
                                                                Py_None)
                                                : DESCFMT2,
                          column = PyErr_NoMemory()));
#undef INDEX
    if (!column)
      goto error;

    PyTuple_SET_ITEM(result, i, column);
    /* owned by result now */
    column = 0;
  }

  Py_INCREF(result);
  self->description_cache[fmtnum] = result;
  return result;

error:
  Py_XDECREF(result);
  Py_XDECREF(column);
  return NULL;
}

/** .. method:: getdescription() -> Tuple[Tuple[str, str], ...]

   If you are trying to get information about a table or view,
   then `pragma table_info <https://sqlite.org/pragma.html#pragma_table_info>`__
   is better.

   Returns a tuple describing each column in the result row.  The
   return is identical for every row of the results.  You can only
   call this method once you have started executing a statement and
   before you have finished::

      # This will error
      cursor.getdescription()

      for row in cursor.execute("select ....."):
         # this works
         print (cursor.getdescription())
         print (row)

   The information about each column is a tuple of ``(column_name,
   declared_column_type)``.  The type is what was declared in the
   ``CREATE TABLE`` statement - the value returned in the row will be
   whatever type you put in for that row and column.  (This is known
   as `manifest typing <https://sqlite.org/different.html#typing>`_
   which is also the way that Python works.  The variable ``a`` could
   contain an integer, and then you could put a string in it.  Other
   static languages such as C or other SQL databases only let you put
   one type in - eg ``a`` could only contain an integer or a string,
   but never both.)

   Example::

      cursor.execute("create table books(title string, isbn number, wibbly wobbly zebra)")
      cursor.execute("insert into books values(?,?,?)", (97, "fjfjfj", 3.7))
      cursor.execute("insert into books values(?,?,?)", ("fjfjfj", 3.7, 97))

      for row in cursor.execute("select * from books"):
         print (cursor.getdescription())
         print (row)

   Output::

     # row 0 - description
     (('title', 'string'), ('isbn', 'number'), ('wibbly', 'wobbly zebra'))
     # row 0 - values
     (97, 'fjfjfj', 3.7)
     # row 1 - description
     (('title', 'string'), ('isbn', 'number'), ('wibbly', 'wobbly zebra'))
     # row 1 - values
     ('fjfjfj', 3.7, 97)

   -* sqlite3_column_name sqlite3_column_decltype

*/
static PyObject *APSWCursor_getdescription(APSWCursor *self)
{
  return APSWCursor_internal_getdescription(self, 0);
}

/** .. attribute:: description
    :type: Tuple[Tuple[str, str, None, None, None, None, None], ...]

    Based on the `DB-API cursor property
    <http://www.python.org/dev/peps/pep-0249/>`__, this returns the
    same as :meth:`getdescription` but with 5 Nones appended.  See
    also :issue:`131`.
*/

static PyObject *APSWCursor_getdescription_dbapi(APSWCursor *self)
{
  return APSWCursor_internal_getdescription(self, 1);
}

/** .. attribute:: description_full
  :type: Tuple[Tuple[str, str, str, str, str], ...]

Only present if SQLITE_ENABLE_COLUMN_METADATA was defined at
compile time.

Returns all information about the query result columns. In
addition to the name and declared type, you also get the database
name, table name, and origin name.

-* sqlite3_column_name sqlite3_column_decltype sqlite3_column_database_name sqlite3_column_table_name sqlite3_column_origin_name

*/
#ifdef SQLITE_ENABLE_COLUMN_METADATA
static PyObject *APSWCursor_getdescription_full(APSWCursor *self)
{
  return APSWCursor_internal_getdescription(self, 2);
}
#endif

static int
APSWCursor_is_dict_binding(PyObject *obj)
{
  /* See https://github.com/rogerbinns/apsw/issues/373 for why this function exists */
  assert(obj);

  /* check the most common cases first */
  if (PyDict_CheckExact(obj))
    return 1;
  if (PyList_CheckExact(obj) || PyTuple_CheckExact(obj))
    return 0;

  /* possible but less likely */
  if (PyDict_Check(obj))
    return 1;
  if (PyList_Check(obj) || PyTuple_Check(obj))
    return 0;

  /* abstract base classes final answer */
  if (PyObject_IsInstance(obj, collections_abc_Mapping) == 1)
    return 1;

  return 0;
}

/* internal function - returns SQLite error code (ie SQLITE_OK if all is well) */
static int
APSWCursor_dobinding(APSWCursor *self, int arg, PyObject *obj)
{

  /* DUPLICATE(ish) code: this is substantially similar to the code in
     set_context_result.  If you fix anything here then do it there as
     well. */

  int res = SQLITE_OK;

  assert(!PyErr_Occurred());

  if (obj == Py_None)
    PYSQLITE_CUR_CALL(res = sqlite3_bind_null(self->statement->vdbestatement, arg));
  else if (PyLong_Check(obj))
  {
    /* nb: PyLong_AsLongLong can cause Python level error */
    long long v = PyLong_AsLongLong(obj);
    PYSQLITE_CUR_CALL(res = sqlite3_bind_int64(self->statement->vdbestatement, arg, v));
  }
  else if (PyFloat_Check(obj))
  {
    double v = PyFloat_AS_DOUBLE(obj);
    PYSQLITE_CUR_CALL(res = sqlite3_bind_double(self->statement->vdbestatement, arg, v));
  }
  else if (PyUnicode_Check(obj))
  {
    const char *strdata = NULL;
    Py_ssize_t strbytes = 0;
    APSW_FAULT_INJECT(DoBindingUnicodeConversionFails, strdata = PyUnicode_AsUTF8AndSize(obj, &strbytes), strdata = (const char *)PyErr_NoMemory());
    if (strdata)
    {
      PYSQLITE_CUR_CALL(res = sqlite3_bind_text64(self->statement->vdbestatement, arg, strdata, strbytes, SQLITE_TRANSIENT, SQLITE_UTF8));
    }
    else
    {
      assert(PyErr_Occurred());
      return -1;
    }
  }
  else if (PyObject_CheckBuffer(obj))
  {
    int asrb;
    Py_buffer py3buffer;

    APSW_FAULT_INJECT(DoBindingAsReadBufferFails, asrb = PyObject_GetBuffer(obj, &py3buffer, PyBUF_SIMPLE), (PyErr_NoMemory(), asrb = -1));
    if (asrb != 0)
      return -1;

    PYSQLITE_CUR_CALL(res = sqlite3_bind_blob64(self->statement->vdbestatement, arg, py3buffer.buf, py3buffer.len, SQLITE_TRANSIENT));
    PyBuffer_Release(&py3buffer);
  }
  else if (PyObject_TypeCheck(obj, &ZeroBlobBindType) == 1)
  {
    PYSQLITE_CUR_CALL(res = sqlite3_bind_zeroblob64(self->statement->vdbestatement, arg, ((ZeroBlobBind *)obj)->blobsize));
  }
  else
  {
    PyErr_Format(PyExc_TypeError, "Bad binding argument type supplied - argument #%d: type %s", (int)(arg + self->bindingsoffset), Py_TYPE(obj)->tp_name);
    return -1;
  }
  if (res != SQLITE_OK)
  {
    SET_EXC(res, self->connection->db);
    return -1;
  }
  if (PyErr_Occurred())
    return -1;
  return 0;
}

/* internal function */
static int
APSWCursor_dobindings(APSWCursor *self)
{
  int nargs, arg, res = -1, sz = 0;
  PyObject *obj;

  assert(!PyErr_Occurred());
  assert(self->bindingsoffset >= 0);

  nargs = sqlite3_bind_parameter_count(self->statement->vdbestatement);
  if (nargs == 0 && !self->bindings)
    return 0; /* common case, no bindings needed or supplied */

  if (nargs > 0 && !self->bindings)
  {
    PyErr_Format(ExcBindings, "Statement has %d bindings but you didn't supply any!", nargs);
    return -1;
  }

  /* a dictionary? */
  if (self->bindings && APSWCursor_is_dict_binding(self->bindings))
  {
    for (arg = 1; arg <= nargs; arg++)
    {
      const char *key;

      PYSQLITE_CUR_CALL(key = sqlite3_bind_parameter_name(self->statement->vdbestatement, arg));

      if (!key)
      {
        PyErr_Format(ExcBindings, "Binding %d has no name, but you supplied a dict (which only has names).", arg - 1);
        return -1;
      }

      assert(*key == ':' || *key == '$' || *key == '@');
      key++; /* first char is a colon / dollar / at which we skip */

      /*
      Here be dragons: PyDict_GetItemString swallows exceptions if
      the item doesn't exist (or any other reason) and apsw has therefore
      always had the behaviour that missing dict keys get bound as
      None/null.  PyMapping_GetItemString does throw exceptions.  So to
      preserve existing behaviour, missing keys from dict are treated as
      None, while missing keys from other types will throw an exception.
      */

      obj = PyDict_Check(self->bindings) ? PyDict_GetItemString(self->bindings, key) : PyMapping_GetItemString(self->bindings, key);
      if (PyErr_Occurred())
        return -1;
      if (!obj)
        /* this is where we could error on missing keys */
        continue;
      if (APSWCursor_dobinding(self, arg, obj) != SQLITE_OK)
      {
        assert(PyErr_Occurred());
        return -1;
      }
    }

    return 0;
  }

  /* it must be a fast sequence */
  /* verify the number of args supplied */
  if (self->bindings)
    sz = PySequence_Fast_GET_SIZE(self->bindings);
  /* there is another statement after this one ... */
  if (statementcache_hasmore(self->statement) && sz - self->bindingsoffset < nargs)
  {
    PyErr_Format(ExcBindings, "Incorrect number of bindings supplied.  The current statement uses %d and there are only %d left.  Current offset is %d",
                 nargs, (self->bindings) ? sz : 0, (int)(self->bindingsoffset));
    return -1;
  }
  /* no more statements */
  if (!statementcache_hasmore(self->statement) && sz - self->bindingsoffset != nargs)
  {
    PyErr_Format(ExcBindings, "Incorrect number of bindings supplied.  The current statement uses %d and there are %d supplied.  Current offset is %d",
                 nargs, (self->bindings) ? sz : 0, (int)(self->bindingsoffset));
    return -1;
  }

  res = SQLITE_OK;

  /* nb sqlite starts bind args at one not zero */
  for (arg = 1; arg <= nargs; arg++)
  {
    obj = PySequence_Fast_GET_ITEM(self->bindings, arg - 1 + self->bindingsoffset);
    if (APSWCursor_dobinding(self, arg, obj))
    {
      assert(PyErr_Occurred());
      return -1;
    }
  }

  self->bindingsoffset += nargs;
  assert(res == 0);
  return 0;
}

static int
APSWCursor_doexectrace(APSWCursor *self, Py_ssize_t savedbindingsoffset)
{
  PyObject *retval = NULL;
  PyObject *sqlcmd = NULL;
  PyObject *bindings = NULL;
  PyObject *exectrace;
  int result;

  exectrace = EXECTRACE;
  assert(exectrace);
  assert(self->statement);

  /* make a string of the command */
  sqlcmd = PyUnicode_FromStringAndSize(self->statement->utf8, self->statement->query_size);

  if (!sqlcmd)
    return -1;

  /* now deal with the bindings */
  if (self->bindings)
  {
    if (APSWCursor_is_dict_binding(self->bindings))
    {
      bindings = self->bindings;
      Py_INCREF(self->bindings);
    }
    else
    {
      APSW_FAULT_INJECT(DoExecTraceBadSlice,
                        bindings = PySequence_GetSlice(self->bindings, savedbindingsoffset, self->bindingsoffset),
                        bindings = PyErr_NoMemory());

      if (!bindings)
      {
        Py_DECREF(sqlcmd);
        return -1;
      }
    }
  }
  else
  {
    bindings = Py_None;
    Py_INCREF(bindings);
  }

  retval = PyObject_CallFunction(exectrace, "ONN", self, sqlcmd, bindings);

  if (!retval)
  {
    assert(PyErr_Occurred());
    return -1;
  }
  result = PyObject_IsTrue(retval);
  Py_DECREF(retval);
  assert(result == -1 || result == 0 || result == 1);
  if (result == -1)
  {
    assert(PyErr_Occurred());
    return -1;
  }
  if (result)
    return 0;

  /* callback didn't want us to continue */
  PyErr_Format(ExcTraceAbort, "Aborted by false/null return value of exec tracer");
  return -1;
}

static PyObject *
APSWCursor_dorowtrace(APSWCursor *self, PyObject *retval)
{
  PyObject *rowtrace = ROWTRACE;

  assert(rowtrace);

  return PyObject_CallFunction(rowtrace, "OO", self, retval);
}

/* Returns a borrowed reference to self if all is ok, else NULL on error */
static PyObject *
APSWCursor_step(APSWCursor *self)
{
  int res;
  int savedbindingsoffset = 0; /* initialised to stop stupid compiler from whining */

  for (;;)
  {
    assert(!PyErr_Occurred());
    PYSQLITE_CUR_CALL(res = (self->statement->vdbestatement) ? (sqlite3_step(self->statement->vdbestatement)) : (SQLITE_DONE));

    switch (res & 0xff)
    {
    case SQLITE_ROW:
      self->status = C_ROW;
      return (PyErr_Occurred()) ? (NULL) : ((PyObject *)self);

    case SQLITE_DONE:
      if (PyErr_Occurred())
      {
        self->status = C_DONE;
        return NULL;
      }
      break;

    default:
      /* FALLTHRU */
    case SQLITE_ERROR: /* SQLITE_BUSY is handled here as well */
      /* there was an error - we need to get actual error code from sqlite3_finalize */
      self->status = C_DONE;
      if (PyErr_Occurred())
        /* we don't care about further errors from the sql */
        resetcursor(self, 1);
      else
      {
        res = resetcursor(self, 0); /* this will get the error code for us */
        assert(res != SQLITE_OK);
      }
      return NULL;
    }
    assert(res == SQLITE_DONE);

    /* done with that statement, are there any more? */
    self->status = C_DONE;
    if (!statementcache_hasmore(self->statement))
    {
      PyObject *next;

      /* in executemany mode ?*/
      if (!self->emiter)
      {
        /* no more so we finalize */
        res = resetcursor(self, 0);
        assert(res == SQLITE_OK);
        return (PyObject *)self;
      }

      /* we are in executemany mode */
      INUSE_CALL(next = PyIter_Next(self->emiter));
      if (PyErr_Occurred())
      {
        assert(!next);
        return NULL;
      }

      if (!next)
      {
        res = resetcursor(self, 0);
        assert(res == SQLITE_OK);
        return (PyObject *)self;
      }

      /* we need to clear just completed and restart original executemany statement */
      INUSE_CALL(statementcache_finalize(self->connection->stmtcache, self->statement));
      self->statement = NULL;
      /* don't need bindings from last round if emiter.next() */
      Py_CLEAR(self->bindings);
      self->bindingsoffset = 0;
      /* verify type of next before putting in bindings */
      if (APSWCursor_is_dict_binding(next))
        self->bindings = next;
      else
      {
        self->bindings = PySequence_Fast(next, "You must supply a dict or a sequence");
        /* we no longer need next irrespective of what happens in line above */
        Py_DECREF(next);
        if (!self->bindings)
          return NULL;
      }
      assert(self->bindings);
    }

    /* finalise and go again */
    if (!self->statement)
    {
      /* we are going again in executemany mode */
      assert(self->emiter);
      INUSE_CALL(self->statement = statementcache_prepare(self->connection->stmtcache, self->emoriginalquery, &self->emoptions));
      res = (self->statement) ? SQLITE_OK : SQLITE_ERROR;
    }
    else
    {
      /* next sql statement */
      INUSE_CALL(res = statementcache_next(self->connection->stmtcache, &self->statement));
      SET_EXC(res, self->connection->db);
    }

    if (res != SQLITE_OK)
    {
      assert((res & 0xff) != SQLITE_BUSY); /* finalize shouldn't be returning busy, only step */
      assert(!self->statement);
      return NULL;
    }

    assert(self->statement);
    savedbindingsoffset = self->bindingsoffset;

    assert(!PyErr_Occurred());

    Py_CLEAR(self->description_cache[0]);
    Py_CLEAR(self->description_cache[1]);
    Py_CLEAR(self->description_cache[2]);

    if (APSWCursor_dobindings(self))
    {
      assert(PyErr_Occurred());
      return NULL;
    }

    if (EXECTRACE)
    {
      if (APSWCursor_doexectrace(self, savedbindingsoffset))
      {
        assert(self->status == C_DONE);
        assert(PyErr_Occurred());
        return NULL;
      }
    }
    assert(self->status == C_DONE);
    self->status = C_BEGIN;
  }

  /* you can't actually get here */
  assert(0);
  return NULL;
}

/** .. method:: execute(statements: str, bindings: Optional[Bindings] = None, *, can_cache: bool = True, prepare_flags: int = 0) -> Cursor

    Executes the statements using the supplied bindings.  Execution
    returns when the first row is available or all statements have
    completed.

    :param statements: One or more SQL statements such as ``select *
      from books`` or ``begin; insert into books ...; select
      last_insert_rowid(); end``.
    :param bindings: If supplied should either be a sequence or a dictionary.  Each item must be one of the :ref:`supported types <types>`
    :param can_cache: If False then the statement cache will not be used to find an already prepared query, nor will it be
      placed in the cache after execution
    :param prepare_flags: `flags <https://sqlite.org/c3ref/c_prepare_normalize.htm>`__ passed to
      `sqlite_prepare_v3 <https://sqlite.org/c3ref/prepare.html>`__

    If you use numbered bindings in the query then supply a sequence.
    Any sequence will work including lists and iterators.  For
    example::

      cursor.execute("insert into books values(?,?)", ("title", "number"))

    .. note::

      A common gotcha is wanting to insert a single string but not
      putting it in a tuple::

        cursor.execute("insert into books values(?)", "a title")

      The string is a sequence of 8 characters and so it will look
      like you are supplying 8 bindings when only one is needed.  Use
      a one item tuple with a trailing comma like this::

        cursor.execute("insert into books values(?)", ("a title",) )

    If you used names in the statement then supply a dictionary as the
    binding.  It is ok to be missing entries from the dictionary -
    None/null will be used.  For example::

       cursor.execute("insert into books values(:title, :isbn, :rating)",
            {"title": "book title", "isbn": 908908908})

    The return is the cursor object itself which is also an iterator.  This allows you to write::

       for row in cursor.execute("select * from books"):
          print(row)

    :raises TypeError: The bindings supplied were neither a dict nor a sequence
    :raises BindingsError: You supplied too many or too few bindings for the statements
    :raises IncompleteExecutionError: There are remaining unexecuted queries from your last execute

    -* sqlite3_prepare_v3 sqlite3_step sqlite3_bind_int64 sqlite3_bind_null sqlite3_bind_text64 sqlite3_bind_double sqlite3_bind_blob64 sqlite3_bind_zeroblob

    .. seealso::

       * :ref:`executionmodel`

*/
static PyObject *
APSWCursor_execute(APSWCursor *self, PyObject *args, PyObject *kwds)
{
  int res;
  int savedbindingsoffset = -1;
  int prepare_flags = 0;
  int can_cache = 1;
  PyObject *retval = NULL;
  PyObject *statements, *bindings = NULL;
  APSWStatementOptions options;

  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  res = resetcursor(self, /* force= */ 0);
  if (res != SQLITE_OK)
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  assert(!self->bindings);
  {
    static char *kwlist[] = {"statements", "bindings", "can_cache", "prepare_flags", NULL};
    Cursor_execute_CHECK;
    argcheck_Optional_Bindings_param bindings_param = {&bindings, Cursor_execute_bindings_MSG};
    argcheck_bool_param can_cache_param = {&can_cache, Cursor_execute_can_cache_MSG};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!|O&$O&i:" Cursor_execute_USAGE, kwlist, &PyUnicode_Type, &statements, argcheck_Optional_Bindings, &bindings_param, argcheck_bool, &can_cache_param, &prepare_flags))
      return NULL;
  }
  self->bindings = bindings;

  options.can_cache = can_cache;
  options.prepare_flags = prepare_flags;

  if (self->bindings)
  {
    if (APSWCursor_is_dict_binding(self->bindings))
      Py_INCREF(self->bindings);
    else
    {
      self->bindings = PySequence_Fast(self->bindings, "You must supply a dict or a sequence");
      if (!self->bindings)
        return NULL;
    }
  }

  assert(!self->statement);
  assert(!PyErr_Occurred());
  INUSE_CALL(self->statement = statementcache_prepare(self->connection->stmtcache, statements, &options));
  if (!self->statement)
  {
    AddTraceBackHere(__FILE__, __LINE__, "APSWCursor_execute.sqlite3_prepare", "{s: O, s: O}",
                     "Connection", self->connection,
                     "statement", OBJ(statements));
    return NULL;
  }
  assert(!PyErr_Occurred());

  self->bindingsoffset = 0;
  savedbindingsoffset = 0;

  if (APSWCursor_dobindings(self))
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  if (EXECTRACE)
  {
    if (APSWCursor_doexectrace(self, savedbindingsoffset))
    {
      assert(PyErr_Occurred());
      return NULL;
    }
  }

  self->status = C_BEGIN;

  retval = APSWCursor_step(self);
  if (!retval)
  {
    assert(PyErr_Occurred());
    return NULL;
  }
  Py_INCREF(retval);
  return retval;
}

/** .. method:: executemany(statements: str, sequenceofbindings: Sequence[Bindings], *, can_cache: bool = True, prepare_flags: int = 0) -> Cursor

  This method is for when you want to execute the same statements over
  a sequence of bindings.  Conceptually it does this::

    for binding in sequenceofbindings:
        cursor.execute(statements, binding)

  Example::

    rows=(  (1, 7),
            (2, 23),
            (4, 92),
            (12, 12) )

    cursor.executemany("insert into nums values(?,?)", rows)

  The return is the cursor itself which acts as an iterator.  Your
  statements can return data.  See :meth:`~Cursor.execute` for more
  information.
*/

static PyObject *
APSWCursor_executemany(APSWCursor *self, PyObject *args, PyObject *kwds)
{
  int res;
  PyObject *retval = NULL;
  PyObject *sequenceofbindings = NULL;
  PyObject *next = NULL;
  PyObject *statements = NULL;
  int savedbindingsoffset = -1;
  int can_cache = 1;
  int prepare_flags = 0;

  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  res = resetcursor(self, /* force= */ 0);
  if (res != SQLITE_OK)
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  assert(!self->bindings);
  assert(!self->emiter);
  assert(!self->emoriginalquery);
  assert(self->status == C_DONE);
  {
    static char *kwlist[] = {"statements", "sequenceofbindings", "can_cache", "prepare_flags", NULL};
    Cursor_executemany_CHECK;
    argcheck_bool_param can_cache_param = {&can_cache, Cursor_executemany_can_cache_MSG};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!O|$O&i:" Cursor_executemany_USAGE, kwlist, &PyUnicode_Type, &statements, &sequenceofbindings, argcheck_bool, &can_cache_param, &prepare_flags))
      return NULL;
  }
  self->emiter = PyObject_GetIter(sequenceofbindings);
  if (!self->emiter)
    return PyErr_Format(PyExc_TypeError, "2nd parameter must be iterable");

  INUSE_CALL(next = PyIter_Next(self->emiter));
  if (!next && PyErr_Occurred())
    return NULL;
  if (!next)
  {
    /* empty list */
    Py_INCREF(self);
    return (PyObject *)self;
  }

  if (APSWCursor_is_dict_binding(next))
    self->bindings = next;
  else
  {
    self->bindings = PySequence_Fast(next, "You must supply a dict or a sequence");
    Py_DECREF(next); /* _Fast makes new reference */
    if (!self->bindings)
      return NULL;
  }

  self->emoptions.can_cache = can_cache;
  self->emoptions.prepare_flags = prepare_flags;

  assert(!self->statement);
  assert(!PyErr_Occurred());
  assert(!self->statement);
  INUSE_CALL(self->statement = statementcache_prepare(self->connection->stmtcache, statements, &self->emoptions));
  if (!self->statement)
  {
    AddTraceBackHere(__FILE__, __LINE__, "APSWCursor_executemany.sqlite3_prepare", "{s: O, s: O}",
                     "Connection", self->connection,
                     "statements", OBJ(statements));
    return NULL;
  }
  assert(!PyErr_Occurred());

  self->emoriginalquery = statements;
  Py_INCREF(self->emoriginalquery);

  self->bindingsoffset = 0;
  savedbindingsoffset = 0;

  if (APSWCursor_dobindings(self))
  {
    assert(PyErr_Occurred());
    return NULL;
  }

  if (EXECTRACE)
  {
    if (APSWCursor_doexectrace(self, savedbindingsoffset))
    {
      assert(PyErr_Occurred());
      return NULL;
    }
  }

  self->status = C_BEGIN;

  retval = APSWCursor_step(self);
  if (!retval)
  {
    assert(PyErr_Occurred());
    return NULL;
  }
  Py_INCREF(retval);
  return retval;
}

/** .. method:: close(force: bool = False) -> None

  It is very unlikely you will need to call this method.  It exists
  because older versions of SQLite required all Connection/Cursor
  activity to be confined to the same thread.  That is no longer the
  case.  Cursors are automatically garbage collected and when there
  are none left will allow the connection to be garbage collected if
  it has no other references.

  A cursor is open if there are remaining statements to execute (if
  your query included multiple statements), or if you called
  :meth:`~Cursor.executemany` and not all of the *sequenceofbindings*
  have been used yet.

  :param force: If False then you will get exceptions if there is
   remaining work to do be in the Cursor such as more statements to
   execute, more data from the executemany binding sequence etc. If
   force is True then all remaining work and state information will be
   silently discarded.

*/

static PyObject *
APSWCursor_close(APSWCursor *self, PyObject *args, PyObject *kwds)
{
  int force = 0;

  CHECK_USE(NULL);
  if (!self->connection)
    Py_RETURN_NONE;

  {
    static char *kwlist[] = {"force", NULL};
    Cursor_close_CHECK;
    argcheck_bool_param force_param = {&force, Cursor_close_force_MSG};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|O&:" Cursor_close_USAGE, kwlist, argcheck_bool, &force_param))
      return NULL;
  }
  APSWCursor_close_internal(self, !!force);

  if (PyErr_Occurred())
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: __next__(self: Cursor) -> Any

    Cursors are iterators
*/
static PyObject *
APSWCursor_next(APSWCursor *self)
{
  PyObject *retval;
  PyObject *item;
  int numcols = -1;
  int i;

  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

again:
  if (self->status == C_BEGIN)
    if (!APSWCursor_step(self))
    {
      assert(PyErr_Occurred());
      return NULL;
    }
  if (self->status == C_DONE)
    return NULL;

  assert(self->status == C_ROW);

  self->status = C_BEGIN;

  /* return the row of data */
  numcols = sqlite3_data_count(self->statement->vdbestatement);
  retval = PyTuple_New(numcols);
  if (!retval)
    goto error;

  for (i = 0; i < numcols; i++)
  {
    INUSE_CALL(item = convert_column_to_pyobject(self->statement->vdbestatement, i));
    if (!item)
      goto error;
    PyTuple_SET_ITEM(retval, i, item);
  }
  if (ROWTRACE)
  {
    PyObject *r2 = APSWCursor_dorowtrace(self, retval);
    Py_DECREF(retval);
    if (!r2)
      return NULL;
    if (r2 == Py_None)
    {
      Py_DECREF(r2);
      goto again;
    }
    return r2;
  }
  return retval;
error:
  Py_XDECREF(retval);
  return NULL;
}

/** .. method:: __iter__(self: Cursor) -> Cursor

    Cursors are iterators
*/

static PyObject *
APSWCursor_iter(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  Py_INCREF(self);
  return (PyObject *)self;
}

/** .. method:: setexectrace(callable: Optional[ExecTracer]) -> None

  Sets the :attr:`execution tracer <Cursor.exectrace>`
*/
static PyObject *
APSWCursor_setexectrace(APSWCursor *self, PyObject *args, PyObject *kwds)
{
  PyObject *callable = NULL;
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  {
    static char *kwlist[] = {"callable", NULL};
    Cursor_setexectrace_CHECK;
    argcheck_Optional_Callable_param callable_param = {&callable, Cursor_setexectrace_callable_MSG};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O&:" Cursor_setexectrace_USAGE, kwlist, argcheck_Optional_Callable, &callable_param))
      return NULL;
  }

  Py_XINCREF(callable);
  Py_XDECREF(self->exectrace);
  self->exectrace = callable;

  Py_RETURN_NONE;
}

/** .. method:: setrowtrace(callable: Optional[RowTracer]) -> None

  Sets the :attr:`row tracer <Cursor.rowtrace>`
*/

static PyObject *
APSWCursor_setrowtrace(APSWCursor *self, PyObject *args, PyObject *kwds)
{
  PyObject *callable = NULL;
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  {
    static char *kwlist[] = {"callable", NULL};
    Cursor_setrowtrace_CHECK;
    argcheck_Optional_Callable_param callable_param = {&callable, Cursor_setrowtrace_callable_MSG};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O&:" Cursor_setrowtrace_USAGE, kwlist, argcheck_Optional_Callable, &callable_param))
      return NULL;
  }

  Py_XINCREF(callable);
  Py_XDECREF(self->rowtrace);
  self->rowtrace = callable;

  Py_RETURN_NONE;
}

/** .. method:: getexectrace() -> Optional[ExecTracer]

  Returns the currently installed :attr:`execution tracer
  <Cursor.exectrace>`

  .. seealso::

    * :ref:`tracing`
*/
static PyObject *
APSWCursor_getexectrace(APSWCursor *self)
{
  PyObject *ret;

  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  ret = (self->exectrace) ? (self->exectrace) : Py_None;
  Py_INCREF(ret);
  return ret;
}

/** .. method:: getrowtrace() -> Optional[RowTracer]

  Returns the currently installed (via :meth:`~Cursor.setrowtrace`)
  row tracer.

  .. seealso::

    * :ref:`tracing`
*/
static PyObject *
APSWCursor_getrowtrace(APSWCursor *self)
{
  PyObject *ret;
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);
  ret = (self->rowtrace) ? (self->rowtrace) : Py_None;
  Py_INCREF(ret);
  return ret;
}

/** .. method:: getconnection() -> Connection

  Returns the :attr:`connection` this cursor is using
*/

static PyObject *
APSWCursor_getconnection(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  Py_INCREF(self->connection);
  return (PyObject *)self->connection;
}

/** .. method:: fetchall() -> list[Tuple[SQLiteValue, ...]]

  Returns all remaining result rows as a list.  This method is defined
  in DBAPI.  It is a longer way of doing ``list(cursor)``.
*/
static PyObject *
APSWCursor_fetchall(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  return PySequence_List((PyObject *)self);
}

/** .. method:: fetchone() -> Optional[Any]

  Returns the next row of data or None if there are no more rows.
*/

static PyObject *
APSWCursor_fetchone(APSWCursor *self)
{
  PyObject *res;

  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  res = APSWCursor_next(self);

  if (res == NULL && !PyErr_Occurred())
    Py_RETURN_NONE;

  return res;
}

/** .. attribute:: exectrace
  :type: Optional[ExecTracer]

  Called with the cursor, statement and bindings for
  each :meth:`~Cursor.execute` or :meth:`~Cursor.executemany` on this
  cursor.

  If *callable* is *None* then any existing execution tracer is
  unregistered.

  .. seealso::

    * :ref:`tracing`
    * :ref:`executiontracer`
    * :attr:`Connection.exectrace`

*/
static PyObject *
APSWCursor_get_exectrace_attr(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  if (self->exectrace)
  {
    Py_INCREF(self->exectrace);
    return self->exectrace;
  }
  Py_RETURN_NONE;
}

static int
APSWCursor_set_exectrace_attr(APSWCursor *self, PyObject *value)
{
  CHECK_USE(-1);
  CHECK_CURSOR_CLOSED(-1);

  if (value != Py_None && !PyCallable_Check(value))
  {
    PyErr_Format(PyExc_TypeError, "exectrace expected a Callable");
    return -1;
  }
  Py_CLEAR(self->exectrace);
  if (value != Py_None)
  {
    Py_INCREF(value);
    self->exectrace = value;
  }
  return 0;
}

/** .. attribute:: rowtrace
  :type: Optional[RowTracer]

  Called with cursor and row being returned.  You can
  change the data that is returned or cause the row to be skipped
  altogether.

  If *callable* is *None* then any existing row tracer is
  unregistered.

  .. seealso::

    * :ref:`tracing`
    * :ref:`rowtracer`
    * :attr:`Connection.rowtrace`

*/
static PyObject *
APSWCursor_get_rowtrace_attr(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  if (self->rowtrace)
  {
    Py_INCREF(self->rowtrace);
    return self->rowtrace;
  }
  Py_RETURN_NONE;
}

static int
APSWCursor_set_rowtrace_attr(APSWCursor *self, PyObject *value)
{
  CHECK_USE(-1);
  CHECK_CURSOR_CLOSED(-1);

  if (value != Py_None && !PyCallable_Check(value))
  {
    PyErr_Format(PyExc_TypeError, "rowtrace expected a Callable");
    return -1;
  }
  Py_CLEAR(self->rowtrace);
  if (value != Py_None)
  {
    Py_INCREF(value);
    self->rowtrace = value;
  }
  return 0;
}

/** .. attribute:: connection
  :type: Connection

  :class:`Connection` this cursor is using
*/
static PyObject *
APSWCursor_getconnection_attr(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  Py_INCREF(self->connection);
  return (PyObject *)self->connection;
}

/** .. attribute:: is_explain
  :type: int

  Returns 0 if executing a normal query, 1 if it is an EXPLAIN query,
  and 2 if an EXPLAIN QUERY PLAN query.

  -* sqlite3_stmt_isexplain
*/
static PyObject *
APSWCursor_is_explain(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  return PyLong_FromLong(sqlite3_stmt_isexplain(self->statement->vdbestatement));
}

/** .. attribute:: is_readonly
  :type: bool

  Returns True if the current query does not change the database.

  Note that called functions, virtual tables etc could make changes though.

  -* sqlite3_stmt_readonly
*/
static PyObject *
APSWCursor_is_readonly(APSWCursor *self)
{
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  if (sqlite3_stmt_readonly(self->statement->vdbestatement))
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

/** .. attribute:: expanded_sql
  :type: str

  The SQL text with bound parameters expanded.  For example::

     execute("select ?, ?", (3, "three"))

  would return::

     select 3, 'three'

  Note that while SQLite supports nulls in strings, their implementation
  of sqlite3_expanded_sql stops at the first null.

  -* sqlite3_expanded_sql
*/
static PyObject *
APSWCursor_expanded_sql(APSWCursor *self)
{
  PyObject *res;
  const char *es;
  CHECK_USE(NULL);
  CHECK_CURSOR_CLOSED(NULL);

  PYSQLITE_VOID_CALL(es = sqlite3_expanded_sql(self->statement->vdbestatement));

  res = convertutf8string(es);
  sqlite3_free((void *)es);
  return res;
}

static PyMethodDef APSWCursor_methods[] = {
    {"execute", (PyCFunction)APSWCursor_execute, METH_VARARGS | METH_KEYWORDS,
     Cursor_execute_DOC},
    {"executemany", (PyCFunction)APSWCursor_executemany, METH_VARARGS | METH_KEYWORDS,
     Cursor_executemany_DOC},
    {"setexectrace", (PyCFunction)APSWCursor_setexectrace, METH_VARARGS | METH_KEYWORDS,
     Cursor_setexectrace_DOC},
    {"setrowtrace", (PyCFunction)APSWCursor_setrowtrace, METH_VARARGS | METH_KEYWORDS,
     Cursor_setrowtrace_DOC},
    {"getexectrace", (PyCFunction)APSWCursor_getexectrace, METH_NOARGS,
     Cursor_getexectrace_DOC},
    {"getrowtrace", (PyCFunction)APSWCursor_getrowtrace, METH_NOARGS,
     Cursor_getrowtrace_DOC},
    {"getconnection", (PyCFunction)APSWCursor_getconnection, METH_NOARGS,
     Cursor_getconnection_DOC},
    {"getdescription", (PyCFunction)APSWCursor_getdescription, METH_NOARGS,
     Cursor_getdescription_DOC},
    {"close", (PyCFunction)APSWCursor_close, METH_VARARGS | METH_KEYWORDS,
     Cursor_close_DOC},
    {"fetchall", (PyCFunction)APSWCursor_fetchall, METH_NOARGS,
     Cursor_fetchall_DOC},
    {"fetchone", (PyCFunction)APSWCursor_fetchone, METH_NOARGS,
     Cursor_fetchone_DOC},
    {0, 0, 0, 0} /* Sentinel */
};

static PyGetSetDef APSWCursor_getset[] = {
    {"description", (getter)APSWCursor_getdescription_dbapi, NULL, Cursor_description_DOC, NULL},
#ifdef SQLITE_ENABLE_COLUMN_METADATA
    {"description_full", (getter)APSWCursor_getdescription_full, NULL, Cursor_description_full_DOC, NULL},
#endif
    {"is_explain", (getter)APSWCursor_is_explain, NULL, Cursor_is_explain_DOC, NULL},
    {"is_readonly", (getter)APSWCursor_is_readonly, NULL, Cursor_is_readonly_DOC, NULL},
    {"expanded_sql", (getter)APSWCursor_expanded_sql, NULL, Cursor_expanded_sql_DOC, NULL},
    {"exectrace", (getter)APSWCursor_get_exectrace_attr, (setter)APSWCursor_set_exectrace_attr, Cursor_exectrace_DOC},
    {"rowtrace", (getter)APSWCursor_get_rowtrace_attr, (setter)APSWCursor_set_rowtrace_attr, Cursor_rowtrace_DOC},
    {"connection", (getter)APSWCursor_getconnection_attr, NULL, Cursor_connection_DOC},
    {NULL, NULL, NULL, NULL, NULL}};

static PyTypeObject APSWCursorType = {
    PyVarObject_HEAD_INIT(NULL, 0) "apsw.Cursor",                                                /*tp_name*/
    sizeof(APSWCursor),                                                                          /*tp_basicsize*/
    0,                                                                                           /*tp_itemsize*/
    (destructor)APSWCursor_dealloc,                                                              /*tp_dealloc*/
    0,                                                                                           /*tp_print*/
    0,                                                                                           /*tp_getattr*/
    0,                                                                                           /*tp_setattr*/
    0,                                                                                           /*tp_compare*/
    0,                                                                                           /*tp_repr*/
    0,                                                                                           /*tp_as_number*/
    0,                                                                                           /*tp_as_sequence*/
    0,                                                                                           /*tp_as_mapping*/
    0,                                                                                           /*tp_hash */
    0,                                                                                           /*tp_call*/
    0,                                                                                           /*tp_str*/
    0,                                                                                           /*tp_getattro*/
    0,                                                                                           /*tp_setattro*/
    0,                                                                                           /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_VERSION_TAG | Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    Cursor_class_DOC,                                                                            /* tp_doc */
    (traverseproc)APSWCursor_tp_traverse,                                                        /* tp_traverse */
    0,                                                                                           /* tp_clear */
    0,                                                                                           /* tp_richcompare */
    offsetof(APSWCursor, weakreflist),                                                           /* tp_weaklistoffset */
    (getiterfunc)APSWCursor_iter,                                                                /* tp_iter */
    (iternextfunc)APSWCursor_next,                                                               /* tp_iternext */
    APSWCursor_methods,                                                                          /* tp_methods */
    0,                                                                                           /* tp_members */
    APSWCursor_getset,                                                                           /* tp_getset */
    0,                                                                                           /* tp_base */
    0,                                                                                           /* tp_dict */
    0,                                                                                           /* tp_descr_get */
    0,                                                                                           /* tp_descr_set */
    0,                                                                                           /* tp_dictoffset */
    (initproc)APSWCursor_init,                                                                   /* tp_init */
    0,                                                                                           /* tp_alloc */
    APSWCursor_new,                                                                              /* tp_new */
    0,                                                                                           /* tp_free */
    0,                                                                                           /* tp_is_gc */
    0,                                                                                           /* tp_bases */
    0,                                                                                           /* tp_mro */
    0,                                                                                           /* tp_cache */
    0,                                                                                           /* tp_subclasses */
    0,                                                                                           /* tp_weaklist */
    0,                                                                                           /* tp_del */
    PyType_TRAILER};
