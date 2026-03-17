/*
   Virtual table code

   See the accompanying LICENSE file.
*/

/**

.. _virtualtables:

Virtual Tables
**************

`Virtual Tables <https://sqlite.org/vtab.html>`__ are a feature
introduced in SQLite 3.3.7. They let a developer provide an underlying
table implementations, while still presenting a normal SQL interface
to the user. The person writing SQL doesn't need to know or care that
some of the tables come from elsewhere.

Some examples of how you might use this:

* Translating to/from information stored in other formats (eg a csv/ini format file)

* Accessing the data remotely (eg you could make a table that backends into Amazon's API)

* Dynamic information (eg currently running processes, files and directories, objects in your program)

* Information that needs reformatting (eg if you have complex rules about how to convert strings to/from Unicode
  in the dataset)

* Information that isn't relationally correct (eg if you have data that has ended up with duplicate "unique" keys
  with code that dynamically corrects it)

* There are other examples on the `SQLite page <https://sqlite.org/vtab.html>`__

You need to have 3 types of object. A :class:`module <VTModule>`, a
:class:`virtual table <VTTable>` and a :class:`cursor
<VTCursor>`. These are documented below. You can also read the `SQLite
C method documentation <https://sqlite.org/vtab.html>`__.  At the C
level, they are just one set of methods. At the Python/APSW level,
they are split over the 3 types of object. The leading **x** is
omitted in Python. You can return SQLite error codes (eg
*SQLITE_READONLY*) by raising the appropriate exceptions (eg
:exc:`ReadOnlyError`).  :meth:`exceptionfor` is a useful helper
function to do the mapping.

*/

/** .. class:: VTModule

.. note::

  There is no actual *VTModule* class - it is shown this way for
  documentation convenience and is present as a `typing protocol
  <https://docs.python.org/3/library/typing.html#typing.Protocol>`__.
  Your module instance should implement all the methods documented here.

A module instance is used to create the virtual tables.  Once you have
a module object, you register it with a connection by calling
:meth:`Connection.createmodule`::

  # make an instance
  mymod=MyModuleClass()

  # register the vtable on connection con
  con.createmodule("modulename", mymod)

  # tell SQLite about the table
  con.execute("create VIRTUAL table tablename USING modulename('arg1', 2)")

The create step is to tell SQLite about the existence of the table.
Any number of tables referring to the same module can be made this
way.  Note the (optional) arguments which are passed to the module.
*/

typedef struct
{
  sqlite3_vtab used_by_sqlite; /* I don't touch this */
  PyObject *vtable;            /* object implementing vtable */
  PyObject *functions;         /* functions returned by vtabFindFunction */
} apsw_vtable;

static struct
{
  const char *methodname;
  const char *declarevtabtracebackname;
  const char *pyexceptionname;
} create_or_connect_strings[] =
    {
        {"Create",
         "VirtualTable.xCreate.sqlite3_declare_vtab",
         "VirtualTable.xCreate"},
        {"Connect",
         "VirtualTable.xConnect.sqlite3_declare_vtab",
         "VirtualTable.xConnect"}};

static int
apswvtabCreateOrConnect(sqlite3 *db,
                        void *pAux,
                        int argc,
                        const char *const *argv,
                        sqlite3_vtab **pVTab,
                        char **errmsg,
                        /* args above are to Create/Connect method */
                        int stringindex)
{
  PyGILState_STATE gilstate;
  vtableinfo *vti;
  PyObject *args = NULL, *pyres = NULL, *schema = NULL, *vtable = NULL;
  apsw_vtable *avi = NULL;
  int res = SQLITE_OK;
  int i;

  gilstate = PyGILState_Ensure();

  vti = (vtableinfo *)pAux;
  assert(db == vti->connection->db);

  args = PyTuple_New(1 + argc);
  if (!args)
    goto pyexception;

  Py_INCREF((PyObject *)(vti->connection));
  PyTuple_SET_ITEM(args, 0, (PyObject *)(vti->connection));
  for (i = 0; i < argc; i++)
  {
    PyObject *str;

    APSW_FAULT_INJECT(VtabCreateBadString, str = convertutf8string(argv[i]), str = PyErr_NoMemory());
    if (!str)
      goto pyexception;
    PyTuple_SET_ITEM(args, 1 + i, str);
  }

  pyres = Call_PythonMethod(vti->datasource, create_or_connect_strings[stringindex].methodname, 1, args);
  if (!pyres)
    goto pyexception;

  /* pyres should be a tuple of two values - a string of sql describing
     the table and an object implementing it */
  if (!PySequence_Check(pyres) || PySequence_Size(pyres) != 2)
  {
    PyErr_Format(PyExc_TypeError, "Expected two values - a string with the table schema and a vtable object implementing it");
    goto pyexception;
  }

  vtable = PySequence_GetItem(pyres, 1);
  if (!vtable)
    goto pyexception;

  avi = PyMem_Malloc(sizeof(apsw_vtable));
  if (!avi)
    goto pyexception;
  assert((void *)avi == (void *)&(avi->used_by_sqlite)); /* detect if weird padding happens */
  memset(avi, 0, sizeof(apsw_vtable));

  schema = PySequence_GetItem(pyres, 0);
  if (!schema)
    goto pyexception;
  if (!PyUnicode_Check(schema)) {
    PyErr_Format(PyExc_TypeError, "Expected string for schema");
    goto pyexception;
  }
  {
    const char *utf8schema = PyUnicode_AsUTF8(schema);
    if(!utf8schema)
      goto pyexception;
    _PYSQLITE_CALL_E(db, res = sqlite3_declare_vtab(db, utf8schema));
    if (res != SQLITE_OK)
    {
      SET_EXC(res, db);
      AddTraceBackHere(__FILE__, __LINE__, create_or_connect_strings[stringindex].declarevtabtracebackname, "{s: O}", "schema", OBJ(schema));
      goto finally;
    }
  }

  assert(res == SQLITE_OK);
  *pVTab = (sqlite3_vtab *)avi;
  avi->vtable = vtable;
  Py_INCREF(avi->vtable);
  avi = NULL;
  goto finally;

pyexception: /* we had an exception in python code */
  res = MakeSqliteMsgFromPyException(errmsg);
  AddTraceBackHere(__FILE__, __LINE__, create_or_connect_strings[stringindex].pyexceptionname,
                   "{s: s, s: s, s: s, s: O}", "modulename", argv[0], "database", argv[1], "tablename", argv[2], "schema", OBJ(schema));

finally: /* cleanup */
  Py_XDECREF(args);
  Py_XDECREF(pyres);
  Py_XDECREF(schema);
  Py_XDECREF(vtable);
  if (avi)
    PyMem_Free(avi);

  PyGILState_Release(gilstate);
  return res;
}

/** .. method:: Connect(connection: Connection, modulename: str, databasename: str, tablename: str, *args: Tuple[SQLiteValue, ...])  -> Tuple[str, VTTable]

    The parameters and return are identical to
    :meth:`~VTModule.Create`.  This method is called
    when there are additional references to the table.  :meth:`~VTModule.Create` will be called the first time and
    :meth:`~VTModule.Connect` after that.

    The advise is to create caches, generated data and other
    heavyweight processing on :meth:`~VTModule.Create` calls and then
    find and reuse that on the subsequent :meth:`~VTModule.Connect`
    calls.

    The corresponding call is :meth:`VTTable.Disconnect`.  If you have a simple virtual table implementation, then just
    set :meth:`~VTModule.Connect` to be the same as :meth:`~VTModule.Create`::

      class MyModule:

           def Create(self, connection, modulename, databasename, tablename, *args):
               # do lots of hard work

           Connect=Create

*/

static int
apswvtabCreate(sqlite3 *db,
               void *pAux,
               int argc,
               const char *const *argv,
               sqlite3_vtab **pVTab,
               char **errmsg)
{
  return apswvtabCreateOrConnect(db, pAux, argc, argv, pVTab, errmsg, 0);
}

/** .. method:: Create(connection: Connection, modulename: str, databasename: str, tablename: str, *args: Tuple[SQLiteValue, ...])  -> Tuple[str, VTTable]

   Called when a table is first created on a :class:`connection
   <Connection>`.

   :param connection: An instance of :class:`Connection`
   :param modulename: The string name under which the module was :meth:`registered <Connection.createmodule>`
   :param databasename: The name of the database.  This will be ``main`` for directly opened files and the name specified in
           `ATTACH <https://sqlite.org/lang_attach.html>`_ statements.
   :param tablename: Name of the table the user wants to create.
   :param args: Any arguments that were specified in the `create virtual table <https://sqlite.org/lang_createvtab.html>`_ statement.

   :returns: A list of two items.  The first is a SQL `create table <https://sqlite.org/lang_createtable.html>`_ statement.  The
        columns are parsed so that SQLite knows what columns and declared types exist for the table.  The second item
        is an object that implements the :class:`table <VTTable>` methods.

   The corresponding call is :meth:`VTTable.Destroy`.
*/

static int
apswvtabConnect(sqlite3 *db,
                void *pAux,
                int argc,
                const char *const *argv,
                sqlite3_vtab **pVTab,
                char **errmsg)
{
  return apswvtabCreateOrConnect(db, pAux, argc, argv, pVTab, errmsg, 1);
}

/** .. class:: VTTable

  .. note::

    There is no actual *VTTable* class - it is shown this way for
    documentation convenience and is present as a `typing protocol
    <https://docs.python.org/3/library/typing.html#typing.Protocol>`__.
    Your table instance should implement the methods documented here.

  The :class:`VTTable` object contains knowledge of the indices, makes
  cursors and can perform transactions.


  .. _vtablestructure:

  A virtual table is structured as a series of rows, each of which has
  the same columns.  The value in a column must be one of the `5
  supported types <https://sqlite.org/datatype3.html>`_, but the
  type can be different between rows for the same column.  The virtual
  table routines identify the columns by number, starting at zero.

  Each row has a **unique** 64 bit integer `rowid
  <https://sqlite.org/autoinc.html>`_ with the :class:`Cursor
  <VTCursor>` routines operating on this number, as well as some of
  the :class:`Table <VTTable>` routines such as :meth:`UpdateChangeRow
  <VTTable.UpdateChangeRow>`.

*/

static void
apswvtabFree(void *context)
{
  vtableinfo *vti = (vtableinfo *)context;
  PyGILState_STATE gilstate;
  gilstate = PyGILState_Ensure();

  Py_XDECREF(vti->datasource);
  /* connection was a borrowed reference so no decref needed */
  PyMem_Free(vti);

  PyGILState_Release(gilstate);
}

static struct
{
  const char *methodname;
  const char *pyexceptionname;
} destroy_disconnect_strings[] =
    {
        {"Destroy",
         "VirtualTable.xDestroy"},
        {"Disconnect",
         "VirtualTable.xDisconnect"}};

/* See SQLite ticket 2099 */
static int
apswvtabDestroyOrDisconnect(sqlite3_vtab *pVtab, int stringindex)
{
  PyObject *vtable, *res = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();
  vtable = ((apsw_vtable *)pVtab)->vtable;

  /* mandatory for Destroy, optional for Disconnect */
  res = Call_PythonMethod(vtable, destroy_disconnect_strings[stringindex].methodname, (stringindex == 0), NULL);
  /* sqlite 3.3.8 ignore return code for disconnect so we always free */
  if (res || stringindex == 1)
  {
    /* see SQLite ticket 2127 */
    if (pVtab->zErrMsg)
      sqlite3_free(pVtab->zErrMsg);

    Py_DECREF(vtable);
    Py_XDECREF(((apsw_vtable *)pVtab)->functions);
    PyMem_Free(pVtab);
    goto finally;
  }

  if (stringindex == 0)
  {
    /* ::TODO:: waiting on ticket 2099 to know if the pVtab should also be freed in case of error return with Destroy. */
#if 0
      /* see SQLite ticket 2127 */
      if(pVtab->zErrMsg)
	sqlite3_free(pVtab->zErrMsg);

      Py_DECREF(vtable);
      PyMem_Free(pVtab);
#endif
  }

  /* pyexception:  we had an exception in python code */
  sqliteres = MakeSqliteMsgFromPyException(&(pVtab->zErrMsg));
  AddTraceBackHere(__FILE__, __LINE__, destroy_disconnect_strings[stringindex].pyexceptionname, "{s: O}", "self", OBJ(vtable));

finally:
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Destroy() -> None

  The opposite of :meth:`VTModule.Create`.  This method is called when
  the table is no longer used.  Note that you must always release
  resources even if you intend to return an error, as it will not be
  called again on error.  SQLite may also leak memory
  if you return an error.
*/

static int
apswvtabDestroy(sqlite3_vtab *pVTab)
{
  return apswvtabDestroyOrDisconnect(pVTab, 0);
}

/** .. method:: Disconnect() -> None

  The opposite of :meth:`VTModule.Connect`.  This method is called when
  a reference to a virtual table is no longer used, but :meth:`VTTable.Destroy` will
  be called when the table is no longer used.
*/

static int
apswvtabDisconnect(sqlite3_vtab *pVTab)
{
  return apswvtabDestroyOrDisconnect(pVTab, 1);
}

/** .. method:: BestIndex(constraints: Sequence[Tuple[int, int], ...], orderbys: Sequence[Tuple[int, int], ...]) -> Any

  This is a complex method. To get going initially, just return
  *None* and you will be fine. Implementing this method reduces
  the number of rows scanned in your table to satisfy queries, but
  only if you have an index or index like mechanism available.

  .. note::

    The implementation of this method differs slightly from the
    `SQLite documentation
    <https://sqlite.org/vtab.html>`__
    for the C API. You are not passed "unusable" constraints. The
    argv/constraintarg positions are not off by one. In the C api, you
    have to return position 1 to get something passed to
    :meth:`VTCursor.Filter` in position 0. With the APSW
    implementation, you return position 0 to get Filter arg 0,
    position 1 to get Filter arg 1 etc.

  The purpose of this method is to ask if you have the ability to
  determine if a row meets certain constraints that doesn't involve
  visiting every row. An example constraint is ``price > 74.99``. In a
  traditional SQL database, queries with constraints can be speeded up
  `with indices <https://sqlite.org/lang_createindex.html>`_. If
  you return None, then SQLite will visit every row in your table and
  evaluate the constraint itself. Your index choice returned from
  BestIndex will also be passed to the :meth:`~VTCursor.Filter` method on your cursor
  object. Note that SQLite may call this method multiple times trying
  to find the most efficient way of answering a complex query.

  **constraints**

  You will be passed the constraints as a sequence of tuples containing two
  items. The first item is the column number and the second item is
  the operation.

     Example query: ``select * from foo where price > 74.99 and
     quantity<=10 and customer='Acme Widgets'``

     If customer is column 0, price column 2 and quantity column 5
     then the constraints will be::

       (2, apsw.SQLITE_INDEX_CONSTRAINT_GT),
       (5, apsw.SQLITE_INDEX_CONSTRAINT_LE),
       (0, apsw.SQLITE_INDEX_CONSTRAINT_EQ)

     Note that you do not get the value of the constraint (ie "Acme
     Widgets", 74.99 and 10 in this example).

  If you do have any suitable indices then you return a sequence the
  same length as constraints with the members mapping to the
  constraints in order. Each can be one of None, an integer or a tuple
  of an integer and a boolean.  Conceptually SQLite is giving you a
  list of constraints and you are returning a list of the same length
  describing how you could satisfy each one.

  Each list item returned corresponding to a constraint is one of:

     None
       This means you have no index for that constraint. SQLite
       will have to iterate over every row for it.

     integer
       This is the argument number for the constraintargs being passed
       into the :meth:`~VTCursor.Filter` function of your
       :class:`cursor <VTCursor>` (the values "Acme Widgets", 74.99
       and 10 in the example).

     (integer, boolean)
       By default SQLite will check what you return. For example if
       you said that you had an index on price, SQLite will still
       check that each row you returned is greater than 74.99. If you
       set the boolean to False then SQLite won't do that double
       checking.

  Example query: ``select * from foo where price > 74.99 and
  quantity<=10 and customer=='Acme Widgets'``.  customer is column 0,
  price column 2 and quantity column 5.  You can index on customer
  equality and price.

  +----------------------------------------+--------------------------------+
  | Constraints (in)                       | Constraints used (out)         |
  +========================================+================================+
  | ::                                     | ::                             |
  |                                        |                                |
  |  (2, apsw.SQLITE_INDEX_CONSTRAINT_GT), |     1,                         |
  |  (5, apsw.SQLITE_INDEX_CONSTRAINT_LE), |     None,                      |
  |  (0, apsw.SQLITE_INDEX_CONSTRAINT_EQ)  |     0                          |
  |                                        |                                |
  +----------------------------------------+--------------------------------+

  When your :class:`~VTCursor.Filter` method in the cursor is called,
  constraintarg[0] will be "Acme Widgets" (customer constraint value)
  and constraintarg[1] will be 74.99 (price constraint value). You can
  also return an index number (integer) and index string to use
  (SQLite attaches no significance to these values - they are passed
  as is to your :meth:`VTCursor.Filter` method as a way for the
  BestIndex method to let the :meth:`~VTCursor.Filter` method know
  which of your indices or similar mechanism to use.

  **orderbys**


  The second argument to BestIndex is a sequence of orderbys because
  the query requested the results in a certain order. If your data is
  already in that order then SQLite can give the results back as
  is. If not, then SQLite will have to sort the results first.

    Example query: ``select * from foo order by price desc, quantity asc``

    Price is column 2, quantity column 5 so orderbys will be::

      (2, True),  # True means descending, False is ascending
      (5, False)

  **Return**

  You should return up to 5 items. Items not present in the return have a default value.

  0: constraints used (default None)
    This must either be None or a sequence the same length as
    constraints passed in. Each item should be as specified above
    saying if that constraint is used, and if so which constraintarg
    to make the value be in your :meth:`VTCursor.Filter` function.

  1: index number (default zero)
    This value is passed as is to :meth:`VTCursor.Filter`

  2: index string (default None)
    This value is passed as is to :meth:`VTCursor.Filter`

  3: orderby consumed (default False)
    Return True if your output will be in exactly the same order as the orderbys passed in

  4: estimated cost (default a huge number)
    Approximately how many disk operations are needed to provide the
    results. SQLite uses the cost to optimise queries. For example if
    the query includes *A or B* and A has 2,000 operations and B has 100
    then it is best to evaluate B before A.

  **A complete example**

  Query is ``select * from foo where price>74.99 and quantity<=10 and
  customer=="Acme Widgets" order by price desc, quantity asc``.
  Customer is column 0, price column 2 and quantity column 5. You can
  index on customer equality and price.

  ::

    BestIndex(constraints, orderbys)

    constraints= ( (2, apsw.SQLITE_INDEX_CONSTRAINT_GT),
                   (5, apsw.SQLITE_INDEX_CONSTRAINT_LE),
                   (0, apsw.SQLITE_INDEX_CONSTRAINT_EQ)  )

    orderbys= ( (2, True), (5, False) )


    # You return

    ( (1, None, 0),   # constraints used
      27,             # index number
      "idx_pr_cust",  # index name
      False,          # results are not in orderbys order
      1000            # about 1000 disk operations to access index
    )


    # Your Cursor.Filter method will be called with:

    27,              # index number you returned
    "idx_pr_cust",   # index name you returned
    "Acme Widgets",  # constraintarg[0] - customer
    74.99            # constraintarg[1] - price

*/

static int
apswvtabBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *indexinfo)
{
  PyGILState_STATE gilstate;
  PyObject *vtable;
  PyObject *constraints = NULL, *orderbys = NULL;
  PyObject *res = NULL, *indices = NULL;
  int i, j;
  int nconstraints = 0;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();

  vtable = ((apsw_vtable *)pVtab)->vtable;

  /* count how many usable constraints there are */
  for (i = 0; i < indexinfo->nConstraint; i++)
    if (indexinfo->aConstraint[i].usable)
      nconstraints++;

  constraints = PyTuple_New(nconstraints);
  if (!constraints)
    goto pyexception;

  /* fill them in */
  for (i = 0, j = 0; i < indexinfo->nConstraint; i++)
  {
    PyObject *constraint = NULL;
    if (!indexinfo->aConstraint[i].usable)
      continue;

    constraint = Py_BuildValue("(iB)", indexinfo->aConstraint[i].iColumn, indexinfo->aConstraint[i].op);
    if (!constraint)
      goto pyexception;

    PyTuple_SET_ITEM(constraints, j, constraint);
    j++;
  }

  /* group bys */
  orderbys = PyTuple_New(indexinfo->nOrderBy);
  if (!orderbys)
    goto pyexception;

  /* fill them in */
  for (i = 0; i < indexinfo->nOrderBy; i++)
  {
    PyObject *order = NULL;

    order = Py_BuildValue("(iN)", indexinfo->aOrderBy[i].iColumn, PyBool_FromLong(indexinfo->aOrderBy[i].desc));
    if (!order)
      goto pyexception;

    PyTuple_SET_ITEM(orderbys, i, order);
  }

  /* actually call the function */
  res = Call_PythonMethodV(vtable, "BestIndex", 1, "(OO)", constraints, orderbys);
  if (!res)
    goto pyexception;

  /* do we have useful index information? */
  if (res == Py_None)
    goto finally;

  /* check we have a sequence */
  if (!PySequence_Check(res) || PySequence_Size(res) > 5)
  {
    PyErr_Format(PyExc_TypeError, "Bad result from BestIndex.  It should be a sequence of up to 5 items");
    AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_check", "{s: O, s: O}", "self", vtable, "result", OBJ(res));
    goto pyexception;
  }

  /* dig the argv indices out */
  if (PySequence_Size(res) == 0)
    goto finally;

  indices = PySequence_GetItem(res, 0);
  if (indices != Py_None)
  {
    if (!PySequence_Check(indices) || PySequence_Size(indices) != nconstraints)
    {
      PyErr_Format(PyExc_TypeError, "Bad constraints (item 0 in BestIndex return).  It should be a sequence the same length as the constraints passed in (%d) items", nconstraints);
      AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_indices", "{s: O, s: O, s: O}",
                       "self", vtable, "result", OBJ(res), "indices", OBJ(indices));
      goto pyexception;
    }
    /* iterate through the items - i is the SQLite sequence number and j is the apsw one (usable entries) */
    for (i = 0, j = 0; i < indexinfo->nConstraint; i++)
    {
      PyObject *constraint = NULL, *argvindex = NULL, *omit = NULL;
      int omitv;
      if (!indexinfo->aConstraint[i].usable)
        continue;
      constraint = PySequence_GetItem(indices, j);
      if (PyErr_Occurred() || !constraint)
        goto pyexception;
      j++;
      /* it can be None */
      if (constraint == Py_None)
      {
        Py_DECREF(constraint);
        continue;
      }
      /* or an integer */
      if (PyLong_Check(constraint))
      {
        indexinfo->aConstraintUsage[i].argvIndex = PyLong_AsLong(constraint) + 1;
        Py_DECREF(constraint);
        continue;
      }
      /* or a sequence two items long */
      if (!PySequence_Check(constraint) || PySequence_Size(constraint) != 2)
      {
        PyErr_Format(PyExc_TypeError, "Bad constraint (#%d) - it should be one of None, an integer or a tuple of an integer and a boolean", j);
        AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_constraint", "{s: O, s: O, s: O, s: O}",
                         "self", vtable, "result", OBJ(res), "indices", OBJ(indices), "constraint", OBJ(constraint));
        Py_DECREF(constraint);
        goto pyexception;
      }
      argvindex = PySequence_GetItem(constraint, 0);
      omit = PySequence_GetItem(constraint, 1);
      if (!argvindex || !omit)
        goto constraintfail;
      if (!PyLong_Check(argvindex))
      {
        PyErr_Format(PyExc_TypeError, "argvindex for constraint #%d should be an integer", j);
        AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_constraint_argvindex", "{s: O, s: O, s: O, s: O, s: O}",
                         "self", vtable, "result", OBJ(res), "indices", OBJ(indices), "constraint", OBJ(constraint), "argvindex", OBJ(argvindex));
        goto constraintfail;
      }
      omitv = PyObject_IsTrue(omit);
      if (omitv == -1)
        goto constraintfail;
      indexinfo->aConstraintUsage[i].argvIndex = PyLong_AsLong(argvindex) + 1;
      indexinfo->aConstraintUsage[i].omit = omitv;
      Py_DECREF(constraint);
      Py_DECREF(argvindex);
      Py_DECREF(omit);
      continue;

    constraintfail:
      Py_DECREF(constraint);
      Py_XDECREF(argvindex);
      Py_XDECREF(omit);
      goto pyexception;
    }
  }

  /* item #1 is idxnum */
  if (PySequence_Size(res) < 2)
    goto finally;
  {
    PyObject *idxnum = PySequence_GetItem(res, 1);
    if (!idxnum)
      goto pyexception;
    if (idxnum != Py_None)
    {
      if (!PyLong_Check(idxnum))
      {
        PyErr_Format(PyExc_TypeError, "idxnum must be an integer");
        AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex.result_indexnum", "{s: O, s: O, s: O}", "self", vtable, "result", OBJ(res), "indexnum", OBJ(idxnum));
        Py_DECREF(idxnum);
        goto pyexception;
      }
      indexinfo->idxNum = PyLong_AsLong(idxnum);
    }
    Py_DECREF(idxnum);
  }

  /* item #2 is idxStr */
  if (PySequence_Size(res) < 3)
    goto finally;
  {
    PyObject *idxstr = NULL;
    idxstr = PySequence_GetItem(res, 2);
    if (!idxstr)
      goto pyexception;
    if (idxstr != Py_None)
    {
      if(!PyUnicode_Check(idxstr)) {
        PyErr_Format(PyExc_TypeError, "Expected a string for idxStr");
        Py_DECREF(idxstr);
        goto pyexception;
      }
      indexinfo->idxStr = sqlite3_mprintf("%s", PyUnicode_AsUTF8(idxstr));
      indexinfo->needToFreeIdxStr = 1;
    }

  }

  /* item 3 is orderByConsumed */
  if (PySequence_Size(res) < 4)
    goto finally;
  {
    PyObject *orderbyconsumed = NULL;
    int iorderbyconsumed;
    orderbyconsumed = PySequence_GetItem(res, 3);
    if (!orderbyconsumed)
      goto pyexception;
    if (orderbyconsumed != Py_None)
    {
      iorderbyconsumed = PyObject_IsTrue(orderbyconsumed);
      if (iorderbyconsumed == -1)
      {
        Py_DECREF(orderbyconsumed);
        goto pyexception;
      }
      indexinfo->orderByConsumed = iorderbyconsumed;
    }
    Py_DECREF(orderbyconsumed);
  }

  /* item 4 (final) is estimated cost */
  if (PySequence_Size(res) < 5)
    goto finally;
  assert(PySequence_Size(res) == 5);
  {
    PyObject *estimatedcost = NULL, *festimatedcost = NULL;
    estimatedcost = PySequence_GetItem(res, 4);
    if (!estimatedcost)
      goto pyexception;
    if (estimatedcost != Py_None)
    {
      festimatedcost = PyNumber_Float(estimatedcost);
      if (!festimatedcost)
      {
        Py_DECREF(estimatedcost);
        goto pyexception;
      }
      indexinfo->estimatedCost = PyFloat_AsDouble(festimatedcost);
    }
    Py_XDECREF(festimatedcost);
    Py_DECREF(estimatedcost);
  }

  goto finally;

pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(&(pVtab->zErrMsg));
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xBestIndex", "{s: O, s: O, s: (OO)}", "self", vtable, "result", OBJ(res), "args", OBJ(constraints), OBJ(orderbys));

finally:
  Py_XDECREF(indices);
  Py_XDECREF(res);
  Py_XDECREF(constraints);
  Py_XDECREF(orderbys);
  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Begin() -> None

  This function is used as part of transactions.  You do not have to
  provide the method.
*/

/** .. method:: Sync() -> None

  This function is used as part of transactions.  You do not have to
  provide the method.
*/

/** .. method:: Commit() -> None

  This function is used as part of transactions.  You do not have to
  provide the method.
*/

/** .. method:: Rollback() -> None

  This function is used as part of transactions.  You do not have to
  provide the method.
*/

static struct
{
  const char *methodname;
  const char *pyexceptionname;
} transaction_strings[] =
    {
        {"Begin",
         "VirtualTable.Begin"},
        {"Sync",
         "VirtualTable.Sync"},
        {"Commit",
         "VirtualTable.Commit"},
        {"Rollback",
         "VirtualTable.Rollback"},

};

static int
apswvtabTransactionMethod(sqlite3_vtab *pVtab, int stringindex)
{
  PyObject *vtable, *res = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();
  vtable = ((apsw_vtable *)pVtab)->vtable;

  res = Call_PythonMethod(vtable, transaction_strings[stringindex].methodname, 0, NULL);
  if (res)
    goto finally;

  /*  pyexception: we had an exception in python code */
  sqliteres = MakeSqliteMsgFromPyException(&(pVtab->zErrMsg));
  AddTraceBackHere(__FILE__, __LINE__, transaction_strings[stringindex].pyexceptionname, "{s: O}", "self", vtable);

finally:
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

static int
apswvtabBegin(sqlite3_vtab *pVtab)
{
  return apswvtabTransactionMethod(pVtab, 0);
}

static int
apswvtabSync(sqlite3_vtab *pVtab)
{
  return apswvtabTransactionMethod(pVtab, 1);
}

static int
apswvtabCommit(sqlite3_vtab *pVtab)
{
  return apswvtabTransactionMethod(pVtab, 2);
}

static int
apswvtabRollback(sqlite3_vtab *pVtab)
{
  return apswvtabTransactionMethod(pVtab, 3);
}

/** .. method:: Open() -> VTCursor

  Returns a :class:`cursor <VTCursor>` object.
*/

typedef struct
{
  sqlite3_vtab_cursor used_by_sqlite; /* I don't touch this */
  PyObject *cursor;                   /* Object implementing cursor */
} apsw_vtable_cursor;

static int
apswvtabOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor)
{
  PyObject *vtable = NULL, *res = NULL;
  PyGILState_STATE gilstate;
  apsw_vtable_cursor *avc = NULL;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();

  vtable = ((apsw_vtable *)pVtab)->vtable;

  res = Call_PythonMethod(vtable, "Open", 1, NULL);
  if (!res)
    goto pyexception;
  avc = PyMem_Malloc(sizeof(apsw_vtable_cursor));
  assert((void *)avc == (void *)&(avc->used_by_sqlite)); /* detect if weird padding happens */
  memset(avc, 0, sizeof(apsw_vtable_cursor));

  avc->cursor = res;
  res = NULL;
  *ppCursor = (sqlite3_vtab_cursor *)avc;
  goto finally;

pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(&(pVtab->zErrMsg));
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xOpen", "{s: O}", "self", OBJ(vtable));

finally:
  Py_XDECREF(res);
  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: UpdateDeleteRow(rowid: int)

  Delete the row with the specified *rowid*.

  :param rowid: 64 bit integer
*/
/** .. method:: UpdateInsertRow(rowid: Optional[int], fields: Tuple[SQLiteValue, ...])  -> Optional[int]

  Insert a row with the specified *rowid*.

  :param rowid: *None* if you should choose the rowid yourself, else a 64 bit integer
  :param fields: A tuple of values the same length and order as columns in your table

  :returns: If *rowid* was *None* then return the id you assigned
    to the row.  If *rowid* was not *None* then the return value
    is ignored.
*/
/** .. method:: UpdateChangeRow(row: int, newrowid: int, fields: Tuple[SQLiteValue, ...])

  Change an existing row.  You may also need to change the rowid - for example if the query was
  ``UPDATE table SET rowid=rowid+100 WHERE ...``

  :param row: The existing 64 bit integer rowid
  :param newrowid: If not the same as *row* then also change the rowid to this.
  :param fields: A tuple of values the same length and order as columns in your table
*/
static int
apswvtabUpdate(sqlite3_vtab *pVtab, int argc, sqlite3_value **argv, sqlite3_int64 *pRowid)
{
  PyObject *vtable, *args = NULL, *res = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = SQLITE_OK;
  int i;
  const char *methodname = "unknown";

  assert(argc); /* should always be >0 */

  gilstate = PyGILState_Ensure();

  vtable = ((apsw_vtable *)pVtab)->vtable;

  /* case 1 - argc=1 means delete row */
  if (argc == 1)
  {
    methodname = "UpdateDeleteRow";
    args = Py_BuildValue("(O&)", convert_value_to_pyobject, argv[0]);
    if (!args)
      goto pyexception;
  }
  /* case 2 - insert a row */
  else if (sqlite3_value_type(argv[0]) == SQLITE_NULL)
  {
    PyObject *newrowid;
    methodname = "UpdateInsertRow";
    args = PyTuple_New(2);
    if (!args)
      goto pyexception;
    if (sqlite3_value_type(argv[1]) == SQLITE_NULL)
    {
      newrowid = Py_None;
      Py_INCREF(newrowid);
    }
    else
    {
      newrowid = convert_value_to_pyobject(argv[1]);
      if (!newrowid)
        goto pyexception;
    }
    PyTuple_SET_ITEM(args, 0, newrowid);
  }
  /* otherwise changing a row */
  else
  {
    PyObject *oldrowid = NULL, *newrowid = NULL;
    methodname = "UpdateChangeRow";
    args = PyTuple_New(3);
    oldrowid = convert_value_to_pyobject(argv[0]);
    APSW_FAULT_INJECT(VtabUpdateChangeRowFail, newrowid = convert_value_to_pyobject(argv[1]), newrowid = PyErr_NoMemory());
    if (!args || !oldrowid || !newrowid)
    {
      Py_XDECREF(oldrowid);
      Py_XDECREF(newrowid);
      goto pyexception;
    }
    PyTuple_SET_ITEM(args, 0, oldrowid);
    PyTuple_SET_ITEM(args, 1, newrowid);
  }

  /* new row values */
  if (argc != 1)
  {
    PyObject *fields = NULL;
    fields = PyTuple_New(argc - 2);
    if (!fields)
      goto pyexception;
    for (i = 0; i + 2 < argc; i++)
    {
      PyObject *field;
      APSW_FAULT_INJECT(VtabUpdateBadField, field = convert_value_to_pyobject(argv[i + 2]), field = PyErr_NoMemory());
      if (!field)
      {
        Py_DECREF(fields);
        goto pyexception;
      }
      PyTuple_SET_ITEM(fields, i, field);
    }
    PyTuple_SET_ITEM(args, PyTuple_GET_SIZE(args) - 1, fields);
  }

  res = Call_PythonMethod(vtable, methodname, 1, args);
  if (!res)
    goto pyexception;

  /* if row deleted then we don't care about return */
  if (argc == 1)
    goto finally;

  if (sqlite3_value_type(argv[0]) == SQLITE_NULL && sqlite3_value_type(argv[1]) == SQLITE_NULL)
  {
    /* did an insert and must provide a row id */
    PyObject *rowid = PyNumber_Long(res);
    if (!rowid)
      goto pyexception;

    *pRowid = PyLong_AsLongLong(rowid);
    Py_DECREF(rowid);
    if (PyErr_Occurred())
    {
      AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xUpdateInsertRow.ReturnedValue", "{s: O}", "result", OBJ(rowid));
      goto pyexception;
    }
  }

  goto finally;

pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(&pVtab->zErrMsg);
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xUpdate", "{s: O, s: i, s: s, s: O}", "self", vtable, "argc", argc, "methodname", methodname, "args", OBJ(args));

finally:
  Py_XDECREF(args);
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: FindFunction(name: str, nargs: int)

  Called to find if the virtual table has its own implementation of a
  particular scalar function. You should return the function if you
  have it, else return None. You do not have to provide this method.

  This method is called while SQLite is `preparing
  <https://sqlite.org/c3ref/prepare.html>`_ a query.  If a query is
  in the :ref:`statement cache <statementcache>` then *FindFunction*
  won't be called again.  If you want to return different
  implementations for the same function over time then you will need
  to disable the :ref:`statement cache <statementcache>`.

  :param name: The function name
  :param nargs: How many arguments the function takes

  .. seealso::

    * :meth:`Connection.overloadfunction`

*/

/*
  We have to save everything returned for the lifetime of the table as
  we don't know when it is no longer used due to `SQLite ticket 2095
  <https://sqlite.org/cvstrac/tktview?tn=2095>`_.

  This taps into the existing scalar function code in connection.c
*/
static int
apswvtabFindFunction(sqlite3_vtab *pVtab, int nArg, const char *zName,
                     void (**pxFunc)(sqlite3_context *, int, sqlite3_value **),
                     void **ppArg)
{
  PyGILState_STATE gilstate;
  int sqliteres = 0;
  PyObject *vtable, *res = NULL;
  FunctionCBInfo *cbinfo = NULL;
  apsw_vtable *av = (apsw_vtable *)pVtab;

  gilstate = PyGILState_Ensure();
  vtable = av->vtable;

  res = Call_PythonMethodV(vtable, "FindFunction", 0, "(Ni)", convertutf8string(zName), nArg);
  if (res != Py_None)
  {
    if (!av->functions)
    {
      APSW_FAULT_INJECT(FindFunctionAllocFailed,
                        av->functions = PyList_New(0),
                        av->functions = PyErr_NoMemory());
    }
    if (!av->functions)
    {
      assert(PyErr_Occurred());
      goto error;
    }
    cbinfo = allocfunccbinfo(zName);
    if (!cbinfo)
      goto error;
    cbinfo->scalarfunc = res;
    res = NULL;
    sqliteres = 1;
    *pxFunc = cbdispatch_func;
    *ppArg = cbinfo;
    PyList_Append(av->functions, (PyObject *)cbinfo);
  }
error:
  Py_XDECREF(res);
  Py_XDECREF(cbinfo);
  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Rename(newname: str) -> None

  Notification that the table will be given a new name. If you return
  without raising an exception, then SQLite renames the table (you
  don't have to do anything). If you raise an exception then the
  renaming is prevented.  You do not have to provide this method.

*/
static int
apswvtabRename(sqlite3_vtab *pVtab, const char *zNew)
{
  PyGILState_STATE gilstate;
  PyObject *vtable, *res = NULL, *newname = NULL;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();
  vtable = ((apsw_vtable *)pVtab)->vtable;

  APSW_FAULT_INJECT(VtabRenameBadName, newname = convertutf8string(zNew), newname = PyErr_NoMemory());
  if (!newname)
  {
    sqliteres = SQLITE_ERROR;
    goto finally;
  }
  /* Marked as optional since sqlite does the actual renaming */
  res = Call_PythonMethodV(vtable, "Rename", 0, "(N)", newname);
  if (!res)
  {
    sqliteres = MakeSqliteMsgFromPyException(NULL);
    AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xRename", "{s: O, s: s}", "self", vtable, "newname", zNew);
  }

finally:
  Py_XDECREF(res);
  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. class:: VTCursor

.. note::

  There is no actual *VTCursor* class - it is shown this way for
  documentation convenience and is present as a `typing protocol
  <https://docs.python.org/3/library/typing.html#typing.Protocol>`__.
  Your cursor instance should implement all the methods documented
  here.


The :class:`VTCursor` object is used for iterating over a table.
There may be many cursors simultaneously so each one needs to keep
track of where      :ref:`Virtual table structure <vtablestructure>`
it is.

.. seealso::

     :ref:`Virtual table structure <vtablestructure>`

*/

/** .. method:: Filter(indexnum: int, indexname: str, constraintargs: Optional[Tuple]) -> None

  This method is always called first to initialize an iteration to the
  first row of the table. The arguments come from the
  :meth:`~VTTable.BestIndex` method in the :class:`table <VTTable>`
  object with constraintargs being a tuple of the constraints you
  requested. If you always return None in BestIndex then indexnum will
  be zero, indexstring will be None and constraintargs will be empty).
*/
static int
apswvtabFilter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr,
               int argc, sqlite3_value **sqliteargv)
{
  PyObject *cursor, *argv = NULL, *res = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = SQLITE_OK;
  int i;

  gilstate = PyGILState_Ensure();

  cursor = ((apsw_vtable_cursor *)pCursor)->cursor;

  argv = PyTuple_New(argc);
  if (!argv)
    goto pyexception;
  for (i = 0; i < argc; i++)
  {
    PyObject *value = convert_value_to_pyobject(sqliteargv[i]);
    if (!value)
      goto pyexception;
    PyTuple_SET_ITEM(argv, i, value);
  }

  res = Call_PythonMethodV(cursor, "Filter", 1, "(iO&O)", idxNum, convertutf8string, idxStr, argv);
  if (res)
    goto finally; /* result is ignored */

pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(&(pCursor->pVtab->zErrMsg)); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xFilter", "{s: O}", "self", cursor);

finally:
  Py_XDECREF(argv);
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Eof() -> bool

  Called to ask if we are at the end of the table. It is called after each call to Filter and Next.

  :returns: False if the cursor is at a valid row of data, else True

  .. note::

    This method can only return True or False to SQLite.  If you have
    an exception in the method or provide a non-boolean return then
    True (no more data) will be returned to SQLite.
*/

static int
apswvtabEof(sqlite3_vtab_cursor *pCursor)
{
  PyObject *cursor, *res = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = 0; /* nb a true/false value not error code */

  gilstate = PyGILState_Ensure();

  /* is there already an error? */
  if (PyErr_Occurred())
    goto finally;

  cursor = ((apsw_vtable_cursor *)pCursor)->cursor;

  res = Call_PythonMethod(cursor, "Eof", 1, NULL);
  if (!res)
    goto pyexception;

  sqliteres = PyObject_IsTrue(res);
  if (sqliteres == 0 || sqliteres == 1)
    goto finally;

pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(&(pCursor->pVtab->zErrMsg)); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xEof", "{s: O}", "self", cursor);

finally:
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Column(number: int) -> SQLiteValue

  Requests the value of the specified column *number* of the current
  row.  If *number* is -1 then return the rowid.

  :returns: Must be one one of the :ref:`5
    supported types <types>`
*/
/* forward decln */
static int set_context_result(sqlite3_context *context, PyObject *obj);

static int
apswvtabColumn(sqlite3_vtab_cursor *pCursor, sqlite3_context *result, int ncolumn)
{
  PyObject *cursor, *res = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = SQLITE_OK, ok;

  gilstate = PyGILState_Ensure();

  cursor = ((apsw_vtable_cursor *)pCursor)->cursor;

  res = Call_PythonMethodV(cursor, "Column", 1, "(i)", ncolumn);
  if (!res)
    goto pyexception;

  ok = set_context_result(result, res);
  if (!PyErr_Occurred())
  {
    assert(ok);
    (void)ok;
    goto finally;
  }
pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(&(pCursor->pVtab->zErrMsg)); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xColumn", "{s: O, s: O}", "self", cursor, "res", OBJ(res));

finally:
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Next() -> None

  Move the cursor to the next row.  Do not have an exception if there
  is no next row.  Instead return False when :meth:`~VTCursor.Eof` is
  subsequently called.

  If you said you had indices in your :meth:`VTTable.BestIndex`
  return, and they were selected for use as provided in the parameters
  to :meth:`~VTCursor.Filter` then you should move to the next
  appropriate indexed and constrained row.
*/
static int
apswvtabNext(sqlite3_vtab_cursor *pCursor)
{
  PyObject *cursor, *res = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();

  cursor = ((apsw_vtable_cursor *)pCursor)->cursor;

  res = Call_PythonMethod(cursor, "Next", 1, NULL);
  if (res)
    goto finally;

  /* pyexception:  we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(&(pCursor->pVtab->zErrMsg)); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xNext", "{s: O}", "self", cursor);

finally:
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Close() -> None

  This is the destructor for the cursor. Note that you must
  cleanup. The method will not be called again if you raise an
  exception.
*/
static int
apswvtabClose(sqlite3_vtab_cursor *pCursor)
{
  PyObject *cursor, *res = NULL;
  PyGILState_STATE gilstate;
  char **zErrMsgLocation = &(pCursor->pVtab->zErrMsg); /* we free pCursor but still need this field */
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();

  cursor = ((apsw_vtable_cursor *)pCursor)->cursor;

  res = Call_PythonMethod(cursor, "Close", 1, NULL);
  PyMem_Free(pCursor); /* always free */
  if (res)
    goto finally;

  /* pyexception: we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(zErrMsgLocation); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xClose", "{s: O}", "self", cursor);

finally:
  Py_DECREF(cursor); /* this is where cursor gets freed */
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/** .. method:: Rowid() -> int

  Return the current rowid.
*/
static int
apswvtabRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid)
{
  PyObject *cursor, *res = NULL, *pyrowid = NULL;
  PyGILState_STATE gilstate;
  int sqliteres = SQLITE_OK;

  gilstate = PyGILState_Ensure();

  cursor = ((apsw_vtable_cursor *)pCursor)->cursor;

  res = Call_PythonMethod(cursor, "Rowid", 1, NULL);
  if (!res)
    goto pyexception;

  /* extract result */
  pyrowid = PyNumber_Long(res);
  if (!pyrowid)
    goto pyexception;
  *pRowid = PyLong_AsLongLong(pyrowid);
  if (!PyErr_Occurred()) /* could be bigger than 64 bits */
    goto finally;

pyexception: /* we had an exception in python code */
  assert(PyErr_Occurred());
  sqliteres = MakeSqliteMsgFromPyException(&(pCursor->pVtab->zErrMsg)); /* SQLite flaw: errMsg should be on the cursor not the table! */
  AddTraceBackHere(__FILE__, __LINE__, "VirtualTable.xRowid", "{s: O}", "self", cursor);

finally:
  Py_XDECREF(pyrowid);
  Py_XDECREF(res);

  PyGILState_Release(gilstate);
  return sqliteres;
}

/* it would be nice to use C99 style initializers here ... */
static struct sqlite3_module apsw_vtable_module =
    {
        1,              /* version */
        apswvtabCreate, /* methods */
        apswvtabConnect,
        apswvtabBestIndex,
        apswvtabDisconnect,
        apswvtabDestroy,
        apswvtabOpen,
        apswvtabClose,
        apswvtabFilter,
        apswvtabNext,
        apswvtabEof,
        apswvtabColumn,
        apswvtabRowid,
        apswvtabUpdate,
        apswvtabBegin,
        apswvtabSync,
        apswvtabCommit,
        apswvtabRollback,
        apswvtabFindFunction,
        apswvtabRename};

/**

Troubleshooting virtual tables
==============================

A big help is using the local variables recipe as described in
:ref:`augmented stack traces <augmentedstacktraces>` which will give
you more details in errors, and shows an example with the complex
:meth:`~VTTable.BestIndex` function.

You may also find errors compounding. For
example if you have an error in the Filter method of a cursor, SQLite
then closes the cursor. If you also return an error in the Close
method then the first error may mask the second or vice versa.

.. note::

   SQLite may ignore responses from your methods if they don't make
   sense. For example in BestIndex, if you set multiple arguments to
   have the same constraintargs position then your Filter won't
   receive any constraintargs at all.
*/

/* end of Virtual table code */
