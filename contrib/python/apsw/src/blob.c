/*
  Blob and ZeroBlob code

  See the accompanying LICENSE file.
*/

/**
.. _blobio:

Blob Input/Output
*****************

A `blob <http://en.wikipedia.org/wiki/Binary_large_object>`_ is a
SQLite `datatype <https://sqlite.org/datatype3.html>`_ representing
a sequence of bytes.  It can be zero or more bytes in size.

SQLite blobs have an absolute maximum size of 2GB and a `default
maximum size <https://sqlite.org/c3ref/c_limit_attached.html>`_ of
1GB.

An alternate approach to using blobs is to store the data in files and
store the filename in the database.  Doing so loses the `ACID
<https://sqlite.org/transactional.html>`_ properties of SQLite.

*/

/* ZEROBLOB CODE */

/** .. class:: zeroblob

  If you want to insert a blob into a row, you previously needed to
  supply the entire blob in one go.  To read just one byte also
  required retrieving the blob in its entirety. For example to insert
  a 100MB file you would have done::

     largedata=open("largefile", "rb").read()
     cur.execute("insert into foo values(?)", (largedata,))

  SQLite 3.5 allowed for incremental Blob I/O so you can read and
  write blobs in small amounts.  You cannot change the size of a blob
  so you need to reserve space which you do through zeroblob which
  creates a blob of the specified size but full of zero bytes.  For
  example you would reserve space for your 100MB one of these two
  ways::

    cur.execute("insert into foo values(zeroblob(100000000))")
    cur.execute("insert into foo values(?),
                 (apsw.zeroblob(100000000),))

  This class is used for the second way.  Once a blob exists in the
  database, you then use the :class:`Blob` class to read and write its
  contents.
*/

/* ZeroBlobBind is defined in apsw.c because of forward references */

static PyObject *
ZeroBlobBind_new(PyTypeObject *type, PyObject *Py_UNUSED(args), PyObject *Py_UNUSED(kwargs))
{
  ZeroBlobBind *self;
  self = (ZeroBlobBind *)type->tp_alloc(type, 0);
  if (self)
    self->blobsize = 0;
  return (PyObject *)self;
}

/** .. method:: __init__(size: int)

  :param size: Number of zeroed bytes to create
*/
static int
ZeroBlobBind_init(ZeroBlobBind *self, PyObject *args, PyObject *kwds)
{
  long long size;

  {
    static char *kwlist[] = {"size", NULL};
    Zeroblob_init_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "L:" Zeroblob_init_USAGE, kwlist, &size))
      return -1;
  }
  if (size < 0)
  {
    PyErr_Format(PyExc_TypeError, "zeroblob size must be >= 0");
    return -1;
  }
  self->blobsize = size;

  return 0;
}

/** .. method:: length() -> int

  Size of zero blob in bytes.
*/
static PyObject *
ZeroBlobBind_len(ZeroBlobBind *self)
{
  return PyLong_FromLong(self->blobsize);
}

static PyMethodDef ZeroBlobBind_methods[] = {
    {"length", (PyCFunction)ZeroBlobBind_len, METH_NOARGS,
     Zeroblob_length_DOC},
    {0, 0, 0, 0}};

static PyTypeObject ZeroBlobBindType = {
    PyVarObject_HEAD_INIT(NULL, 0) "apsw.zeroblob",                         /*tp_name*/
    sizeof(ZeroBlobBind),                                                   /*tp_basicsize*/
    0,                                                                      /*tp_itemsize*/
    0,                                                                      /*tp_dealloc*/
    0,                                                                      /*tp_print*/
    0,                                                                      /*tp_getattr*/
    0,                                                                      /*tp_setattr*/
    0,                                                                      /*tp_compare*/
    0,                                                                      /*tp_repr*/
    0,                                                                      /*tp_as_number*/
    0,                                                                      /*tp_as_sequence*/
    0,                                                                      /*tp_as_mapping*/
    0,                                                                      /*tp_hash */
    0,                                                                      /*tp_call*/
    0,                                                                      /*tp_str*/
    0,                                                                      /*tp_getattro*/
    0,                                                                      /*tp_setattro*/
    0,                                                                      /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_VERSION_TAG, /*tp_flags*/
    Zeroblob_class_DOC,                                                     /* tp_doc */
    0,                                                                      /* tp_traverse */
    0,                                                                      /* tp_clear */
    0,                                                                      /* tp_richcompare */
    0,                                                                      /* tp_weaklistoffset */
    0,                                                                      /* tp_iter */
    0,                                                                      /* tp_iternext */
    ZeroBlobBind_methods,                                                   /* tp_methods */
    0,                                                                      /* tp_members */
    0,                                                                      /* tp_getset */
    0,                                                                      /* tp_base */
    0,                                                                      /* tp_dict */
    0,                                                                      /* tp_descr_get */
    0,                                                                      /* tp_descr_set */
    0,                                                                      /* tp_dictoffset */
    (initproc)ZeroBlobBind_init,                                            /* tp_init */
    0,                                                                      /* tp_alloc */
    ZeroBlobBind_new,                                                       /* tp_new */
    0,                                                                      /* tp_free */
    0,                                                                      /* tp_is_gc */
    0,                                                                      /* tp_bases */
    0,                                                                      /* tp_mro */
    0,                                                                      /* tp_cache */
    0,                                                                      /* tp_subclasses */
    0,                                                                      /* tp_weaklist */
    0,                                                                      /* tp_del */
    PyType_TRAILER
};

/* BLOB TYPE */
struct APSWBlob
{
  PyObject_HEAD
      Connection *connection;
  sqlite3_blob *pBlob;
  unsigned inuse;        /* track if we are in use preventing concurrent thread mangling */
  int curoffset;         /* SQLite only supports 32 bit signed int offsets */
  PyObject *weakreflist; /* weak reference tracking */
};

typedef struct APSWBlob APSWBlob;

static PyTypeObject APSWBlobType;

/* BLOB CODE */

/** .. class:: Blob

  This object is created by :meth:`Connection.blobopen` and provides
  access to a blob in the database.  It behaves like a Python file.
  At the C level it wraps a `sqlite3_blob
  <https://sqlite.org/c3ref/blob.html>`_.

  .. note::

    You cannot change the size of a blob using this object. You should
    create it with the correct size in advance either by using
    :class:`zeroblob` or the `zeroblob()
    <https://sqlite.org/lang_corefunc.html>`_ function.

  See the :ref:`example <example_blob_io>`.
*/

static void
APSWBlob_init(APSWBlob *self, Connection *connection, sqlite3_blob *blob)
{
  Py_INCREF(connection);
  self->connection = connection;
  self->pBlob = blob;
  self->curoffset = 0;
  self->inuse = 0;
  self->weakreflist = NULL;
}

static int
APSWBlob_close_internal(APSWBlob *self, int force)
{
  int setexc = 0;
  PyObject *err_type, *err_value, *err_traceback;

  if (force == 2)
    PyErr_Fetch(&err_type, &err_value, &err_traceback);

  /* note that sqlite3_blob_close always works even if an error is
     returned - see sqlite ticket #2815 */

  if (self->pBlob)
  {
    int res;
    PYSQLITE_BLOB_CALL(res = sqlite3_blob_close(self->pBlob));
    if (res != SQLITE_OK)
    {
      switch (force)
      {
      case 0:
        SET_EXC(res, self->connection->db);
        setexc = 1;
        break;
      case 1:
        break;
      case 2:
        SET_EXC(res, self->connection->db);
        apsw_write_unraisable(NULL);
      }
    }
    self->pBlob = 0;
  }

  /* Remove from connection dependents list.  Has to be done before we
     decref self->connection otherwise connection could dealloc and
     we'd still be in list */
  if (self->connection)
    Connection_remove_dependent(self->connection, (PyObject *)self);

  Py_CLEAR(self->connection);

  if (force == 2)
    PyErr_Restore(err_type, err_value, err_traceback);

  return setexc;
}

static void
APSWBlob_dealloc(APSWBlob *self)
{
  APSW_CLEAR_WEAKREFS;

  APSWBlob_close_internal(self, 2);

  Py_TYPE(self)->tp_free((PyObject *)self);
}

/* If the blob is closed, we return the same error as normal python files */
#define CHECK_BLOB_CLOSED                                                    \
  do                                                                         \
  {                                                                          \
    if (!self->pBlob)                                                        \
      return PyErr_Format(PyExc_ValueError, "I/O operation on closed blob"); \
  } while (0)

/** .. method:: length() -> int

  Returns the size of the blob in bytes.

  -* sqlite3_blob_bytes
*/

static PyObject *
APSWBlob_length(APSWBlob *self)
{
  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;
  return PyLong_FromLong(sqlite3_blob_bytes(self->pBlob));
}

/** .. method:: read(length: int = -1) -> bytes

  Reads amount of data requested, or till end of file, whichever is
  earlier. Attempting to read beyond the end of the blob returns an
  empty bytes in the same manner as end of file on normal file
  objects.  Negative numbers read remaining data.

  -* sqlite3_blob_read
*/

static PyObject *
APSWBlob_read(APSWBlob *self, PyObject *args, PyObject *kwds)
{
  int length = -1;
  int res;
  PyObject *buffy = 0;
  char *thebuffer;

  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;

  /* The python file read routine treats negative numbers as read till
     end of file, which I think is rather silly.  (Try reading -3
     bytes from /dev/zero on a 64 bit machine with lots of swap to see
     why).  In any event we remain consistent with Python file
     objects */
  {
    static char *kwlist[] = {"length", NULL};
    Blob_read_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i:" Blob_read_USAGE, kwlist, &length))
      return NULL;
  }

  if (
      (self->curoffset == sqlite3_blob_bytes(self->pBlob)) /* eof */
      ||
      (length == 0))
    return PyBytes_FromStringAndSize(NULL, 0);

  if (length < 0)
    length = sqlite3_blob_bytes(self->pBlob) - self->curoffset;

  /* trying to read more than is in the blob? */
  /* ::TODO:: use 64 arithmetic to avoid overflow */
  if (self->curoffset + length > sqlite3_blob_bytes(self->pBlob))
    length = sqlite3_blob_bytes(self->pBlob) - self->curoffset;

  buffy = PyBytes_FromStringAndSize(NULL, length);

  if (!buffy)
    return NULL;

  thebuffer = PyBytes_AS_STRING(buffy);
  PYSQLITE_BLOB_CALL(res = sqlite3_blob_read(self->pBlob, thebuffer, length, self->curoffset));
  if (PyErr_Occurred())
    return NULL;

  if (res != SQLITE_OK)
  {
    Py_DECREF(buffy);
    SET_EXC(res, self->connection->db);
    return NULL;
  }
  else
    self->curoffset += length;
  assert(self->curoffset <= sqlite3_blob_bytes(self->pBlob));
  return buffy;
}

/** .. method:: readinto(buffer: Union[bytearray, array.array[Any], memoryview], offset: int = 0, length: int = -1) -> None

  Reads from the blob into a buffer you have supplied.  This method is
  useful if you already have a buffer like object that data is being
  assembled in, and avoids allocating results in :meth:`Blob.read` and
  then copying into buffer.

  :param buffer: A writable buffer like object.
                 There is a bytearray type that is very useful.
                 `arrays <https://docs.python.org/3/library/array.html>`__ also work.

  :param offset: The position to start writing into the buffer
                 defaulting to the beginning.

  :param length: How much of the blob to read.  The default is the
                 remaining space left in the buffer.  Note that if
                 there is more space available than blob left then you
                 will get a *ValueError* exception.

  -* sqlite3_blob_read
*/

static PyObject *
APSWBlob_readinto(APSWBlob *self, PyObject *args, PyObject *kwds)
{
  int res = SQLITE_OK;
  long long offset = 0, length = -1;
  PyObject *buffer = NULL;

  int aswb;

  int bloblen;
  Py_buffer py3buffer;

  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;
  {
    static char *kwlist[] = {"buffer", "offset", "length", NULL};
    Blob_readinto_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|LL:" Blob_readinto_USAGE, kwlist, &buffer, &offset, &length))
      return NULL;
  }

#define ERREXIT(x)  \
  do                \
  {                 \
    x;              \
    goto errorexit; \
  } while (0)

  memset(&py3buffer, 0, sizeof(py3buffer));
  aswb = PyObject_GetBuffer(buffer, &py3buffer, PyBUF_WRITABLE | PyBUF_SIMPLE);
  if (aswb)
    return NULL;

  bloblen = sqlite3_blob_bytes(self->pBlob);

  if (length < 0)
    length = py3buffer.len - offset;

  if (offset < 0 || offset > py3buffer.len)
    ERREXIT(PyErr_Format(PyExc_ValueError, "offset is less than zero or beyond end of buffer"));

  if (offset + length > py3buffer.len)
    ERREXIT(PyErr_Format(PyExc_ValueError, "Data would go beyond end of buffer"));

  if (length > bloblen - self->curoffset)
    ERREXIT(PyErr_Format(PyExc_ValueError, "More data requested than blob length"));

  APSW_FAULT_INJECT(BlobReadIntoPyError,
    PYSQLITE_BLOB_CALL(res = sqlite3_blob_read(self->pBlob, (char *)(py3buffer.buf) + offset, length, self->curoffset)),
    PyErr_NoMemory());
  if (PyErr_Occurred())
    ERREXIT(NULL);

  if (res != SQLITE_OK)
  {
    SET_EXC(res, self->connection->db);
    ERREXIT(NULL);
  }
  self->curoffset += length;

  PyBuffer_Release(&py3buffer);
  Py_RETURN_NONE;

errorexit:
  PyBuffer_Release(&py3buffer);
  return NULL;
#undef ERREXIT
}

/** .. method:: seek(offset: int, whence: int = 0) -> None

  Changes current position to *offset* biased by *whence*.

  :param offset: New position to seek to.  Can be positive or negative number.
  :param whence: Use 0 if *offset* is relative to the beginning of the blob,
                 1 if *offset* is relative to the current position,
                 and 2 if *offset* is relative to the end of the blob.
  :raises ValueError: If the resulting offset is before the beginning (less than zero) or beyond the end of the blob.
*/

static PyObject *
APSWBlob_seek(APSWBlob *self, PyObject *args, PyObject *kwds)
{
  int offset, whence = 0;
  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;

  {
    static char *kwlist[] = {"offset", "whence", NULL};
    Blob_seek_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "i|i:" Blob_seek_USAGE, kwlist, &offset, &whence))
      return NULL;
  }
  switch (whence)
  {
  default:
    return PyErr_Format(PyExc_ValueError, "whence parameter should be 0, 1 or 2");
  case 0: /* relative to beginning of file */
    if (offset < 0 || offset > sqlite3_blob_bytes(self->pBlob))
      goto out_of_range;
    self->curoffset = offset;
    break;
  case 1: /* relative to current position */
    if (self->curoffset + offset < 0 || self->curoffset + offset > sqlite3_blob_bytes(self->pBlob))
      goto out_of_range;
    self->curoffset += offset;
    break;
  case 2: /* relative to end of file */
    if (sqlite3_blob_bytes(self->pBlob) + offset < 0 || sqlite3_blob_bytes(self->pBlob) + offset > sqlite3_blob_bytes(self->pBlob))
      goto out_of_range;
    self->curoffset = sqlite3_blob_bytes(self->pBlob) + offset;
    break;
  }
  Py_RETURN_NONE;
out_of_range:
  return PyErr_Format(PyExc_ValueError, "The resulting offset would be less than zero or past the end of the blob");
}

/** .. method:: tell() -> int

  Returns the current offset.
*/

static PyObject *
APSWBlob_tell(APSWBlob *self)
{
  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;
  return PyLong_FromLong(self->curoffset);
}

/** .. method:: write(data: bytes) -> None

  Writes the data to the blob.

  :param data: bytes to write

  :raises TypeError: Wrong data type

  :raises ValueError: If the data would go beyond the end of the blob.
      You cannot increase the size of a blob by writing beyond the end.
      You need to use :class:`zeroblob` to set the desired size first when
      inserting the blob.

  -* sqlite3_blob_write
*/
static PyObject *
APSWBlob_write(APSWBlob *self, PyObject *args, PyObject *kwds)
{
  int ok = 0, res = SQLITE_OK;
  Py_buffer data;

  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;

  {
    static char *kwlist[] = {"data", NULL};
    Blob_write_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "y*:" Blob_write_USAGE, kwlist, &data))
      return NULL;
  }

  if (((int)(data.len + self->curoffset)) < self->curoffset)
  {
    PyErr_Format(PyExc_ValueError, "Data is too large (integer wrap)");
    goto finally;
  }

  if (((int)(data.len + self->curoffset)) > sqlite3_blob_bytes(self->pBlob))
  {
    PyErr_Format(PyExc_ValueError, "Data would go beyond end of blob");
    goto finally;
  }

  APSW_FAULT_INJECT(BlobWritePyError,
    PYSQLITE_BLOB_CALL(res = sqlite3_blob_write(self->pBlob, data.buf, data.len, self->curoffset)),
    PyErr_NoMemory());
  if (PyErr_Occurred())
    goto finally;

  if (res != SQLITE_OK)
  {
    SET_EXC(res, self->connection->db);
    goto finally;
  }
  self->curoffset += data.len;
  assert(self->curoffset <= sqlite3_blob_bytes(self->pBlob));
  ok = 1;

finally:
  PyBuffer_Release(&data);
  if (ok)
    Py_RETURN_NONE;
  else
    return NULL;
}

/** .. method:: close(force: bool = False) -> None

  Closes the blob.  Note that even if an error occurs the blob is
  still closed.

  .. note::

     In some cases errors that technically occurred in the
     :meth:`~Blob.read` and :meth:`~Blob.write` routines may not be
     reported until close is called.  Similarly errors that occurred
     in those methods (eg calling :meth:`~Blob.write` on a read-only
     blob) may also be re-reported in :meth:`~Blob.close`.  (This
     behaviour is what the underlying SQLite APIs do - it is not APSW
     doing it.)

  It is okay to call :meth:`~Blob.close` multiple times.

  :param force: Ignores any errors during close.

  -* sqlite3_blob_close
*/

static PyObject *
APSWBlob_close(APSWBlob *self, PyObject *args, PyObject *kwds)
{
  int setexc;
  int force = 0;

  CHECK_USE(NULL);

  {
    static char *kwlist[] = {"force", NULL};
    Blob_close_CHECK;
    argcheck_bool_param force_param = {&force, Blob_close_force_MSG};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|O&:" Blob_close_USAGE, kwlist, argcheck_bool, &force_param))
      return NULL;
  }
  setexc = APSWBlob_close_internal(self, !!force);

  if (setexc)
    return NULL;

  Py_RETURN_NONE;
}

/** .. method:: __enter__() -> Blob

  You can use a blob as a `context manager
  <http://docs.python.org/reference/datamodel.html#with-statement-context-managers>`_
  as defined in :pep:`0343`.  When you use *with* statement,
  the blob is always :meth:`closed <Blob.close>` on exit from the block, even if an
  exception occurred in the block.

  For example::

    with connection.blobopen() as blob:
        blob.write("...")
        res=blob.read(1024)

*/

static PyObject *
APSWBlob_enter(APSWBlob *self)
{
  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;

  Py_INCREF(self);
  return (PyObject *)self;
}

/** .. method:: __exit__(etype: Optional[type[BaseException]], evalue: Optional[BaseException], etraceback: Optional[types.TracebackType]) -> Optional[bool]

  Implements context manager in conjunction with
  :meth:`~Blob.__enter__`.  Any exception that happened in the
  *with* block is raised after closing the blob.
*/

static PyObject *
APSWBlob_exit(APSWBlob *self, PyObject *Py_UNUSED(args))
{
  int setexc;
  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;

  setexc = APSWBlob_close_internal(self, 0);
  if (setexc)
    return NULL;

  Py_RETURN_FALSE;
}

/** .. method:: reopen(rowid: int) -> None

  Change this blob object to point to a different row.  It can be
  faster than closing an existing blob an opening a new one.

  -* sqlite3_blob_reopen
*/

static PyObject *
APSWBlob_reopen(APSWBlob *self, PyObject *args, PyObject *kwds)
{
  int res;
  long long rowid;

  CHECK_USE(NULL);
  CHECK_BLOB_CLOSED;

  {
    static char *kwlist[] = {"rowid", NULL};
    Blob_reopen_CHECK;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "L:" Blob_reopen_USAGE, kwlist, &rowid))
      return NULL;
  }
  /* no matter what happens we always reset current offset */
  self->curoffset = 0;

  PYSQLITE_BLOB_CALL(res = sqlite3_blob_reopen(self->pBlob, rowid));
  if (PyErr_Occurred())
    return NULL;

  if (res != SQLITE_OK)
  {
    SET_EXC(res, self->connection->db);
    return NULL;
  }
  Py_RETURN_NONE;
}

static PyMethodDef APSWBlob_methods[] = {
    {"length", (PyCFunction)APSWBlob_length, METH_NOARGS,
     Blob_length_DOC},
    {"read", (PyCFunction)APSWBlob_read, METH_VARARGS | METH_KEYWORDS,
     Blob_read_DOC},
    {"readinto", (PyCFunction)APSWBlob_readinto, METH_VARARGS | METH_KEYWORDS,
     Blob_readinto_DOC},
    {"seek", (PyCFunction)APSWBlob_seek, METH_VARARGS | METH_KEYWORDS,
     Blob_seek_DOC},
    {"tell", (PyCFunction)APSWBlob_tell, METH_NOARGS,
     Blob_tell_DOC},
    {"write", (PyCFunction)APSWBlob_write, METH_VARARGS | METH_KEYWORDS,
     Blob_write_DOC},
    {"reopen", (PyCFunction)APSWBlob_reopen, METH_VARARGS | METH_KEYWORDS,
     Blob_reopen_DOC},
    {"close", (PyCFunction)APSWBlob_close, METH_VARARGS | METH_KEYWORDS,
     Blob_close_DOC},
    {"__enter__", (PyCFunction)APSWBlob_enter, METH_NOARGS,
     Blob_enter_DOC},
    {"__exit__", (PyCFunction)APSWBlob_exit, METH_VARARGS,
     Blob_exit_DOC},
    {0, 0, 0, 0} /* Sentinel */
};

static PyTypeObject APSWBlobType = {
    PyVarObject_HEAD_INIT(NULL, 0) "apsw.Blob",       /*tp_name*/
    sizeof(APSWBlob),                                 /*tp_basicsize*/
    0,                                                /*tp_itemsize*/
    (destructor)APSWBlob_dealloc,                     /*tp_dealloc*/
    0,                                                /*tp_print*/
    0,                                                /*tp_getattr*/
    0,                                                /*tp_setattr*/
    0,                                                /*tp_compare*/
    0,                                                /*tp_repr*/
    0,                                                /*tp_as_number*/
    0,                                                /*tp_as_sequence*/
    0,                                                /*tp_as_mapping*/
    0,                                                /*tp_hash */
    0,                                                /*tp_call*/
    0,                                                /*tp_str*/
    0,                                                /*tp_getattro*/
    0,                                                /*tp_setattro*/
    0,                                                /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_VERSION_TAG, /*tp_flags*/
    Blob_class_DOC,                                   /* tp_doc */
    0,                                                /* tp_traverse */
    0,                                                /* tp_clear */
    0,                                                /* tp_richcompare */
    offsetof(APSWBlob, weakreflist),                  /* tp_weaklistoffset */
    0,                                                /* tp_iter */
    0,                                                /* tp_iternext */
    APSWBlob_methods,                                 /* tp_methods */
    0,                                                /* tp_members */
    0,                                                /* tp_getset */
    0,                                                /* tp_base */
    0,                                                /* tp_dict */
    0,                                                /* tp_descr_get */
    0,                                                /* tp_descr_set */
    0,                                                /* tp_dictoffset */
    0,                                                /* tp_init */
    0,                                                /* tp_alloc */
    0,                                                /* tp_new */
    0,                                                /* tp_free */
    0,                                                /* tp_is_gc */
    0,                                                /* tp_bases */
    0,                                                /* tp_mro */
    0,                                                /* tp_cache */
    0,                                                /* tp_subclasses */
    0,                                                /* tp_weaklist */
    0,                                                /* tp_del */
    PyType_TRAILER
};
