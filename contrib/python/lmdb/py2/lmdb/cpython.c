/*
 * Copyright 2013 The py-lmdb authors, all rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 *
 * OpenLDAP is a registered trademark of the OpenLDAP Foundation.
 *
 * Individual files and/or contributed packages may be copyright by
 * other parties and/or subject to additional restrictions.
 *
 * This work also contains materials derived from public sources.
 *
 * Additional information about OpenLDAP can be obtained at
 * <http://www.openldap.org/>.
 */

#define PY_SSIZE_T_CLEAN

/* Include order matters! */
#include "Python.h"

/* Search lib/win32 first, then fallthrough to <stdint.h> as required.*/
#include "stdint.h"

#include <errno.h>
#include <stdarg.h>
#include <string.h>

#ifdef _WIN32
#   define bool int
#   define true 1
#   define false 0
#else
#   include <stdbool.h>
#endif

#include <sys/stat.h>

#include "structmember.h"

#ifdef HAVE_MEMSINK
#define USING_MEMSINK
#error #include "memsink.h"
#endif

#ifdef _WIN32
#include <windows.h> /* HANDLE */
#include <direct.h>
#endif

#include "lmdb.h"
#include "preload.h"


/* Comment out for copious debug. */
#define NODEBUG

#ifdef NODEBUG
#   define DEBUG(s, ...)
#else
#   define DEBUG(s, ...) fprintf(stderr, \
    "lmdb.cpython: %s:%d: " s "\n", __func__, __LINE__, ## __VA_ARGS__);
#endif

#define MDEBUG(s, ...) DEBUG("%p: " s, self, ## __VA_ARGS__);


/* Inlining control for compatible compilers. */
#if (__GNUC__ > 3 || (__GNUC__ == 3 && __GNUC_MINOR__ >= 4))
#   define NOINLINE __attribute__((noinline))
#else
#   define NOINLINE
#endif


/**
 * On Win32, Environment.copyfd() needs _get_osfmodule() from the C library,
 * except that function performs no input validation. So instead we import
 * msvcrt standard library module, which wraps _get_osfmodule() in a way that
 * is crash-safe.
 */
#ifdef _WIN32
static PyObject *msvcrt;
#endif


/** PyLong representing integer 0. */
static PyObject *py_zero;
/** PyLong representing INT_MAX. */
static PyObject *py_int_max;
/** PyLong representing SIZE_MAX. */
static PyObject *py_size_max;
/** lmdb.Error type. */
static PyObject *Error;

/** Typedefs and forward declarations. */
static PyTypeObject PyDatabase_Type;
static PyTypeObject PyEnvironment_Type;
static PyTypeObject PyTransaction_Type;
static PyTypeObject PyCursor_Type;
static PyTypeObject PyIterator_Type;

typedef struct CursorObject CursorObject;
typedef struct DbObject DbObject;
typedef struct EnvObject EnvObject;
typedef struct IterObject IterObject;
typedef struct TransObject TransObject;


/* ------------------------ */
/* Python 3.x Compatibility */
/* ------------------------ */

#if PY_MAJOR_VERSION >= 3

#   define MOD_RETURN(mod) return mod;
#   define MODINIT_NAME PyInit_cpython

#   define MAKE_ID(id) PyCapsule_New((void *) (1 + (id)), NULL, NULL)
#   define READ_ID(obj) (((int) (long) PyCapsule_GetPointer(obj, NULL)) - 1)

#else

#   define MOD_RETURN(mod) return
#   define MODINIT_NAME initcpython

#   define MAKE_ID(id) PyInt_FromLong((long) id)
#   define READ_ID(obj) PyInt_AS_LONG(obj)

#   define PyUnicode_InternFromString PyString_InternFromString
#   define PyBytes_AS_STRING PyString_AS_STRING
#   define PyBytes_GET_SIZE PyString_GET_SIZE
#   define PyBytes_CheckExact PyString_CheckExact
#   define PyBytes_FromStringAndSize PyString_FromStringAndSize
#   define _PyBytes_Resize _PyString_Resize
#   define PyMemoryView_FromMemory(x, y, z) PyBuffer_FromMemory(x, y)

#   ifndef PyBUF_READ
#       define PyBUF_READ 0
#   endif

/* Python 2.5 */
#   ifndef Py_TYPE
#       define Py_TYPE(ob) (((PyObject*)(ob))->ob_type)
#   endif

#   ifndef PyVarObject_HEAD_INIT
#       define PyVarObject_HEAD_INIT(x, y) \
            PyObject_HEAD_INIT(x) y,
#   endif

#endif

struct list_head {
    struct lmdb_object *prev;
    struct lmdb_object *next;
};

#define LmdbObject_HEAD \
    PyObject_HEAD \
    struct list_head siblings; \
    struct list_head children; \
    int valid;

struct lmdb_object {
    LmdbObject_HEAD
};

#define OBJECT_INIT(o) \
    ((struct lmdb_object *)o)->siblings.prev = NULL; \
    ((struct lmdb_object *)o)->siblings.next = NULL; \
    ((struct lmdb_object *)o)->children.prev = NULL; \
    ((struct lmdb_object *)o)->children.next = NULL; \
    ((struct lmdb_object *)o)->valid = 1;


/** lmdb._Database */
struct DbObject {
    LmdbObject_HEAD
    /** Python Environment reference. Not refcounted; when the last strong ref
     * to Environment is released, DbObject.tp_clear() will be called, causing
     * DbObject.env and DbObject.dbi to be cleared. This is to prevent a
     * cyclical reference from DB->Env keeping the environment alive. */
    struct EnvObject *env;
    /** MDB database handle. */
    MDB_dbi dbi;
    /** Flags at time of creation. */
    unsigned int flags;
};

/** lmdb.Environment */
struct EnvObject {
    LmdbObject_HEAD
    /** Python-managed list of weakrefs to this object. */
    PyObject *weaklist;
    /** MDB environment object. */
    MDB_env *env;
    /** DBI for main database, opened during Environment construction. */
    DbObject *main_db;
    /**  1 if env opened read-only; transactions must always be read-only. */
    int readonly;
    /** Spare read-only transaction . */
    struct MDB_txn *spare_txn;
};

/** TransObject.flags bitfield values. */
enum trans_flags {
    /** Buffers should be yielded by get. */
    TRANS_BUFFERS       = 1,
    /** Transaction can be can go on freelist instead of deallocation. */
    TRANS_RDONLY        = 2,
    /** Transaction is spare, ready for mdb_txn_renew() */
    TRANS_SPARE         = 4
};

/** lmdb.Transaction */
struct TransObject {
    LmdbObject_HEAD
    /** Python-managed list of weakrefs to this object. */
    PyObject *weaklist;
    EnvObject *env;
#ifdef HAVE_MEMSINK
    /** Copy-on-invalid list head. */
    PyObject *sink_head;
#endif
    /** MDB transaction object. */
    MDB_txn *txn;
    /** Bitfield of trans_flags values. */
    int flags;
    /** Default database if none specified. */
    DbObject *db;
    /** Number of mutations occurred since start of transaction. Required to
     * know when cursor key/value must be refreshed. */
    int mutations;
};

/** lmdb.Cursor */
struct CursorObject {
    LmdbObject_HEAD
    /** Transaction cursor belongs to. */
    TransObject *trans;
    /** 1 if mdb_cursor_get() has been called and it last returned 0. */
    int positioned;
    /** MDB-level cursor object. */
    MDB_cursor *curs;
    /** mv_size==0 if positioned==0, otherwise points to current key. */
    MDB_val key;
    /** mv_size==0 if positioned==0, otherwise points to current value. */
    MDB_val val;
    /** If TransObject.mutations!=last_mutation, must MDB_GET_CURRENT to
     * refresh `key' and `val'. */
    int last_mutation;
    /** DBI flags at time of creation. */
    unsigned int dbi_flags;
};


typedef PyObject *(*IterValFunc)(CursorObject *);

/** lmdb.Iterator
 *
 * This is separate from Cursor since we want to define Cursor.next() to mean
 * MDB_NEXT, and a Python iterator's next() has different semantics.
 */
struct IterObject {
    PyObject_HEAD
    /** Cursor being iterated, or NULL for freelist iterator. */
    CursorObject *curs;
    /** 1 if iteration has started (Cursor should advance on next()). */
    int started;
    /** Operation used to advance cursor. */
    MDB_cursor_op op;
    /** Iterator value function, should be item(), key(), or value(). */
    IterValFunc val_func;
};


/**
 * Link `child` into `parent`'s list of dependent objects. Use LINK_CHILD()
 * maro to avoid casting PyObject to lmdb_object.
 */
static void link_child(struct lmdb_object *parent, struct lmdb_object *child)
{
    struct lmdb_object *sibling = parent->children.next;
    if(sibling) {
        child->siblings.next = sibling;
        sibling->siblings.prev = child;
    }
    parent->children.next = child;
}

#define LINK_CHILD(parent, child) link_child((void *)parent, (void *)child);


/**
 * Remove `child` from `parent`'s list of dependent objects. Use UNLINK_CHILD
 * macro to avoid casting PyObject to lmdb_object.
 */
static void unlink_child(struct lmdb_object *parent, struct lmdb_object *child)
{
    if(parent) {
        struct lmdb_object *prev = child->siblings.prev;
        struct lmdb_object *next = child->siblings.next;
        if(prev) {
            prev->siblings.next = next;
                 /* If double unlink_child(), this test my legitimately fail: */
        } else if(parent->children.next == child) {
            parent->children.next = next;
        }
        if(next) {
            next->siblings.prev = prev;
        }
        child->siblings.prev = NULL;
        child->siblings.next = NULL;
    }
}

#define UNLINK_CHILD(parent, child) unlink_child((void *)parent, (void *)child);


/**
 * Notify dependents of `parent` that `parent` is about to become invalid,
 * and that they should free any dependent resources.
 *
 * To save effort, tp_clear is overloaded to be the invalidation function,
 * instead of carrying a separate pointer. Objects are added to their parent's
 * list during construction and removed during deallocation.
 *
 * When the environment is closed, it walks its list calling tp_clear on each
 * child, which in turn walk their own lists. Child transactions are added to
 * their parent transaction's list. Iterators keep no significant state, so
 * they are not tracked.
 *
 * Use INVALIDATE() macro to avoid casting PyObject to lmdb_object.
 */
static void invalidate(struct lmdb_object *parent)
{
    struct lmdb_object *child = parent->children.next;
    while(child) {
        struct lmdb_object *next = child->siblings.next;
        DEBUG("invalidating parent=%p child %p", parent, child)
        Py_TYPE(child)->tp_clear((PyObject *) child);
        child = next;
    }
}

#define INVALIDATE(parent) invalidate((void *)parent);


/* ---------- */
/* Exceptions */
/* ---------- */

struct error_map {
    int code;
    const char *name;
};

/** Array of Error subclasses corresponding to `error_map'. */
static PyObject **error_tbl;
/** Mapping from LMDB error code to py-lmdb exception class. */
static const struct error_map error_map[] = {
    {MDB_KEYEXIST, "KeyExistsError"},
    {MDB_NOTFOUND, "NotFoundError"},
    {MDB_PAGE_NOTFOUND, "PageNotFoundError"},
    {MDB_CORRUPTED, "CorruptedError"},
    {MDB_PANIC, "PanicError"},
    {MDB_VERSION_MISMATCH, "VersionMismatchError"},
    {MDB_INVALID, "InvalidError"},
    {MDB_MAP_FULL, "MapFullError"},
    {MDB_DBS_FULL, "DbsFullError"},
    {MDB_READERS_FULL, "ReadersFullError"},
    {MDB_TLS_FULL, "TlsFullError"},
    {MDB_TXN_FULL, "TxnFullError"},
    {MDB_CURSOR_FULL, "CursorFullError"},
    {MDB_PAGE_FULL, "PageFullError"},
    {MDB_MAP_RESIZED, "MapResizedError"},
    {MDB_INCOMPATIBLE, "IncompatibleError"},
    {MDB_BAD_RSLOT, "BadRslotError"},
    {MDB_BAD_DBI, "BadDbiError"},
    {MDB_BAD_TXN, "BadTxnError"},
    {MDB_BAD_VALSIZE, "BadValsizeError"},
    {EACCES, "ReadonlyError"},
    {EINVAL, "InvalidParameterError"},
    {EAGAIN, "LockError"},
    {ENOMEM, "MemoryError"},
    {ENOSPC, "DiskError"}
};


/* ---------- */
/* Exceptions */
/* ---------- */

/**
 * Raise an exception appropriate for the given `rc` MDB error code.
 */
static void * NOINLINE
err_set(const char *what, int rc)
{
    size_t count = sizeof error_map / sizeof error_map[0];
    PyObject *klass = Error;
    size_t i;

    if(rc) {
        for(i = 0; i < count; i++) {
            if(error_map[i].code == rc) {
                klass = error_tbl[i];
                break;
            }
        }
    }

    PyErr_Format(klass, "%s: %s", what, mdb_strerror(rc));
    return NULL;
}

/**
 * Raise an exception from a format string.
 */
static void * NOINLINE
err_format(int rc, const char *fmt, ...)
{
    char buf[128];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof buf, fmt, ap);
    buf[sizeof buf - 1] = '\0';
    va_end(ap);
    return err_set(buf, rc);
}

static void * NOINLINE
err_invalid(void)
{
    PyErr_Format(Error, "Attempt to operate on closed/deleted/dropped object.");
    return NULL;
}

static void * NOINLINE
type_error(const char *what)
{
    PyErr_Format(PyExc_TypeError, "%s", what);
    return NULL;
}

/**
 * Convert a PyObject to filesystem bytes. Must call fspath_fini() when done.
 * Return 0 on success, or set an exception and return -1 on failure.
 */
static PyObject *
get_fspath(PyObject *src)
{
    if(PyBytes_CheckExact(src)) {
        Py_INCREF(src);
        return src;
    }
    if(! PyUnicode_CheckExact(src)) {
        type_error("Filesystem path must be Unicode or bytes.");
        return NULL;
    }
    return PyUnicode_AsEncodedString(src, Py_FileSystemDefaultEncoding,
                                     "strict");
}

/* ------- */
/* Helpers */
/* ------- */

/**
 * Describes the type of a struct field.
 */
enum field_type {
    /** Last field in set, stop converting. */
    TYPE_EOF,
    /** Unsigned 32bit integer. */
    TYPE_UINT,
    /** size_t */
    TYPE_SIZE,
    /** void pointer */
    TYPE_ADDR
};

/**
 * Describes a struct field.
 */
struct dict_field {
    /** Field type. */
    enum field_type type;
    /** Field name in target dict. */
    const char *name;
    /* Offset into structure where field is found. */
    int offset;
};

/**
 * Return a new reference to Py_True if the given argument is true, otherwise
 * a new reference to Py_False.
 */
static PyObject *
py_bool(int pred)
{
    PyObject *obj = pred ? Py_True : Py_False;
    Py_INCREF(obj);
    return obj;
}

/**
 * Convert the structure `o` described by `fields` to a dict and return the new
 * dict.
 */
static PyObject *
dict_from_fields(void *o, const struct dict_field *fields)
{
    PyObject *dict = PyDict_New();
    if(! dict) {
        return NULL;
    }

    while(fields->type != TYPE_EOF) {
        uint8_t *p = ((uint8_t *) o) + fields->offset;
        unsigned PY_LONG_LONG l = 0;
        PyObject *lo;

        if(fields->type == TYPE_UINT) {
            l = *(unsigned int *)p;
        } else if(fields->type == TYPE_SIZE) {
            l = *(size_t *)p;
        } else if(fields->type == TYPE_ADDR) {
            l = (intptr_t) *(void **)p;
        }

        if(! ((lo = PyLong_FromUnsignedLongLong(l)))) {
            Py_DECREF(dict);
            return NULL;
        }

        if(PyDict_SetItemString(dict, fields->name, lo)) {
            Py_DECREF(lo);
            Py_DECREF(dict);
            return NULL;
        }
        Py_DECREF(lo);
        fields++;
    }
    return dict;
}

/**
 * Given an MDB_val `val`, convert it to a Python string or bytes object,
 * depending on the Python version. Returns a new reference to the object on
 * sucess, or NULL on failure.
 */
static PyObject *
obj_from_val(MDB_val *val, int as_buffer)
{
    if(as_buffer) {
        return PyMemoryView_FromMemory(val->mv_data, val->mv_size, PyBUF_READ);
    }
    return PyBytes_FromStringAndSize(val->mv_data, val->mv_size);
}

/**
 * Given some Python object, try to get at its raw data. For string or bytes
 * objects, this is the object value. For Unicode objects, this is the UTF-8
 * representation of the object value. For all other objects, attempt to invoke
 * the Python 2.x buffer protocol.
 */
static int NOINLINE
val_from_buffer(MDB_val *val, PyObject *buf)
{
    if(PyBytes_CheckExact(buf)) {
        val->mv_data = PyBytes_AS_STRING(buf);
        val->mv_size = PyBytes_GET_SIZE(buf);
        return 0;
    }
    if(PyUnicode_CheckExact(buf)) {
        type_error("Won't implicitly convert Unicode to bytes; use .encode()");
        return -1;
    }
    return PyObject_AsReadBuffer(buf,
        (const void **) &val->mv_data,
        (Py_ssize_t *) &val->mv_size);
}

/* ------------------- */
/* Concurrency control */
/* ------------------- */

#define UNLOCKED(out, e) \
    Py_BEGIN_ALLOW_THREADS \
    out = (e); \
    Py_END_ALLOW_THREADS

#define PRELOAD_UNLOCKED(_rc, _data, _size) \
    Py_BEGIN_ALLOW_THREADS \
    preload(_rc, _data, _size); \
    Py_END_ALLOW_THREADS

/* ---------------- */
/* Argument parsing */
/* ---------------- */

#define OFFSET(k, y) offsetof(struct k, y)
#define SPECSIZE() (sizeof(argspec) / sizeof(argspec[0]))
enum arg_type {
    ARG_DB,     /** DbObject*               */
    ARG_TRANS,  /** TransObject*            */
    ARG_ENV,    /** EnvObject*              */
    ARG_OBJ,    /** PyObject*               */
    ARG_BOOL,   /** int                     */
    ARG_BUF,    /** MDB_val                 */
    ARG_STR,    /** char*                   */
    ARG_INT,    /** int                     */
    ARG_SIZE    /** size_t                  */
};
struct argspec {
    const char *string;
    unsigned short type;
    unsigned short offset;
};

static PyTypeObject *type_tbl[] = {
    &PyDatabase_Type,
    &PyTransaction_Type,
    &PyEnvironment_Type
};


static int NOINLINE
parse_ulong(PyObject *obj, uint64_t *l, PyObject *max)
{
    int rc = PyObject_RichCompareBool(obj, py_zero, Py_GE);
    if(rc == -1) {
        return -1;
    } else if(! rc) {
        PyErr_Format(PyExc_OverflowError, "Integer argument must be >= 0");
        return -1;
    }
    rc = PyObject_RichCompareBool(obj, max, Py_LE);
    if(rc == -1) {
        return -1;
    } else if(! rc) {
        PyErr_Format(PyExc_OverflowError, "Integer argument exceeds limit.");
        return -1;
    }
#if PY_MAJOR_VERSION >= 3
    *l = PyLong_AsUnsignedLongLongMask(obj);
#else
    *l = PyInt_AsUnsignedLongLongMask(obj);
#endif
    return 0;
}

/**
 * Parse a single argument specified by `spec` into `out`, returning 0 on
 * success or setting an exception and returning -1 on error.
 */
static int
parse_arg(const struct argspec *spec, PyObject *val, void *out)
{
    void *dst = ((uint8_t *)out) + spec->offset;
    int ret = 0;
    uint64_t l;

    if(val != Py_None) {
        switch((enum arg_type) spec->type) {
        case ARG_DB:
        case ARG_TRANS:
        case ARG_ENV:
            if(val->ob_type != type_tbl[spec->type]) {
                type_error("invalid type");
                return -1;
            }
            /* fallthrough */
        case ARG_OBJ:
            *((PyObject **) dst) = val;
            break;
        case ARG_BOOL:
            *((int *)dst) = PyObject_IsTrue(val);
            break;
        case ARG_BUF:
            ret = val_from_buffer((MDB_val *)dst, val);
            break;
        case ARG_STR: {
            MDB_val mv;
            if(! (ret = val_from_buffer(&mv, val))) {
                *((char **) dst) = mv.mv_data;
            }
            break;
        }
        case ARG_INT:
            if(! (ret = parse_ulong(val, &l, py_int_max))) {
                *((int *) dst) = (int)l;
            }
            break;
        case ARG_SIZE:
            if(! (ret = parse_ulong(val, &l, py_size_max))) {
                *((size_t *) dst) = (size_t)l;
            }
            break;
        }
    }
    return ret;
}

/**
 * Walk `argspec`, building a Python dictionary mapping keyword arguments to a
 * PyInt describing their offset in the array. Used to reduce keyword argument
 * parsing from O(specsize) to O(number of supplied kwargs).
 */
static int NOINLINE
make_arg_cache(int specsize, const struct argspec *argspec, PyObject **cache)
{
    Py_ssize_t i;

    if(! ((*cache = PyDict_New()))) {
        return -1;
    }

    for(i = 0; i < specsize; i++) {
        const struct argspec *spec = argspec + i;
        PyObject *key = PyUnicode_InternFromString(spec->string);
        PyObject *val = MAKE_ID(i);
        if((! (key && val)) || PyDict_SetItem(*cache, key, val)) {
            return -1;
        }
        Py_DECREF(val);
    }
    return 0;
}

/**
 * Like PyArg_ParseTupleAndKeywords except types are specialized for this
 * module, keyword strings aren't dup'd every call and the code is >3x smaller.
 */
static int NOINLINE
parse_args(int valid, int specsize, const struct argspec *argspec,
           PyObject **cache, PyObject *args, PyObject *kwds, void *out)
{
    unsigned set = 0;
    unsigned i;

    if(! valid) {
        err_invalid();
        return -1;
    }

    if(args) {
        Py_ssize_t size = PyTuple_GET_SIZE(args);
        if(size > specsize) {
            type_error("too many positional arguments.");
            return -1;
        }
        if(specsize < size) {
            size = specsize;
        }
        for(i = 0; i < size; i++) {
            if(parse_arg(argspec + i, PyTuple_GET_ITEM(args, i), out)) {
                return -1;
            }
            set |= 1 << i;
        }
    }

    if(kwds) {
        Py_ssize_t ppos = 0;
        PyObject *pkey;
        PyObject *pvalue;

        if((! *cache) && make_arg_cache(specsize, argspec, cache)) {
            return -1;
        }

        while(PyDict_Next(kwds, &ppos, &pkey, &pvalue)) {
            PyObject *specidx;
            int i;

            if(! ((specidx = PyDict_GetItem(*cache, pkey)))) {
                type_error("unrecognized keyword argument");
                return -1;
            }

            i = READ_ID(specidx);
            if(set & (1 << i)) {
                PyErr_Format(PyExc_TypeError, "duplicate argument: %U", pkey);
                return -1;
            }

            if(parse_arg(argspec + i, pvalue, out)) {
                return -1;
            }
        }
    }
    return 0;
}

/**
 * Return 1 if `db` is associated with the given `env`, otherwise raise an
 * exception. Used to prevent DBIs from unrelated envs from being mixed
 * together (which in future, would cause one env to access another's cursor
 * pointers).
 */
static int
db_owner_check(DbObject *db, EnvObject *env)
{
    if(db->env != env) {
        err_set("Database handle belongs to another environment.", 0);
        return 0;
    }
    return 1;
}


/* -------------------------------------------------------- */
/* Functionality shared between Transaction and Environment */
/* -------------------------------------------------------- */

static PyObject *
make_trans(EnvObject *env, DbObject *db, TransObject *parent, int write,
           int buffers)
{
    MDB_txn *parent_txn;
    MDB_txn *txn;
    TransObject *self;
    int flags;
    int rc;

    DEBUG("make_trans(env=%p, parent=%p, write=%d, buffers=%d)",
        env, parent, write, buffers)
    if(! env->valid) {
        return err_invalid();
    }

    if(! db) {
        db = env->main_db;
    } else if(! db_owner_check(db, env)) {
        return NULL;
    }

    parent_txn = NULL;
    if(parent) {
        if(parent->flags & TRANS_RDONLY) {
            return err_set("Read-only transactions cannot be nested.", EINVAL);
        }
        if(! parent->valid) {
            return err_invalid();
        }
        parent_txn = parent->txn;
    }

    if(write && env->readonly) {
        const char *msg =
            "Cannot start write transaction with read-only environment.";
        return err_set(msg, EACCES);
    }

    if((!write) && env->spare_txn) {
        txn = env->spare_txn;
        DEBUG("using cached txn", txn)
        env->spare_txn = NULL;
        UNLOCKED(rc, mdb_txn_renew(txn));
        if(rc) {
            mdb_txn_abort(txn);
            return err_set("mdb_txn_renew", rc);
        }
    }
    else {
        flags = write ? 0 : MDB_RDONLY;
        UNLOCKED(rc, mdb_txn_begin(env->env, parent_txn, flags, &txn));
        if(rc) {
            return err_set("mdb_txn_begin", rc);
        }
    }

    if(! ((self = PyObject_New(TransObject, &PyTransaction_Type)))) {
        mdb_txn_abort(txn);
        return NULL;
    }
    self->txn = txn;


    OBJECT_INIT(self)
    LINK_CHILD(env, self)
    self->weaklist = NULL;
    self->env = env;
    Py_INCREF(env);
    self->db = db;
    Py_INCREF(db);
#ifdef HAVE_MEMSINK
    self->sink_head = NULL;
#endif

    self->mutations = 0;
    self->flags = 0;
    if(! write) {
        self->flags |= TRANS_RDONLY;
    }
    if(buffers) {
        self->flags |= TRANS_BUFFERS;
    }
    return (PyObject *)self;
}

static PyObject *
make_cursor(DbObject *db, TransObject *trans)
{
    CursorObject *self;
    MDB_cursor *curs;
    int rc;

    if(! trans->valid) {
        return err_invalid();
    }
    if(! db) {
        db = trans->env->main_db;
    } else if(! db_owner_check(db, trans->env)) {
        return NULL;
    }

    UNLOCKED(rc, mdb_cursor_open(trans->txn, db->dbi, &curs));
    if(rc) {
        return err_set("mdb_cursor_open", rc);
    }

    self = PyObject_New(CursorObject, &PyCursor_Type);
    if (!self) {
        mdb_cursor_close(curs);
        return NULL;
    }

    DEBUG("sizeof cursor = %d", (int) sizeof *self)
    OBJECT_INIT(self)
    LINK_CHILD(trans, self)
    self->curs = curs;
    self->positioned = 0;
    self->key.mv_size = 0;
    self->key.mv_data = NULL;
    self->val.mv_size = 0;
    self->val.mv_data = NULL;
    self->trans = trans;
    self->last_mutation = trans->mutations;
    self->dbi_flags = db->flags;
    Py_INCREF(self->trans);
    return (PyObject *) self;
}


/* -------- */
/* Database */
/* -------- */

static DbObject *
db_from_name(EnvObject *env, MDB_txn *txn, const char *name,
             unsigned int flags)
{
    MDB_dbi dbi;
    unsigned int f;
    int rc;
    DbObject *dbo;

    UNLOCKED(rc, mdb_dbi_open(txn, name, flags, &dbi));
    if(rc) {
        err_set("mdb_dbi_open", rc);
        return NULL;
    }
    if((rc = mdb_dbi_flags(txn, dbi, &f))) {
        err_set("mdb_dbi_flags", rc);
        mdb_dbi_close(env->env, dbi);
        return NULL;
    }

    if(! ((dbo = PyObject_New(DbObject, &PyDatabase_Type)))) {
        return NULL;
    }

    OBJECT_INIT(dbo)
    LINK_CHILD(env, dbo)
    dbo->env = env; /* no refcount */
    dbo->dbi = dbi;
    dbo->flags = f;
    DEBUG("DbObject '%s' opened at %p", name, dbo)
    return dbo;
}

/**
 * Use a temporary transaction to manufacture a new _Database object for
 * `name`.
 */
static DbObject *
txn_db_from_name(EnvObject *env, const char *name,
                 unsigned int flags)
{
    int rc;
    MDB_txn *txn;
    DbObject *dbo;

    int begin_flags = (name == NULL || env->readonly) ? MDB_RDONLY : 0;
    UNLOCKED(rc, mdb_txn_begin(env->env, NULL, begin_flags, &txn));
    if(rc) {
        err_set("mdb_txn_begin", rc);
        return NULL;
    }

    if(! ((dbo = db_from_name(env, txn, name, flags)))) {
        Py_BEGIN_ALLOW_THREADS
        mdb_txn_abort(txn);
        Py_END_ALLOW_THREADS
        return NULL;
    }

    UNLOCKED(rc, mdb_txn_commit(txn));
    if(rc) {
        Py_DECREF(dbo);
        return err_set("mdb_txn_commit", rc);
    }
    return dbo;
}

static int
db_clear(DbObject *self)
{
    if(self->env) {
        UNLINK_CHILD(self->env, self)
        self->env = NULL;
    }
    self->valid = 0;
    return 0;
}

/**
 * _Database.flags()
 */
static PyObject *
db_flags(DbObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *dct;
    unsigned int f;

    if (args) {
        Py_ssize_t size = PyTuple_GET_SIZE(args);
        if(size > 1) {
            return type_error("too many positional arguments.");
        }
    }

    dct = PyDict_New();
    f = self->flags;
    PyDict_SetItemString(dct, "reverse_key", py_bool(f & MDB_REVERSEKEY));
    PyDict_SetItemString(dct, "dupsort", py_bool(f & MDB_DUPSORT));
    PyDict_SetItemString(dct, "integerkey", py_bool(f & MDB_INTEGERKEY));
    PyDict_SetItemString(dct, "integerdup", py_bool(f & MDB_INTEGERDUP));
    PyDict_SetItemString(dct, "dupfixed", py_bool(f & MDB_DUPFIXED));
    return dct;
}

/**
 * _Database.__del__()
 */
static void
db_dealloc(DbObject *self)
{
    db_clear(self);
    PyObject_Del(self);
}

static struct PyMethodDef db_methods[] = {
    {"flags", (PyCFunction)db_flags, METH_VARARGS|METH_KEYWORDS},
    {0, 0, 0, 0}
};

static PyTypeObject PyDatabase_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_Database",                /*tp_name*/
    sizeof(DbObject),           /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    (destructor)db_dealloc,     /*tp_dealloc*/
    0,                          /*tp_print*/
    0,                          /*tp_getattr*/
    0,                          /*tp_setattr*/
    0,                          /*tp_compare*/
    0,                          /*tp_repr*/
    0,                          /*tp_as_number*/
    0,                          /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                          /*tp_call*/
    0,                          /*tp_str*/
    0,                          /*tp_getattro*/
    0,                          /*tp_setattro*/
    0,                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,         /*tp_flags*/
    0,                          /*tp_doc*/
    0,                          /*tp_traverse*/
    (inquiry)db_clear,          /*tp_clear*/
    0,                          /*tp_richcompare*/
    0,                          /*tp_weaklistoffset*/
    0,                          /*tp_iter*/
    0,                          /*tp_iternext*/
    db_methods                  /*tp_methods*/
};


/* ----------- */
/* Environment */
/* ----------- */

static void
trans_dealloc(TransObject *self);

static void
txn_abort(MDB_txn *txn);

static int
env_clear(EnvObject *self)
{
    MDB_txn * txn;

    MDEBUG("env_clear")

    INVALIDATE(self)
    self->valid = 0;
    Py_CLEAR(self->main_db);

    txn = self->spare_txn;
    if(txn) {
        MDEBUG("killing spare txn %p", txn);
        txn_abort(txn);
        self->spare_txn = NULL;
    }

    if(self->env) {
        DEBUG("Closing env")
        Py_BEGIN_ALLOW_THREADS
        mdb_env_close(self->env);
        Py_END_ALLOW_THREADS
        self->env = NULL;
    }
    return 0;
}

/**
 * Environment.__del__()
 */
static void
env_dealloc(EnvObject *self)
{
    if(self->weaklist != NULL) {
        MDEBUG("Clearing weaklist..")
        PyObject_ClearWeakRefs((PyObject *) self);
    }

    env_clear(self);
    PyObject_Del(self);
}

/**
 * Environment.close()
 */
static PyObject *
env_close(EnvObject *self)
{
    env_clear(self);
    Py_RETURN_NONE;
}

/**
 * Environment() -> new object.
 */
static PyObject *
env_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    struct env_new {
        PyObject *path;
        size_t map_size;
        int subdir;
        int readonly;
        int metasync;
        int sync;
        int map_async;
        int mode;
        int create;
        int readahead;
        int writemap;
        int meminit;
        int max_readers;
        int max_dbs;
        int max_spare_txns;
        int lock;
    } arg = {NULL, 10485760, 1, 0, 1, 1, 0, 0755, 1, 1, 0, 1, 126, 0, 1, 1};

    static const struct argspec argspec[] = {
        {"path", ARG_OBJ, OFFSET(env_new, path)},
        {"map_size", ARG_SIZE, OFFSET(env_new, map_size)},
        {"subdir", ARG_BOOL, OFFSET(env_new, subdir)},
        {"readonly", ARG_BOOL, OFFSET(env_new, readonly)},
        {"metasync", ARG_BOOL, OFFSET(env_new, metasync)},
        {"sync", ARG_BOOL, OFFSET(env_new, sync)},
        {"map_async", ARG_BOOL, OFFSET(env_new, map_async)},
        {"mode", ARG_INT, OFFSET(env_new, mode)},
        {"create", ARG_BOOL, OFFSET(env_new, create)},
        {"readahead", ARG_BOOL, OFFSET(env_new, readahead)},
        {"writemap", ARG_BOOL, OFFSET(env_new, writemap)},
        {"meminit", ARG_INT, OFFSET(env_new, meminit)},
        {"max_readers", ARG_INT, OFFSET(env_new, max_readers)},
        {"max_dbs", ARG_INT, OFFSET(env_new, max_dbs)},
        {"max_spare_txns", ARG_INT, OFFSET(env_new, max_spare_txns)},
        {"lock", ARG_BOOL, OFFSET(env_new, lock)}
    };

    PyObject *fspath_obj = NULL;
    EnvObject *self;
    const char *fspath;
    int flags;
    int rc;
    int mode;

    static PyObject *cache = NULL;
    if(parse_args(1, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }

    if(! arg.path) {
        return type_error("'path' argument required");
    }

    if(! ((self = PyObject_New(EnvObject, type)))) {
        return NULL;
    }

    OBJECT_INIT(self)
    self->weaklist = NULL;
    self->main_db = NULL;
    self->env = NULL;
    self->spare_txn = NULL;

    if((rc = mdb_env_create(&self->env))) {
        err_set("mdb_env_create", rc);
        goto fail;
    }

    if((rc = mdb_env_set_mapsize(self->env, arg.map_size))) {
        err_set("mdb_env_set_mapsize", rc);
        goto fail;
    }

    if((rc = mdb_env_set_maxreaders(self->env, arg.max_readers))) {
        err_set("mdb_env_set_maxreaders", rc);
        goto fail;
    }

    if((rc = mdb_env_set_maxdbs(self->env, arg.max_dbs))) {
        err_set("mdb_env_set_maxdbs", rc);
        goto fail;
    }

    if(! ((fspath_obj = get_fspath(arg.path)))) {
        goto fail;
    }
    fspath = PyBytes_AS_STRING(fspath_obj);

    if(arg.create && arg.subdir && !arg.readonly) {
        if(mkdir(fspath, arg.mode) && errno != EEXIST) {
            PyErr_SetFromErrnoWithFilename(PyExc_OSError, fspath);
            goto fail;
        }
    }

    flags = MDB_NOTLS;
    if(! arg.subdir) {
        flags |= MDB_NOSUBDIR;
    }
    if(arg.readonly) {
        flags |= MDB_RDONLY;
    }
    self->readonly = arg.readonly;
    if(! arg.metasync) {
        flags |= MDB_NOMETASYNC;
    }
    if(! arg.sync) {
        flags |= MDB_NOSYNC;
    }
    if(arg.map_async) {
        flags |= MDB_MAPASYNC;
    }
    if(! arg.readahead) {
        flags |= MDB_NORDAHEAD;
    }
    if(arg.writemap) {
        flags |= MDB_WRITEMAP;
    }
    if(! arg.meminit) {
        flags |= MDB_NOMEMINIT;
    }
    if(! arg.lock) {
        flags |= MDB_NOLOCK;
    }

    /* Strip +x. */
    mode = arg.mode & ~0111;

    DEBUG("mdb_env_open(%p, '%s', %d, %o);", self->env, fspath, flags, mode)
    UNLOCKED(rc, mdb_env_open(self->env, fspath, flags, mode));
    if(rc) {
        err_set(fspath, rc);
        goto fail;
    }

    self->main_db = txn_db_from_name(self, NULL, 0);
    if(self->main_db) {
        self->valid = 1;
        DEBUG("EnvObject '%s' opened at %p", fspath, self)
        return (PyObject *) self;
    }

fail:
    DEBUG("initialization failed")
    Py_CLEAR(fspath_obj);
    Py_CLEAR(self);
    return NULL;
}

/**
 * Environment.begin()
 */
static PyObject *
env_begin(EnvObject *self, PyObject *args, PyObject *kwds)
{
    struct env_begin {
        DbObject *db;
        TransObject *parent;
        int write;
        int buffers;
    } arg = {self->main_db, NULL, 0, 0};

    static const struct argspec argspec[] = {
        {"db", ARG_DB, OFFSET(env_begin, db)},
        {"parent", ARG_TRANS, OFFSET(env_begin, parent)},
        {"write", ARG_BOOL, OFFSET(env_begin, write)},
        {"buffers", ARG_BOOL, OFFSET(env_begin, buffers)},
    };

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }
    return make_trans(self, arg.db, arg.parent, arg.write, arg.buffers);
}

/**
 * Environment.copy()
 */
static PyObject *
env_copy(EnvObject *self, PyObject *args, PyObject *kwds)
{
    struct env_copy {
        PyObject *path;
        int compact;
        TransObject *txn;
    } arg = {NULL, 0, NULL};

    static const struct argspec argspec[] = {
        {"path", ARG_OBJ, OFFSET(env_copy, path)},
        {"compact", ARG_BOOL, OFFSET(env_copy, compact)},
        {"txn", ARG_TRANS, OFFSET(env_copy, txn)}
    };

    PyObject *fspath_obj;
    const char *fspath_s;
    int flags;
    int rc;
#ifdef HAVE_PATCHED_LMDB
    MDB_txn *txn;
#endif

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }
    if(! arg.path) {
        return type_error("path argument required");
    }
    if(! ((fspath_obj = get_fspath(arg.path)))) {
        return NULL;
    }

#ifdef HAVE_PATCHED_LMDB
    if (arg.txn) {
        txn = arg.txn->txn;
        if (!arg.compact) {
            return type_error("txn argument only compatible with compact=True");
        }
    }
    else {
        txn = NULL;
    }
#else
    if (arg.txn) {
        return type_error("Non-patched LMDB doesn't support transaction with env.copy");
    }
#endif

    fspath_s = PyBytes_AS_STRING(fspath_obj);
    flags = arg.compact ? MDB_CP_COMPACT : 0;
#ifdef HAVE_PATCHED_LMDB
    UNLOCKED(rc, mdb_env_copy3(self->env, fspath_s, flags, txn));
#else
    UNLOCKED(rc, mdb_env_copy2(self->env, fspath_s, flags));
#endif
    Py_CLEAR(fspath_obj);
    if(rc) {
#ifdef HAVE_PATCHED_LMDB
        return err_set("mdb_env_copy3", rc);
#else
        return err_set("mdb_env_copy2", rc);
#endif
    }
    Py_RETURN_NONE;
}

/**
 * Environment.copyfd(fd)
 */
static PyObject *
env_copyfd(EnvObject *self, PyObject *args, PyObject *kwds)
{
    struct env_copyfd {
        int fd;
        int compact;
        TransObject *txn;
    } arg = {-1, 0};
    int rc;
#ifdef HAVE_PATCHED_LMDB
    MDB_txn *txn;
#endif
#ifdef _WIN32
    PyObject *temp;
    Py_ssize_t handle;
#endif

    static const struct argspec argspec[] = {
        {"fd", ARG_INT, OFFSET(env_copyfd, fd)},
        {"compact", ARG_BOOL, OFFSET(env_copyfd, compact)},
        {"txn", ARG_TRANS, OFFSET(env_copyfd, txn)},
    };
    int flags;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }
    if(arg.fd == -1) {
        return type_error("fd argument required");
    }
    flags = arg.compact ? MDB_CP_COMPACT : 0;

#ifdef HAVE_PATCHED_LMDB
    if (arg.txn) {
        txn = arg.txn->txn;
        if (!arg.compact) {
            return type_error("txn argument only compatible with compact=True");
        }
    }
    else {
        txn = NULL;
    }
#else
    if (arg.txn) {
        return type_error("Non-patched LMDB doesn't support transaction with env.copyfd");
    }
#endif

#ifdef _WIN32
    temp = PyObject_CallMethod(msvcrt, "get_osfhandle", "i", arg.fd);
    if(! temp) {
        return NULL;
    }
    handle = PyNumber_AsSsize_t(temp, PyExc_OverflowError);
    Py_DECREF(temp);
    if(PyErr_Occurred()) {
        return NULL;
    }
    #define HANDLE_ARG (HANDLE)handle
#else
    #define HANDLE_ARG arg.fd
#endif

#ifdef HAVE_PATCHED_LMDB
    UNLOCKED(rc, mdb_env_copyfd3(self->env, HANDLE_ARG, flags, txn));
#else
    UNLOCKED(rc, mdb_env_copyfd2(self->env, HANDLE_ARG, flags));
#endif

    if(rc) {
        return err_set("mdb_env_copyfd3", rc);
    }
    Py_RETURN_NONE;
}

/**
 * Environment.info() -> dict
 */
static PyObject *
env_info(EnvObject *self)
{
    static const struct dict_field fields[] = {
        {TYPE_ADDR, "map_addr",    offsetof(MDB_envinfo, me_mapaddr)},
        {TYPE_SIZE, "map_size",    offsetof(MDB_envinfo, me_mapsize)},
        {TYPE_SIZE, "last_pgno",   offsetof(MDB_envinfo, me_last_pgno)},
        {TYPE_SIZE, "last_txnid",  offsetof(MDB_envinfo, me_last_txnid)},
        {TYPE_UINT, "max_readers", offsetof(MDB_envinfo, me_maxreaders)},
        {TYPE_UINT, "num_readers", offsetof(MDB_envinfo, me_numreaders)},
        {TYPE_EOF, NULL, 0}
    };
    MDB_envinfo info;
    int rc;

    if(! self->valid) {
        return err_invalid();
    }

    UNLOCKED(rc, mdb_env_info(self->env, &info));
    if(rc) {
        err_set("mdb_env_info", rc);
        return NULL;
    }
    return dict_from_fields(&info, fields);
}

/**
 * Environment.flags() -> dict
 */
static PyObject *
env_flags(EnvObject *self)
{
    PyObject *dct;
    unsigned int flags;
    int rc;

    if(! self->valid) {
        return err_invalid();
    }

    if((rc = mdb_env_get_flags(self->env, &flags))) {
        err_set("mdb_env_get_flags", rc);
        return NULL;
    }

    dct = PyDict_New();
    PyDict_SetItemString(dct, "subdir", py_bool(!(flags & MDB_NOSUBDIR)));
    PyDict_SetItemString(dct, "readonly", py_bool(flags & MDB_RDONLY));
    PyDict_SetItemString(dct, "metasync", py_bool(!(flags & MDB_NOMETASYNC)));
    PyDict_SetItemString(dct, "sync", py_bool(!(flags & MDB_NOSYNC)));
    PyDict_SetItemString(dct, "map_async", py_bool(flags & MDB_MAPASYNC));
    PyDict_SetItemString(dct, "readahead", py_bool(!(flags & MDB_NORDAHEAD)));
    PyDict_SetItemString(dct, "writemap", py_bool(flags & MDB_WRITEMAP));
    PyDict_SetItemString(dct, "meminit", py_bool(!(flags & MDB_NOMEMINIT)));
    PyDict_SetItemString(dct, "lock", py_bool(!(flags & MDB_NOLOCK)));
    return dct;
}

/**
 * Environment.max_key_size() -> int
 */
static PyObject *
env_max_key_size(EnvObject *self)
{
    int key_size;
    if(! self->valid) {
        return err_invalid();
    }
    key_size = mdb_env_get_maxkeysize(self->env);
    return PyLong_FromLongLong(key_size);
}

/**
 * Environment.max_key_size() -> int
 */
static PyObject *
env_max_readers(EnvObject *self)
{
    unsigned int readers;
    int rc;

    if(! self->valid) {
        return err_invalid();
    }
    if((rc = mdb_env_get_maxreaders(self->env, &readers))) {
        return err_set("mdb_env_get_maxreaders", rc);
    }
    return PyLong_FromLongLong(readers);
}

/**
 * Environment.open_db() -> handle
 */
static PyObject *
env_open_db(EnvObject *self, PyObject *args, PyObject *kwds)
{
    struct env_open_db {
        const char *key;
        TransObject *txn;
        int reverse_key;
        int dupsort;
        int create;
        int integerkey;
        int integerdup;
        int dupfixed;
    } arg = {NULL, NULL, 0, 0, 1};

    static const struct argspec argspec[] = {
        {"key", ARG_STR, OFFSET(env_open_db, key)},
        {"txn", ARG_TRANS, OFFSET(env_open_db, txn)},
        {"reverse_key", ARG_BOOL, OFFSET(env_open_db, reverse_key)},
        {"dupsort", ARG_BOOL, OFFSET(env_open_db, dupsort)},
        {"create", ARG_BOOL, OFFSET(env_open_db, create)},
        {"integerkey", ARG_BOOL, OFFSET(env_open_db, integerkey)},
        {"integerdup", ARG_BOOL, OFFSET(env_open_db, integerdup)},
        {"dupfixed", ARG_BOOL, OFFSET(env_open_db, dupfixed)},
    };
    int flags;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }

    if (!arg.key && (arg.reverse_key || arg.dupsort || arg.integerkey ||
                     arg.integerdup || arg.dupfixed)) {
        return PyErr_Format(PyExc_ValueError,
                            "May not set flags on the main database");
    }

    flags = 0;
    if(arg.reverse_key) {
        flags |= MDB_REVERSEKEY;
    }
    if(arg.dupsort) {
        flags |= MDB_DUPSORT;
    }
    if(arg.create) {
        flags |= MDB_CREATE;
    }
    if(arg.integerkey) {
        flags |= MDB_INTEGERKEY;
    }
    if(arg.integerdup) {
        flags |= MDB_DUPSORT;
        flags |= MDB_DUPFIXED;
        flags |= MDB_INTEGERDUP;
    }
    if(arg.dupfixed) {
        flags |= MDB_DUPSORT;
        flags |= MDB_DUPFIXED;
    }

    if(arg.txn) {
        return (PyObject *) db_from_name(self, arg.txn->txn, arg.key, flags);
    } else {
        return (PyObject *) txn_db_from_name(self, arg.key, flags);
    }
}

/**
 * Environment.path() -> Unicode
 */
static PyObject *
env_path(EnvObject *self)
{
    const char *path;
    int rc;

    if(! self->valid) {
        return err_invalid();
    }

    if((rc = mdb_env_get_path(self->env, &path))) {
        return err_set("mdb_env_get_path", rc);
    }
    return PyUnicode_FromString(path);
}

static const struct dict_field mdb_stat_fields[] = {
    {TYPE_UINT, "psize",          offsetof(MDB_stat, ms_psize)},
    {TYPE_UINT, "depth",          offsetof(MDB_stat, ms_depth)},
    {TYPE_SIZE, "branch_pages",   offsetof(MDB_stat, ms_branch_pages)},
    {TYPE_SIZE, "leaf_pages",     offsetof(MDB_stat, ms_leaf_pages)},
    {TYPE_SIZE, "overflow_pages", offsetof(MDB_stat, ms_overflow_pages)},
    {TYPE_SIZE, "entries",        offsetof(MDB_stat, ms_entries)},
    {TYPE_EOF, NULL, 0}
};

/**
 * Environment.stat() -> dict
 */
static PyObject *
env_stat(EnvObject *self)
{
    MDB_stat st;
    int rc;

    if(! self->valid) {
        return err_invalid();
    }

    UNLOCKED(rc, mdb_env_stat(self->env, &st));
    if(rc) {
        err_set("mdb_env_stat", rc);
        return NULL;
    }
    return dict_from_fields(&st, mdb_stat_fields);
}

/**
 * Callback to receive string result for env_readers(). Return 0 on success or
 * -1 on error.
 */
static int env_readers_callback(const char *msg, void *str_)
{
    PyObject **str = str_;
    PyObject *s = PyUnicode_FromString(msg);
    PyObject *new;
    if(! s) {
        return -1;
    }
    new = PyUnicode_Concat(*str, s);
    Py_CLEAR(*str);
    *str = new;
    if(! new) {
        return -1;
    }
    return 0;
}

/**
 * Environment.readers() -> string
 */
static PyObject *
env_readers(EnvObject *self)
{
    PyObject *str;
    if(! self->valid) {
        return err_invalid();
    }

    if(! ((str = PyUnicode_FromString("")))) {
        return NULL;
    }

    if(mdb_reader_list(self->env, env_readers_callback, &str)) {
        Py_CLEAR(str);
    }
    return str;
}

/**
 * Environment.reader_check() -> int
 */
static PyObject *
env_reader_check(EnvObject *self)
{
    int rc;
    int dead;

    if(! self->valid) {
        return err_invalid();
    }

    if((rc = mdb_reader_check(self->env, &dead))) {
        return err_set("mdb_reader_check", rc);
    }
    return PyLong_FromLongLong(dead);
}

/**
 * Environment.set_mapsize(size) -> None
 */
static PyObject *
env_reader_set_mapsize(EnvObject *self, PyObject *args, PyObject *kwargs)
{
    struct env_set_mapsize {
        size_t map_size;
    } arg = {0};

    static const struct argspec argspec[] = {
        {"map_size", ARG_SIZE, OFFSET(env_set_mapsize, map_size)}
    };
    int rc;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache,
                  args, kwargs, &arg)) {
        return NULL;
    }

    rc = mdb_env_set_mapsize(self->env, arg.map_size);
    if(rc) {
        return err_set("mdb_env_set_mapsize", rc);
    }
    Py_RETURN_NONE;
}

/**
 * Environment.sync()
 */
static PyObject *
env_sync(EnvObject *self, PyObject *args)
{
    struct env_sync {
        int force;
    } arg = {0};

    static const struct argspec argspec[] = {
        {"force", ARG_BOOL, OFFSET(env_sync, force)}
    };
    int rc;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, NULL, &arg)) {
        return NULL;
    }

    UNLOCKED(rc, mdb_env_sync(self->env, arg.force));
    if(rc) {
        return err_set("mdb_env_sync", rc);
    }
    Py_RETURN_NONE;
}

/**
 * Environment.__enter__()
 */
static PyObject *env_enter(EnvObject *self)
{
    Py_INCREF(self);
    return (PyObject *)self;
}

/**
 * Environment.__exit__()
 */
static PyObject *env_exit(EnvObject *self, PyObject *args)
{
    env_clear(self);
    Py_RETURN_NONE;
}

static struct PyMethodDef env_methods[] = {
    {"__enter__", (PyCFunction)env_enter, METH_NOARGS},
    {"__exit__", (PyCFunction)env_exit, METH_VARARGS},
    {"begin", (PyCFunction)env_begin, METH_VARARGS|METH_KEYWORDS},
    {"close", (PyCFunction)env_close, METH_NOARGS},
    {"copy", (PyCFunction)env_copy, METH_VARARGS|METH_KEYWORDS},
    {"copyfd", (PyCFunction)env_copyfd, METH_VARARGS|METH_KEYWORDS},
    {"info", (PyCFunction)env_info, METH_NOARGS},
    {"flags", (PyCFunction)env_flags, METH_NOARGS},
    {"max_key_size", (PyCFunction)env_max_key_size, METH_NOARGS},
    {"max_readers", (PyCFunction)env_max_readers, METH_NOARGS},
    {"open_db", (PyCFunction)env_open_db, METH_VARARGS|METH_KEYWORDS},
    {"path", (PyCFunction)env_path, METH_NOARGS},
    {"stat", (PyCFunction)env_stat, METH_NOARGS},
    {"readers", (PyCFunction)env_readers, METH_NOARGS},
    {"reader_check", (PyCFunction)env_reader_check, METH_NOARGS},
    {"set_mapsize", (PyCFunction)env_reader_set_mapsize,
     METH_VARARGS|METH_KEYWORDS},
    {"sync", (PyCFunction)env_sync, METH_VARARGS},
    {NULL, NULL}
};

static PyTypeObject PyEnvironment_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "Environment",              /*tp_name*/
    sizeof(EnvObject),          /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    (destructor) env_dealloc,   /*tp_dealloc*/
    0,                          /*tp_print*/
    0,                          /*tp_getattr*/
    0,                          /*tp_setattr*/
    0,                          /*tp_compare*/
    0,                          /*tp_repr*/
    0,                          /*tp_as_number*/
    0,                          /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                          /*tp_call*/
    0,                          /*tp_str*/
    0,                          /*tp_getattro*/
    0,                          /*tp_setattro*/
    0,                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,         /*tp_flags*/
    0,                          /*tp_doc*/
    0,                          /*tp_traverse*/
    (inquiry) env_clear,        /*tp_clear*/
    0,                          /*tp_richcompare*/
    offsetof(EnvObject, weaklist), /*tp_weaklistoffset*/
    0,                          /*tp_iter*/
    0,                          /*tp_iternext*/
    env_methods,                /*tp_methods*/
    0,                          /*tp_members*/
    0,                          /*tp_getset*/
    0,                          /*tp_base*/
    0,                          /*tp_dict*/
    0,                          /*tp_descr_get*/
    0,                          /*tp_descr_set*/
    0,                          /*tp_dictoffset*/
    0,                          /*tp_init*/
    0,                          /*tp_alloc*/
    env_new,                    /*tp_new*/
};


/* ------- */
/* Cursors */
/* ------- */

static int
cursor_clear(CursorObject *self)
{
    if(self->valid) {
        INVALIDATE(self)
        UNLINK_CHILD(self->trans, self)
        Py_BEGIN_ALLOW_THREADS
        mdb_cursor_close(self->curs);
        Py_END_ALLOW_THREADS
        self->valid = 0;
    }
    Py_CLEAR(self->trans);
    return 0;
}

/**
 * Cursor.__del__()
 */
static void
cursor_dealloc(CursorObject *self)
{
    DEBUG("destroying cursor")
    cursor_clear(self);
    PyObject_Del(self);
}

/**
 * Environment.Cursor(db, trans) -> new instance.
 */
static PyObject *
cursor_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    struct cursor_new {
        DbObject *db;
        TransObject *trans;
    } arg = {NULL, NULL};

    static const struct argspec argspec[] = {
        {"db", ARG_DB, OFFSET(cursor_new, db)},
        {"txn", ARG_TRANS, OFFSET(cursor_new, trans)}
    };

    static PyObject *cache = NULL;
    if(parse_args(1, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }

    if(! (arg.db && arg.trans)) {
        return type_error("db and transaction parameters required.");
    }
    return make_cursor(arg.db, arg.trans);
}

/**
 * Cursor.count() -> long
 */
static PyObject *
cursor_count(CursorObject *self)
{
    size_t count;
    int rc;

    if(! self->valid) {
        return err_invalid();
    }

    UNLOCKED(rc, mdb_cursor_count(self->curs, &count));
    if(rc) {
        return err_set("mdb_cursor_count", rc);
    }
    return PyLong_FromUnsignedLongLong(count);
}

/**
 * Apply `op` to the cursor using the Cursor instance's `key` and `val`
 * MDB_vals. On completion, check for an error and if one occurred, set the
 * cursor to the unpositioned state. Finally record the containing
 * transaction's last mutation count, so we know if `key` and `val` become
 * invalid before the next attempt to read them.
 */
static int
_cursor_get_c(CursorObject *self, enum MDB_cursor_op op)
{
    int rc;

    Py_BEGIN_ALLOW_THREADS;
    rc = mdb_cursor_get(self->curs, &self->key, &self->val, op);
    Py_END_ALLOW_THREADS;

    self->positioned = rc == 0;
    self->last_mutation = self->trans->mutations;
    if(rc) {
        self->key.mv_size = 0;
        self->val.mv_size = 0;
        if(rc != MDB_NOTFOUND) {
            if(! (rc == EINVAL && op == MDB_GET_CURRENT)) {
                err_set("mdb_cursor_get", rc);
                return -1;
            }
        }
    }
    return 0;
}

/**
 * Wrap _cursor_get_c() to return True or False depending on whether the
 * Cursor's final state is positioned.
 */
static PyObject *
_cursor_get(CursorObject *self, enum MDB_cursor_op op)
{
    if(! self->valid) {
        return err_invalid();
    }
    if(_cursor_get_c(self, op)) {
        return NULL;
    }
    return py_bool(self->positioned);
}

/**
 * Cursor.delete(dupdata=False) -> bool
 */
static PyObject *
cursor_delete(CursorObject *self, PyObject *args, PyObject *kwds)
{
    struct cursor_delete {
        int dupdata;
    } arg = {0};

    static const struct argspec argspec[] = {
        {"dupdata", ARG_BOOL, OFFSET(cursor_delete, dupdata)}
    };
    int res;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }

    res = 0;
    if(self->positioned) {
        int rc;
        int flags = arg.dupdata ? MDB_NODUPDATA : 0;
        DEBUG("deleting key '%.*s'",
              (int) self->key.mv_size,
              (char*) self->key.mv_data)
        UNLOCKED(rc, mdb_cursor_del(self->curs, flags));
        self->trans->mutations++;
        if(rc) {
            return err_set("mdb_cursor_del", rc);
        }
        res = 1;
        _cursor_get_c(self, MDB_GET_CURRENT);
    }
    return py_bool(res);
}

/**
 * Cursor.first() -> bool
 */
static PyObject *
cursor_first(CursorObject *self)
{
    return _cursor_get(self, MDB_FIRST);
}

/**
 * Cursor.first_dup() -> bool
 */
static PyObject *
cursor_first_dup(CursorObject *self)
{
    return _cursor_get(self, MDB_FIRST_DUP);
}

static PyObject *
cursor_value(CursorObject *self);

/**
 * Cursor.getmulti() -> Iterable of (key, value)
 */
static PyObject *
cursor_get_multi(CursorObject *self, PyObject *args, PyObject *kwds)
{
    struct cursor_get {
        PyObject *keys;
        int dupdata;
        size_t dupfixed_bytes;
        int keyfixed;
    } arg = {Py_None, 0, 0, 0};

    int i, as_buffer;
    PyObject *iter, *item, *tup, *key, *val;
    PyObject *pylist = NULL;
    MDB_cursor_op get_op, next_op;
    bool done, first;

    static const struct argspec argspec[] = {
        {"keys", ARG_OBJ, OFFSET(cursor_get, keys)},
        {"dupdata", ARG_BOOL, OFFSET(cursor_get, dupdata)},
        {"dupfixed_bytes", ARG_SIZE, OFFSET(cursor_get, dupfixed_bytes)},
        {"keyfixed", ARG_BOOL, OFFSET(cursor_get, keyfixed)}
    };

    size_t buffer_pos = 0, buffer_size = 8;
    size_t key_size, val_size, item_size = 0;
    char *buffer = NULL;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }

    if(arg.dupfixed_bytes < 0) {
        return type_error("dupfixed_bytes must be a positive integer.");
    }else if ((arg.dupfixed_bytes > 0 || arg.keyfixed) && !arg.dupdata) {
        return type_error("dupdata is required for dupfixed_bytes/keyfixed.");
    }else if (arg.keyfixed && !arg.dupfixed_bytes){
        return type_error("dupfixed_bytes is required for keyfixed.");
    }

    if(! ((iter = PyObject_GetIter(arg.keys)))) {
        return NULL;
    }

    /* Choose ops */
    if(arg.dupfixed_bytes) {
        get_op = MDB_GET_MULTIPLE;
        next_op = MDB_NEXT_MULTIPLE;
    } else {
        get_op = MDB_GET_CURRENT;
        next_op = MDB_NEXT_DUP;
    }

    as_buffer = self->trans->flags & TRANS_BUFFERS;
    val_size = arg.dupfixed_bytes;
    if (!arg.keyfixed){ /* Init list */
        pylist = PyList_New(0);
    }
    first = true;
    while((item = PyIter_Next(iter))) {
        MDB_val mkey;

        if(val_from_buffer(&mkey, item)) {
            goto failiter;
        } /* val_from_buffer sets exception */

        self->key = mkey;
        if(_cursor_get_c(self, MDB_SET_KEY)) {
            goto failiter;
        }

        done = false;
        while (!done) {

            if(! self->positioned) {
                done = true;
            }
            else if(_cursor_get_c(self, get_op)) {
                goto failiter;
            } else {
                key = obj_from_val(&self->key, as_buffer);
                PRELOAD_UNLOCKED(0, self->val.mv_data, self->val.mv_size);

                if(!arg.dupfixed_bytes) {
                    /* Not dupfixed, MDB_GET_CURRENT returns single item */
                    val = obj_from_val(&self->val, as_buffer);
                    tup = PyTuple_New(2);

                    if (tup && key && val) {
                        PyTuple_SET_ITEM(tup, 0, key);
                        PyTuple_SET_ITEM(tup, 1, val);
                        PyList_Append(pylist, tup);
                        Py_DECREF(tup);
                    } else {
                        Py_DECREF(key);
                        Py_DECREF(val);
                        Py_DECREF(tup);
                    }
                } else {
                    /* dupfixed, MDB_GET_MULTIPLE returns batch, iterate values */
                    int items = (int) self->val.mv_size/val_size;
                    if (first) {
                        key_size = (size_t) self->key.mv_size;
                        item_size = key_size + val_size;
                        if (arg.keyfixed) { /* Init structured array buffer */
                            buffer = malloc(buffer_size * item_size);
                        }
                        first = false;
                    }

                    for(i=0; i<items; i++) {
                        char *val_data = (char *) self->val.mv_data + (i * val_size);
                        if (arg.keyfixed) {
                            /* Add to array buffer */
                            char *k, *v;
                            if (buffer_pos >= buffer_size) { // Grow buffer
                                buffer_size = buffer_size * 2;
                                buffer = realloc(buffer, buffer_size * item_size);
                            }
                            k = buffer + (buffer_pos * item_size);
                            v = k + key_size;
                            memcpy(k, (char *) self->key.mv_data, key_size);
                            memcpy(v, val_data, val_size);

                            buffer_pos++;
                        } else {
                            /* Add to list of tuples */
                            if(as_buffer) {
                                val = PyMemoryView_FromMemory(
                                    val_data, (size_t) arg.dupfixed_bytes, PyBUF_READ);
                            } else {
                                val = PyBytes_FromStringAndSize(
                                    val_data, (size_t) arg.dupfixed_bytes);
                            }
                            tup = PyTuple_New(2);
                            if (tup && key && val) {
                                Py_INCREF(key); // Hold key in loop
                                PyTuple_SET_ITEM(tup, 0, key);
                                PyTuple_SET_ITEM(tup, 1, val);
                                PyList_Append(pylist, tup);
                                Py_DECREF(tup);
                            } else {
                                Py_DECREF(val);
                                Py_DECREF(tup);
                            }
                        }
                    }
                    Py_DECREF(key); // Release key
                }

                if(arg.dupdata){
                    if(_cursor_get_c(self, next_op)) {
                        goto failiter;
                    }
                }
                else {
                    done = true;
                }
            }
        }
        Py_DECREF(item);
    }

    Py_DECREF(iter);
    if(PyErr_Occurred()) {
        goto fail;
    }

    if (arg.keyfixed){
        int rc;
        Py_buffer pybuf;
        size_t newsize = buffer_pos * item_size;
        buffer = realloc(buffer, newsize);
        rc = PyBuffer_FillInfo(&pybuf, NULL, buffer, newsize, 0, PyBUF_SIMPLE);
        // FIXME:  check rc
        return PyMemoryView_FromBuffer(&pybuf);
    } else {
        return pylist;
    }

failiter:
    Py_DECREF(item);
    Py_DECREF(iter);
fail:
    if (buffer) {
        free(buffer);
    }
    return NULL;
}

/**
 * Cursor.get() -> result
 */
static PyObject *
cursor_get(CursorObject *self, PyObject *args, PyObject *kwds)
{
    struct cursor_get {
        MDB_val key;
        PyObject *default_;
    } arg = {{0, 0}, Py_None};

    static const struct argspec argspec[] = {
        {"key", ARG_BUF, OFFSET(cursor_get, key)},
        {"default", ARG_OBJ, OFFSET(cursor_get, default_)}
    };

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }

    if(! arg.key.mv_data) {
        return type_error("key must be given.");
    }

    self->key = arg.key;
    if(_cursor_get_c(self, MDB_SET_KEY)) {
        return NULL;
    }
    if(! self->positioned) {
        Py_INCREF(arg.default_);
        return arg.default_;
    }
    return cursor_value(self);
}

/**
 * Cursor.item() -> (key, value)
 */
static PyObject *
cursor_item(CursorObject *self)
{
    int as_buffer;
    PyObject *key;
    PyObject *val;
    PyObject *tup;
    int rc = 0;

    if(! self->valid) {
        return err_invalid();
    }
    /* Must refresh `key` and `val` following mutation. */
    if(self->last_mutation != self->trans->mutations &&
       _cursor_get_c(self, MDB_GET_CURRENT)) {
        return NULL;
    }

    as_buffer = self->trans->flags & TRANS_BUFFERS;
    key = obj_from_val(&self->key, as_buffer);
    PRELOAD_UNLOCKED(rc, self->val.mv_data, self->val.mv_size);
    val = obj_from_val(&self->val, as_buffer);
    tup = PyTuple_New(2);
    if(tup && key && val) {
        PyTuple_SET_ITEM(tup, 0, key);
        PyTuple_SET_ITEM(tup, 1, val);
        return tup;
    }
    Py_CLEAR(key);
    Py_CLEAR(val);
    Py_CLEAR(tup);
    return NULL;
}

/**
 * Cursor.key() -> result
 */
static PyObject *
cursor_key(CursorObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }
    /* Must refresh `key` and `val` following mutation. */
    if(self->last_mutation != self->trans->mutations &&
       _cursor_get_c(self, MDB_GET_CURRENT)) {
        return NULL;
    }
    return obj_from_val(&self->key, self->trans->flags & TRANS_BUFFERS);
}

/**
 * Cursor.last() -> bool
 */
static PyObject *
cursor_last(CursorObject *self)
{
    return _cursor_get(self, MDB_LAST);
}

/**
 * Cursor.last_dup() -> bool
 */
static PyObject *
cursor_last_dup(CursorObject *self)
{
    return _cursor_get(self, MDB_LAST_DUP);
}

/**
 * Cursor.next() -> bool
 */
static PyObject *
cursor_next(CursorObject *self)
{
    return _cursor_get(self, MDB_NEXT);
}

/**
 * Cursor.next_dup() -> bool
 */
static PyObject *
cursor_next_dup(CursorObject *self)
{
    return _cursor_get(self, MDB_NEXT_DUP);
}

/**
 * Cursor.next_nodup() -> bool
 */
static PyObject *
cursor_next_nodup(CursorObject *self)
{
    return _cursor_get(self, MDB_NEXT_NODUP);
}

/**
 * Cursor.prev() -> bool
 */
static PyObject *
cursor_prev(CursorObject *self)
{
    return _cursor_get(self, MDB_PREV);
}

/**
 * Cursor.prev_dup() -> bool
 */
static PyObject *
cursor_prev_dup(CursorObject *self)
{
    return _cursor_get(self, MDB_PREV_DUP);
}

/**
 * Cursor.prev_nodup() -> bool
 */
static PyObject *
cursor_prev_nodup(CursorObject *self)
{
    return _cursor_get(self, MDB_PREV_NODUP);
}

/**
 * Cursor.putmulti(iter|dict) -> (consumed, added)
 */
static PyObject *
cursor_put_multi(CursorObject *self, PyObject *args, PyObject *kwds)
{
    struct cursor_put {
        PyObject *items;
        int dupdata;
        int overwrite;
        int append;
    } arg = {Py_None, 1, 1, 0};

    PyObject *iter;
    PyObject *item;

    static const struct argspec argspec[] = {
        {"items", ARG_OBJ, OFFSET(cursor_put, items)},
        {"dupdata", ARG_BOOL, OFFSET(cursor_put, dupdata)},
        {"overwrite", ARG_BOOL, OFFSET(cursor_put, overwrite)},
        {"append", ARG_BOOL, OFFSET(cursor_put, append)}
    };
    int flags;
    int rc;
    Py_ssize_t consumed;
    Py_ssize_t added;
    PyObject *ret = NULL;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }

    flags = 0;
    if(! arg.dupdata) {
        flags |= MDB_NODUPDATA;
    }
    if(! arg.overwrite) {
        flags |= MDB_NOOVERWRITE;
    }
    if(arg.append) {
        flags |= (self->trans->db->flags & MDB_DUPSORT) ? MDB_APPENDDUP : MDB_APPEND;
    }

    if(! ((iter = PyObject_GetIter(arg.items)))) {
        return NULL;
    }

    consumed = 0;
    added = 0;
    while((item = PyIter_Next(iter))) {
        MDB_val mkey, mval;
        if(! (PyTuple_CheckExact(item) && PyTuple_GET_SIZE(item) == 2)) {
            PyErr_SetString(PyExc_TypeError,
                            "putmulti() elements must be 2-tuples");
            Py_DECREF(item);
            Py_DECREF(iter);
            return NULL;
        }

        if(val_from_buffer(&mkey, PyTuple_GET_ITEM(item, 0)) ||
           val_from_buffer(&mval, PyTuple_GET_ITEM(item, 1))) {
            Py_DECREF(item);
            Py_DECREF(iter);
            return NULL; /* val_from_buffer sets exception */
        }

        UNLOCKED(rc, mdb_cursor_put(self->curs, &mkey, &mval, flags));
        self->trans->mutations++;
        switch(rc) {
        case MDB_SUCCESS:
            added++;
            break;
        case MDB_KEYEXIST:
            break;
        default:
            Py_DECREF(item);
            Py_DECREF(iter);
            return err_format(rc, "mdb_cursor_put() element #%d", consumed);
        }

        Py_DECREF(item);
        consumed++;
    }

    Py_DECREF(iter);
    if(! PyErr_Occurred()) {
        ret = Py_BuildValue("(nn)", consumed, added);
    }
    return ret;
}

/**
 * Cursor.put() -> bool
 */
static PyObject *
cursor_put(CursorObject *self, PyObject *args, PyObject *kwds)
{
    struct cursor_put {
        MDB_val key;
        MDB_val val;
        int dupdata;
        int overwrite;
        int append;
    } arg = {{0, 0}, {0, 0}, 1, 1, 0};

    static const struct argspec argspec[] = {
        {"key", ARG_BUF, OFFSET(cursor_put, key)},
        {"value", ARG_BUF, OFFSET(cursor_put, val)},
        {"dupdata", ARG_BOOL, OFFSET(cursor_put, dupdata)},
        {"overwrite", ARG_BOOL, OFFSET(cursor_put, overwrite)},
        {"append", ARG_BOOL, OFFSET(cursor_put, append)}
    };
    int flags;
    int rc;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }

    flags = 0;
    if(! arg.dupdata) {
        flags |= MDB_NODUPDATA;
    }
    if(! arg.overwrite) {
        flags |= MDB_NOOVERWRITE;
    }
    if(arg.append) {
        flags |= (self->trans->db->flags & MDB_DUPSORT) ? MDB_APPENDDUP : MDB_APPEND;
    }

    UNLOCKED(rc, mdb_cursor_put(self->curs, &arg.key, &arg.val, flags));
    self->trans->mutations++;
    if(rc) {
        if(rc == MDB_KEYEXIST) {
            Py_RETURN_FALSE;
        }
        return err_set("mdb_put", rc);
    }
    Py_RETURN_TRUE;
}

/**
 * Shared between Cursor.replace() and Transaction.replace()
 */
static PyObject *
do_cursor_replace(CursorObject *self, MDB_val *key, MDB_val *val)
{
    int rc = 0;
    PyObject *old;
    MDB_val newval = *val;

    if(self->dbi_flags & MDB_DUPSORT) {
        self->key = *key;
        if(_cursor_get_c(self, MDB_SET_KEY)) {
            return NULL;
        }
        if(self->positioned) {
            PRELOAD_UNLOCKED(rc, self->val.mv_data, self->val.mv_size);
            if(! ((old = obj_from_val(&self->val, 0)))) {
                return NULL;
            }
            UNLOCKED(rc, mdb_cursor_del(self->curs, MDB_NODUPDATA));
            self->trans->mutations++;
            if(rc) {
                Py_CLEAR(old);
                return err_set("mdb_cursor_del", rc);
            }
        } else {
            old = Py_None;
            Py_INCREF(old);
        }
    } else {
        /* val is updated if MDB_KEYEXIST. */
        int flags = MDB_NOOVERWRITE;
        UNLOCKED(rc, mdb_cursor_put(self->curs, key, val, flags));
        self->trans->mutations++;
        if(! rc) {
            Py_RETURN_NONE;
        } else if(rc != MDB_KEYEXIST) {
            return err_set("mdb_put", rc);
        }

        if(! ((old = obj_from_val(val, 0)))) {
            return NULL;
        }
    }

    UNLOCKED(rc, mdb_cursor_put(self->curs, key, &newval, 0));
    if(rc) {
        Py_DECREF(old);
        return err_set("mdb_put", rc);
    }
    return old;
}

/**
 * Cursor.replace() -> None|result
 */
static PyObject *
cursor_replace(CursorObject *self, PyObject *args, PyObject *kwds)
{
    struct cursor_replace {
        MDB_val key;
        MDB_val val;
    } arg = {{0, 0}, {0, 0}};

    static const struct argspec argspec[] = {
        {"key", ARG_BUF, OFFSET(cursor_replace, key)},
        {"value", ARG_BUF, OFFSET(cursor_replace, val)}
    };

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }

    return do_cursor_replace(self, &arg.key, &arg.val);
}

/**
 * Cursor.pop() -> None|result
 */
static PyObject *
cursor_pop(CursorObject *self, PyObject *args, PyObject *kwds)
{
    struct cursor_pop {
        MDB_val key;
    } arg = {{0, 0}};

    static const struct argspec argspec[] = {
        {"key", ARG_BUF, OFFSET(cursor_pop, key)},
    };
    PyObject *old;
    int rc = 0;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }

    self->key = arg.key;
    if(_cursor_get_c(self, MDB_SET_KEY)) {
        return NULL;
    }
    if(! self->positioned) {
        Py_RETURN_NONE;
    }
    PRELOAD_UNLOCKED(rc, self->val.mv_data, self->val.mv_size);
    if(! ((old = obj_from_val(&self->val, 0)))) {
        return NULL;
    }

    UNLOCKED(rc, mdb_cursor_del(self->curs, 0));
    self->trans->mutations++;
    if(rc) {
        Py_DECREF(old);
        return err_set("mdb_cursor_del", rc);
    }
    return old;
}

/**
 * Cursor.set_key(key) -> bool
 */
static PyObject *
cursor_set_key(CursorObject *self, PyObject *arg)
{
    if(! self->valid) {
        return err_invalid();
    }
    if(val_from_buffer(&self->key, arg)) {
        return NULL;
    }
    return _cursor_get(self, MDB_SET_KEY);
}

/**
 * Cursor.set_key_dup(key, value) -> bool
 */
static PyObject *
cursor_set_key_dup(CursorObject *self, PyObject *args, PyObject *kwds)
{
    struct cursor_set_key_dup {
        MDB_val key;
        MDB_val value;
    } arg = {{0, 0}, {0, 0}};

    static const struct argspec argspec[] = {
        {"key", ARG_BUF, OFFSET(cursor_set_key_dup, key)},
        {"value", ARG_BUF, OFFSET(cursor_set_key_dup, value)}
    };

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }
    self->key = arg.key;
    self->val = arg.value;
    return _cursor_get(self, MDB_GET_BOTH);
}

/**
 * Cursor.set_range(key) -> bool
 */
static PyObject *
cursor_set_range(CursorObject *self, PyObject *arg)
{
    if(! self->valid) {
        return err_invalid();
    }
    if(val_from_buffer(&self->key, arg)) {
        return NULL;
    }
    if(self->key.mv_size) {
        return _cursor_get(self, MDB_SET_RANGE);
    }
    return _cursor_get(self, MDB_FIRST);
}

/**
 * Cursor.set_range_dup(key, value) -> bool
 */
static PyObject *
cursor_set_range_dup(CursorObject *self, PyObject *args, PyObject *kwds)
{
    PyObject *ret;
    struct cursor_set_range_dup {
        MDB_val key;
        MDB_val value;
    } arg = {{0, 0}, {0, 0}};

    static const struct argspec argspec[] = {
        {"key", ARG_BUF, OFFSET(cursor_set_range_dup, key)},
        {"value", ARG_BUF, OFFSET(cursor_set_range_dup, value)}
    };

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }

    self->key = arg.key;
    self->val = arg.value;
    ret = _cursor_get(self, MDB_GET_BOTH_RANGE);

    /* issue #126: MDB_GET_BOTH_RANGE does not satisfy its documentation, and
     * fails to update `key` and `value` on success. Therefore explicitly call
     * MDB_GET_CURRENT after MDB_GET_BOTH_RANGE. */
    _cursor_get_c(self, MDB_GET_CURRENT);

    return ret;
}

/**
 * Cursor.value() -> result
 */
static PyObject *
cursor_value(CursorObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }
    /* Must refresh `key` and `val` following mutation. */
    if(self->last_mutation != self->trans->mutations &&
        _cursor_get_c(self, MDB_GET_CURRENT)) {
        return NULL;
    }
    PRELOAD_UNLOCKED(0, self->val.mv_data, self->val.mv_size);

    return obj_from_val(&self->val, self->trans->flags & TRANS_BUFFERS);
}

static PyObject *
new_iterator(CursorObject *cursor, IterValFunc val_func, MDB_cursor_op op)
{
    IterObject *iter = PyObject_New(IterObject, &PyIterator_Type);
    if (!iter) {
        return NULL;
    }

    iter->val_func = val_func;
    iter->curs = cursor;
    Py_INCREF(cursor);
    iter->started = 0;
    iter->op = op;

    DEBUG("new_iterator: %p", (void *)iter)
    return (PyObject *) iter;
}

static PyObject *
iter_from_args(CursorObject *self, PyObject *args, PyObject *kwds,
               signed int pos_op, enum MDB_cursor_op op,
               int keys_default, int values_default)
{
    struct iter_from_args {
        int keys;
        int values;
    } arg = {keys_default, values_default};

    static const struct argspec argspec[] = {
        {"keys", ARG_BOOL, OFFSET(iter_from_args, keys)},
        {"values", ARG_BOOL, OFFSET(iter_from_args, values)}
    };
    void *val_func;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }

    if(pos_op != -1 && !self->positioned) {
        if(_cursor_get_c(self, (enum MDB_cursor_op) pos_op)) {
            return NULL;
        }
    }

    if(! arg.values) {
        val_func = cursor_key;
    } else if(! arg.keys) {
        val_func = cursor_value;
    } else {
        val_func = cursor_item;
    }
    return new_iterator(self, val_func, op);
}

static PyObject *
cursor_iter(CursorObject *self)
{
    return iter_from_args(self, NULL, NULL, MDB_FIRST, MDB_NEXT, 1, 1);
}

/**
 * Cursor.iternext() -> Iterator
 */
static PyObject *
cursor_iternext(CursorObject *self, PyObject *args, PyObject *kwargs)
{
    return iter_from_args(self, args, kwargs, MDB_FIRST, MDB_NEXT, 1, 1);
}

/**
 * Cursor.iternext_dup() -> Iterator
 */
static PyObject *
cursor_iternext_dup(CursorObject *self, PyObject *args, PyObject *kwargs)
{
    return iter_from_args(self, args, kwargs, -1, MDB_NEXT_DUP, 0, 1);
}

/**
 * Cursor.iternext_nodup() -> Iterator
 */
static PyObject *
cursor_iternext_nodup(CursorObject *self, PyObject *args, PyObject *kwargs)
{
    return iter_from_args(self, args, kwargs, MDB_FIRST, MDB_NEXT_NODUP, 1, 0);
}

/**
 * Cursor.iterprev() -> Iterator
 */
static PyObject *
cursor_iterprev(CursorObject *self, PyObject *args, PyObject *kwargs)
{
    return iter_from_args(self, args, kwargs, MDB_LAST, MDB_PREV, 1, 1);
}

/**
 * Cursor.iterprev_dup() -> Iterator
 */
static PyObject *
cursor_iterprev_dup(CursorObject *self, PyObject *args, PyObject *kwargs)
{
    return iter_from_args(self, args, kwargs, -1, MDB_PREV_DUP, 0, 1);
}

/**
 * Cursor.iterprev_nodup() -> Iterator
 */
static PyObject *
cursor_iterprev_nodup(CursorObject *self, PyObject *args, PyObject *kwargs)
{
    return iter_from_args(self, args, kwargs, MDB_LAST, MDB_PREV_NODUP, 1, 0);
}

/**
 * Cursor._iter_from() -> Iterator
 */
static PyObject *
cursor_iter_from(CursorObject *self, PyObject *args)
{
    struct cursor_iter_from {
        MDB_val key;
        int reverse;
    } arg = {{0, 0}, 0};

    static const struct argspec argspec[] = {
        {"key", ARG_BUF, OFFSET(cursor_iter_from, key)},
        {"reverse", ARG_BOOL, OFFSET(cursor_iter_from, reverse)}
    };
    enum MDB_cursor_op op;
    int rc;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, NULL, &arg)) {
        return NULL;
    }

    if((! arg.key.mv_size) && (! arg.reverse)) {
        rc = _cursor_get_c(self, MDB_FIRST);
    } else {
        self->key = arg.key;
        rc = _cursor_get_c(self, MDB_SET_RANGE);
    }

    if(rc) {
        return NULL;
    }

    op = MDB_NEXT;
    if(arg.reverse) {
        op = MDB_PREV;
        if(! self->positioned) {
            if(_cursor_get_c(self, MDB_LAST)) {
                return NULL;
            }
        }
    }

    DEBUG("positioned? %d", self->positioned)
    return new_iterator(self, (void *)cursor_item, op);
}

/**
 * Cursor.__enter__()
 */
static PyObject *cursor_enter(CursorObject *self)
{
    Py_INCREF(self);
    return (PyObject *)self;
}

/**
 * Cursor.__exit__()
 */
static PyObject *cursor_exit(CursorObject *self, PyObject *args)
{
    cursor_clear(self);
    Py_RETURN_NONE;
}

/**
 * Cursor.close()
 */
static PyObject *cursor_close(CursorObject *self)
{
    cursor_clear(self);
    Py_RETURN_NONE;
}

static struct PyMethodDef cursor_methods[] = {
    {"__enter__", (PyCFunction)cursor_enter, METH_NOARGS},
    {"__exit__", (PyCFunction)cursor_exit, METH_VARARGS},
    {"close", (PyCFunction)cursor_close, METH_NOARGS},
    {"count", (PyCFunction)cursor_count, METH_NOARGS},
    {"delete", (PyCFunction)cursor_delete, METH_VARARGS|METH_KEYWORDS},
    {"first", (PyCFunction)cursor_first, METH_NOARGS},
    {"first_dup", (PyCFunction)cursor_first_dup, METH_NOARGS},
    {"get", (PyCFunction)cursor_get, METH_VARARGS|METH_KEYWORDS},
    {"getmulti", (PyCFunction)cursor_get_multi, METH_VARARGS|METH_KEYWORDS},
    {"item", (PyCFunction)cursor_item, METH_NOARGS},
    {"iternext", (PyCFunction)cursor_iternext, METH_VARARGS|METH_KEYWORDS},
    {"iternext_dup", (PyCFunction)cursor_iternext_dup, METH_VARARGS|METH_KEYWORDS},
    {"iternext_nodup", (PyCFunction)cursor_iternext_nodup, METH_VARARGS|METH_KEYWORDS},
    {"iterprev", (PyCFunction)cursor_iterprev, METH_VARARGS|METH_KEYWORDS},
    {"iterprev_dup", (PyCFunction)cursor_iterprev_dup, METH_VARARGS|METH_KEYWORDS},
    {"iterprev_nodup", (PyCFunction)cursor_iterprev_nodup, METH_VARARGS|METH_KEYWORDS},
    {"key", (PyCFunction)cursor_key, METH_NOARGS},
    {"last", (PyCFunction)cursor_last, METH_NOARGS},
    {"last_dup", (PyCFunction)cursor_last_dup, METH_NOARGS},
    {"next", (PyCFunction)cursor_next, METH_NOARGS},
    {"next_dup", (PyCFunction)cursor_next_dup, METH_NOARGS},
    {"next_nodup", (PyCFunction)cursor_next_nodup, METH_NOARGS},
    {"prev", (PyCFunction)cursor_prev, METH_NOARGS},
    {"prev_dup", (PyCFunction)cursor_prev_dup, METH_NOARGS},
    {"prev_nodup", (PyCFunction)cursor_prev_nodup, METH_NOARGS},
    {"put", (PyCFunction)cursor_put, METH_VARARGS|METH_KEYWORDS},
    {"putmulti", (PyCFunction)cursor_put_multi, METH_VARARGS|METH_KEYWORDS},
    {"replace", (PyCFunction)cursor_replace, METH_VARARGS|METH_KEYWORDS},
    {"pop", (PyCFunction)cursor_pop, METH_VARARGS|METH_KEYWORDS},
    {"set_key", (PyCFunction)cursor_set_key, METH_O},
    {"set_key_dup", (PyCFunction)cursor_set_key_dup, METH_VARARGS|METH_KEYWORDS},
    {"set_range", (PyCFunction)cursor_set_range, METH_O},
    {"set_range_dup", (PyCFunction)cursor_set_range_dup, METH_VARARGS|METH_KEYWORDS},
    {"value", (PyCFunction)cursor_value, METH_NOARGS},
    {"_iter_from", (PyCFunction)cursor_iter_from, METH_VARARGS},
    {NULL, NULL}
};

static PyTypeObject PyCursor_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "Cursor",                   /*tp_name*/
    sizeof(CursorObject),       /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    (destructor) cursor_dealloc,/*tp_dealloc*/
    0,                          /*tp_print*/
    0,                          /*tp_getattr*/
    0,                          /*tp_setattr*/
    0,                          /*tp_compare*/
    0,                          /*tp_repr*/
    0,                          /*tp_as_number*/
    0,                          /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                          /*tp_call*/
    0,                          /*tp_str*/
    0,                          /*tp_getattro*/
    0,                          /*tp_setattro*/
    0,                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,         /*tp_flags*/
    0,                          /*tp_doc*/
    0,                          /*tp_traverse*/
    (inquiry) cursor_clear,     /*tp_clear*/
    0,                          /*tp_richcompare*/
    0,                          /*tp_weaklistoffset*/
    (getiterfunc)cursor_iter,   /*tp_iter*/
    0,                          /*tp_iternext*/
    cursor_methods,             /*tp_methods*/
    0,                          /*tp_members*/
    0,                          /*tp_getset*/
    0,                          /*tp_base*/
    0,                          /*tp_dict*/
    0,                          /*tp_descr_get*/
    0,                          /*tp_descr_set*/
    0,                          /*tp_dictoffset*/
    0,                          /*tp_init*/
    0,                          /*tp_alloc*/
    cursor_new,                 /*tp_new*/
    0,                          /*tp_free*/
};


/* --------- */
/* Iterators */
/* --------- */


/**
 * Iterator.__del__()
 */
static void
iter_dealloc(IterObject *self)
{
    DEBUG("destroying iterator")
    Py_CLEAR(self->curs);
    PyObject_Del(self);
}

/**
 * Iterator.__iter__() -> self
 */
static PyObject *
iter_iter(IterObject *self)
{
    Py_INCREF(self);
    return (PyObject *)self;
}

/**
 * Iterator.next() -> result
 */
static PyObject *
iter_next(IterObject *self)
{
    if(! self->curs->valid) {
        return err_invalid();
    }
    if(! self->curs->positioned) {
        return NULL;
    }

    if(self->started) {
        if(_cursor_get_c(self->curs, self->op)) {
            return NULL;
        }
        if(! self->curs->positioned) {
            return NULL;
        }
    }

    self->started = 1;
    return self->val_func(self->curs);
}

static PyTypeObject PyIterator_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_Iterator",                 /*tp_name*/
    sizeof(IterObject),         /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    (destructor) iter_dealloc,  /*tp_dealloc*/
    0,                          /*tp_print*/
    0,                          /*tp_getattr*/
    0,                          /*tp_setattr*/
    0,                          /*tp_compare*/
    0,                          /*tp_repr*/
    0,                          /*tp_as_number*/
    0,                          /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                          /*tp_call*/
    0,                          /*tp_str*/
    0,                          /*tp_getattro*/
    0,                          /*tp_setattro*/
    0,                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,         /*tp_flags*/
    0,                          /*tp_doc*/
    0,                          /*tp_traverse*/
    0,                          /*tp_clear*/
    0,                          /*tp_richcompare*/
    0,                          /*tp_weaklistoffset*/
    (getiterfunc)iter_iter,     /*tp_iter*/
    (iternextfunc)iter_next,    /*tp_iternext*/
    0,                          /*tp_methods*/
    0,                          /*tp_members*/
    0,                          /*tp_getset*/
    0,                          /*tp_base*/
    0,                          /*tp_dict*/
    0,                          /*tp_descr_get*/
    0,                          /*tp_descr_set*/
    0,                          /*tp_dictoffset*/
    0,                          /*tp_init*/
    0,                          /*tp_alloc*/
    0,                          /*tp_new*/
    0,                          /*tp_free*/
};



/* ------------ */
/* Transactions */
/* ------------ */

static void
txn_abort(MDB_txn *self)
{
    Py_BEGIN_ALLOW_THREADS
    MDEBUG("aborting")
    mdb_txn_abort(self);
    Py_END_ALLOW_THREADS
}


static int
trans_clear(TransObject *self)
{
    MDEBUG("clearing trans")
    INVALIDATE(self)
#ifdef HAVE_MEMSINK
    ms_notify((PyObject *) self, &self->sink_head);
#endif

    if(self->txn) {
        txn_abort(self->txn);
        self->txn = NULL;
    }
    MDEBUG("db is/was %p", self->db)
    Py_CLEAR(self->db);
    self->valid = 0;
    if(self->env) {
        UNLINK_CHILD(self->env, self)
        Py_CLEAR(self->env);
    }
    return 0;
}

/**
 * Transaction.__del__()
 */
static void
trans_dealloc(TransObject *self)
{
    MDB_txn * txn = self->txn;
    if(self->weaklist != NULL) {
        MDEBUG("Clearing weaklist..")
        PyObject_ClearWeakRefs((PyObject *) self);
    }

    if(txn && self->env && !self->env->spare_txn &&
      (self->flags & TRANS_RDONLY)) {
        MDEBUG("caching trans")
        mdb_txn_reset(txn);
        self->env->spare_txn = txn;
        self->txn = NULL;
    }

    MDEBUG("deleting trans")
    trans_clear(self);
    PyObject_Del(self);
}

/**
 * Transaction(env, db) -> new instance.
 */
static PyObject *
trans_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    struct trans_new {
        EnvObject *env;
        DbObject *db;
        TransObject *parent;
        int write;
        int buffers;
    } arg = {NULL, NULL, NULL, 0, 0};

    static const struct argspec argspec[] = {
        {"env", ARG_ENV, OFFSET(trans_new, env)},
        {"db", ARG_DB, OFFSET(trans_new, db)},
        {"parent", ARG_TRANS, OFFSET(trans_new, parent)},
        {"write", ARG_BOOL, OFFSET(trans_new, write)},
        {"buffers", ARG_BOOL, OFFSET(trans_new, buffers)}
    };

    static PyObject *cache = NULL;
    if(parse_args(1, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }
    if(! arg.env) {
        return type_error("'env' argument required");
    }
    return make_trans(arg.env, arg.db, arg.parent, arg.write, arg.buffers);
}

/**
 * Transaction.abort()
 */
static PyObject *
trans_abort(TransObject *self)
{
    if(self->valid) {
        DEBUG("invalidate")
        INVALIDATE(self)
#ifdef HAVE_MEMSINK
        ms_notify((PyObject *) self, &self->sink_head);
#endif
        if(self->flags & TRANS_RDONLY) {
            DEBUG("resetting")
            /* Reset to spare state, ready for _dealloc to freelist it. */
            mdb_txn_reset(self->txn);
            self->flags |= TRANS_SPARE;
        } else {
            DEBUG("aborting")
            Py_BEGIN_ALLOW_THREADS
            mdb_txn_abort(self->txn);
            Py_END_ALLOW_THREADS
            self->txn = NULL;
        }
        self->valid = 0;
    }
    Py_RETURN_NONE;
}

/**
 * Transaction.commit()
 */
static PyObject *
trans_commit(TransObject *self)
{
    int rc;

    if(! self->valid) {
        return err_invalid();
    }
    DEBUG("invalidate")
    INVALIDATE(self)
#ifdef HAVE_MEMSINK
    ms_notify((PyObject *) self, &self->sink_head);
#endif
    if(self->flags & TRANS_RDONLY) {
        DEBUG("resetting")
        /* Reset to spare state, ready for _dealloc to freelist it. */
        mdb_txn_reset(self->txn);
        self->flags |= TRANS_SPARE;
    } else {
        DEBUG("committing")
        UNLOCKED(rc, mdb_txn_commit(self->txn));
        self->txn = NULL;
        if(rc) {
            return err_set("mdb_txn_commit", rc);
        }
    }
    self->valid = 0;
    Py_RETURN_NONE;
}

/**
 * Transaction.cursor() -> Cursor
 */
static PyObject *
trans_cursor(TransObject *self, PyObject *args, PyObject *kwds)
{
    struct trans_cursor {
        DbObject *db;
    } arg = {self->db};

    static const struct argspec argspec[] = {
        {"db", ARG_DB, OFFSET(trans_cursor, db)}
    };

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }
    return make_cursor(arg.db, self);
}

/**
 * Transaction.delete() -> bool
 */
static PyObject *
trans_delete(TransObject *self, PyObject *args, PyObject *kwds)
{
    struct trans_delete {
        MDB_val key;
        MDB_val val;
        DbObject *db;
    } arg = {{0, 0}, {0, 0}, self->db};

    static const struct argspec argspec[] = {
        {"key", ARG_BUF, OFFSET(trans_delete, key)},
        {"value", ARG_BUF, OFFSET(trans_delete, val)},
        {"db", ARG_DB, OFFSET(trans_delete, db)}
    };
    MDB_val *val_ptr;
    int rc;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }
    if(! db_owner_check(arg.db, self->env)) {
        return NULL;
    }
    val_ptr = arg.val.mv_size ? &arg.val : NULL;
    self->mutations++;
    UNLOCKED(rc, mdb_del(self->txn, arg.db->dbi, &arg.key, val_ptr));
    if(rc) {
        if(rc == MDB_NOTFOUND) {
             Py_RETURN_FALSE;
        }
        return err_set("mdb_del", rc);
    }
    Py_RETURN_TRUE;
}

/**
 * Transaction.drop(db)
 */
static PyObject *
trans_drop(TransObject *self, PyObject *args, PyObject *kwds)
{
    struct trans_drop {
        DbObject *db;
        int delete;
    } arg = {NULL, 1};

    static const struct argspec argspec[] = {
        {"db", ARG_DB, OFFSET(trans_drop, db)},
        {"delete", ARG_BOOL, OFFSET(trans_drop, delete)}
    };
    int rc;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }
    if(! arg.db) {
        return type_error("'db' argument required.");
    } else if(! db_owner_check(arg.db, self->env)) {
        return NULL;
    }

    UNLOCKED(rc, mdb_drop(self->txn, arg.db->dbi, arg.delete));
    self->mutations++;
    if(rc) {
        return err_set("mdb_drop", rc);
    }
    Py_RETURN_NONE;
}

/**
 * Transaction.get() -> result
 */
static PyObject *
trans_get(TransObject *self, PyObject *args, PyObject *kwds)
{
    struct trans_get {
        MDB_val key;
        PyObject *default_;
        DbObject *db;
    } arg = {{0, 0}, Py_None, self->db};

    static const struct argspec argspec[] = {
        {"key", ARG_BUF, OFFSET(trans_get, key)},
        {"default", ARG_OBJ, OFFSET(trans_get, default_)},
        {"db", ARG_DB, OFFSET(trans_get, db)}
    };
    MDB_val val;
    int rc;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }
    if(! db_owner_check(arg.db, self->env)) {
        return NULL;
    }

    if(! arg.key.mv_data) {
        return type_error("key must be given.");
    }

    Py_BEGIN_ALLOW_THREADS
    rc = mdb_get(self->txn, arg.db->dbi, &arg.key, &val);
    preload(rc, val.mv_data, val.mv_size);
    Py_END_ALLOW_THREADS

    if(rc) {
        if(rc == MDB_NOTFOUND) {
            Py_INCREF(arg.default_);
            return arg.default_;
        }
        return err_set("mdb_get", rc);
    }
    return obj_from_val(&val, self->flags & TRANS_BUFFERS);
}

/**
 * Transaction.put() -> bool
 */
static PyObject *
trans_put(TransObject *self, PyObject *args, PyObject *kwds)
{
    struct trans_put {
        MDB_val key;
        MDB_val value;
        int dupdata;
        int overwrite;
        int append;
        DbObject *db;
    } arg = {{0, 0}, {0, 0}, 1, 1, 0, self->db};

    static const struct argspec argspec[] = {
        {"key", ARG_BUF, OFFSET(trans_put, key)},
        {"value", ARG_BUF, OFFSET(trans_put, value)},
        {"dupdata", ARG_BOOL, OFFSET(trans_put, dupdata)},
        {"overwrite", ARG_BOOL, OFFSET(trans_put, overwrite)},
        {"append", ARG_BOOL, OFFSET(trans_put, append)},
        {"db", ARG_DB, OFFSET(trans_put, db)}
    };
    int flags;
    int rc;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }
    if(! db_owner_check(arg.db, self->env)) {
        return NULL;
    }

    flags = 0;
    if(! arg.dupdata) {
        flags |= MDB_NODUPDATA;
    }
    if(! arg.overwrite) {
        flags |= MDB_NOOVERWRITE;
    }
    if(arg.append) {
        flags |= MDB_APPEND;
    }

    DEBUG("inserting '%.*s' (%d) -> '%.*s' (%d)",
        (int)arg.key.mv_size, (char *)arg.key.mv_data,
        (int)arg.key.mv_size,
        (int)arg.value.mv_size, (char *)arg.value.mv_data,
        (int)arg.value.mv_size)

    self->mutations++;
    UNLOCKED(rc, mdb_put(self->txn, (arg.db)->dbi,
                         &arg.key, &arg.value, flags));
    if(rc) {
        if(rc == MDB_KEYEXIST) {
            Py_RETURN_FALSE;
        }
        return err_set("mdb_put", rc);
    }
    Py_RETURN_TRUE;
}

static PyObject *
make_cursor(DbObject *db, TransObject *trans);
static PyObject *
do_cursor_replace(CursorObject *self, MDB_val *key, MDB_val *val);

/**
 * Transaction.replace() -> None|result
 */
static PyObject *
trans_replace(TransObject *self, PyObject *args, PyObject *kwds)
{
    struct trans_replace {
        MDB_val key;
        MDB_val value;
        DbObject *db;
    } arg = {{0, 0}, {0, 0}, self->db};

    static const struct argspec argspec[] = {
        {"key", ARG_BUF, OFFSET(trans_replace, key)},
        {"value", ARG_BUF, OFFSET(trans_replace, value)},
        {"db", ARG_DB, OFFSET(trans_replace, db)}
    };
    PyObject *ret;
    CursorObject *cursor;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }
    if(! db_owner_check(arg.db, self->env)) {
        return NULL;
    }

    ret = NULL;
    cursor = (CursorObject *) make_cursor(arg.db, self);
    if(cursor) {
        ret = do_cursor_replace(cursor, &arg.key, &arg.value);
        Py_DECREF(cursor);
    }
    return ret;
}

static int
_cursor_get_c(CursorObject *self, enum MDB_cursor_op op);

/**
 * Transaction.pop() -> None|result
 */
static PyObject *
trans_pop(TransObject *self, PyObject *args, PyObject *kwds)
{
    struct trans_pop {
        MDB_val key;
        DbObject *db;
    } arg = {{0, 0}, self->db};

    static const struct argspec argspec[] = {
        {"key", ARG_BUF, OFFSET(trans_pop, key)},
        {"db", ARG_DB, OFFSET(trans_pop, db)}
    };
    CursorObject *cursor;
    PyObject *old;
    int rc = 0;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }
    if(! db_owner_check(arg.db, self->env)) {
        return NULL;
    }

    if(! ((cursor = (CursorObject *) make_cursor(arg.db, self)))) {
        return NULL;
    }

    cursor->key = arg.key;
    if(_cursor_get_c(cursor, MDB_SET_KEY)) {
        Py_DECREF((PyObject *)cursor);
        return NULL;
    }
    if(! cursor->positioned) {
        Py_DECREF((PyObject *)cursor);
        Py_RETURN_NONE;
    }

    PRELOAD_UNLOCKED(rc, cursor->val.mv_data, cursor->val.mv_size);
    if(! ((old = obj_from_val(&cursor->val, 0)))) {
        Py_DECREF((PyObject *)cursor);
        return NULL;
    }

    UNLOCKED(rc, mdb_cursor_del(cursor->curs, 0));
    Py_DECREF((PyObject *)cursor);
    self->mutations++;
    if(rc) {
        Py_DECREF(old);
        return err_set("mdb_cursor_del", rc);
    }
    return old;
}

/**
 * Transaction.__enter__()
 */
static PyObject *trans_enter(TransObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }
    Py_INCREF(self);
    return (PyObject *)self;
}

/**
 * Transaction.__exit__()
 */
static PyObject *trans_exit(TransObject *self, PyObject *args)
{
    if(! self->valid) {
        return err_invalid();
    }
    if(PyTuple_GET_ITEM(args, 0) == Py_None) {
        return trans_commit(self);
    } else {
        return trans_abort(self);
    }
}

/**
 * Transaction.id() -> int
 */
static PyObject *trans_id(TransObject *self)
{
    size_t id;

    if(! self->valid) {
        return err_invalid();
    }

    id = mdb_txn_id(self->txn);
    return PyLong_FromUnsignedLong(id);
}

/**
 * Transaction.stat() -> dict
 */
static PyObject *
trans_stat(TransObject *self, PyObject *args, PyObject *kwds)
{
    struct trans_stat {
        DbObject *db;
    } arg = {self->db};

    static const struct argspec argspec[] = {
        {"db", ARG_DB, OFFSET(trans_stat, db)}
    };
    MDB_stat st;
    int rc;

    static PyObject *cache = NULL;
    if(parse_args(self->valid, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }
    if(! db_owner_check(arg.db, self->env)) {
        return NULL;
    }

    UNLOCKED(rc, mdb_stat(self->txn, arg.db->dbi, &st));
    if(rc) {
        return err_set("mdb_stat", rc);
    }
    return dict_from_fields(&st, mdb_stat_fields);
}

static struct PyMethodDef trans_methods[] = {
    {"__enter__", (PyCFunction)trans_enter, METH_NOARGS},
    {"__exit__", (PyCFunction)trans_exit, METH_VARARGS},
    {"abort", (PyCFunction)trans_abort, METH_NOARGS},
    {"commit", (PyCFunction)trans_commit, METH_NOARGS},
    {"cursor", (PyCFunction)trans_cursor, METH_VARARGS|METH_KEYWORDS},
    {"delete", (PyCFunction)trans_delete, METH_VARARGS|METH_KEYWORDS},
    {"drop", (PyCFunction)trans_drop, METH_VARARGS|METH_KEYWORDS},
    {"get", (PyCFunction)trans_get, METH_VARARGS|METH_KEYWORDS},
    {"put", (PyCFunction)trans_put, METH_VARARGS|METH_KEYWORDS},
    {"replace", (PyCFunction)trans_replace, METH_VARARGS|METH_KEYWORDS},
    {"pop", (PyCFunction)trans_pop, METH_VARARGS|METH_KEYWORDS},
    {"id", (PyCFunction)trans_id, METH_NOARGS},
    {"stat", (PyCFunction)trans_stat, METH_VARARGS|METH_KEYWORDS},
    {NULL, NULL}
};

static PyTypeObject PyTransaction_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "Transaction",              /*tp_name*/
    sizeof(TransObject),        /*tp_basicsize*/
    0,                          /*tp_itemsize*/
    (destructor) trans_dealloc, /*tp_dealloc*/
    0,                          /*tp_print*/
    0,                          /*tp_getattr*/
    0,                          /*tp_setattr*/
    0,                          /*tp_compare*/
    0,                          /*tp_repr*/
    0,                          /*tp_as_number*/
    0,                          /*tp_as_sequence*/
    0,                          /*tp_as_mapping*/
    0,                          /*tp_hash*/
    0,                          /*tp_call*/
    0,                          /*tp_str*/
    0,                          /*tp_getattro*/
    0,                          /*tp_setattro*/
    0,                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,         /*tp_flags*/
    0,                          /*tp_doc*/
    0,                          /*tp_traverse*/
    (inquiry) trans_clear,      /*tp_clear*/
    0,                          /*tp_richcompare*/
    offsetof(TransObject, weaklist), /*tp_weaklistoffset*/
    0,                          /*tp_iter*/
    0,                          /*tp_iternext*/
    trans_methods,              /*tp_methods*/
    0,                          /*tp_members*/
    0,                          /*tp_getset*/
    0,                          /*tp_base*/
    0,                          /*tp_dict*/
    0,                          /*tp_descr_get*/
    0,                          /*tp_descr_set*/
    0,                          /*tp_dictoffset*/
    0,                          /*tp_init*/
    0,                          /*tp_alloc*/
    trans_new,                  /*tp_new*/
    0,                          /*tp_free*/
};


/**
 * Construct a PyString and append it to a list, returning 0 on success or -1
 * on error.
 */
static int
append_string(PyObject *list, const char *s)
{
#if PY_MAJOR_VERSION >= 3
    PyObject *o = PyUnicode_FromString(s);
#else
    PyObject *o = PyBytes_FromStringAndSize(s, strlen(s));
#endif

    if(! o) {
        return -1;
    }
    if(PyList_Append(list, o)) {
        Py_DECREF(o);
        return -1;
    }
    Py_DECREF(o);
    return 0;
}


/**
 * lmdb.enable_drop_gil()
 */
static PyObject *
enable_drop_gil(void)
{
    Py_RETURN_NONE;
}

/**
 * lmdb.get_version() -> tuple
 */
static PyObject *
get_version(PyObject *mod, PyObject *args, PyObject *kwds)
{
    struct version_args {
        int subpatch;
    } arg = {0};

    static const struct argspec argspec[] = {
        {"subpatch", ARG_BOOL, OFFSET(version_args, subpatch)},
    };

    static PyObject *cache = NULL;
    if(parse_args(1, SPECSIZE(), argspec, &cache, args, kwds, &arg)) {
        return NULL;
    }

    if (arg.subpatch) {
#ifdef HAVE_PATCHED_LMDB
        const int subpatch = 1;
#else
        const int subpatch = 0;
#endif
        return Py_BuildValue("iiii", MDB_VERSION_MAJOR,
            MDB_VERSION_MINOR, MDB_VERSION_PATCH, subpatch);
    }
    return Py_BuildValue("iii", MDB_VERSION_MAJOR,
        MDB_VERSION_MINOR, MDB_VERSION_PATCH);
}

static struct PyMethodDef module_methods[] = {
    {"enable_drop_gil", (PyCFunction) enable_drop_gil, METH_NOARGS, ""},
    {"version", (PyCFunction) get_version, METH_VARARGS|METH_KEYWORDS, ""},
    {0, 0, 0, 0}
};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "cpython",
    NULL,
    -1,
    module_methods,
    NULL,
    NULL,
    NULL,
    NULL
};
#endif

/**
 * Initialize and publish the LMDB built-in types.
 */
static int init_types(PyObject *mod, PyObject *__all__)
{
    static PyTypeObject *types[] = {
        &PyEnvironment_Type,
        &PyCursor_Type,
        &PyTransaction_Type,
        &PyIterator_Type,
        &PyDatabase_Type,
        NULL
    };

    int i;
    for(i = 0; types[i]; i++) {
        PyTypeObject *type = types[i];
        char const * name = type->tp_name;

        if(PyType_Ready(type)) {
            return -1;
        }
        if(PyObject_SetAttrString(mod, name, (PyObject *)type)) {
            return -1;
        }

        /* As a special case, export _Database */
        if((name[0] != '_' || 0 == strcmp(name, "_Database")) &&
           append_string(__all__, name)) {
            return -1;
        }
    }

    return 0;
}

/**
 * Initialize a bunch of constants used to ease number compares.
 */
static int init_constants(PyObject *mod)
{
    if(! ((py_zero = PyLong_FromUnsignedLongLong(0)))) {
        return -1;
    }
    if(! ((py_int_max = PyLong_FromUnsignedLongLong(INT_MAX)))) {
        return -1;
    }
    if(! ((py_size_max = PyLong_FromUnsignedLongLong(SIZE_MAX)))) {
        return -1;
    }
    return 0;
}

/**
 * Create lmdb.Error exception class, and one subclass for each entry in
 * `error_map`.
 */
static int init_errors(PyObject *mod, PyObject *__all__)
{
    size_t count;
    char qualname[64];
    size_t i;

    Error = PyErr_NewException("lmdb.Error", NULL, NULL);
    if(! Error) {
        return -1;
    }
    if(PyObject_SetAttrString(mod, "Error", Error)) {
        return -1;
    }
    if(append_string(__all__, "Error")) {
        return -1;
    }

    count = (sizeof error_map / sizeof error_map[0]);
    error_tbl = malloc(sizeof(PyObject *) * count);
    if(! error_tbl) {
        return -1;
    }

    for(i = 0; i < count; i++) {
        const struct error_map *error = &error_map[i];
        PyObject *klass;

        snprintf(qualname, sizeof qualname, "lmdb.%s", error->name);
        qualname[sizeof qualname - 1] = '\0';

        if(! ((klass = PyErr_NewException(qualname, Error, NULL)))) {
            return -1;
        }

        error_tbl[i] = klass;
        if(PyObject_SetAttrString(mod, error->name, klass)) {
            return -1;
        }
        if(append_string(__all__, error->name)) {
            return -1;
        }
    }
    return 0;
}

/**
 * Do all required to initialize the lmdb.cpython module.
 */
PyMODINIT_FUNC
MODINIT_NAME(void)
{
    PyObject *__all__;
#if PY_MAJOR_VERSION >= 3
    PyObject *mod = PyModule_Create(&moduledef);
#else
    PyObject *mod = Py_InitModule3("cpython", module_methods, "");
#endif
    if(! mod) {
        MOD_RETURN(NULL);
    }

    if(! ((__all__ = PyList_New(0)))) {
        MOD_RETURN(NULL);
    }

    if(init_types(mod, __all__)) {
        MOD_RETURN(NULL);
    }
    if(append_string(__all__, "enable_drop_gil")) {
        MOD_RETURN(NULL);
    }
    if(append_string(__all__, "version")) {
        MOD_RETURN(NULL);
    }

#ifdef HAVE_MEMSINK
    MemSink_IMPORT;
    if(ms_init_source(&PyTransaction_Type, offsetof(TransObject, sink_head))) {
        MOD_RETURN(NULL);
    }
#endif

#ifdef _WIN32
    if(! ((msvcrt = PyImport_ImportModule("msvcrt")))) {
        MOD_RETURN(NULL);
    }
#endif

    if(init_constants(mod)) {
        MOD_RETURN(NULL);
    }
    if(init_errors(mod, __all__)) {
        MOD_RETURN(NULL);
    }
    if(PyObject_SetAttrString(mod, "open", (PyObject *)&PyEnvironment_Type)) {
        MOD_RETURN(NULL);
    }

    if(PyObject_SetAttrString(mod, "__all__", __all__)) {
        MOD_RETURN(NULL);
    }
    Py_DECREF(__all__);

    MOD_RETURN(mod);
}
