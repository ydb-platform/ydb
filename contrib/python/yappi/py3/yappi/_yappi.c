/*

 yappi
 Yet Another Python Profiler

 Sumer Cip 2019

*/

#include "config.h"

#if !defined(HAVE_LONG_LONG)
#error "Yappi requires long longs!"
#endif

#if PY_VERSION_HEX >= 0x030B0000  // Python 3.11+
  #ifndef Py_BUILD_CORE
    #define Py_BUILD_CORE
  #endif
  #include "internal/pycore_genobject.h"
  #include "internal/pycore_frame.h"
  #include "opcode.h"
#endif

#include "bytesobject.h"
#include "frameobject.h"
#include "callstack.h"
#include "hashtab.h"
#include "debug.h"
#include "timing.h"
#include "freelist.h"
#include "mem.h"
#include "tls.h"

#define SUPPRESS_WARNING(a) (void)a

PyDoc_STRVAR(_yappi__doc__, "Yet Another Python Profiler");

// linked list for holding callee/caller info in the pit
// we need to record the timing data on the pairs (parent, child)
typedef struct {
    unsigned int index;
    unsigned long callcount;
    unsigned long nonrecursive_callcount;   // how many times the child function is called non-recursively?
    long long tsubtotal;                    // the time that the child function spent excluding its children (include recursive parent-child calls)
    long long ttotal;                       // the total time that the child function spent
    struct _pit_children_info *next;
} _pit_children_info;

typedef struct {
    // we currently use frame pointer for identifying the running coroutine on
    // a specific _pit.
    PyFrameObject *frame;

    // the first time this coroutine is seen on stack
    long long t0;

    struct _coro *next;
} _coro;

// module definitions
typedef struct {
    PyObject *name;
    PyObject *modname;
    unsigned long lineno;
    unsigned long callcount;

    // the number of actual calls when the function is recursive.
    unsigned long nonrecursive_callcount;

    // time function spent excluding its children (include recursive calls)
    long long tsubtotal;

    // the total time that a function spent
    long long ttotal;
    unsigned int builtin;

    // a number that uniquely identifies the _pit during the lifetime of a profile
    // session (for multiple start/stop pairs) We could use PyCodeObject for this
    // too, but then there are builtin calls, it is better to have a custom id
    // per-pit instead of dealing with the keys and their references.
    unsigned int index;

    // concurrent running coroutines on this _pit
    _coro *coroutines;

    // TODO: Comment
    PyObject *fn_descriptor;

    _pit_children_info *children;
} _pit; // profile_item

typedef struct {
    int paused;
    long long paused_at;
} _glstate;

typedef struct {
    _cstack *cs;
    _htab *rec_levels;

    // mapping tag:htab of pits
    _htab *tags;

    // internal tid given by user callback or yappi. Will be unique per profile session.
    uintptr_t id;

    // the real OS thread id.
    long tid;

    PyObject *name;

    // profiling start CPU time
    long long t0;

    // how many times this thread is scheduled
    unsigned long sched_cnt;

    long long last_seen;

    PyThreadState *ts_ptr;

    _glstate gl_state;
} _ctx; // context

typedef struct {
    PyObject *ctx_id;
    PyObject *tag;
    PyObject *name;
    PyObject *modname;
} _fast_func_stat_filter;

typedef struct
{
    _fast_func_stat_filter func_filter;
    PyObject *enumfn;
} _ctxenumarg;

typedef struct
{
    _ctxenumarg *enum_args;
    uintptr_t tag;
    _ctx *ctx;
} _ctxfuncenumarg;

typedef struct {
    int builtins;
    int multicontext;
} _flag; // flags passed from yappi.start()

typedef enum
{
    NATIVE_THREAD = 0x00,
    GREENLET = 0x01,
} _ctx_type_t;

// globals
static PyObject *YappiProfileError;
static _htab *contexts;
static _flag flags;
static _freelist *flpit;
static _freelist *flctx;
static int yappinitialized;
static unsigned int ycurfuncindex; // used for providing unique index for functions
static long long ycurthreadindex = 0;
static int yapphavestats;   // start() called at least once or stats cleared?
static int yapprunning;
static int paused;
static time_t yappstarttime;
static long long yappstarttick;
static long long yappstoptick;
static tls_key_t* tl_prev_ctx_key = NULL;
static _ctx *prev_ctx = NULL;
static _ctx *current_ctx = NULL;
static _ctx *initial_ctx = NULL; // used for holding the context that called start()
static PyObject *context_id_callback = NULL;
static PyObject *tag_callback = NULL;
static PyObject *context_name_callback = NULL;
static PyObject *test_timings; // used for testing
static const uintptr_t DEFAULT_TAG = 0;
static _ctx_type_t ctx_type = NATIVE_THREAD;

// defines
#define UNINITIALIZED_STRING_VAL "N/A"

#define PyStr_AS_CSTRING(s) PyUnicode_AsUTF8(s)
#define PyStr_Check(s) PyUnicode_Check(s)
#define PyStr_FromString(s) PyUnicode_FromString(s)
#define PyStr_FromFormatV(fmt, vargs) PyUnicode_FromFormatV(fmt, vargs)

#define PyLong_AsVoidPtr (uintptr_t)PyLong_AsVoidPtr
 
// forwards
static _ctx * _profile_thread(PyThreadState *ts);
static void _pause_greenlet_ctx(_ctx *ctx);
static void _resume_greenlet_ctx(_ctx *ctx);
static int _pitenumdel(_hitem *item, void *arg);

// funcs

static PyCodeObject *
FRAME2CODE(PyFrameObject *frame) {
#if PY_VERSION_HEX >= 0x030A0000 // Python 3.10+
    return PyFrame_GetCode(frame);
#else
    return frame->f_code;
#endif
}

static void _DebugPrintObjects(unsigned int arg_count, ...)
{
    unsigned int i;
    va_list vargs;
    va_start(vargs, arg_count);

    for (i=0; i<arg_count; i++) {
        PyObject_Print(va_arg(vargs, PyObject *), stdout, Py_PRINT_RAW);
    }
    printf("\n");
    va_end(vargs);
}

int 
IS_SUSPENDED(PyFrameObject *frame) {
#if PY_VERSION_HEX >= 0x030B0000  // Python 3.11+
    PyGenObject *gen = (PyGenObject *)PyFrame_GetGenerator(frame);
    if (gen == NULL) {
        return 0;
    }
#if PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION == 14
    unsigned char curr_op_code = (*frame->f_frame->instr_ptr).op.code;
    return curr_op_code == YIELD_VALUE || curr_op_code == INSTRUMENTED_YIELD_VALUE;
#elif PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION == 13
    return FRAME_STATE_SUSPENDED(gen->gi_frame_state);
#else
    return gen->gi_frame_state == FRAME_SUSPENDED;
#endif
#elif PY_VERSION_HEX >= 0x030A0000 // Python 3.10+
    return (frame->f_state == FRAME_SUSPENDED);
#else
    return (frame->f_stacktop != NULL);
#endif
}

int IS_ASYNC(PyFrameObject *frame)
{
    return FRAME2CODE(frame)->co_flags & CO_COROUTINE ||
        FRAME2CODE(frame)->co_flags & CO_ITERABLE_COROUTINE ||
        FRAME2CODE(frame)->co_flags & CO_ASYNC_GENERATOR;
}

static PyObject *
PyStr_FromFormat(const char *fmt, ...)
{
    PyObject* ret;
    va_list vargs;

    va_start(vargs, fmt);
    ret = PyStr_FromFormatV(fmt, vargs);
    va_end(vargs);
    return ret;
}

// module functions

static void
_log_err(unsigned int code)
{
    yerr("Internal Error. [%u]", code);
}

static _pit *
_create_pit(void)
{
    _pit *pit;

    pit = flget(flpit);
    if (!pit)
        return NULL;
    
    pit->callcount = 0;
    pit->nonrecursive_callcount = 0;
    pit->ttotal = 0;
    pit->tsubtotal = 0;
    pit->name = NULL;
    pit->modname = NULL;
    pit->lineno = 0;
    pit->builtin = 0;
    pit->index = ycurfuncindex++;
    pit->children = NULL;
    pit->coroutines = NULL;
    pit->fn_descriptor = NULL;

    return pit;
}

static _ctx *
_create_ctx(void)
{
    _ctx *ctx;

    ctx = flget(flctx);
    if (!ctx)
        return NULL;
    ctx->cs = screate(100);
    if (!ctx->cs)
        return NULL;

    ctx->tags = htcreate(HT_TAG_SIZE);
    if (!ctx->tags)
        return NULL;

    ctx->sched_cnt = 0;
    ctx->id = 0;
    ctx->tid = 0;
    ctx->name = NULL;
    ctx->t0 = tickcount();
    ctx->last_seen = ctx->t0;
    ctx->rec_levels = htcreate(HT_RLEVEL_SIZE);
    if (!ctx->rec_levels)
        return NULL;
    return ctx;
}

PyObject *
_call_funcobjargs(PyObject *func, PyObject *args)
{
    // restore to correct ctx after a func call on Python side. 
    // Python side might have context switched to another thread which might 
    // have changed to current_ctx, after a while when the func returns
    // we will be in a correct ThreadState * but current_ctx might not
    // be correct.
    PyObject *result;
    _ctx *_local_current_ctx, *_local_prev_ctx;

    _local_current_ctx = current_ctx;
    _local_prev_ctx = prev_ctx;
    result = PyObject_CallFunctionObjArgs(func, args);
    current_ctx = _local_current_ctx;
    prev_ctx = _local_prev_ctx;

    return result;
}

static PyObject *
_current_context_name(void)
{
    PyObject *name;

    if (!context_name_callback) {
        return NULL;
    }

    name = _call_funcobjargs(context_name_callback, NULL);
     if (!name) {
        PyErr_Print();
        goto err;
    }

    if (name == Py_None) {
        // Name not available yet - will try again on the next call
        goto later;
    }

    if (!PyStr_Check(name)) {
        yerr("context name callback returned non-string");
        goto err;
    }

    return name;

err:
    PyErr_Clear();
    Py_CLEAR(context_name_callback);  /* Don't use the callback again. */
    Py_XDECREF(name);
    return NULL;
later:
    Py_XDECREF(name);
    return NULL;
}

static uintptr_t
_current_tag(void)
{
    PyObject *r;
    uintptr_t result;

    if (!tag_callback) {
        return DEFAULT_TAG;
    }

    r = _call_funcobjargs(tag_callback, NULL);
    if (!r) {
        PyErr_Print();
        goto error;
    }

    result = PyLong_AsVoidPtr(r);
    Py_DECREF(r);
    if (PyErr_Occurred()) {
        yerr("tag_callback returned non-integer (overflow?)");
        goto error;
    }

    return result;
error:
    PyErr_Clear();
    Py_CLEAR(tag_callback); // don't use callback again
    return 0;
}

static uintptr_t
_current_context_id(PyThreadState *ts)
{
    uintptr_t rc;
    PyObject *callback_rc, *ytid;

    if (context_id_callback) {
        callback_rc = _call_funcobjargs(context_id_callback, NULL);
        if (!callback_rc) {
            PyErr_Print();
            goto error;
        }
        rc = PyLong_AsVoidPtr(callback_rc);
        Py_DECREF(callback_rc);
        if (PyErr_Occurred()) {
            yerr("context id callback returned non-integer (overflow?)");
            goto error;
        }

        return rc;
    } else {
        // Use thread_id instead of ts pointer, because when we create/delete many threads, some
        // of them do not show up in the thread_stats, because ts pointers are recycled in the VM.
        // Also, OS tids are recycled, too. The only valid way is to give ctx's custom tids which
        // are hold in a per-thread structure. Again: we use an integer instead of directly mapping the ctx
        // pointer to some per-thread structure because other threading libraries do not necessarily
        // have direct ThreadState->Thread mapping. Greenlets, for example, will only have a single
        // thread. Therefore, we need to identify the "context" concept independent from ThreadState 
        // objects.

        if (!flags.multicontext) {
            return 0;
        }

        // Below code is same as ThreadState_GetDict() but on some Python versions
        // that function only works on current thread. In our situation there is 
        // a possibility that current_context_id is called for another thread while
        // enumerating the active threads. This is why we implement it ourselves.
        if (!ts->dict) {
            ts->dict = PyDict_New();
            if (ts->dict == NULL) {
                PyErr_Clear();
                return 0;
            }
        }

        ytid = PyDict_GetItemString(ts->dict, "_yappi_tid");
        if (!ytid) {
            ytid = PyLong_FromLongLong(ycurthreadindex++);
            PyDict_SetItemString(ts->dict, "_yappi_tid", ytid);
        }
        rc = PyLong_AsVoidPtr(ytid);

        return rc;
    }

error:
    PyErr_Clear();
    Py_CLEAR(context_id_callback); // don't use callback again
    return 0;
}

static _ctx *
_thread2ctx(PyThreadState *ts)
{
    _hitem *it;
    it = hfind(contexts, _current_context_id(ts));
    if (!it) {
        // callback functions in some circumtances, can be called before the context entry is not
        // created. (See issue 21). To prevent this problem we need to ensure the context entry for
        // the thread is always available here.
        //
        // This path is also excercised when new greenlets are encountered on an already profiled thread.
        return _profile_thread(ts);
    }
    return (_ctx *)it->val;
}

// the pit will be cleared by the relevant freelist. we do not free it here.
// we only DECREF the CodeObject or the MethodDescriptive string.
static void
_del_pit(_pit *pit)
{
    // the pit will be freed by fldestrot() in clear_stats, otherwise it stays
    // for later enumeration
    _pit_children_info *it,*next;

    // free children
    it = pit->children;
    while(it) {
        next = (_pit_children_info *)it->next;
        yfree(it);
        it = next;
    }
    pit->children = NULL;
    Py_DECREF(pit->fn_descriptor);
}

static PyObject *
_pycfunction_module_name(PyCFunctionObject *cfn)
{
    PyObject *obj;
    PyObject *name;

    // The __module__ attribute, can be anything
    obj = cfn->m_module;

    if (!obj) {
        // TODO: Is this always correct?
        name = PyStr_FromString("__builtin__");
    } else if (PyStr_Check(obj)) {
        Py_INCREF(obj);
        name = obj;
    } else if (PyModule_Check(obj)) {
        const char *s = PyModule_GetName(obj);
        if (!s) {
            goto error;
        }
        name = PyStr_FromString(s);
    } else {
        // Something else - str(obj)
        name = PyObject_Str(obj);
    }

    return name;

error:
    PyErr_Clear();
    return PyStr_FromString("<unknown>");
}

_htab *
_get_pits_tbl(uintptr_t current_tag)
{
    _hitem *it;
    _htab *pits;

    it = hfind(current_ctx->tags, current_tag);
    if (!it) {
        pits = htcreate(HT_TAGGED_PIT_SIZE);
        if (!pits) {
            return NULL;
        }

        if (!hadd(current_ctx->tags, current_tag, (uintptr_t)pits)) {
            return NULL;
        }

        return pits;
    }

    return (_htab *)it->val;
}

static _pit *
_ccode2pit(void *cco, uintptr_t current_tag)
{
    PyCFunctionObject *cfn;
    _hitem *it;
    PyObject *name, *mo, *obj_type, *method_descriptor;
    _htab *pits;

    pits = _get_pits_tbl(current_tag);
    if (!pits) {
        return NULL;
    }

    cfn = cco;
    // Issue #15:
    // Hashing cfn to the pits table causes different object methods
    // to be hashed into the same slot. Use cfn->m_ml for hashing the
    // Python C functions.
    it = hfind(pits, (uintptr_t)cfn->m_ml);
    if (!it) {
        _pit *pit = _create_pit();
        if (!pit)
            return NULL;
        if (!hadd(pits, (uintptr_t)cfn->m_ml, (uintptr_t)pit))
            return NULL;

        pit->builtin = 1;
        pit->modname = _pycfunction_module_name(cfn);
        pit->lineno = 0;
        pit->fn_descriptor = NULL;

        // built-in method?
        if (cfn->m_self != NULL) {
            name = PyStr_FromString(cfn->m_ml->ml_name);
            if (name != NULL) {
                obj_type = PyObject_Type(cfn->m_self);

                // use method descriptor instead of instance methods for builtin
                // objects. Othwerwise, there might be some errors since we INCREF
                // on the bound method. See: https://github.com/sumerc/yappi/issues/60
                method_descriptor = PyObject_GetAttr(obj_type, name);
                if (method_descriptor) {
                    pit->fn_descriptor = method_descriptor;
                    Py_INCREF(method_descriptor);
                }

                // get name from type+name
                mo = _PyType_Lookup((PyTypeObject *)obj_type, name);
                Py_XINCREF(mo);
                Py_XDECREF(obj_type);
                Py_DECREF(name);
                if (mo != NULL) {
                    pit->name = PyObject_Repr(mo);
                    Py_DECREF(mo);
                    return pit;
                }
            }
            PyErr_Clear();
        }

        if (pit->fn_descriptor == NULL) {
            pit->fn_descriptor = (PyObject *)cfn;
            Py_INCREF(cfn);
        }

        pit->name = PyStr_FromString(cfn->m_ml->ml_name);
        return pit;
    }
    return ((_pit *)it->val);
}

static PyObject *_get_locals(PyFrameObject *fobj) {
#if PY_VERSION_HEX >= 0x030A0000 // Python 3.10+
    return PyEval_GetLocals();
#else
    PyFrame_FastToLocals(fobj);
    PyObject* locals = fobj->f_locals;
    PyFrame_LocalsToFast(fobj, 0);
    return locals;
#endif
}

// maps the PyCodeObject to our internal pit item via hash table.
static _pit *
_code2pit(PyFrameObject *fobj, uintptr_t current_tag)
{
    _hitem *it;
    PyCodeObject *cobj;
    _pit *pit;
    _htab *pits;
    PyObject *co_varnames;

    pits = _get_pits_tbl(current_tag);
    if (!pits) {
        return NULL;
    }

    cobj = FRAME2CODE(fobj);
    it = hfind(pits, (uintptr_t)cobj);
    if (it) {
        return ((_pit *)it->val);
    }

    pit = _create_pit();
    if (!pit)
        return NULL;
    if (!hadd(pits, (uintptr_t)cobj, (uintptr_t)pit))
        return NULL;

    pit->name = NULL;
    Py_INCREF(cobj->co_filename);
    pit->modname = cobj->co_filename;
    pit->lineno = cobj->co_firstlineno;
    pit->fn_descriptor = (PyObject *)cobj;
    Py_INCREF(cobj);

    if (cobj->co_argcount) {
        // There has been a lot going on with `co_varnames`, but finally in 
        // 3.11.0rc1, it is added as a public API
#if PY_VERSION_HEX >= 0x030B0000 // Python 3.11+
        co_varnames = PyCode_GetVarnames(cobj);
#else
        co_varnames = cobj->co_varnames;
#endif
        const char *firstarg = PyStr_AS_CSTRING(PyTuple_GET_ITEM(co_varnames, 0));

        if (!strcmp(firstarg, "self")) {
            PyObject* locals = _get_locals(fobj);
            if (locals) {
                PyObject* self = PyDict_GetItemString(locals, "self");
                if (self) {
                    PyObject *class_obj = PyObject_GetAttrString(self, "__class__");
                    if (class_obj) {
                        PyObject *class_name = PyObject_GetAttrString(class_obj, "__name__");
                        if (class_name) {
                            pit->name = PyStr_FromFormat("%s.%s", PyStr_AS_CSTRING(class_name), PyStr_AS_CSTRING(cobj->co_name));
                            Py_DECREF(class_name);
                        } else {
                            PyErr_Clear();
                        }
                        Py_DECREF(class_obj);
                    } else {
                        PyErr_Clear();
                    }
                }
            }
        }
    }
    if (!pit->name) {
        Py_INCREF(cobj->co_name);
        pit->name = cobj->co_name;
    }

    return pit;
}

static _pit *
_get_frame(void)
{
    _cstackitem *ci;

    ci = shead(current_ctx->cs);
    if (!ci) {
        return NULL;
    }
    return ci->ckey;
}

static _cstackitem *
_push_frame(_pit *cp)
{
    return spush(current_ctx->cs, cp);
}

static _pit *
_pop_frame(void)
{
    _cstackitem *ci;

    ci = spop(current_ctx->cs);
    if (!ci) {
        return NULL;
    }
    return ci->ckey;
}

static _pit_children_info *
_add_child_info(_pit *parent, _pit *current)
{
    _pit_children_info *newci;

    // TODO: Optimize by moving to a freelist?
    newci = ymalloc(sizeof(_pit_children_info));
    if (!newci) {
        return NULL;
    }
    newci->index = current->index;
    newci->callcount = 0;
    newci->nonrecursive_callcount = 0;
    newci->ttotal = 0;
    newci->tsubtotal = 0;
    newci->next = (struct _pit_children_info *)parent->children;
    parent->children = (_pit_children_info *)newci;

    return newci;
}

static _pit_children_info *
_get_child_info(_pit *parent, _pit *current, int add_if_not_exists)
{
    _pit_children_info *citem;

    if (!parent || !current) {
        return NULL;
    }

    citem = parent->children;
    while(citem) {
        if (citem->index == current->index) {
            break;
        }
        citem = (_pit_children_info *)citem->next;
    }

    if (add_if_not_exists && !citem) {
        citem = _add_child_info(parent, current);
    }

    return citem;
}

static uintptr_t
get_rec_level(uintptr_t key)
{
    _hitem *it;

    it = hfind(current_ctx->rec_levels, key);
    if (!it) {
        _log_err(1);
        return -1; // should not happen
    }
    return it->val;
}

static int
incr_rec_level(uintptr_t key)
{
    _hitem *it;

    it = hfind(current_ctx->rec_levels, key);
    if (it) {
        it->val++;
    } else {
        if (!hadd(current_ctx->rec_levels, key, 1))
        {
            _log_err(2);
            return 0; // should not happen
        }
    }
    return 1;
}

static int
decr_rec_level(uintptr_t key)
{
    _hitem *it;
    uintptr_t v;

    it = hfind(current_ctx->rec_levels, key);
    if (it) {
        v = it->val--;  /*supress warning -- it is safe to cast long vs pointers*/
        if (v == 0)
        {
            hfree(current_ctx->rec_levels, it);
        }
    } else {
        _log_err(3);
        return 0; // should not happen
    }
    return 1;
}


static long long
_ctx_tickcount(void) {
    long long now;

    now = tickcount();
    current_ctx->last_seen = now;
    return now;
}

static long long
_get_frame_elapsed(void)
{
    _cstackitem *ci;
    _pit *cp;
    long long result;

    ci = shead(current_ctx->cs);
    if (!ci) {
        return 0LL;
    }
    cp = ci->ckey;

    if (test_timings) {
        uintptr_t rlevel = get_rec_level((uintptr_t)cp);
        PyObject *formatted_string = PyStr_FromFormat(
                "%s_%d", PyStr_AS_CSTRING(cp->name), rlevel);

        PyObject *tval = PyDict_GetItem(test_timings, formatted_string);
        Py_DECREF(formatted_string);
        if (tval) {
            result = PyLong_AsLongLong(tval);
        } else {
            result = DEFAULT_TEST_ELAPSED_TIME;
        }

    } else {
        result = _ctx_tickcount() - ci->t0;
    }

    return result;
}

static void
_print_coros(_pit *cp)
{
    _coro *coro;

    printf("Printing coroutines on %s...\n", PyStr_AS_CSTRING(cp->name));
    coro = cp->coroutines;
    while(coro) {
        printf("CORO %s %p %lld\n", PyStr_AS_CSTRING(cp->name), coro->frame, coro->t0);
        coro = (_coro *)coro->next;
    }
}

static int 
_coro_enter(_pit *cp, PyFrameObject *frame)
{
    _coro *coro;

    if (!(get_timing_clock_type() == WALL_CLOCK) || 
        (get_rec_level((uintptr_t)cp) != 1)) {
            return 0;
    }

    // if we already have this coro then it was yielded before
    coro = cp->coroutines;
    while(coro) {
        if (coro->frame == frame) {
            return 0;
        }
        coro = (_coro *)coro->next;
    }

    //printf("CORO ENTER %s %p %lld\n", PyStr_AS_CSTRING(cp->name), frame, tickcount());

    coro = ymalloc(sizeof(_coro));
    if (!coro) {
        return -1;
    }

    coro->frame = frame;
    coro->t0 = tickcount();
    coro->next = NULL;
    
    if (cp->coroutines) {
        coro->next = (struct _coro *)cp->coroutines;
    }
    cp->coroutines = coro;

    return 1;
}

static long long 
_coro_exit(_pit *cp, PyFrameObject *frame)
{
    _coro *coro, *prev;
    long long _t0;

    if (!(get_timing_clock_type() == WALL_CLOCK) || 
        (get_rec_level((uintptr_t)cp) != 1)) {
            return 0;
    }

    //printf("CORO EXIT %s %p %lld\n", PyStr_AS_CSTRING(cp->name), frame, tickcount());

    prev = NULL;
    coro = cp->coroutines;
    while(coro) {
        if (coro->frame == frame) {
            _t0 = coro->t0;
            if (prev) {
                prev->next = coro->next;
            } else {
                cp->coroutines = (_coro *)coro->next;
            }
            yfree(coro);
            //printf("CORO EXIT(elapsed) %s %p %lld\n", PyStr_AS_CSTRING(cp->name), frame, tickcount()-_t0);
            return tickcount() - _t0;
        }
        prev = coro;
        coro = (_coro *)coro->next;
    }

    // expected: a func leaves without enter
    return 0;
}


static void
_call_enter(PyObject *self, PyFrameObject *frame, PyObject *arg, int ccall)
{
    _pit *cp,*pp;
    _cstackitem *ci;
    _pit_children_info *pci;
    uintptr_t current_tag;

    // printf("call ENTER:%s %s\n", PyStr_AS_CSTRING(frame->f_code->co_filename),
    //                              PyStr_AS_CSTRING(frame->f_code->co_name));
    
    current_tag = _current_tag();

    //printf("call ENTER:%s %s %d\n", PyStr_AS_CSTRING(frame->f_code->co_filename),
    //                             PyStr_AS_CSTRING(frame->f_code->co_name), current_tag);

    if (ccall) {
        cp = _ccode2pit((PyCFunctionObject *)arg, current_tag);
    } else {
        cp = _code2pit(frame, current_tag);
    }

    // something went wrong. No mem, or another error. we cannot find
    // a corresponding pit. just run away:)
    if (!cp) {
        _log_err(4);
        return;
    }

    // create/update children info if we have a valid parent
    pp = _get_frame();
    if (pp) {
        pci = _get_child_info(pp, cp, 1);
        if (!pci) {
            _log_err(12); // defensive runaway
            return;
        }
        incr_rec_level((uintptr_t)pci);
    }

    ci = _push_frame(cp);
    if (!ci) { // runaway! (defensive)
        _log_err(5);
        return;
    }

    ci->t0 = _ctx_tickcount();

    incr_rec_level((uintptr_t)cp);

    // TODO: Comment
    if (IS_ASYNC(frame)) {
        _coro_enter(cp, frame);
    }
}

static void
_call_leave(PyObject *self, PyFrameObject *frame, PyObject *arg, int ccall)
{
    long long elapsed;
    _pit *cp, *pp, *ppp;
    _pit_children_info *pci,*ppci;
    int yielded = 0;
    pci = ppci = NULL;

    // printf("call LEAVE:%s %s\n", PyStr_AS_CSTRING(frame->f_code->co_filename),
    //                              PyStr_AS_CSTRING(frame->f_code->co_name));

    elapsed = _get_frame_elapsed();

    // leaving a frame while callstack is empty?
    cp = _pop_frame();
    if (!cp) {
        return;
    }

    // if the function that the frame belongs is a coroutine, we check if we RETURN
    // or await the coroutine to calculate the correct walltime
    if (IS_ASYNC(frame)) {
        if (IS_SUSPENDED(frame)) {
            yielded = 1;
            if (get_timing_clock_type() == WALL_CLOCK) {
                // In fact setting this zero means following:
                // for this specific pit, we say that if WALL_CLOCK is on,
                // we only aggregate time between first non-recursive enter and
                // last non-recursive exit
                elapsed = 0;
            }
        } else {
            long long coro_elapsed = _coro_exit(cp, frame);

            if (coro_elapsed > 0) {
                elapsed = coro_elapsed;
            }
        }
    }

    if (!yielded) {
        cp->callcount++;
    }

    // is this the last function in the callstack?
    pp = _pop_frame();
    if (!pp) {
        // update actual pit
        cp->ttotal += elapsed;
        cp->tsubtotal += elapsed;
        if (!yielded) {
            cp->nonrecursive_callcount++;
        }
        decr_rec_level((uintptr_t)cp);
        return;
    }
    // get children info
    pci = _get_child_info(pp, cp, 0);
    if(!pci)
    {
        _log_err(6);
        return; // defensive
    }

    // a calls b. b's elapsed time is subtracted from a's tsub and 
    // a adds its own elapsed it is leaving.
    pp->tsubtotal -= elapsed;
    cp->tsubtotal += elapsed;
    
    if (!yielded) {
        pci->callcount++;
    }

    // a->b->c. b->c is substracted from a->b.
    ppp = _get_frame();
    if (ppp) {
        ppci = _get_child_info(ppp, pp, 0);
        if(!ppci) {
            _log_err(7);
            return;
        }
        ppci->tsubtotal -= elapsed;
    }
    pci->tsubtotal += elapsed;

    // wait for the top-level function/parent/child to update timing values accordingly.
    if (get_rec_level((uintptr_t)cp) == 1) {
        cp->ttotal += elapsed;
        if (!yielded) {
            cp->nonrecursive_callcount++;
            pci->nonrecursive_callcount++;
        }
    }

    if (get_rec_level((uintptr_t)pci) == 1) {
        pci->ttotal += elapsed;
    }

    decr_rec_level((uintptr_t)pci);
    decr_rec_level((uintptr_t)cp);

    if (!_push_frame(pp)) {
        _log_err(8);
        return; //defensive
    }
}

static int
_pitenumdel(_hitem *item, void *arg)
{
    _del_pit((_pit *)item->val);
    return 0;
}

static int
_tagenumdel(_hitem *item, void *arg)
{
    _htab *pits;

    pits = (_htab *)item->val;
    henum(pits, _pitenumdel, NULL);
    htdestroy(pits);

    return 0;
}

// context will be cleared by the free list. we do not free it here.
// we only free the context call stack.
static void
_del_ctx(_ctx * ctx)
{
    sdestroy(ctx->cs);
    htdestroy(ctx->rec_levels);

    henum(ctx->tags, _tagenumdel, NULL);
    htdestroy(ctx->tags);

    Py_CLEAR(ctx->name);
}

static int
_yapp_callback(PyObject *self, PyFrameObject *frame, int what,
               PyObject *arg)
{
    PyObject *last_type, *last_value, *last_tb;
    _ctx* tl_prev_ctx;
    PyErr_Fetch(&last_type, &last_value, &last_tb);

    //printf("call EVENT %d %s %s", what, PyStr_AS_CSTRING(frame->f_code->co_filename),
    //                         PyStr_AS_CSTRING(frame->f_code->co_name));

    // get current ctx
    current_ctx = _thread2ctx(PyThreadState_GET());
    if (!current_ctx) {
        _log_err(9);
        goto finally;
    }

    if (ctx_type == GREENLET && get_timing_clock_type() == CPU_CLOCK) {
        tl_prev_ctx = (_ctx*)(get_tls_key_value(tl_prev_ctx_key));

        if (tl_prev_ctx != current_ctx) {
            if (tl_prev_ctx) {
                _pause_greenlet_ctx(tl_prev_ctx);
                _resume_greenlet_ctx(current_ctx);
            }
            if (set_tls_key_value(tl_prev_ctx_key, current_ctx) != 0)
                goto finally;
        }
    }

    // do not profile if multi-context is off and the context is different than
    // the context that called start.
    if (!flags.multicontext && current_ctx != initial_ctx) {
        goto finally;
    }

    // update ctx stats
    if (prev_ctx != current_ctx) {
        current_ctx->sched_cnt++;
    }
    prev_ctx = current_ctx;
    if (!current_ctx->name)
    {
        current_ctx->name = _current_context_name();
    }

    switch (what) {
    case PyTrace_CALL:
        _call_enter(self, frame, arg, 0);
        break;
    case PyTrace_RETURN: // either normally or with an exception
        _call_leave(self, frame, arg, 0);
        break;
    /* case PyTrace_EXCEPTION:
        If the exception results in the function exiting, a
        PyTrace_RETURN event will be generated, so we don't need to
        handle it. */

    case PyTrace_C_CALL:
        if (PyCFunction_Check(arg))
            _call_enter(self, frame, arg, 1); // set ccall to true
        break;

    case PyTrace_C_RETURN:
    case PyTrace_C_EXCEPTION:
        if (PyCFunction_Check(arg))
            _call_leave(self, frame, arg, 1);
        break;
    default:
        break;
    }

    goto finally;

finally:
    if (last_type) {
        PyErr_Restore(last_type, last_value, last_tb);
    }

    // there shall be no context switch happenning inside 
    // profile events and no concurent running events is possible
    if (current_ctx->ts_ptr != PyThreadState_GET()) {
        // printf("call EVENT %d %s %s %p %p\n", what, PyStr_AS_CSTRING(frame->f_code->co_filename),
        //                     PyStr_AS_CSTRING(frame->f_code->co_name), 
        //                     current_ctx->ts_ptr, PyThreadState_GET());

        //abort();
        _log_err(15);
    }

    return 0;
}

static void
_pause_greenlet_ctx(_ctx *ctx)
{
    ydprintf("pausing context: %ld", ctx->id);
    ctx->gl_state.paused = 1;
    ctx->gl_state.paused_at = tickcount();
}

static void
_resume_greenlet_ctx(_ctx *ctx)
{
    long long shift;
    int i;

    ydprintf("resuming context: %ld", ctx->id);

    if (!ctx->gl_state.paused) {
        return;
    }

    ctx->gl_state.paused = 0;
    shift = tickcount() - ctx->gl_state.paused_at;
    ctx->t0 += shift;

    for (i = 0; i <= ctx->cs->head; i++) {
        ctx->cs->_items[i].t0 += shift;
    }

    ydprintf("resuming context: %ld, shift: %lld", ctx->id, shift);
}

static void 
_eval_setprofile(PyThreadState *ts)
{
#if PY_VERSION_HEX > 0x030c0000
    PyEval_SetProfileAllThreads(_yapp_callback, NULL);
#elif PY_VERSION_HEX > 0x030b0000
    _PyEval_SetProfile(ts, _yapp_callback, NULL);
#elif PY_VERSION_HEX < 0x030a00b1
    ts->use_tracing = 1;
    ts->c_profilefunc = _yapp_callback;
#else
    ts->cframe->use_tracing = 1;
    ts->c_profilefunc = _yapp_callback;
#endif
}

static void
_eval_unsetprofile(PyThreadState *ts)
{
#if PY_VERSION_HEX > 0x030c0000
    PyEval_SetProfileAllThreads(NULL, NULL);
#elif PY_VERSION_HEX > 0x030b0000
    _PyEval_SetProfile(ts, NULL, NULL);
#elif PY_VERSION_HEX < 0x030a00b1
    ts->use_tracing = 0;
    ts->c_profilefunc = NULL;
#else
    ts->cframe->use_tracing = 0;
    ts->c_profilefunc = NULL;
#endif
}

static _ctx *
_bootstrap_thread(PyThreadState *ts)
{
    _eval_setprofile(ts);
    return NULL;
}

static _ctx *
_profile_thread(PyThreadState *ts)
{
    uintptr_t ctx_id;
    _ctx *ctx;
    _hitem *it;

    ctx_id = _current_context_id(ts);
    it = hfind(contexts, ctx_id);
    if (!it) {
        ctx = _create_ctx();
        if (!ctx) {
            return NULL;
        }
        if (!hadd(contexts, ctx_id, (uintptr_t)ctx)) {
            _del_ctx(ctx);
            if (!flput(flctx, ctx)) {
                _log_err(10);
            }
            _log_err(11);
            return NULL;
        }
    } else {
        ctx = (_ctx *)it->val;
    }
    _eval_setprofile(ts);
    ctx->id = ctx_id;
    ctx->tid = ts->thread_id;
    ctx->ts_ptr = ts;
    ctx->gl_state.paused = 0;
    ctx->gl_state.paused_at = 0;

    // printf("Thread profile STARTED. ctx=%p, ts_ptr=%p, ctx_id=%ld, ts param=%p\n", ctx, 
    //        ctx->ts_ptr, ctx->id, ts);

    return ctx;
}

static _ctx*
_unprofile_thread(PyThreadState *ts)
{
    _eval_unsetprofile(ts);

    return NULL; //dummy return for enum_threads() func. prototype
}

static void
_ensure_thread_profiled(PyThreadState *ts)
{
    if (ts->c_profilefunc != _yapp_callback)
        _profile_thread(ts);
}

static void
_enum_threads(_ctx* (*f) (PyThreadState *))
{
    PyThreadState *ts;
    PyInterpreterState* is;

    for(is=PyInterpreterState_Head();is!=NULL;is = PyInterpreterState_Next(is))
    {
        for (ts=PyInterpreterState_ThreadHead(is) ; ts != NULL; ts = ts->next) {
            f(ts);
        }
    }
}

static int
_init_profiler(void)
{
    // already initialized? only after clear_stats() and first time, this flag
    // will be unset.
    if (!yappinitialized) {
        contexts = htcreate(HT_CTX_SIZE);
        if (!contexts)
            goto error;
        flpit = flcreate(sizeof(_pit), FL_PIT_SIZE);
        if (!flpit)
            goto error;
        flctx = flcreate(sizeof(_ctx), FL_CTX_SIZE);
        if (!flctx)
            goto error;
        tl_prev_ctx_key = create_tls_key();
        if (!tl_prev_ctx_key)
            goto error;
        yappinitialized = 1;
    }
    return 1;

error:
    if (contexts) {
        htdestroy(contexts);
        contexts = NULL;
    }
    if (flpit) {
        fldestroy(flpit);
        flpit = NULL;
    }
    if (flctx) {
        fldestroy(flctx);
        flctx = NULL;
    }
    if (tl_prev_ctx_key) {
        delete_tls_key(tl_prev_ctx_key);
        tl_prev_ctx_key = NULL;
    }

    return 0;
}

static PyObject*
profile_event(PyObject *self, PyObject *args)
{
    const char *ev;
    PyObject *arg;
    PyObject *event;
    PyFrameObject * frame;

    if (!PyArg_ParseTuple(args, "OOO", &frame, &event, &arg)) {
        return NULL;
    }

    _ensure_thread_profiled(PyThreadState_GET());

    ev = PyStr_AS_CSTRING(event);

    if (strcmp("call", ev)==0)
        _yapp_callback(self, frame, PyTrace_CALL, arg);
    else if (strcmp("return", ev)==0)
        _yapp_callback(self, frame, PyTrace_RETURN, arg);
    else if (strcmp("c_call", ev)==0)
        _yapp_callback(self, frame, PyTrace_C_CALL, arg);
    else if (strcmp("c_return", ev)==0)
        _yapp_callback(self, frame, PyTrace_C_RETURN, arg);
    else if (strcmp("c_exception", ev)==0)
        _yapp_callback(self, frame, PyTrace_C_EXCEPTION, arg);

    Py_RETURN_NONE;
}

static long long
_calc_cumdiff(long long a, long long b)
{
    long long r;

    r = a - b;
    if (r < 0)
        return 0;
    return r;
}

static int
_ctxenumdel(_hitem *item, void *arg)
{
    _del_ctx(((_ctx *)item->val) );
    return 0;
}

// start profiling. return 1 on success, or 0 and set exception.
static int
_start(void)
{
    if (yapprunning)
        return 1;

    if (!_init_profiler()) {
        PyErr_SetString(YappiProfileError, "profiler cannot be initialized.");
        return 0;
    }

    if (flags.multicontext) {
        _enum_threads(&_bootstrap_thread);
    } else {
        _ensure_thread_profiled(PyThreadState_GET());
        initial_ctx = _thread2ctx(PyThreadState_GET());
    }

    yapprunning = 1;
    yapphavestats = 1;
    time (&yappstarttime);
    yappstarttick = tickcount();

    return 1;
}

static void
_stop(void)
{
    if (!yapprunning)
        return;

    _enum_threads(&_unprofile_thread);

    yapprunning = 0;
    yappstoptick = tickcount();
}

static PyObject*
clear_stats(PyObject *self, PyObject *args)
{

    if (!yapphavestats) {
        Py_RETURN_NONE;
    }

    current_ctx = NULL;
    prev_ctx = NULL;
    initial_ctx = NULL;

    henum(contexts, _ctxenumdel, NULL);
    htdestroy(contexts);
    contexts = NULL;

    fldestroy(flpit);
    flpit = NULL;

    fldestroy(flctx);
    flctx = NULL;

    delete_tls_key(tl_prev_ctx_key);
    tl_prev_ctx_key = NULL;

    yappinitialized = 0;
    yapphavestats = 0;
    ycurfuncindex = 0;

    Py_CLEAR(test_timings);

// check for mem leaks if DEBUG_MEM is specified
#ifdef DEBUG_MEM
    YMEMLEAKCHECK();
#endif

    Py_RETURN_NONE;
}

// normalizes the time count if test_timing is not set.
static double
_normt(long long tickcount)
{
    if (!test_timings) {
        return tickcount * tickfactor();
    }
    return (double)tickcount;
}

static int
_ctxenumstat(_hitem *item, void *arg)
{
    PyObject *efn;
    const char *tcname;
    _ctx *ctx;
    long long cumdiff;
    PyObject *exc;

    ctx = (_ctx *)item->val;

    if(ctx->sched_cnt == 0) {
        // we return here because if sched_cnt is zero, then this means not any single function
        // executed in the context of this thread. We do not want to show any thread stats for this case especially
        // because of the following case: start()/lots of MT calls/stop()/clear_stats()/start()/get_thread_stats()
        // still returns the threads from the previous cleared session. That is because Python VM does not free them
        // in the second start() call, we enumerate the active threads from the threading module and they are still there.
        // second invocation of test_start_flags() generates this situation.
        return 0;
    }

    if (ctx->name)
        tcname = PyStr_AS_CSTRING(ctx->name);
    else
        tcname = UNINITIALIZED_STRING_VAL;

    efn = (PyObject *)arg;

    cumdiff = _calc_cumdiff(ctx->last_seen, ctx->t0);

    exc = PyObject_CallFunction(efn, "((skkfk))", tcname, ctx->id, ctx->tid,
        cumdiff * tickfactor(), ctx->sched_cnt);
    if (!exc) {
        PyErr_Print();
        return 1; // abort enumeration
    }

    Py_DECREF(exc);
    return 0;
}

static PyObject*
enum_context_stats(PyObject *self, PyObject *args)
{
    PyObject *enumfn;

    if (!yapphavestats) {
        Py_RETURN_NONE;
    }

    if (!PyArg_ParseTuple(args, "O", &enumfn)) {
        PyErr_SetString(YappiProfileError, "invalid param to enum_context_stats");
        return NULL;
    }

    if (!PyCallable_Check(enumfn)) {
        PyErr_SetString(YappiProfileError, "enum function must be callable");
        return NULL;
    }

    henum(contexts, _ctxenumstat, enumfn);

    Py_RETURN_NONE;
}

int _pit_filtered(_pit *pt, _ctxfuncenumarg *eargs)
{
    _fast_func_stat_filter filter;

    filter = eargs->enum_args->func_filter;

    if (filter.name) {
        if (!PyObject_RichCompareBool(pt->name, filter.name, Py_EQ)) {
            return 1;
        }
    }

    if (filter.modname) {
        if (!PyObject_RichCompareBool(pt->modname, filter.modname, Py_EQ)) {
            return 1;
        }
    }

    return 0;
}

static int
_pitenumstat(_hitem *item, void *arg)
{
    _pit *pt;
    PyObject *exc;
    PyObject *children;
    PyObject *ctx_name;
    _pit_children_info *pci;
    _ctxfuncenumarg *eargs;

    children = NULL;
    pt = (_pit *)item->val;
    eargs = (_ctxfuncenumarg *)arg;

    if (_pit_filtered(pt, eargs)) {
        return 0;
    }

    // do not show builtin pits if specified
    if  ((!flags.builtins) && (pt->builtin)) {
        return 0;
    }

    // convert children function index list to PyList
    children = PyList_New(0);
    pci = pt->children;
    while(pci) {
        PyObject *stats_tuple;
        // normalize tsubtotal. tsubtotal being negative is an expected situation.
        if (pci->tsubtotal < 0) {
            pci->tsubtotal = 0;
        }
        if (pci->callcount == 0)
            pci->callcount = 1;
        stats_tuple = Py_BuildValue("Ikkff", pci->index, pci->callcount,
                pci->nonrecursive_callcount, _normt(pci->ttotal),
                _normt(pci->tsubtotal));
        PyList_Append(children, stats_tuple);
        Py_DECREF(stats_tuple);
        pci = (_pit_children_info *)pci->next;
    }
    // normalize values
    if (pt->tsubtotal < 0)
        pt->tsubtotal = 0;
    if (pt->callcount == 0)
        pt->callcount = 1;

    // normally _yapp_callback sets ctx->name if not set but there is a possibility
    // that we might end up here even it is not set. We also do not default it to 
    // Py_None as we will Py_CLEAR() on it later on.
    ctx_name = eargs->ctx->name;
    if (!ctx_name) {
        ctx_name = Py_None;
    }

    exc = PyObject_CallFunction(eargs->enum_args->enumfn, "((OOkkkIffIOkOkO))", 
                        pt->name, pt->modname, pt->lineno, pt->callcount,
                        pt->nonrecursive_callcount, pt->builtin, 
                        _normt(pt->ttotal), _normt(pt->tsubtotal),
                        pt->index, children, eargs->ctx->id, ctx_name,
                        eargs->tag, pt->fn_descriptor);

    if (!exc) {
        PyErr_Print();
        Py_XDECREF(children);
        return 1; // abort enumeration
    }

    Py_DECREF(exc);
    Py_XDECREF(children);

    return 0;
}

static int
_tagenumstat(_hitem *item, void *arg)
{
    _htab *pits;
    uintptr_t current_tag;
    _ctxfuncenumarg *eargs;
    _fast_func_stat_filter filter;

    current_tag = item->key;
    eargs = (_ctxfuncenumarg *)arg;
    filter = eargs->enum_args->func_filter;
    eargs->tag = current_tag;

    if (filter.tag) {
        if (current_tag != PyLong_AsVoidPtr(filter.tag)) {
            return 0;
        }
    }

    pits =  (_htab *)item->val;
    henum(pits, _pitenumstat, arg);

    return 0;
}

static int 
_ctxfuncenumstat(_hitem *item, void *arg)
{
    _ctxfuncenumarg ext_args;
    PyObject *filtered_ctx_id;

    ext_args.ctx = (_ctx *)item->val; 
    ext_args.enum_args = (_ctxenumarg *)arg;
    ext_args.tag = 0; // default

    filtered_ctx_id = ext_args.enum_args->func_filter.ctx_id;
    if (filtered_ctx_id) {
        if (ext_args.ctx->id != PyLong_AsVoidPtr(filtered_ctx_id)) {
            return 0;
        }
    }

    henum(ext_args.ctx->tags, _tagenumstat, &ext_args);

    return 0;
}

static PyObject*
start(PyObject *self, PyObject *args)
{
    if (yapprunning)
        Py_RETURN_NONE;

    if (!PyArg_ParseTuple(args, "ii", &flags.builtins, &flags.multicontext))
        return NULL;

    if (!_start())
        // error
        return NULL;

    Py_RETURN_NONE;
}

static PyObject*
stop(PyObject *self, PyObject *args)
{
    _stop();
    Py_RETURN_NONE;
}

int
_filterdict_to_statfilter(PyObject *filter_dict, _fast_func_stat_filter* filter)
{
    // we use a _statfilter struct to hold the struct and not to always 
    // as the filter_dict to get its values for each pit enumerated. This
    // is for performance
    PyObject *fv;

    fv = PyDict_GetItemString(filter_dict, "tag");
    if (fv) {
        PyLong_AsVoidPtr(fv);
        if (PyErr_Occurred()) {
            yerr("invalid tag passed to get_func_stats.");
            filter->tag = NULL;
            return 0;
        }
        filter->tag = fv;
    }
    fv = PyDict_GetItemString(filter_dict, "ctx_id");
    if (fv) {
        PyLong_AsVoidPtr(fv);
        if (PyErr_Occurred()) {
            yerr("invalid ctx_id passed to get_func_stats.");
            filter->ctx_id = NULL;
            return 0;
        }
        filter->ctx_id = fv;
    }

    fv = PyDict_GetItemString(filter_dict, "name");
    if (fv) {
        filter->name = fv;
    }
    fv = PyDict_GetItemString(filter_dict, "module");
    if (fv) {
        filter->modname = fv;
    }

    return 1;
}

static PyObject*
enum_func_stats(PyObject *self, PyObject *args)
{
    PyObject *filter_dict;
    _ctxenumarg ext_args;

    filter_dict = NULL;
    memset(&ext_args, 0, sizeof(_ctxenumarg)); // make sure everything is NULLed

    if (!yapphavestats) {
        Py_RETURN_NONE;
    }

    if (!PyArg_ParseTuple(args, "OO", &ext_args.enumfn, &filter_dict)) {
        PyErr_SetString(YappiProfileError, "invalid param to enum_func_stats");
        return NULL;
    }

    if (!PyDict_Check(filter_dict)) {
        PyErr_SetString(YappiProfileError, "filter param should be a dict");
        return NULL;
    }

    if (!PyCallable_Check(ext_args.enumfn)) {
        PyErr_SetString(YappiProfileError, "enum function must be callable");
        return NULL;
    }

    if (!_filterdict_to_statfilter(filter_dict, &ext_args.func_filter)) {
        return NULL;
    }

    henum(contexts, _ctxfuncenumstat, &ext_args);

    Py_RETURN_NONE;
}

static PyObject *
is_running(PyObject *self, PyObject *args)
{
    return Py_BuildValue("i", yapprunning);
}

static PyObject *
get_mem_usage(PyObject *self, PyObject *args)
{
    return Py_BuildValue("l", ymemusage());
}

static PyObject *
set_tag_callback(PyObject *self, PyObject *args)
{
    PyObject* new_callback;

    if (!PyArg_ParseTuple(args, "O", &new_callback)) {
        return NULL;
    }

    if (new_callback == Py_None) {
        Py_CLEAR(tag_callback);
        Py_RETURN_NONE;
    } else if (!PyCallable_Check(new_callback)) {
        PyErr_SetString(PyExc_TypeError, "callback should be a function.");
        return NULL;
    }
    Py_XDECREF(tag_callback);
    Py_INCREF(new_callback);
    tag_callback = new_callback;

    Py_RETURN_NONE;
}

static PyObject *
set_context_id_callback(PyObject *self, PyObject *args)
{
    PyObject* new_callback;

    if (!PyArg_ParseTuple(args, "O", &new_callback)) {
        return NULL;
    }

    if (new_callback == Py_None) {
        Py_CLEAR(context_id_callback);
        Py_RETURN_NONE;
    } else if (!PyCallable_Check(new_callback)) {
        PyErr_SetString(PyExc_TypeError, "callback should be a function.");
        return NULL;
    }
    Py_XDECREF(context_id_callback);
    Py_INCREF(new_callback);
    context_id_callback = new_callback;

    Py_RETURN_NONE;
}

static PyObject *
set_context_name_callback(PyObject *self, PyObject *args)
{
    PyObject* new_callback;
    if (!PyArg_ParseTuple(args, "O", &new_callback)) {
        return NULL;
    }

    if (new_callback == Py_None) {
        Py_CLEAR(context_name_callback);
        Py_RETURN_NONE;
    } else if (!PyCallable_Check(new_callback)) {
        PyErr_SetString(PyExc_TypeError, "callback should be a function.");
        return NULL;
    }
    Py_XDECREF(context_name_callback);
    Py_INCREF(new_callback);
    context_name_callback = new_callback;

    Py_RETURN_NONE;
}

static PyObject *
set_context_backend(PyObject *self, PyObject *args)
{
    _ctx_type_t input_type;
    
    if (!PyArg_ParseTuple(args, "i", &input_type)) {
        return NULL;
    }
    
    if (input_type == ctx_type)
    {
        Py_RETURN_NONE;
    }
    
    if (yapphavestats) {
        PyErr_SetString(YappiProfileError, "backend type cannot be changed while stats are available. clear stats first.");
        return NULL;
    }

    if (input_type != NATIVE_THREAD && input_type != GREENLET)  {
        PyErr_SetString(YappiProfileError, "Invalid backend type.");
        return NULL;
    }

    ctx_type = input_type;

    Py_RETURN_NONE;
}

static PyObject *
set_test_timings(PyObject *self, PyObject *args)
{
    if (!PyArg_ParseTuple(args, "O", &test_timings)) {
        return NULL;
    }

    if (!PyDict_Check(test_timings))
    {
        PyErr_SetString(YappiProfileError, "timings should be dict.");
        return NULL;
    }
    Py_INCREF(test_timings);

    Py_RETURN_NONE;
}

static PyObject *
set_clock_type(PyObject *self, PyObject *args)
{
    clock_type_t clock_type;
    
    if (!PyArg_ParseTuple(args, "i", &clock_type)) {
        return NULL;
    }
    
    // return silently if same clock_type
    if (clock_type == get_timing_clock_type())
    {
        Py_RETURN_NONE;
    }
    
    if (yapphavestats) {
        PyErr_SetString(YappiProfileError, "clock type cannot be changed previous stats are available. clear the stats first.");
        return NULL;
    }
    
    if (!set_timing_clock_type(clock_type)) {
        PyErr_SetString(YappiProfileError, "Invalid clock type.");
        return NULL;
    }   
    
    Py_RETURN_NONE;
}

static PyObject *
get_context_backend(PyObject *self, PyObject *args)
{
    if (ctx_type == GREENLET) {
        return Py_BuildValue("s", "GREENLET");
    }  else {
        return Py_BuildValue("s", "NATIVE_THREAD");
    }
}

static PyObject *
get_clock_time(PyObject *self, PyObject *args)
{
    return PyFloat_FromDouble(tickfactor() * tickcount());
}

static PyObject *
get_clock_info(PyObject *self, PyObject *args)
{
    PyObject *api = NULL;
    PyObject *result = NULL;
    PyObject *resolution = NULL;
    clock_type_t clk_type;

    result = PyDict_New();
    
    clk_type = get_timing_clock_type();
    if (clk_type == WALL_CLOCK) {
#if defined(_WINDOWS)
        api = Py_BuildValue("s", "queryperformancecounter");
        resolution = Py_BuildValue("s", "100ns");    
#else
        api = Py_BuildValue("s", "gettimeofday");
        resolution = Py_BuildValue("s", "100ns");
#endif
    }  else {
#if defined(USE_CLOCK_TYPE_GETTHREADTIMES)
        api = Py_BuildValue("s", "getthreadtimes");
        resolution = Py_BuildValue("s", "100ns");
#elif defined(USE_CLOCK_TYPE_THREADINFO)
        api = Py_BuildValue("s", "threadinfo");
        resolution = Py_BuildValue("s", "1000ns");
#elif defined(USE_CLOCK_TYPE_CLOCKGETTIME)
        api = Py_BuildValue("s", "clockgettime");
        resolution = Py_BuildValue("s", "1ns");
#elif defined(USE_CLOCK_TYPE_RUSAGE)
        api = Py_BuildValue("s", "getrusage");
        resolution = Py_BuildValue("s", "1000ns");
#endif
    }

    PyDict_SetItemString(result, "api", api);
    PyDict_SetItemString(result, "resolution", resolution);

    Py_XDECREF(api);
    Py_XDECREF(resolution);
    return result;
}

static PyObject *
get_clock_type(PyObject *self, PyObject *args)
{
    clock_type_t clk_type;
    
    clk_type = get_timing_clock_type();
    if (clk_type == WALL_CLOCK) {
        return Py_BuildValue("s", "wall");
    }  else {
        return Py_BuildValue("s", "cpu");
    }
}


static PyObject*
get_start_flags(PyObject *self, PyObject *args)
{
    PyObject *result = NULL;
    PyObject *profile_builtins = NULL;
    PyObject *profile_multicontext = NULL;
    
    if (!yapphavestats) {
        Py_RETURN_NONE;
    }

    profile_builtins = Py_BuildValue("i", flags.builtins);
    profile_multicontext = Py_BuildValue("i", flags.multicontext);
    result = PyDict_New();
    PyDict_SetItemString(result, "profile_builtins", profile_builtins);
    PyDict_SetItemString(result, "profile_multicontext", profile_multicontext);
    
    Py_XDECREF(profile_builtins);
    Py_XDECREF(profile_multicontext);
    return result;
}

static PyObject*
_pause(PyObject *self, PyObject *args)
{
    if (yapprunning) {
        paused = 1;
        _stop();
    }

    Py_RETURN_NONE;
}

static PyObject*
_resume(PyObject *self, PyObject *args)
{
    if (paused)
    {
        paused = 0;
        if (!_start())
            // error
            return NULL;
    }

    Py_RETURN_NONE;
}

static PyMethodDef yappi_methods[] = {
    {"start", start, METH_VARARGS, NULL},
    {"stop", stop, METH_NOARGS, NULL},
    {"enum_func_stats", enum_func_stats, METH_VARARGS, NULL},
    {"enum_context_stats", enum_context_stats, METH_VARARGS, NULL},
    {"enum_thread_stats", enum_context_stats, METH_VARARGS, NULL},
    {"clear_stats", clear_stats, METH_VARARGS, NULL},
    {"is_running", is_running, METH_VARARGS, NULL},
    {"get_clock_type", get_clock_type, METH_VARARGS, NULL},
    {"set_clock_type", set_clock_type, METH_VARARGS, NULL},
    {"get_clock_time", get_clock_time, METH_VARARGS, NULL},
    {"get_clock_info", get_clock_info, METH_VARARGS, NULL},
    {"get_mem_usage", get_mem_usage, METH_VARARGS, NULL},
    {"set_context_id_callback", set_context_id_callback, METH_VARARGS, NULL},
    {"set_tag_callback", set_tag_callback, METH_VARARGS, NULL},
    {"set_context_name_callback", set_context_name_callback, METH_VARARGS, NULL},
    {"set_context_backend", set_context_backend, METH_VARARGS, NULL},
    {"get_context_backend", get_context_backend, METH_VARARGS, NULL},
    {"_get_start_flags", get_start_flags, METH_VARARGS, NULL}, // for internal usage.
    {"_set_test_timings", set_test_timings, METH_VARARGS, NULL}, // for internal usage.
    {"_profile_event", profile_event, METH_VARARGS, NULL}, // for internal usage.
    {"_pause", _pause, METH_VARARGS, NULL}, // for internal usage.
    {"_resume", _resume, METH_VARARGS, NULL}, // for internal usage.
    {NULL, NULL, 0, NULL}      /* sentinel */
};

static struct PyModuleDef _yappi_module = {
    PyModuleDef_HEAD_INIT,
    "_yappi",
    _yappi__doc__,
    -1,
    yappi_methods,
    NULL,
    NULL,
    NULL,
    NULL
};

PyMODINIT_FUNC
PyInit__yappi(void)
{
    PyObject *m, *d;

    m = PyModule_Create(&_yappi_module);
    if (m == NULL)
        return NULL;

    d = PyModule_GetDict(m);
    YappiProfileError = PyErr_NewException("_yappi.error", NULL, NULL);
    PyDict_SetItemString(d, "error", YappiProfileError);

    // init the profiler memory and internal constants
    yappinitialized = 0;
    yapphavestats = 0;
    yapprunning = 0;
    paused = 0;
    flags.builtins = 0;
    flags.multicontext = 0;
    test_timings = NULL;

    SUPPRESS_WARNING(_DebugPrintObjects);
    SUPPRESS_WARNING(_print_coros);

    if (!_init_profiler()) {
        PyErr_SetString(YappiProfileError, "profiler cannot be initialized.");
        return NULL;
    }

    return m;
}
