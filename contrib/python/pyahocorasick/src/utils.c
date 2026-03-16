/*
    This is part of pyahocorasick Python module.

    Helpers functions.
    This file is included directly.

    Author    : Wojciech Muła, wojciech_mula@poczta.onet.pl
    WWW       : http://0x80.pl
    License   : public domain
*/

//#define MEMORY_DEBUG
#ifdef MEMORY_DEBUG
#ifndef MEMORY_DUMP_PATH
#   define MEMORY_DUMP_PATH "memory.dump"
#endif
const char* debug_path = MEMORY_DUMP_PATH;
FILE* debug_file;
int memory_dump          = 1;   // dump to file
int alloc_num            = 0;   // id of allocation
int alloc_fail           = -1;  // id of allocation that will fail
int alloc_trap_on_fail   = 0;   // rather failing, execute trap (for gdb use)
int realloc_num          = 0;   // id of allocation
int realloc_fail         = -1;  // id of allocation that will fail
int realloc_trap_on_fail = 0;   // rather failing, execute trap (for gdb use)

static int
env_getint(const char* name, int def) {
    const char* val = getenv(name);
    if (val != NULL)
        return atoi(val);
    else
        return def;
}

static int
env_exists(const char* name) {
    return (getenv(name) != NULL);
}

static
void initialize_memory_debug(void) {
    if (env_exists("ALLOC_NODUMP")) {
        memory_dump = 0;
    }

    alloc_fail = env_getint("ALLOC_FAIL", alloc_fail);
    realloc_fail = env_getint("REALLOC_FAIL", realloc_fail);

    alloc_trap_on_fail = env_exists("ALLOC_TRAP");
    realloc_trap_on_fail = env_exists("REALLOC_TRAP");

    if (memory_dump) {
        debug_file = fopen(debug_path, "wt");
        if (debug_file == NULL) {
            PyErr_WarnEx(PyExc_RuntimeWarning, "Cannot open file, logging on stderr", 1);
            debug_file = stderr;
        }
    }
}
#endif

void* memory_alloc(Py_ssize_t size) {
#ifdef MEMORY_DEBUG
    if (alloc_num == alloc_fail) {
        if (alloc_trap_on_fail) {
            __builtin_trap();
        }

        printf("DEBUG: allocation #%d failed\n", alloc_num);
        alloc_num += 1;
        return NULL;
    }
#endif
    void* res = PyMem_Malloc(size);

#ifdef MEMORY_DEBUG
    alloc_num += 1;
    if (memory_dump)
        fprintf(debug_file, "A %d %p %ld\n", alloc_num, res, size);
#endif

    return res;
}


void* memory_realloc(void* ptr, size_t size) {
#ifdef MEMORY_DEBUG
    if (realloc_num == realloc_fail) {
        if (realloc_trap_on_fail) {
            __builtin_trap();
        }

        printf("DEBUG: reallocation #%d failed\n", realloc_num);
        realloc_num += 1;
        return NULL;
    }
#endif
    void* res = PyMem_Realloc(ptr, size);

#ifdef MEMORY_DEBUG
    realloc_num += 1;
    if (memory_dump) {
        fprintf(debug_file, "R %d %p %p %ld\n", realloc_num, ptr, res, size);
    }
#endif

    return res;
}


void memory_free(void* ptr) {
#ifdef MEMORY_DEBUG
    if (memory_dump)
        fprintf(debug_file, "F %p\n", ptr);
#endif
    PyMem_Free(ptr);
}


void memory_safefree(void* ptr) {
    if (ptr != NULL) {
        memory_free(ptr);
    }
}


#if !defined(PY3K) || !defined(AHOCORASICK_UNICODE)
//  define when pymod_get_string makes a copy of string
#   define INPUT_KEEPS_COPY
#endif

#if defined INPUT_KEEPS_COPY
#    define maybe_free(flag, word) memory_free(word);
#    define maybe_decref(flag, ref)
#elif defined PEP393_UNICODE
#    define maybe_free(flag, word) if (flag) { memory_free(word); }
#    define maybe_decref(flag, ref) if (ref && !flag) { Py_DECREF(ref); }
#else
#    define maybe_free(flag, word)
#    define maybe_decref(flag, ref) if (ref) { Py_DECREF(ref); }
#endif

/* returns bytes or unicode internal buffer */
static PyObject*
pymod_get_string(PyObject* obj, TRIE_LETTER_TYPE** word, Py_ssize_t* wordlen, bool* is_copy) {

#ifdef INPUT_KEEPS_COPY
    Py_ssize_t i;
    char* bytes;
#endif

#if defined PEP393_UNICODE
    if (F(PyUnicode_Check)(obj)) {
    PyUnicode_READY(obj);
    if (PyUnicode_KIND(obj) == PyUnicode_4BYTE_KIND) {
            *word = (TRIE_LETTER_TYPE*)(PyUnicode_4BYTE_DATA(obj));
            *wordlen = PyUnicode_GET_LENGTH(obj);
            *is_copy = false;
            Py_INCREF(obj);

        return obj;
    } else {
        *word = PyUnicode_AsUCS4Copy(obj);
        *wordlen = PyUnicode_GET_LENGTH(obj);
        *is_copy = true;
        // No INCREF - we have our copy
        return obj;
    }
    }
    else {
    PyErr_SetString(PyExc_TypeError, "string expected");
    return NULL;
    }
#elif defined PY3K
#   ifdef AHOCORASICK_UNICODE
        if (F(PyUnicode_Check)(obj)) {
            *word = (TRIE_LETTER_TYPE*)(PyUnicode_AS_UNICODE(obj));
            *wordlen = PyUnicode_GET_SIZE(obj);
            Py_INCREF(obj);
            return obj;
        }
        else {
            PyErr_SetString(PyExc_TypeError, "string expected");
            return NULL;
        }
#   else
#       ifndef INPUT_KEEPS_COPY
#           error "defines inconsistency"
#       endif
        if (F(PyBytes_Check)(obj)) {
            *wordlen = PyBytes_GET_SIZE(obj);
            *word    = (TRIE_LETTER_TYPE*)memory_alloc(*wordlen * TRIE_LETTER_SIZE);
            if (*word == NULL) {
                PyErr_NoMemory();
                return NULL;
            }

            bytes = PyBytes_AS_STRING(obj);
            for (i=0; i < *wordlen; i++) {
                (*word)[i] = bytes[i];
            }
            // Note: there is no INCREF
            return obj;
        }
        else {
            PyErr_SetString(PyExc_TypeError, "bytes expected");
            return NULL;
        }
#   endif
#else // PY_MAJOR_VERSION == 3
#       ifndef INPUT_KEEPS_COPY
#           error "defines inconsistency"
#       endif
    if (F(PyString_Check)(obj)) {
        *wordlen = PyString_GET_SIZE(obj);
        *word    = (TRIE_LETTER_TYPE*)memory_alloc(*wordlen * TRIE_LETTER_SIZE);
        if (*word == NULL) {
            PyErr_NoMemory();
            return NULL;
        }


        bytes = PyString_AS_STRING(obj);
        for (i=0; i < *wordlen; i++) {
            (*word)[i] = bytes[i];
        };

        Py_INCREF(obj);
        return obj;
    } else {
        PyErr_SetString(PyExc_TypeError, "string required");
        return NULL;
    }
#endif
}

static bool
__read_sequence__from_tuple(PyObject* obj, TRIE_LETTER_TYPE** word, Py_ssize_t* wordlen) {
    Py_ssize_t i;
    Py_ssize_t size = PyTuple_GET_SIZE(obj);
    TRIE_LETTER_TYPE* tmpword;

    tmpword = (TRIE_LETTER_TYPE*)memory_alloc(size * TRIE_LETTER_SIZE);
    if (UNLIKELY(tmpword == NULL)) {
        PyErr_NoMemory();
        return false;
    }

    for (i=0; i < size; i++) {
        Py_ssize_t value = F(PyNumber_AsSsize_t)(F(PyTuple_GetItem)(obj, i), PyExc_ValueError);
        if (value == -1 && PyErr_Occurred()) {
            PyErr_Format(PyExc_ValueError, "item #%zd is not a number", i);
            memory_free(tmpword);
            return false;
        }


        // TODO: both min and max values should be configured
#if TRIE_LETTER_SIZE == 4
    #define MAX_VAL 4294967295l
#else
    #define MAX_VAL 65535ul
#endif
        if (value < 0 || value > MAX_VAL) {
            PyErr_Format(PyExc_ValueError, "item #%zd: value %zd outside range [%d..%lu]", i, value, 0, MAX_VAL);
            memory_free(tmpword);
            return false;
        }

        tmpword[i] = (TRIE_LETTER_TYPE)value;
    }

    *word = tmpword;
    *wordlen = size;

    return true;
}


static bool
pymod_get_sequence(PyObject* obj, TRIE_LETTER_TYPE** word, Py_ssize_t* wordlen) {
    if (LIKELY(F(PyTuple_Check)(obj))) {
        return __read_sequence__from_tuple(obj, word, wordlen);
    } else {
        PyErr_Format(PyExc_TypeError, "argument is not a supported sequence type");
        return false;
    }
}


/* parse optional indexes used in few functions [start, [end]] */
static int
pymod_parse_start_end(
    PyObject* args,
    int idx_start, int idx_end,
    const Py_ssize_t min, const Py_ssize_t max,
    Py_ssize_t* Start, Py_ssize_t* End
) {
    PyObject* obj;
#define start (*Start)
#define end (*End)

    start   = min;
    end     = max;

    // first argument
    obj = F(PyTuple_GetItem)(args, idx_start);
    if (obj == NULL) {
        PyErr_Clear();
        return 0;
    }

    obj = F(PyNumber_Index)(obj);
    if (obj == NULL)
        return -1;

    start = F(PyNumber_AsSsize_t)(obj, PyExc_IndexError);
    Py_DECREF(obj);
    if (start == -1 and PyErr_Occurred())
        return -1;

    if (start < 0)
        start = max + start;

    if (start < min or start >= max) {
        PyErr_Format(PyExc_IndexError, "start index not in range %zd..%zd", min, max);
        return -1;
    }

    // second argument
    obj = F(PyTuple_GetItem)(args, idx_end);
    if (obj == NULL) {
        PyErr_Clear();
        return 0;
    }

    obj = F(PyNumber_Index)(obj);
    if (obj == NULL)
        return -1;

    end = F(PyNumber_AsSsize_t)(obj, PyExc_IndexError);
    Py_DECREF(obj);
    if (end == -1 and PyErr_Occurred())
        return -1;

    if (end < 0)
        end = max - 1 + end;

    if (end < min or end > max) {
        PyErr_Format(PyExc_IndexError, "end index not in range %zd..%zd", min, max);
        return -1;
    }

    return 0;

#undef start
#undef end
}


void init_input(struct Input* input) {
    input->word = NULL;
    input->py_word = NULL;
}


bool prepare_input(PyObject* self, PyObject* tuple, struct Input* input) {
#define automaton ((Automaton*)self)
    if (automaton->key_type == KEY_STRING) {
        input->py_word = pymod_get_string(tuple, &input->word, &input->wordlen, &input->is_copy);
        if (not input->py_word)
            return false;
    } else {
        input->is_copy = true; // we always create a copy of sequence
        input->py_word = NULL;
        if (not pymod_get_sequence(tuple, &input->word, &input->wordlen)) {
            return false;
        }
    }
#undef automaton

    return true;
}


bool prepare_input_from_tuple(PyObject* self, PyObject* args, int index, struct Input* input) {
    PyObject* tuple;

    tuple = F(PyTuple_GetItem)(args, index);
    if (tuple)
        return prepare_input(self, tuple, input);
    else
        return false;
}


void destroy_input(struct Input* input) {
    maybe_decref(input->is_copy, input->py_word)
    maybe_free(input->is_copy, input->word)
}


void assign_input(struct Input* dst, struct Input* src) {

    dst->wordlen    = src->wordlen;
    dst->word       = src->word;
    dst->py_word    = src->py_word; // Note: there is no INCREF
}
