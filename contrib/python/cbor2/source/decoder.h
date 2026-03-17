#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdbool.h>
#include <stdint.h>

// Default readahead buffer size for streaming reads
#define CBOR2_DEFAULT_READ_SIZE 4096

typedef struct {
    PyObject_HEAD
    PyObject *read;    // cached read() method of fp
    PyObject *tag_hook;
    PyObject *object_hook;
    PyObject *shareables;
    PyObject *stringref_namespace;
    PyObject *str_errors;
    bool immutable;
    Py_ssize_t shared_index;
    Py_ssize_t decode_depth;

    // Readahead buffer for streaming
    char *readahead;            // allocated buffer
    Py_ssize_t readahead_size;  // size of allocated buffer
    Py_ssize_t read_pos;        // current position in buffer
    Py_ssize_t read_len;        // valid bytes in buffer
} CBORDecoderObject;

extern PyTypeObject CBORDecoderType;

PyObject * CBORDecoder_new(PyTypeObject *, PyObject *, PyObject *);
int CBORDecoder_init(CBORDecoderObject *, PyObject *, PyObject *);
PyObject * CBORDecoder_decode(CBORDecoderObject *);
