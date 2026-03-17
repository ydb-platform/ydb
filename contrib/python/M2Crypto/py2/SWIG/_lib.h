/* Copyright (c) 1999 Ng Pheng Siong. All rights reserved. */
/* $Id$ */

#include <openssl/bn.h>

typedef struct _blob {
	unsigned char *data;
	int len;
} Blob;

Blob *blob_new(int len, const char *errmsg);
Blob *blob_copy(Blob *from, const char *errmsg);
void blob_free(Blob *blob);

static int m2_PyObject_AsReadBuffer(PyObject *obj, const void **buffer,
                                       Py_ssize_t *buffer_len);
static int m2_PyObject_AsReadBufferInt(PyObject *obj, const void **buffer,
                                       int *buffer_len);
static int m2_PyString_AsStringAndSizeInt(PyObject *obj, char **s, int *len);

static BIGNUM* m2_PyObject_AsBIGNUM(PyObject* value, PyObject* _py_exc) ;

/* Always use these two together, to correctly handle non-memoryview objects. */
static int m2_PyObject_GetBufferInt(PyObject *obj, Py_buffer *view, int flags);
static void m2_PyBuffer_Release(PyObject *obj, Py_buffer *view);

int bn_gencb_callback(int p, int n, BN_GENCB *gencb);
int passphrase_callback(char *buf, int num, int v, void *userdata);

void lib_init(void);

