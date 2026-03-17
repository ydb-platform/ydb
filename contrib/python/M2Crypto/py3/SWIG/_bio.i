/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
/* Copyright (c) 1999 Ng Pheng Siong. All rights reserved.
 *
 * Portions created by Open Source Applications Foundation (OSAF) are
 * Copyright (C) 2004-2005 OSAF. All Rights Reserved.
 * Author: Heikki Toivonen
 *
 * Copyright 2018 Daniel Wozniak. All Rights Reserved.*/
/* $Id$ */

%{
#include <openssl/bio.h>
%}

%apply Pointer NONNULL { BIO * };
%apply Pointer NONNULL { BIO_METHOD * };

%rename(bio_s_bio) BIO_s_bio;
extern BIO_METHOD *BIO_s_bio(void);
%rename(bio_s_mem) BIO_s_mem;
extern BIO_METHOD *BIO_s_mem(void);
%rename(bio_s_socket) BIO_s_socket;
extern BIO_METHOD *BIO_s_socket(void);
%rename(bio_f_ssl) BIO_f_ssl;
extern BIO_METHOD *BIO_f_ssl(void);
%rename(bio_f_buffer) BIO_f_buffer;
extern BIO_METHOD *BIO_f_buffer(void);
%rename(bio_f_cipher) BIO_f_cipher;
extern BIO_METHOD *BIO_f_cipher(void);

%rename(bio_new) BIO_new;
extern BIO *BIO_new(BIO_METHOD *);
%rename(bio_new_socket) BIO_new_socket;
extern BIO *BIO_new_socket(int, int);
%rename(bio_new_fd) BIO_new_pyfd;
%rename(bio_new_pyfd) BIO_new_pyfd;
%rename(bio_free_all) BIO_free_all;
%threadallow BIO_free_all;
extern void BIO_free_all(BIO *);
%rename(bio_dup_chain) BIO_dup_chain;
extern BIO *BIO_dup_chain(BIO *);

%rename(bio_push) BIO_push;
extern BIO *BIO_push(BIO *, BIO *);
%rename(bio_pop) BIO_pop;
extern BIO *BIO_pop(BIO *);

%rename(bio_eof) BIO_eof;
extern int BIO_eof(BIO *);

%constant int bio_noclose             = BIO_NOCLOSE;
%constant int bio_close               = BIO_CLOSE;
%constant int BIO_FLAGS_READ          = 0x01;
%constant int BIO_FLAGS_WRITE         = 0x02;
%constant int BIO_FLAGS_IO_SPECIAL    = 0x04;
%constant int BIO_FLAGS_RWS = (BIO_FLAGS_READ|BIO_FLAGS_WRITE|BIO_FLAGS_IO_SPECIAL);
%constant int BIO_FLAGS_SHOULD_RETRY  = 0x08;
%constant int BIO_FLAGS_MEM_RDONLY    = 0x200;

%warnfilter(454) _bio_err;
%inline %{
static PyObject *_bio_err;


void pyfd_init(void);

void bio_init(PyObject *bio_err) {
    Py_INCREF(bio_err);
    _bio_err = bio_err;
    pyfd_init();
}

int bio_free(BIO *bio) {
    int ret;

    Py_BEGIN_ALLOW_THREADS
    ret = BIO_free(bio);
    Py_END_ALLOW_THREADS
    if (ret == 0) {
        m2_PyErr_Msg(_bio_err);
    }
    return ret;
}

BIO * bio_new_file(const char *filename, const char *mode) {
    BIO *ret;

    Py_BEGIN_ALLOW_THREADS
    ret = BIO_new_file(filename, mode);
    Py_END_ALLOW_THREADS

    if (ret == NULL) {
        m2_PyErr_Msg(_bio_err);
    }

    return ret;
}

BIO *bio_new_pyfile(PyObject *pyfile, int bio_close) {
    FILE *fp = NULL;
    BIO *bio = NULL;

    fp = PyFile_AsFile(pyfile);

    bio = BIO_new_fp(fp, bio_close);

    /* returns NULL if error occurred */
    if (bio == NULL) {
        /* Find out the name of the file so we can have good error
         * message. */
        PyObject *pyname = m2_PyFile_Name(pyfile);
        char *name = PyBytes_AsString(pyname);

        if (name == NULL) {
            PyErr_Format(_bio_err,
                         "Opening of the new BIO on file failed!");
        }
        else {
            PyErr_Format(_bio_err,
                         "Opening of the new BIO on file %s failed!", name);
        }
        Py_DECREF(pyname);
    }
    return bio;
}

PyObject *bio_read(BIO *bio, int num) {
    PyObject *blob;
    void *buf;
    int r;

    if (!(buf = PyMem_Malloc(num))) {
        PyErr_SetString(PyExc_MemoryError, "bio_read");
        return NULL;
    }
    Py_BEGIN_ALLOW_THREADS
    r = BIO_read(bio, buf, num);
    Py_END_ALLOW_THREADS
    if (r < 0) {
        PyMem_Free(buf);
        if (ERR_peek_error()) {
            m2_PyErr_Msg(_bio_err);
            return NULL;
        }
        Py_RETURN_NONE;
    }

    blob = PyBytes_FromStringAndSize(buf, r);

    PyMem_Free(buf);
    return blob;
}

PyObject *bio_gets(BIO *bio, int num) {
    PyObject *blob;
    void *buf;
    int r;

    if (!(buf = PyMem_Malloc(num))) {
        PyErr_SetString(PyExc_MemoryError, "bio_gets");
        return NULL;
    }
    Py_BEGIN_ALLOW_THREADS
    r = BIO_gets(bio, buf, num);
    Py_END_ALLOW_THREADS
    if (r < 1) {
        PyMem_Free(buf);
        if (ERR_peek_error()) {
            m2_PyErr_Msg(_bio_err);
            return NULL;
        }
        Py_RETURN_NONE;
    }

    blob = PyBytes_FromStringAndSize(buf, r);

    PyMem_Free(buf);
    return blob;
}

int bio_write(BIO *bio, PyObject *from) {
    const void *fbuf;
    int flen = 0, ret;

    if (m2_PyObject_AsReadBufferInt(from, &fbuf, &flen) == -1)
        return -1;

    Py_BEGIN_ALLOW_THREADS
    ret = BIO_write(bio, fbuf, flen);
    Py_END_ALLOW_THREADS
    if (ret < 0) {
        if (ERR_peek_error()) {
            m2_PyErr_Msg(_bio_err);
            return -1;
        }
    }
    return ret;
}

/* XXX Casting size_t to int. */
int bio_ctrl_pending(BIO *bio) {
    return (int)BIO_ctrl_pending(bio);
}

int bio_ctrl_wpending(BIO *bio) {
    return (int)BIO_ctrl_wpending(bio);
}

int bio_ctrl_get_write_guarantee(BIO *a) {
    return BIO_ctrl_get_write_guarantee(a);
}

int bio_reset(BIO *bio) {
    return (int)BIO_reset(bio);
}
%}

%threadallow bio_flush;
%inline %{
int bio_flush(BIO *bio) {
    return (int)BIO_flush(bio);
}

int bio_seek(BIO *bio, int offset) {
    return (int)BIO_seek(bio, offset);
}

int bio_tell(BIO* bio) {
    return BIO_tell(bio);
}

void bio_set_flags(BIO *bio, int flags) {
    BIO_set_flags(bio, flags);
}

int bio_get_flags(BIO *bio) {
    return BIO_get_flags(bio);
}

/*
 * sets the cipher of BIO @param b to c using key @param key and IV @iv.
 * @param enc should be set to 1 for encryption and zero to decryption.
 *
 * @return
 *
 */
PyObject *bio_set_cipher(BIO *b, EVP_CIPHER *c, PyObject *key, PyObject *iv, int op) {
    const void *kbuf, *ibuf;
    Py_ssize_t klen, ilen;

    if ((m2_PyObject_AsReadBuffer(key, &kbuf, &klen) == -1)
        || (m2_PyObject_AsReadBuffer(iv, &ibuf, &ilen) == -1)) {
            PyErr_SetString(PyExc_ValueError, "Reading of key or IV from buffer failed.");
            return NULL;
        }

#if OPENSSL_VERSION_NUMBER >= 0x30000000L
    if (!BIO_set_cipher(b, (const EVP_CIPHER *)c,
            (unsigned char *)kbuf, (unsigned char *)ibuf, op)) {
        PyErr_SetString(PyExc_OSError, "Setting of cipher failed.");
        return NULL;
    }
#else
    // https://www.openssl.org/docs/man1.0.2/man3/BIO_set_cipher.html
    // BIO_set_cipher in OpenSSL < 3.0 doesn't return any value (error or otherwise).
    BIO_set_cipher(b, (const EVP_CIPHER *)c,
        (unsigned char *)kbuf, (unsigned char *)ibuf, op);
#endif
    Py_RETURN_NONE;
}

int bio_set_mem_eof_return(BIO *b, int v) {
    return (int)BIO_set_mem_eof_return(b, v);
}

int bio_get_fd(BIO *bio) {
    return BIO_get_fd(bio, NULL);
}
%}

%warnfilter(454) methods_fdp;
%threadallow bio_do_handshake;
%inline %{
int bio_do_handshake(BIO *bio) {
    return BIO_do_handshake(bio);
}

/* macro */
int bio_make_bio_pair(BIO* b1, BIO* b2) {
    return BIO_make_bio_pair(b1, b2);
}

int bio_set_write_buf_size(BIO* b, size_t size) {
    return BIO_set_write_buf_size(b, size);
}

int bio_should_retry(BIO* a) {
    return BIO_should_retry(a);
}

int bio_should_read(BIO* a) {
    return BIO_should_read(a);
}

int bio_should_write(BIO* a) {
    return BIO_should_write(a);
}

/* Macros for things not defined before 1.1.0 */
#if OPENSSL_VERSION_NUMBER < 0x10100000L
static BIO_METHOD *
BIO_meth_new( int type, const char *name )
{
    BIO_METHOD *method;
    Py_BEGIN_ALLOW_THREADS
    method = PyMem_Malloc(sizeof(BIO_METHOD));
    Py_END_ALLOW_THREADS
    memset( method, 0, sizeof(BIO_METHOD) );

    method->type = type;
    method->name = name;

    return method;
}

static void
BIO_meth_free( BIO_METHOD *meth )
{
    if ( meth == NULL ) {
        return;
    }

    PyMem_Free(meth);
}
#define BIO_meth_set_write(m, f) (m)->bwrite = (f)
#define BIO_meth_set_read(m, f) (m)->bread = (f)
#define BIO_meth_set_puts(m, f) (m)->bputs = (f)
#define BIO_meth_set_gets(m, f) (m)->bgets = (f)
#define BIO_meth_set_ctrl(m, f) (m)->ctrl = (f)
#define BIO_meth_set_create(m, f) (m)->create = (f)
#define BIO_meth_set_destroy(m, f) (m)->destroy = (f)
#define BIO_set_shutdown(b, x) (b)->shutdown = x
#define BIO_get_shutdown(b) (b)->shutdown
#define BIO_set_init(b, x)    b->init = x
#define BIO_get_init(b) (b)->init
#define BIO_set_data(b, x)    b->ptr = x
#define BIO_clear_flags(b, x)    b->flags &= ~(x)
#define BIO_get_data(b)    b->ptr
#endif

/* implment custom BIO_s_pyfd */

#ifdef _WIN32
#  define clear_sys_error()       SetLastError(0)
/* Linux doesn't use underscored calls yet */
#  define open(p, f, m) _open(p, f, m)
#  define read(f, b, n) _read(f, b, n)
#  define write(f, b, n) _write(f, b, n)
#  define close(f) _close(f)
#  define lseek(fd, o, w) _lseek(fd, o, w)
#else
# define clear_sys_error()       errno=0
#endif

typedef struct pyfd_struct {
    int fd;
} BIO_PYFD_CTX;

/* Setting up methods_fdp */
static BIO_METHOD *methods_fdp;

static int pyfd_write(BIO *b, const char *in, int inl) {
    int ret, fd;

    if (BIO_get_fd(b, &fd) == -1) {
        PyErr_SetString(_bio_err, "BIO has not been initialized.");
        return -1;
    }
    clear_sys_error();
    ret = write(fd, in, inl);
    BIO_clear_retry_flags(b);
    if (ret <= 0) {
        if (BIO_fd_should_retry(ret))
            BIO_set_retry_write(b);
    }
    return ret;
}

static int pyfd_read(BIO *b, char *out, int outl) {
    int ret = 0, fd;

    if (BIO_get_fd(b, &fd) == -1) {
        PyErr_SetString(_bio_err, "BIO has not been initialized.");
        return -1;
    }
    if (out != NULL) {
        clear_sys_error();
        ret = read(fd, out, outl);
        BIO_clear_retry_flags(b);
        if (ret <= 0) {
            if (BIO_fd_should_retry(ret))
                BIO_set_retry_read(b);
        }
    }
    return ret;
}

static int pyfd_puts(BIO *bp, const char *str) {
    int n, ret;

    n = strlen(str);
    ret = pyfd_write(bp, str, n);
    return ret;
}

static int pyfd_gets(BIO *bp, char *buf, int size) {
    int ret = 0;
    char *ptr = buf;
    char *end = buf + size - 1;

    /* See
    https://github.com/openssl/openssl/pull/3442
    We were here just repeating a bug from OpenSSL
    */
    while (ptr < end && pyfd_read(bp, ptr, 1) > 0) {
        if (*ptr++ == '\n')
           break;
    }

    ptr[0] = '\0';

    if (buf[0] != '\0')
        ret = strlen(buf);
    return ret;
}

static int pyfd_new(BIO* b) {
    BIO_PYFD_CTX* ctx;

    ctx = OPENSSL_zalloc(sizeof(*ctx));
    if (ctx == NULL)
        return 0;

    ctx->fd = -1;

    BIO_set_data(b, ctx);
    BIO_set_shutdown(b, 0);
    BIO_set_init(b, 1);

    return 1;
    }

static int pyfd_free(BIO* b) {
    BIO_PYFD_CTX* ctx;

    if (b == 0)
        return 0;

    ctx = BIO_get_data(b);
    if (ctx == NULL)
        return 0;

    if (BIO_get_shutdown(b) && BIO_get_init(b))
        close(ctx->fd);

    BIO_set_data(b, NULL);
    BIO_set_shutdown(b, 0);
    BIO_set_init(b, 0);

    OPENSSL_free(ctx);

    return 1;
}

static long pyfd_ctrl(BIO *b, int cmd, long num, void *ptr) {
    BIO_PYFD_CTX* ctx;
    int *ip;
    long ret = 1;

    ctx = BIO_get_data(b);
    if (ctx == NULL)
        return 0;

    switch (cmd) {
    case BIO_CTRL_RESET:
        num = 0;
    case BIO_C_FILE_SEEK:
        ret = (long)lseek(ctx->fd, num, 0);
        break;
    case BIO_C_FILE_TELL:
    case BIO_CTRL_INFO:
        ret = (long)lseek(ctx->fd, 0, 1);
        break;
    case BIO_C_SET_FD:
        pyfd_free(b);
        if (*((int *)ptr) > -1) {
            if (!pyfd_new(b) || !(ctx = BIO_get_data(b)))
                return 0;
            ctx->fd = *((int *)ptr);
            BIO_set_shutdown(b, (int)num);
            BIO_set_init(b, 1);
            }
        break;
    case BIO_C_GET_FD:
        if (BIO_get_init(b)) {
            ip = (int *)ptr;
            if (ip != NULL)
                *ip = ctx->fd;
            ret = ctx->fd;
        } else
            ret = -1;
        break;
    case BIO_CTRL_GET_CLOSE:
        ret = BIO_get_shutdown(b);
        break;
    case BIO_CTRL_SET_CLOSE:
        BIO_set_shutdown(b, (int)num);
        break;
    case BIO_CTRL_PENDING:
    case BIO_CTRL_WPENDING:
        ret = 0;
        break;
    case BIO_CTRL_DUP:
    case BIO_CTRL_FLUSH:
        ret = 1;
        break;
    default:
        ret = 0;
        break;
    }
    return ret;
}

void pyfd_init(void) {
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
    methods_fdp = BIO_meth_new(
        BIO_get_new_index()|BIO_TYPE_DESCRIPTOR|BIO_TYPE_SOURCE_SINK,
        "python file descriptor");
#else
    methods_fdp = BIO_meth_new(
        100 |BIO_TYPE_DESCRIPTOR|BIO_TYPE_SOURCE_SINK,
        "python file descriptor");
#endif

    BIO_meth_set_write(methods_fdp, pyfd_write);
    BIO_meth_set_read(methods_fdp, pyfd_read);
    BIO_meth_set_puts(methods_fdp, pyfd_puts);
    BIO_meth_set_gets(methods_fdp, pyfd_gets);
    BIO_meth_set_ctrl(methods_fdp, pyfd_ctrl);
    BIO_meth_set_create(methods_fdp, pyfd_new);
    BIO_meth_set_destroy(methods_fdp, pyfd_free);
}

BIO* BIO_new_pyfd(int fd, int close_flag) {
    BIO *ret;

    ret = BIO_new(methods_fdp);
    BIO_set_fd(ret, fd, close_flag);
    return ret;
    }
%}

