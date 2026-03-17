/*
 * -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*-
 * vim: syntax=c sts=4 sw=4
 *
 * ASN1_OBJECT manipulation functions from OBJ_obj2txt(3SSL).
 *
 * Pavel Shramov
 * IMEC MSU
 */
%{
#include <openssl/objects.h>
%}

%apply Pointer NONNULL { ASN1_OBJECT * };
%apply Pointer NONNULL { const char * };

%rename(obj_nid2obj) OBJ_nid2obj;
extern ASN1_OBJECT * OBJ_nid2obj(int n);
%rename(obj_nid2ln)  OBJ_nid2ln;
extern const char *  OBJ_nid2ln(int n);
%rename(obj_nid2sn)  OBJ_nid2sn;
extern const char *  OBJ_nid2sn(int n);

%rename(obj_obj2nid) OBJ_obj2nid;
extern int OBJ_obj2nid(const ASN1_OBJECT *o);

%rename(obj_ln2nid) OBJ_ln2nid;
extern int OBJ_ln2nid(const char *ln);
%rename(obj_sn2nid) OBJ_sn2nid;
extern int OBJ_sn2nid(const char *sn);

%rename(obj_txt2nid) OBJ_txt2nid;
extern int OBJ_txt2nid(const char *s);

%rename(obj_txt2obj) OBJ_txt2obj;
extern ASN1_OBJECT * OBJ_txt2obj(const char *s, int no_name);


%rename(_obj_obj2txt) OBJ_obj2txt;
extern int OBJ_obj2txt(char *, int, const ASN1_OBJECT *, int);


%inline %{
/*
   From the manpage for OBJ_obt2txt ():
   BUGS
      OBJ_obj2txt() is awkward and messy to use: it doesn’t follow the
      convention of other OpenSSL functions where the buffer can be set
      to NULL to determine the amount of data that should be written.
      Instead buf must point to a valid buffer and buf_len should be set
      to a positive value. A buffer length of 80 should be more than
      enough to handle any OID encountered in practice.

   The first call to OBJ_obj2txt () therefore passes a non-NULL dummy
   buffer. This wart is reportedly removed in OpenSSL 0.9.8b, although
   the manpage has not been updated.

   OBJ_obj2txt always prints \0 at the end. But the return value
   is the number of "good" bytes written. So memory is allocated for
   len + 1 bytes but only len bytes are marshalled to python.
*/
PyObject *obj_obj2txt(const ASN1_OBJECT *obj, int no_name)
{
    int len;
    PyObject *ret;
    char *buf;
    char dummy[1];

    len = OBJ_obj2txt(dummy, 1, obj, no_name);
    if (len < 0) {
        m2_PyErr_Msg(PyExc_RuntimeError);
        return NULL;
    } else if (len == 0) {
        /* XXX: For OpenSSL prior to 0.9.8b.

          Changes between 0.9.8a and 0.9.8b  [04 May 2006]
          ...
          *) Several fixes and enhancements to the OID generation code. The old code
             sometimes allowed invalid OIDs (1.X for X >= 40 for example), couldn't
             handle numbers larger than ULONG_MAX, truncated printing and had a
             non standard OBJ_obj2txt() behaviour.
             [Steve Henson]
        */

        len = 80;
    }

    buf = PyMem_Malloc(len + 1);
    len = OBJ_obj2txt(buf, len + 1, obj, no_name);

    ret = PyBytes_FromStringAndSize(buf, len);

    PyMem_Free(buf);

    return ret;
}
%}
