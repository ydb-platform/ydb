/* See https://www.python-ldap.org/ for details. */

#ifndef __h_berval
#define __h_berval

#include "common.h"

PyObject *LDAPberval_to_object(const struct berval *bv);
PyObject *LDAPberval_to_unicode_object(const struct berval *bv);

#endif /* __h_berval_ */
