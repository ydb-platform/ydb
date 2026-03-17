/* See https://www.python-ldap.org/ for details. */

#ifndef __h_message
#define __h_message

#include "common.h"

extern PyObject *LDAPmessage_to_python(LDAP *ld, LDAPMessage *m, int add_ctrls,
                                       int add_intermediates);

#endif /* __h_message_ */
