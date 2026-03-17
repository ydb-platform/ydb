/* See https://www.python-ldap.org/ for details. */

#ifndef __h_ldapcontrol
#define __h_ldapcontrol

#include "common.h"

void LDAPinit_control(PyObject *d);
void LDAPControl_List_DEL(LDAPControl **);
int LDAPControls_from_object(PyObject *, LDAPControl ***);
PyObject *LDAPControls_to_List(LDAPControl **ldcs);

#endif /* __h_ldapcontrol */
