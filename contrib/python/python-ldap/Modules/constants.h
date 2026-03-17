/* See https://www.python-ldap.org/ for details. */

#ifndef __h_constants_
#define __h_constants_

#include "common.h"

extern int LDAPinit_constants(PyObject *m);
extern PyObject *LDAPconstant(int);

extern PyObject *LDAPexception_class;
extern PyObject *LDAPerror(LDAP *);
extern PyObject *LDAPraise_for_message(LDAP *, LDAPMessage *m);
PyObject *LDAPerr(int errnum);

#ifndef LDAP_CONTROL_PAGE_OID
#define LDAP_CONTROL_PAGE_OID "1.2.840.113556.1.4.319"
#endif /* !LDAP_CONTROL_PAGE_OID */

#ifndef LDAP_CONTROL_VALUESRETURNFILTER
#define LDAP_CONTROL_VALUESRETURNFILTER "1.2.826.0.1.3344810.2.3"       /* RFC 3876 */
#endif /* !LDAP_CONTROL_VALUESRETURNFILTER */

#endif /* __h_constants_ */
