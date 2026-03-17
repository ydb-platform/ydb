/* common utility macros
 * See https://www.python-ldap.org/ for details. */

#ifndef __h_common
#define __h_common

#define PY_SSIZE_T_CLEAN

#include "Python.h"

#if defined(HAVE_CONFIG_H)
#error #include "config.h"
#endif

#include <lber.h>
#include <ldap.h>
#include <ldap_features.h>

#if LDAP_VENDOR_VERSION < 20400
#error Current python-ldap requires OpenLDAP 2.4.x
#endif

#if LDAP_VENDOR_VERSION >= 20448
  /* openldap.h with ldap_init_fd() was introduced in 2.4.48
   * see https://bugs.openldap.org/show_bug.cgi?id=8671
   */
#define HAVE_LDAP_INIT_FD 1
#include <openldap.h>
#elif (defined(__APPLE__) && (LDAP_VENDOR_VERSION == 20428))
/* macOS system libldap 2.4.28 does not have ldap_init_fd symbol */
#undef HAVE_LDAP_INIT_FD
#else
  /* ldap_init_fd() has been around for a very long time
   * SSSD has been defining the function for a while, so it's probably OK.
   */
#define HAVE_LDAP_INIT_FD 1
#define LDAP_PROTO_TCP 1
#define LDAP_PROTO_UDP 2
#define LDAP_PROTO_IPC 3
extern int ldap_init_fd(ber_socket_t fd, int proto, LDAP_CONST char *url,
                        LDAP **ldp);
#endif

#if defined(MS_WINDOWS)
#include <winsock.h>
#else /* unix */
#include <netdb.h>
#include <sys/time.h>
#include <sys/types.h>
#endif

#include <string.h>
#define streq( a, b ) \
	( (*(a)==*(b)) && 0==strcmp(a,b) )

extern PyObject *LDAPerror_TypeError(const char *, PyObject *);

void LDAPadd_methods(PyObject *d, PyMethodDef *methods);

#define PyNone_Check(o) ((o) == Py_None)

/* Py2/3 compatibility */
#if PY_VERSION_HEX >= 0x03000000
/* In Python 3, alias PyInt to PyLong */
#define PyInt_FromLong PyLong_FromLong
#endif

#endif /* __h_common_ */
