/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 2019-2022 The OpenLDAP Foundation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

/* openldap.h - Header for openldap specific interfaces. */

#ifndef _OPENLDAP_H
#define _OPENLDAP_H 1

#include <ldap.h>

LDAP_BEGIN_DECL

#define LDAP_PROTO_TCP 1 /* ldap://  */
#define LDAP_PROTO_UDP 2 /* reserved */
#define LDAP_PROTO_IPC 3 /* ldapi:// */
#define LDAP_PROTO_EXT 4 /* user-defined socket/sockbuf */

LDAP_F( int )
ldap_init_fd LDAP_P((
	ber_socket_t fd,
	int proto,
	LDAP_CONST char *url,
	LDAP **ldp ));

LDAP_END_DECL

#endif /* _OPENLDAP_H */
