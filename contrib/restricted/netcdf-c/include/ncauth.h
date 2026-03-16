/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/

/*
Common authorization tracking.
Currently for DAP2 and DAP4 protocols.
Every curl connection will need a copy of this.
*/

#ifndef NCAUTH_H
#define NCAUTH_H

/* Need these support includes */
#include "ncrc.h"

typedef struct NCauth {
    struct curlflags {
        int proto_https; /* is https: supported? */
	int encode; /*turn on CURLOPT_ACCEPT_ENCODING*/
	int verbose; /*CURLOPT_VERBOSE*/
	int timeout; /*CURLOPT_TIMEOUT*/
	int connecttimeout; /*CURLOPT_CONNECTTIMEOUT*/
	int maxredirs; /*CURLOPT_MAXREDIRS*/
	char* useragent; /*CURLOPT_USERAGENT*/
	int cookiejarcreated;
	char* cookiejar; /*CURLOPT_COOKIEJAR,CURLOPT_COOKIEFILE*/
	char* netrc; /*CURLOPT_NETRC,CURLOPT_NETRC_FILE*/
    } curlflags;
    struct ssl {
	int   verifypeer; /* CURLOPT_SSL_VERIFYPEER;
                             do not do this when cert might be self-signed
                             or temporarily incorrect */
	int   verifyhost; /* CURLOPT_SSL_VERIFYHOST; for client-side verification */
        char* certificate; /*CURLOPT_SSLCERT*/
	char* key; /*CURLOPT_SSLKEY*/
	char* keypasswd; /*CURLOPT_SSLKEYPASSWD*/
        char* cainfo; /* CURLOPT_CAINFO; certificate authority */
	char* capath;  /*CURLOPT_CAPATH*/
    } ssl;
    struct proxy {
	char *host; /*CURLOPT_PROXY*/
	int port; /*CURLOPT_PROXYPORT*/
	char* user; /*CURLOPT_PROXYUSERNAME*/
	char* pwd; /*CURLOPT_PROXYPASSWORD*/
    } proxy;
    struct credentials {
	char *user; /*CURLOPT_USERNAME*/
	char *pwd; /*CURLOPT_PASSWORD*/
    } creds;
    char* s3profile;
} NCauth;

#if defined(__cplusplus)
extern "C" {
#endif

extern int NC_authsetup(NCauth**, NCURI*);
extern void NC_authfree(NCauth*);
extern char* NC_combinehostport(NCURI*);
extern int NC_parsecredentials(const char* userpwd, char** userp, char** pwdp);

extern int NC_authgets3creds(NCauth* auth, const char* profile, const char** accessidp, const char** secretkeyp);

#if defined(__cplusplus)
}
#endif

#endif /*NCAUTH_H*/
