/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/


#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_STDARG_H
#include <stdarg.h>
#endif
#include "netcdf.h"
#include "ncbytes.h"
#include "ncuri.h"
#include "nclog.h"
#include "ncpathmgr.h"
#include "ncs3sdk.h"
#include "ncauth.h"

#ifdef _MSC_VER
#include <windows.h>
#endif

#include "ncrc.h"

#undef DEBUG

#undef MEMCHECK
#define MEMCHECK(x) if((x)==NULL) {goto nomem;} else {}

/* Define the curl flag defaults in envv style */
static const char* AUTHDEFAULTS[] = {
"HTTP.SSL.VERIFYPEER","-1", /* Use default */
"HTTP.SSL.VERIFYHOST","-1", /* Use default */
"HTTP.TIMEOUT","1800", /*seconds */ /* Long but not infinite */
"HTTP.CONNECTTIMEOUT","50", /*seconds */ /* Long but not infinite */
"HTTP.ENCODE","1", /* Use default */
NULL,
};

/* Forward */
static int setauthfield(NCauth* auth, const char* flag, const char* value);
static void setdefaults(NCauth*);

/**************************************************/
/* External Entry Points */

int
NC_parseproxy(NCauth* auth, const char* surl)
{
    int ret = NC_NOERR;
    NCURI* uri = NULL;
    if(surl == NULL || strlen(surl) == 0)
	return (NC_NOERR); /* nothing there*/
    if(ncuriparse(surl,&uri))
	return (NC_EURL);
    auth->proxy.user = uri->user;
    auth->proxy.pwd = uri->password;
    auth->proxy.host = strdup(uri->host);
    if(uri->port != NULL)
        auth->proxy.port = atoi(uri->port);
    else
        auth->proxy.port = 80;
    return (ret);
}

char*
NC_combinehostport(NCURI* uri)
{
    size_t len;
    char* host = NULL;
    char* port = NULL;
    char* hp = NULL;
    if(uri == NULL) return NULL;
    host = uri->host;
    port = uri->port;
    if(uri == NULL || host == NULL) return NULL;
    if(port != NULL && strlen(port) == 0) port = NULL;
    len = strlen(host);
    if(port != NULL) len += (1+strlen(port));
    hp = (char*)malloc(len+1);
    if(hp == NULL) return NULL;
    snprintf(hp, len+1, "%s%s%s", host, port ? ":" : "", port ? port : "");
    return hp;
}

int
NC_authsetup(NCauth** authp, NCURI* uri)
{
    int ret = NC_NOERR;
    char* uri_hostport = NULL;
    NCauth* auth = NULL;
    struct AWSprofile* ap = NULL;

    if(uri != NULL)
      uri_hostport = NC_combinehostport(uri);
    else
      {ret = NC_EDAP; goto done;}  /* Generic EDAP error. */
    if((auth=calloc(1,sizeof(NCauth)))==NULL)
        {ret = NC_ENOMEM; goto done;}

    setdefaults(auth);

    /* Note, we still must do this function even if
       ncrc_getglobalstate()->rc.ignore is set in order
       to getinfo e.g. host+port  from url
    */

    setauthfield(auth,"HTTP.VERBOSE",
			NC_rclookup("HTTP.VERBOSE",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.TIMEOUT",
			NC_rclookup("HTTP.TIMEOUT",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.CONNECTTIMEOUT",
			NC_rclookup("HTTP.CONNECTTIMEOUT",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.USERAGENT",
			NC_rclookup("HTTP.USERAGENT",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.COOKIEFILE",
			NC_rclookup("HTTP.COOKIEFILE",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.COOKIE_FILE",
			NC_rclookup("HTTP.COOKIE_FILE",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.COOKIEJAR",
			NC_rclookup("HTTP.COOKIEJAR",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.COOKIE_JAR",
			NC_rclookup("HTTP.COOKIE_JAR",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.PROXY.SERVER",
			NC_rclookup("HTTP.PROXY.SERVER",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.PROXY_SERVER",
			NC_rclookup("HTTP.PROXY_SERVER",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.SSL.CERTIFICATE",
			NC_rclookup("HTTP.SSL.CERTIFICATE",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.SSL.KEY",
			NC_rclookup("HTTP.SSL.KEY",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.SSL.KEYPASSWORD",
			NC_rclookup("HTTP.SSL.KEYPASSWORD",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.SSL.CAINFO",
			NC_rclookup("HTTP.SSL.CAINFO",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.SSL.CAPATH",
			NC_rclookup("HTTP.SSL.CAPATH",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.SSL.VERIFYPEER",
			NC_rclookup("HTTP.SSL.VERIFYPEER",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.SSL.VERIFYHOST",
			NC_rclookup("HTTP.SSL.VERIFYHOST",uri_hostport,uri->path));
    /* Alias for VERIFYHOST + VERIFYPEER */
    setauthfield(auth,"HTTP.SSL.VALIDATE",
			NC_rclookup("HTTP.SSL.VALIDATE",uri_hostport,uri->path));
    setauthfield(auth,"HTTP.NETRC",
			NC_rclookup("HTTP.NETRC",uri_hostport,uri->path));

    { /* Handle various cases for user + password */
      /* First, see if the user+pwd was in the original url */
      char* user = NULL;
      char* pwd = NULL;
      if(uri->user != NULL && uri->password != NULL) {
	    user = uri->user;
	    pwd = uri->password;
      } else {
   	    user = NC_rclookup("HTTP.CREDENTIALS.USER",uri_hostport,uri->path);
	    pwd = NC_rclookup("HTTP.CREDENTIALS.PASSWORD",uri_hostport,uri->path);
      }
      if(user != NULL && pwd != NULL) {
        user = strdup(user); /* so we can consistently reclaim */
        pwd = strdup(pwd);
      } else {
	    /* Could not get user and pwd, so try USERPASSWORD */
	    const char* userpwd = NC_rclookup("HTTP.CREDENTIALS.USERPASSWORD",uri_hostport,uri->path);
	    if(userpwd != NULL) {
      	        if((ret = NC_parsecredentials(userpwd,&user,&pwd))) goto done;
	    }
      }
      setauthfield(auth,"HTTP.CREDENTIALS.USERNAME",user);
      setauthfield(auth,"HTTP.CREDENTIALS.PASSWORD",pwd);
      nullfree(user);
      nullfree(pwd);
    }

    /* Get the Default profile */
    if((ret=NC_authgets3profile("no",&ap))) goto done;
    if(ap == NULL)
        if((ret=NC_authgets3profile("default",&ap))) goto done;
    if(ap != NULL)
        auth->s3profile = strdup(ap->name);
    else
        auth->s3profile = NULL;

    if(authp) {*authp = auth; auth = NULL;}
done:
    nullfree(uri_hostport);
    return (ret);
}

void
NC_authfree(NCauth* auth)
{
    if(auth == NULL) return;
    if(auth->curlflags.cookiejarcreated) {
#ifdef _MSC_VER
        DeleteFile(auth->curlflags.cookiejar);
#else
        remove(auth->curlflags.cookiejar);
#endif
    }
    nullfree(auth->curlflags.useragent);
    nullfree(auth->curlflags.cookiejar);
    nullfree(auth->curlflags.netrc);
    nullfree(auth->ssl.certificate);
    nullfree(auth->ssl.key);
    nullfree(auth->ssl.keypasswd);
    nullfree(auth->ssl.cainfo);
    nullfree(auth->ssl.capath);
    nullfree(auth->proxy.host);
    nullfree(auth->proxy.user);
    nullfree(auth->proxy.pwd);
    nullfree(auth->creds.user);
    nullfree(auth->creds.pwd);
    nullfree(auth->s3profile);
    nullfree(auth);
}

/**************************************************/

static int
setauthfield(NCauth* auth, const char* flag, const char* value)
{
    int ret = NC_NOERR;
    if(value == NULL) goto done;
    if(strcmp(flag,"HTTP.ENCODE")==0) {
        if(atoi(value)) {auth->curlflags.encode = 1;} else {auth->curlflags.encode = 0;}
#ifdef DEBUG
        nclog(NCLOGNOTE,"HTTP.encode: %ld", (long)auth->curlflags.encode);
#endif
    }
    if(strcmp(flag,"HTTP.VERBOSE")==0) {
        if(atoi(value)) auth->curlflags.verbose = 1;
#ifdef DEBUG
            nclog(NCLOGNOTE,"HTTP.VERBOSE: %ld", (long)auth->curlflags.verbose);
#endif
    }
    if(strcmp(flag,"HTTP.TIMEOUT")==0) {
        if(atoi(value)) auth->curlflags.timeout = atoi(value);
#ifdef DEBUG
            nclog(NCLOGNOTE,"HTTP.TIMEOUT: %ld", (long)auth->curlflags.timeout);
#endif
    }
    if(strcmp(flag,"HTTP.CONNECTTIMEOUT")==0) {
        if(atoi(value)) auth->curlflags.connecttimeout = atoi(value);
#ifdef DEBUG
            nclog(NCLOGNOTE,"HTTP.CONNECTTIMEOUT: %ld", (long)auth->curlflags.connecttimeout);
#endif
    }
    if(strcmp(flag,"HTTP.USERAGENT")==0) {
        if(atoi(value)) auth->curlflags.useragent = strdup(value);
        MEMCHECK(auth->curlflags.useragent);
#ifdef DEBUG
            nclog(NCLOGNOTE,"HTTP.USERAGENT: %s", auth->curlflags.useragent);
#endif
    }
    if(
	strcmp(flag,"HTTP.COOKIEFILE")==0
        || strcmp(flag,"HTTP.COOKIE_FILE")==0
        || strcmp(flag,"HTTP.COOKIEJAR")==0
        || strcmp(flag,"HTTP.COOKIE_JAR")==0
      ) {
	nullfree(auth->curlflags.cookiejar);
        auth->curlflags.cookiejar = strdup(value);
        MEMCHECK(auth->curlflags.cookiejar);
#ifdef DEBUG
            nclog(NCLOGNOTE,"HTTP.COOKIEJAR: %s", auth->curlflags.cookiejar);
#endif
    }
    if(strcmp(flag,"HTTP.PROXY.SERVER")==0 || strcmp(flag,"HTTP.PROXY_SERVER")==0) {
        ret = NC_parseproxy(auth,value);
        if(ret != NC_NOERR) goto done;
#ifdef DEBUG
            nclog(NCLOGNOTE,"HTTP.PROXY.SERVER: %s", value);
#endif
    }
    if(strcmp(flag,"HTTP.SSL.VERIFYPEER")==0) {
	int v;
        if((v = atoi(value))) {
	    auth->ssl.verifypeer = v;
#ifdef DEBUG
                nclog(NCLOGNOTE,"HTTP.SSL.VERIFYPEER: %d", v);
#endif
	}
    }
    if(strcmp(flag,"HTTP.SSL.VERIFYHOST")==0) {
	int v;
        if((v = atoi(value))) {
	    auth->ssl.verifyhost = v;
#ifdef DEBUG
                nclog(NCLOGNOTE,"HTTP.SSL.VERIFYHOST: %d", v);
#endif
	}
    }
    if(strcmp(flag,"HTTP.SSL.VALIDATE")==0) {
        if(atoi(value)) {
	    auth->ssl.verifypeer = 1;
	    auth->ssl.verifyhost = 2;
	}
    }

    if(strcmp(flag,"HTTP.SSL.CERTIFICATE")==0) {
	nullfree(auth->ssl.certificate);
        auth->ssl.certificate = strdup(value);
        MEMCHECK(auth->ssl.certificate);
#ifdef DEBUG
            nclog(NCLOGNOTE,"HTTP.SSL.CERTIFICATE: %s", auth->ssl.certificate);
#endif
    }

    if(strcmp(flag,"HTTP.SSL.KEY")==0) {
	nullfree(auth->ssl.key);
        auth->ssl.key = strdup(value);
        MEMCHECK(auth->ssl.key);
#ifdef DEBUG
            nclog(NCLOGNOTE,"HTTP.SSL.KEY: %s", auth->ssl.key);
#endif
    }

    if(strcmp(flag,"HTTP.SSL.KEYPASSWORD")==0) {
	nullfree(auth->ssl.keypasswd) ;
        auth->ssl.keypasswd = strdup(value);
        MEMCHECK(auth->ssl.keypasswd);
#ifdef DEBUG
            nclog(NCLOGNOTE,"HTTP.SSL.KEYPASSWORD: %s", auth->ssl.keypasswd);
#endif
    }

    if(strcmp(flag,"HTTP.SSL.CAINFO")==0) {
	nullfree(auth->ssl.cainfo) ;
        auth->ssl.cainfo = strdup(value);
        MEMCHECK(auth->ssl.cainfo);
#ifdef DEBUG
            nclog(NCLOGNOTE,"HTTP.SSL.CAINFO: %s", auth->ssl.cainfo);
#endif
    }

    if(strcmp(flag,"HTTP.SSL.CAPATH")==0) {
	nullfree(auth->ssl.capath) ;
        auth->ssl.capath = strdup(value);
        MEMCHECK(auth->ssl.capath);
#ifdef DEBUG
            nclog(NCLOGNOTE,"HTTP.SSL.CAPATH: %s", auth->ssl.capath);
#endif
    }
    if(strcmp(flag,"HTTP.NETRC")==0) {
        nullfree(auth->curlflags.netrc);
        auth->curlflags.netrc = strdup(value);
        MEMCHECK(auth->curlflags.netrc);
#ifdef DEBUG
            nclog(NCLOGNOTE,"HTTP.NETRC: %s", auth->curlflags.netrc);
#endif
    }

    if(strcmp(flag,"HTTP.CREDENTIALS.USERNAME")==0) {
        nullfree(auth->creds.user);
        auth->creds.user = strdup(value);
        MEMCHECK(auth->creds.user);
    }
    if(strcmp(flag,"HTTP.CREDENTIALS.PASSWORD")==0) {
        nullfree(auth->creds.pwd);
        auth->creds.pwd = strdup(value);
        MEMCHECK(auth->creds.pwd);
    }

done:
    return (ret);

nomem:
    return (NC_ENOMEM);
}

/*
Given form user:pwd, parse into user and pwd
and do %xx unescaping
*/
int
NC_parsecredentials(const char* userpwd, char** userp, char** pwdp)
{
  char* user = NULL;
  char* pwd = NULL;

  if(userpwd == NULL)
	return NC_EINVAL;
  user = strdup(userpwd);
  if(user == NULL)
	return NC_ENOMEM;
  pwd = strchr(user,':');
  if(pwd == NULL) {
    free(user);
	return NC_EINVAL;
  }
  *pwd = '\0';
  pwd++;
  if(userp)
	*userp = ncuridecode(user);
  if(pwdp)
	*pwdp = ncuridecode(pwd);
  free(user);
  return NC_NOERR;
}

static void
setdefaults(NCauth* auth)
{
    int ret = NC_NOERR;
    const char** p;
    for(p=AUTHDEFAULTS;*p;p+=2) {
	ret = setauthfield(auth,p[0],p[1]);
	if(ret) {
            nclog(NCLOGERR, "RC file defaulting failed for: %s=%s",p[0],p[1]);
	}
    }
}
