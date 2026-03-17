/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

/* WARNING: oc2/occurlfunctions.c and libdap4/d4curlfunctions.c
should be merged since they are essentially the same file.
In the meantime, changes to one should be propagated to the other.
*/

#include "d4includes.h"
#include "d4curlfunctions.h"

#define MAX_REDIRECTS 20L

/* Mnemonic */
#define OPTARG uintptr_t

/* Condition on libcurl version */
/* Set up an alias as needed */
#ifndef HAVE_CURLOPT_KEYPASSWD
#define CURLOPT_KEYPASSWD CURLOPT_SSLKEYPASSWD
#endif

#define D4BUFFERSIZE "HTTP.READ.BUFFERSIZE"
#define D4KEEPALIVE "HTTP.KEEPALIVE"

#ifdef HAVE_CURLOPT_BUFFERSIZE
#ifndef CURL_MAX_READ_SIZE
#define CURL_MAX_READ_SIZE  (512*1024)
#endif
#endif

#define SETCURLOPT(state,flag,value) {if(set_curlopt(state,flag,(void*)value) != NC_NOERR) {goto done;}}

/* forward */
static int set_curlflag(NCD4INFO*, int flag);
static int set_curlopt(NCD4INFO*, int flag, void* value);

/*
Set a specific curl flag; primary wrapper for curl_easy_setopt
*/
static int
set_curlopt(NCD4INFO* state, int flag, void* value)
{
    int ret = NC_NOERR;
    CURLcode cstat = CURLE_OK;
    cstat = curl_easy_setopt(state->curl->curl,flag,value);
    if(cstat != CURLE_OK)
	ret = NC_ECURL;
    return THROW(ret);
}

/*
Update a specific flag from state
*/
static int
set_curlflag(NCD4INFO* state, int flag)
{
    int ret = NC_NOERR;
    switch (flag) {
    case CURLOPT_USERPWD: /* Do both user and pwd */
        if(state->auth->creds.user != NULL
           && state->auth->creds.pwd != NULL) {
	    SETCURLOPT(state, CURLOPT_USERNAME, state->auth->creds.user);
	    SETCURLOPT(state, CURLOPT_PASSWORD, state->auth->creds.pwd);
            SETCURLOPT(state, CURLOPT_HTTPAUTH, (OPTARG)CURLAUTH_ANY);
	}
	break;
    case CURLOPT_COOKIEJAR: case CURLOPT_COOKIEFILE:
        if(state->auth->curlflags.cookiejar) {
	    /* Assume we will read and write cookies to same place */
	    SETCURLOPT(state, CURLOPT_COOKIEJAR, state->auth->curlflags.cookiejar);
	    SETCURLOPT(state, CURLOPT_COOKIEFILE, state->auth->curlflags.cookiejar);
        }
	break;
    case CURLOPT_NETRC: case CURLOPT_NETRC_FILE:
	if(state->auth->curlflags.netrc) {
	    SETCURLOPT(state, CURLOPT_NETRC, (OPTARG)CURL_NETRC_OPTIONAL);
	    /* IF HTTP.NETRC is set with "", then assume the default .netrc file (which is apparently CWD) */
	    if(strlen(state->auth->curlflags.netrc)>0)
	        SETCURLOPT(state, CURLOPT_NETRC_FILE, state->auth->curlflags.netrc);
        }
	break;
    case CURLOPT_VERBOSE:
	if(state->auth->curlflags.verbose)
	    SETCURLOPT(state, CURLOPT_VERBOSE, (OPTARG)1L);
	break;
    case CURLOPT_TIMEOUT:
	if(state->auth->curlflags.timeout)
	    SETCURLOPT(state, CURLOPT_TIMEOUT, (OPTARG)((long)state->auth->curlflags.timeout));
	break;
    case CURLOPT_CONNECTTIMEOUT:
	if(state->auth->curlflags.connecttimeout)
	    SETCURLOPT(state, CURLOPT_CONNECTTIMEOUT, (OPTARG)((long)state->auth->curlflags.connecttimeout));
	break;
    case CURLOPT_USERAGENT:
        if(state->auth->curlflags.useragent)
	    SETCURLOPT(state, CURLOPT_USERAGENT, state->auth->curlflags.useragent);
	break;
    case CURLOPT_FOLLOWLOCATION:
        SETCURLOPT(state, CURLOPT_FOLLOWLOCATION, (OPTARG)1L);
	break;
    case CURLOPT_MAXREDIRS:
	SETCURLOPT(state, CURLOPT_MAXREDIRS, (OPTARG)MAX_REDIRECTS);
	break;
    case CURLOPT_ERRORBUFFER:
	SETCURLOPT(state, CURLOPT_ERRORBUFFER, state->curl->errdata.errorbuf);
	break;
    case CURLOPT_ACCEPT_ENCODING:
	if(state->auth->curlflags.encode) {
	    SETCURLOPT(state, CURLOPT_ACCEPT_ENCODING, "" ); /* Accept all available encodings */
	} else {
    	    SETCURLOPT(state, CURLOPT_ACCEPT_ENCODING, NULL);
        }
	break;
    case CURLOPT_PROXY:
	if(state->auth->proxy.host != NULL) {
	    SETCURLOPT(state, CURLOPT_PROXY, state->auth->proxy.host);
	    SETCURLOPT(state, CURLOPT_PROXYPORT, (OPTARG)(long)state->auth->proxy.port);
	    if(state->auth->proxy.user != NULL
	       && state->auth->proxy.pwd != NULL) {
                SETCURLOPT(state, CURLOPT_PROXYUSERNAME, state->auth->proxy.user);
                SETCURLOPT(state, CURLOPT_PROXYPASSWORD, state->auth->proxy.pwd);
#ifdef CURLOPT_PROXYAUTH
	        SETCURLOPT(state, CURLOPT_PROXYAUTH, (long)CURLAUTH_ANY);
#endif
	    }
	}
	break;
    case CURLOPT_SSL_VERIFYPEER:
	/* VERIFYPEER == 0 => VERIFYHOST == 0 */
	/* We need to have 2 states: default and a set value */
	/* So -1 => default >= 0 => use value */
	if(state->auth->ssl.verifypeer >= 0) {
            SETCURLOPT(state, CURLOPT_SSL_VERIFYPEER, (OPTARG)(state->auth->ssl.verifypeer));
	    if(state->auth->ssl.verifypeer == 0) state->auth->ssl.verifyhost = 0;
        }
	break;
    case CURLOPT_SSL_VERIFYHOST:
#ifdef HAVE_LIBCURL_766
	if(state->auth->ssl.verifyhost >= 0) {
            SETCURLOPT(state, CURLOPT_SSL_VERIFYHOST, (OPTARG)(state->auth->ssl.verifyhost));
	}
#endif
	break;
    case CURLOPT_SSLCERT:
        if(state->auth->ssl.certificate)
            SETCURLOPT(state, CURLOPT_SSLCERT, state->auth->ssl.certificate);
	break;
    case CURLOPT_SSLKEY:
        if(state->auth->ssl.key)
            SETCURLOPT(state, CURLOPT_SSLKEY, state->auth->ssl.key);
        if(state->auth->ssl.keypasswd)
            /* libcurl prior to 7.16.4 used 'CURLOPT_SSLKEYPASSWD' */
            SETCURLOPT(state, CURLOPT_SSLKEYPASSWD, state->auth->ssl.keypasswd);
	break;
    case CURLOPT_CAINFO:
        if(state->auth->ssl.cainfo)
            SETCURLOPT(state, CURLOPT_CAINFO, state->auth->ssl.cainfo);
	break;
    case CURLOPT_CAPATH:
	if(state->auth->ssl.capath)
            SETCURLOPT(state, CURLOPT_CAPATH, state->auth->ssl.capath);
        break;
    case CURLOPT_USE_SSL:
	break;

#ifdef HAVE_CURLOPT_BUFFERSIZE
    case CURLOPT_BUFFERSIZE:
	SETCURLOPT(state, CURLOPT_BUFFERSIZE, (OPTARG)state->curl->buffersize);
	break;
#endif

#ifdef HAVE_CURLOPT_KEEPALIVE
    case CURLOPT_TCP_KEEPALIVE:
	if(state->curl->keepalive.active != 0)
	    SETCURLOPT(state, CURLOPT_TCP_KEEPALIVE, (OPTARG)1L);
	if(state->curl->keepalive.idle > 0)
	    SETCURLOPT(state, CURLOPT_TCP_KEEPIDLE, (OPTARG)state->curl->keepalive.idle);
	if(state->curl->keepalive.interval > 0)
	    SETCURLOPT(state, CURLOPT_TCP_KEEPINTVL, (OPTARG)state->curl->keepalive.interval);
	break;
#endif

    default:
        nclog(NCLOGWARN,"Attempt to update unexpected curl flag: %d",flag);
	break;
    }
done:
    return THROW(ret);
}

/* Set various general curl flags per fetch  */
int
NCD4_set_flags_perfetch(NCD4INFO* state)
{
    int ret = NC_NOERR;
    /* currently none */
    return THROW(ret);
}

/* Set various general curl flags per link */

int
NCD4_set_flags_perlink(NCD4INFO* state)
{
    int ret = NC_NOERR;
    /* Following are always set */
    if(ret == NC_NOERR) ret = set_curlflag(state,CURLOPT_ACCEPT_ENCODING);
    if(ret == NC_NOERR) ret = set_curlflag(state,CURLOPT_NETRC);
    if(ret == NC_NOERR) ret = set_curlflag(state,CURLOPT_VERBOSE);
    if(ret == NC_NOERR) ret = set_curlflag(state,CURLOPT_TIMEOUT);
    if(ret == NC_NOERR) ret = set_curlflag(state,CURLOPT_USERAGENT);
    if(ret == NC_NOERR) ret = set_curlflag(state,CURLOPT_COOKIEJAR);
    if(ret == NC_NOERR) ret = set_curlflag(state,CURLOPT_USERPWD);
    if(ret == NC_NOERR) ret = set_curlflag(state,CURLOPT_PROXY);
    if(ret == NC_NOERR) ret = set_curlflag(state,CURLOPT_SSL_VERIFYPEER);
    if(ret == NC_NOERR) ret = set_curlflag(state,CURLOPT_SSL_VERIFYHOST);
    if(ret == NC_NOERR) ret = set_curlflag(state,CURLOPT_SSLCERT);
    if(ret == NC_NOERR) ret = set_curlflag(state,CURLOPT_SSLKEY);
    if(ret == NC_NOERR) ret = set_curlflag(state,CURLOPT_CAINFO);
    if(ret == NC_NOERR) ret = set_curlflag(state,CURLOPT_CAPATH);
    if(ret == NC_NOERR) ret = set_curlflag(state,CURLOPT_USE_SSL);
    if(ret == NC_NOERR) ret = set_curlflag(state, CURLOPT_FOLLOWLOCATION);
    if(ret == NC_NOERR) ret = set_curlflag(state, CURLOPT_MAXREDIRS);
    if(ret == NC_NOERR) ret = set_curlflag(state, CURLOPT_ERRORBUFFER);

    /* Optional */
#ifdef HAVE_CURLOPT_BUFFERSIZE
    if(ret == NC_NOERR && state->curl->buffersize > 0)
        ret = set_curlflag(state, CURLOPT_BUFFERSIZE);
#endif
#ifdef HAVE_CURLOPT_KEEPALIVE
    if(ret == NC_NOERR && state->curl->keepalive.active != 0)
        ret = set_curlflag(state, CURLOPT_TCP_KEEPALIVE);
#endif

#if 0
    /* Set the CURL. options */
    if(ret == NC_NOERR) ret = set_curl_options(state);
#endif
    return THROW(ret);
}

#if 0
/**
Directly set any options starting with 'CURL.'
*/
static int
set_curl_options(NCD4INFO* state)
{
    int ret = NC_NOERR;
    NClist* store = NULL;
    int i;
    char* hostport = NULL;

    hostport = NC_combinehostport(state->uri);

    store = ncrc_getglobalstate()->rcinfo.triples;

    for(i=0;i<nclistlength(store);i++) {
        struct CURLFLAG* flag;
	NCTriple* triple = (NCTriple*)nclistget(store,i);
        size_t hostlen = (triple->host ? strlen(triple->host) : 0);
        const char* flagname;
        if(strncmp("CURL.",triple->key,5) != 0) continue; /* not a curl flag */
        /* do hostport prefix comparison */
        if(hostport != NULL) {
	  int t = 0;
	  if(triple->host != NULL)
              t = strncmp(hostport,triple->host,hostlen);
          if(t !=  0) continue;
        }
        flagname = triple->key+5; /* 5 == strlen("CURL."); */
        flag = NCD4_curlflagbyname(flagname);
        if(flag == NULL) {ret = NC_ECURL; goto done;}
        ret = set_curlopt(state,flag->flag,cvt(triple->value,flag->type));
    }
 done:
    nullfree(hostport);
    return THROW(ret);
}

static void*
cvt(char* value, enum CURLFLAGTYPE type)
{
    switch (type) {
    case CF_LONG: {
	/* Try to convert to long value */
	const char* p = value;
	char* q = NULL;
	long longvalue = strtol(p,&q,10);
	if(*q != '\0')
	    return NULL;
	return (void*)longvalue;
	}
    case CF_STRING:
	return (void*)value;
    case CF_UNKNOWN: case CF_OTHER:
	return (void*)value;
    }
    return NULL;
}
#endif

void
NCD4_curl_debug(NCD4INFO* state)
{
    state->auth->curlflags.verbose = 1;
    set_curlflag(state,CURLOPT_VERBOSE);
    set_curlflag(state,CURLOPT_ERRORBUFFER);
}

/* Misc. */

/* Determine if this version of curl supports
       "file://..." &/or "https://..." urls.
*/
void
NCD4_curl_protocols(NCD4INFO* state)
{
    const char* const* proto; /*weird*/
    curl_version_info_data* curldata;
    curldata = curl_version_info(CURLVERSION_NOW);
    for(proto=curldata->protocols;*proto;proto++) {
        if(strcmp("http",*proto)==0) {state->auth->curlflags.proto_https=1;}
    }
#ifdef D4DEBUG
    nclog(NCLOGNOTE,"Curl https:// support = %d",state->auth->curlflags.proto_https);
#endif
}

/*
    Extract state values from .rc file
*/
ncerror
NCD4_get_rcproperties(NCD4INFO* state)
{
    ncerror err = NC_NOERR;
    char* option = NULL;
#ifdef HAVE_CURLOPT_BUFFERSIZE
    option = NC_rclookup(D4BUFFERSIZE,state->dmruri->uri,NULL);
    if(option != NULL && strlen(option) != 0) {
	long bufsize;
	if(strcasecmp(option,"max")==0)
	    bufsize = CURL_MAX_READ_SIZE;
	else if(sscanf(option,"%ld",&bufsize) != 1 || bufsize <= 0)
	    fprintf(stderr,"Illegal %s size\n",D4BUFFERSIZE);
        state->curl->buffersize = bufsize;
    }
#endif
#ifdef HAVE_CURLOPT_KEEPALIVE
    option = NC_rclookup(D4KEEPALIVE,state->dmruri->uri,NULL);
    if(option != NULL && strlen(option) != 0) {
	/* The keepalive value is of the form 0 or n/m,
           where n is the idle time and m is the interval time;
           setting either to zero will prevent that field being set.*/
	if(strcasecmp(option,"on")==0) {
	    state->curl->keepalive.active = 1;
	} else {
	    unsigned long idle=0;
	    unsigned long interval=0;
	    if(sscanf(option,"%lu/%lu",&idle,&interval) != 2)
	        fprintf(stderr,"Illegal KEEPALIVE VALUE: %s\n",option);
	    state->curl->keepalive.idle = (long)idle;
	    state->curl->keepalive.interval = (long)interval;
	    state->curl->keepalive.active = 1;
	}
    }
#endif
    return err;
}

#if 0
/*
"Inverse" of set_curlflag;
Given a flag and value, it updates state.
Update a specific flag from state->curlflags.
*/
int
NCD4_set_curlstate(NCD4INFO* state, int flag, void* value)
{
    int ret = NC_NOERR;
    switch (flag) {
    case CURLOPT_USERPWD:
        if(info->creds.userpwd != NULL) free(info->creds.userpwd);
	info->creds.userpwd = strdup((char*)value);
	break;
    case CURLOPT_COOKIEJAR: case CURLOPT_COOKIEFILE:
        if(info->curlflags.cookiejar != NULL) free(info->curlflags.cookiejar);
	info->curlflags.cookiejar = strdup((char*)value);
	break;
    case CURLOPT_NETRC: case CURLOPT_NETRC_FILE:
        if(info->curlflags.netrc != NULL) free(info->curlflags.netrc);
	info->curlflags.netrc = strdup((char*)value);
	break;
    case CURLOPT_VERBOSE:
	info->curlflags.verbose = (long)value;
	break;
    case CURLOPT_TIMEOUT:
	info->curlflags.timeout = (long)value;
	break;
    case CURLOPT_CONNECTTIMEOUT:
	info->curlflags.connecttimeout = (long)value;
	break;
    case CURLOPT_USERAGENT:
        if(info->curlflags.useragent != NULL) free(info->curlflags.useragent);
        info->curlflags.useragent = strdup((char*)value);
	break;
    case CURLOPT_FOLLOWLOCATION:
	/* no need to store; will always be set */
	break;
    case CURLOPT_MAXREDIRS:
	/* no need to store; will always be set */
	break;
    case CURLOPT_ERRORBUFFER:
	/* no need to store; will always be set */
	break;
    case CURLOPT_ACCEPT_ENCODING:
	/* no need to store; will always be set to fixed value */
	break;
    case CURLOPT_PROXY:
	/* We assume that the value is the proxy url */
	if(info->proxy.host != NULL) free(info->proxy.host);
	if(info->proxy.userpwd != NULL) free(info->proxy.userpwd);
	info->proxy.host = NULL;
	info->proxy.userpwd = NULL;
	if(!NCD4_parseproxy(state,(char*)value))
		{ret = NC_EINVAL; goto done;}
	break;
    case CURLOPT_SSLCERT:
	if(info->ssl.certificate != NULL) free(info->ssl.certificate);
	info->ssl.certificate = strdup((char*)value);
	break;
    case CURLOPT_SSLKEY:
	if(info->ssl.key != NULL) free(info->ssl.key);
	info->ssl.key = strdup((char*)value);
	break;
    case CURLOPT_KEYPASSWD:
	if(info->ssl.keypasswd!= NULL) free(info->ssl.keypasswd);
	info->ssl.keypasswd = strdup((char*)value);
	break;
    case CURLOPT_SSL_VERIFYPEER:
      info->ssl.verifypeer = (long)value;
      break;
    case CURLOPT_SSL_VERIFYHOST:
      info->ssl.verifyhost = (long)value;
      break;
    case CURLOPT_CAINFO:
      if(info->ssl.cainfo != NULL) free(info->ssl.cainfo);
      info->ssl.cainfo = strdup((char*)value);
      break;
    case CURLOPT_CAPATH:
	if(info->ssl.capath != NULL) free(info->ssl.capath);
	info->ssl.capath = strdup((char*)value);
	break;

    default: break;
    }
done:
    return THROW(ret);
}
#endif

void
NCD4_curl_printerror(NCD4INFO* state)
{
    fprintf(stderr,"curl error details: %s\n",state->curl->errdata.errorbuf);
}

CURLcode
NCD4_reportcurlerror(CURLcode cstat)
{
    if(cstat != CURLE_OK) {
        fprintf(stderr,"CURL Error: %s\n",curl_easy_strerror(cstat));
    }
    fflush(stderr);
    return cstat;
}
