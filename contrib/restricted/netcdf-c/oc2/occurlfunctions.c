/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

/* WARNING: oc2/occurlfunctions.c and libdap4/d4curlfunctions.c
should be merged since they are essentially the same file.
In the meantime, changes to one should be propagated to the other.
*/

#include "config.h"
#include <stdlib.h>
#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif
#include "ncrc.h"
#include "ocinternal.h"
#include "ocdebug.h"
#include "occurlfunctions.h"

#define OC_MAX_REDIRECTS 20L

/* Mnemonic */
#define OPTARG uintptr_t

/* Define some .rc file entries of interest*/
#define NETRCFILETAG "HTTP.NETRC"

#define SETCURLOPT(state,flag,value) {if(ocset_curlopt(state,flag,(void*)value) != NC_NOERR) {goto done;}}

/*
Set a specific curl flag; primary wrapper for curl_easy_setopt
*/
OCerror
ocset_curlopt(OCstate* state, int flag, void* value)
{
    OCerror stat = OC_NOERR;
    CURLcode cstat = CURLE_OK;
    cstat = OCCURLERR(state,curl_easy_setopt(state->curl,flag,value));
    if(cstat != CURLE_OK)
	stat = OC_ECURL;
    return stat;
}

/* Check return value */
#define CHECK(state,flag,value) {if(ocset_curlopt(state,flag,(void*)value) != OC_NOERR) {goto done;}}

/*
Update a specific flag from state
*/
OCerror
ocset_curlflag(OCstate* state, int flag)
{
    OCerror stat = OC_NOERR;

    switch (flag) {

    case CURLOPT_USERPWD: /* Does both user and pwd */
        if(state->auth->creds.user != NULL && state->auth->creds.pwd != NULL) {
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
	SETCURLOPT(state, CURLOPT_MAXREDIRS, (OPTARG)OC_MAX_REDIRECTS);
	break;

    case CURLOPT_ERRORBUFFER:
	SETCURLOPT(state, CURLOPT_ERRORBUFFER, state->error.curlerrorbuf);
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
	    if(state->auth->proxy.user != NULL && state->auth->proxy.pwd != NULL) {
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
	SETCURLOPT(state, CURLOPT_BUFFERSIZE, (OPTARG)state->curlbuffersize);
	break;
#endif

#ifdef HAVE_CURLOPT_KEEPALIVE
    case CURLOPT_TCP_KEEPALIVE:
	if(state->curlkeepalive.active != 0)
	    SETCURLOPT(state, CURLOPT_TCP_KEEPALIVE, (OPTARG)1L);
	if(state->curlkeepalive.idle > 0)
	    SETCURLOPT(state, CURLOPT_TCP_KEEPIDLE, (OPTARG)state->curlkeepalive.idle);
	if(state->curlkeepalive.interval > 0)
	    SETCURLOPT(state, CURLOPT_TCP_KEEPINTVL, (OPTARG)state->curlkeepalive.interval);
	break;
#endif

    default:
        nclog(NCLOGWARN,"Attempt to update unexpected curl flag: %d",flag);
	break;
    }
done:
    return stat;
}


/* Set various general curl flags per fetch  */
OCerror
ocset_flags_perfetch(OCstate* state)
{
    OCerror stat = OC_NOERR;
    /* currently none */
    return stat;
}

/* Set various general curl flags per link */

OCerror
ocset_flags_perlink(OCstate* state)
{
    OCerror stat = OC_NOERR;

    /* Following are always set */
    if(stat == OC_NOERR) stat = ocset_curlflag(state,CURLOPT_ACCEPT_ENCODING);
    if(stat == OC_NOERR) stat = ocset_curlflag(state,CURLOPT_NETRC);
    if(stat == OC_NOERR) stat = ocset_curlflag(state,CURLOPT_VERBOSE);
    if(stat == OC_NOERR) stat = ocset_curlflag(state,CURLOPT_TIMEOUT);
    if(stat == OC_NOERR) stat = ocset_curlflag(state,CURLOPT_CONNECTTIMEOUT);
    if(stat == OC_NOERR) stat = ocset_curlflag(state,CURLOPT_USERAGENT);
    if(stat == OC_NOERR) stat = ocset_curlflag(state,CURLOPT_COOKIEJAR);
    if(stat == OC_NOERR) stat = ocset_curlflag(state,CURLOPT_USERPWD);
    if(stat == OC_NOERR) stat = ocset_curlflag(state,CURLOPT_PROXY);
    if(stat == OC_NOERR) stat = ocset_curlflag(state,CURLOPT_SSL_VERIFYPEER);
    if(stat == OC_NOERR) stat = ocset_curlflag(state,CURLOPT_SSL_VERIFYHOST);
    if(stat == OC_NOERR) stat = ocset_curlflag(state,CURLOPT_SSLCERT);
    if(stat == OC_NOERR) stat = ocset_curlflag(state,CURLOPT_SSLKEY);
    if(stat == OC_NOERR) stat = ocset_curlflag(state,CURLOPT_CAINFO);
    if(stat == OC_NOERR) stat = ocset_curlflag(state,CURLOPT_CAPATH);
    if(stat == OC_NOERR) stat = ocset_curlflag(state,CURLOPT_USE_SSL);
    if(stat == OC_NOERR) stat = ocset_curlflag(state, CURLOPT_FOLLOWLOCATION);
    if(stat == OC_NOERR) stat = ocset_curlflag(state, CURLOPT_MAXREDIRS);
    if(stat == OC_NOERR) stat = ocset_curlflag(state, CURLOPT_ERRORBUFFER);

#ifdef HAVE_CURLOPT_BUFFERSIZE
    /* Optional */
    if(stat == OC_NOERR && state->curlbuffersize > 0)
	stat = ocset_curlflag(state, CURLOPT_BUFFERSIZE);
#endif
#ifdef HAVE_CURLOPT_KEEPALIVE
    if(stat == NC_NOERR && state->curlkeepalive.active != 0)
        stat = ocset_curlflag(state, CURLOPT_TCP_KEEPALIVE);
#endif

    return stat;
}

void
oc_curl_debug(OCstate* state)
{
    state->auth->curlflags.verbose = 1;
    ocset_curlflag(state,CURLOPT_VERBOSE);
    ocset_curlflag(state,CURLOPT_ERRORBUFFER);
}

/* Misc. */

int
ocrc_netrc_required(OCstate* state)
{
    char* netrcfile = NC_rclookup(NETRCFILETAG,state->uri->uri,NULL);
    return (netrcfile != NULL || state->auth->curlflags.netrc != NULL ? 0 : 1);
}

void
oc_curl_printerror(OCstate* state)
{
    fprintf(stderr,"curl error details: %s\n",state->curlerror);
}

/* See if http: protocol is supported */
void
oc_curl_protocols(OCstate* state)
{
    const char* const* proto; /*weird*/
    curl_version_info_data* curldata;
    curldata = curl_version_info(CURLVERSION_NOW);
    for(proto=curldata->protocols;*proto;proto++) {
        if(strcmp("http",*proto)==0)
	    state->auth->curlflags.proto_https=1;
    }
}
