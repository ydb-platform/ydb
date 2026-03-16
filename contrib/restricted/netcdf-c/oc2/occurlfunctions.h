/*! \file

Copyright 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002,
2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014,
2015, 2016, 2017, 2018
University Corporation for Atmospheric Research/Unidata.

See \ref copyright file for more info.

*/
/*
 * occurlfunction.h
 *
 *  Created on: Mar 5, 2009
 *      Author: rikki
 */

#ifndef _CURLFUNCTION_H_
#define _CURLFUNCTION_H_


/* Condition on libcurl version */
/* Set up an alias as needed */
#ifndef HAVE_CURLOPT_KEYPASSWD
#define CURLOPT_KEYPASSWD CURLOPT_SSLKEYPASSWD
#endif
#ifndef HAVE_CURLINFO_RESPONSE_CODE
#define CURLINFO_RESPONSE_CODE CURLINFO_HTTP_CODE
#endif

extern OCerror ocset_curlopt(OCstate* state, int flag, void* value);

struct OCCURLFLAG* occurlflagbyflag(int flag);
struct OCCURLFLAG* occurlflagbyname(const char* name);

extern OCerror ocset_flags_perfetch(OCstate*);
extern OCerror ocset_flags_perlink(OCstate*);

extern OCerror ocset_curlflag(OCstate*,int);

extern void oc_curl_debug(OCstate* state);

extern void oc_curl_printerror(OCstate* state);
extern int ocrc_netrc_required(OCstate* state);
extern void oc_curl_protocols(OCstate* state);

/* From occurlflags.c */
extern struct OCCURLFLAG* occurlflags(void);
extern struct OCCURLFLAG* occurlflagbyname(const char*);
extern struct OCCURLFLAG* occurlflagbyflag(int);
extern char*  occombinehostport(const NCURI* uri);

#endif /*_CURLFUNCTION_H_*/
