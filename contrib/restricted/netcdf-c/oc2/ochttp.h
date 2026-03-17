/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#ifndef HTTP_H
#define HTTP_H 1

extern int curlopen(CURL** curlp);
extern void curlclose(CURL*);

extern OCerror ocfetchurl(CURL*, const char*, NCbytes*, long*);
extern OCerror ocfetchurl_file(CURL*, const char*, FILE*, off_t*, long*);

extern long ocfetchhttpcode(CURL* curl);

extern OCerror ocfetchlastmodified(CURL* curl, char* url, long* filetime);

extern OCerror occurlopen(CURL** curlp);
extern void occurlclose(CURL* curlp);

extern OCerror ocping(const char* url);

#endif /*HTTP_H*/
