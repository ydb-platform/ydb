/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * Header file for dhttp.c
 * @author Dennis Heimbigner
 */

#ifndef NCHTTP_H
#define NCHTTP_H

typedef enum HTTPMETHOD {
HTTPNONE=0, HTTPGET=1, HTTPPUT=2, HTTPPOST=3, HTTPHEAD=4, HTTPDELETE=5
} HTTPMETHOD;

/* Forward */
struct CURL;
struct NCS3INFO;
struct NCURI;

/* Common state For S3 vs Simple Curl */
typedef enum NC_HTTPFORMAT {HTTPS3=1, HTTPCURL=2} NC_HTTPFORMAT;

typedef struct NC_HTTP_STATE {
    enum NC_HTTPFORMAT format; /* Discriminator */
    char* path; /* original url */
    struct NCURI* url; /* parsed url */
    long httpcode;
    char* errmsg; /* do not free if format is HTTPCURL */
#ifdef NETCDF_ENABLE_S3
    struct NC_HTTP_S3 {
        void* s3client;
	struct NCS3INFO* info;
    } s3;
#endif
    struct NC_HTTP_CURL {
        struct CURL* curl;
        char errbuf[2048]; /* assert(CURL_ERROR_SIZE <= 2048) */
        struct Response {
            NClist* headset; /* which headers to capture */
            NClist* headers; /* Set of captured headers */
    	    NCbytes* buf; /* response content; call owns; do not free */
        } response;
        struct Request {
            HTTPMETHOD method;
	    size_t payloadsize;
	    void* payload; /* caller owns; do not free */
  	    size_t payloadpos;
            NClist* headers;
        } request;
    } curl;
} NC_HTTP_STATE;

/* External API */
extern int nc_http_open(const char* url, NC_HTTP_STATE** statep);
extern int nc_http_open_verbose(const char* url, int verbose, NC_HTTP_STATE** statep);
extern int nc_http_size(NC_HTTP_STATE* state, long long* sizep);
extern int nc_http_read(NC_HTTP_STATE* state, size64_t start, size64_t count, NCbytes* buf);
extern int nc_http_write(NC_HTTP_STATE* state, NCbytes* payload);
extern int nc_http_close(NC_HTTP_STATE* state);
extern int nc_http_reset(NC_HTTP_STATE* state);

#endif /*NCHTTP_H*/
