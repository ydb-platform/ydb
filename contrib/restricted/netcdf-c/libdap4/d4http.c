/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#include "d4includes.h"
#include "d4curlfunctions.h"

static size_t WriteMemoryCallback(void*, size_t, size_t, void*);
static int curlerrtoncerr(CURLcode cstat);

struct Fetchdata {
        FILE* stream;
        size_t size;
};

long
NCD4_fetchhttpcode(CURL* curl)
{
    long httpcode = 200;
    CURLcode cstat = CURLE_OK;
    /* Extract the http code */
#ifdef HAVE_CURLINFO_RESPONSE_CODE
    cstat = curl_easy_getinfo(curl,CURLINFO_RESPONSE_CODE,&httpcode);
#else
    cstat = curl_easy_getinfo(curl,CURLINFO_HTTP_CONNECTCODE,&httpcode);
#endif
    if(cstat != CURLE_OK) {
	httpcode = 0;
        nclog(NCLOGERR, "curl error: %s", curl_easy_strerror(cstat));
    }
    return httpcode;
}

int
NCD4_fetchurl(CURL* curl, const char* url, NCbytes* buf, long* filetime, int* httpcodep)
{
    int ret = NC_NOERR;
    CURLcode cstat = CURLE_OK;
    size_t len;
    long httpcode = 0;

    /* send all data to this function  */
    cstat = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    if (cstat != CURLE_OK)
        goto done;

    /* we pass our file to the callback function */
    cstat = curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*)buf);
    if (cstat != CURLE_OK)
        goto done;

    /* One last thing; always try to get the last modified time */
    cstat = curl_easy_setopt(curl, CURLOPT_FILETIME, (long)1);

    /* Set the URL */
    cstat = curl_easy_setopt(curl, CURLOPT_URL, (void*)"");
    cstat = curl_easy_setopt(curl, CURLOPT_URL, (void*)url);
    if (cstat != CURLE_OK)
        goto done;

    cstat = curl_easy_perform(curl);

    if(cstat == CURLE_PARTIAL_FILE) {
        /* Log it but otherwise ignore */
        nclog(NCLOGWARN, "curl error: %s; ignored",
               curl_easy_strerror(cstat));
        cstat = CURLE_OK;
    }
    if(cstat != CURLE_OK) goto done;

    httpcode = NCD4_fetchhttpcode(curl);
    if(httpcodep) *httpcodep = (int)httpcode;

    /* Get the last modified time */
    if(filetime != NULL)
        cstat = curl_easy_getinfo(curl,CURLINFO_FILETIME,filetime);
    if(cstat != CURLE_OK) goto done;

    /* Null terminate the buffer*/
    len = ncbyteslength(buf);
    ncbytesappend(buf, '\0');
    ncbytessetlength(buf, len); /* don't count null in buffer size*/
#ifdef D4DEBUG
    nclog(NCLOGNOTE,"buffersize: %lu bytes",(d4size_t)ncbyteslength(buf));
#endif

done:
    if(cstat != CURLE_OK) {
        nclog(NCLOGERR, "curl error: %s", curl_easy_strerror(cstat));
        ret = curlerrtoncerr(cstat);
    } else switch (httpcode) {
           case 400: ret = NC_EDATADAP; break;
           case 401: ret = NC_EACCESS; break;
	   case 403: ret = NC_EAUTH; break;
           case 404: ret = ENOENT; break;
           case 500: ret = NC_EDAPSVC; break;
           case 200: break;
           default: ret = NC_ECURL; break;
    }
    return THROW(ret);
}

static size_t
WriteMemoryCallback(void *ptr, size_t size, size_t nmemb, void *data)
{
    size_t realsize = size * nmemb;
    NCbytes* buf = (NCbytes*) data;
    if(realsize == 0)
        nclog(NCLOGWARN,"WriteMemoryCallback: zero sized chunk");
    /* Optimize for reading potentially large dods datasets */
    while(!ncbytesavail(buf,realsize)) {
        /* double the size of the packet (unless the buf is empty) */
	if(ncbytesalloc(buf) == 0)
	    ncbytessetalloc(buf,1024);
	else
	   ncbytessetalloc(buf,2*ncbytesalloc(buf));
    }
    ncbytesappendn(buf, ptr, realsize);
#ifdef PROGRESS
    nclog(NCLOGNOTE,"callback: %lu bytes",(d4size_t)realsize);
#endif
    return realsize;
}

int
NCD4_curlopen(CURL** curlp)
{
    int ret = NC_NOERR;
    CURLcode cstat = CURLE_OK;
    CURL* curl;
    /* initialize curl*/
    curl = curl_easy_init();
    if (curl == NULL)
        ret = NC_ECURL;
    else {
        cstat = curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1);
        if (cstat != CURLE_OK)
            ret = NC_ECURL;
    }
    if (curlp)
        *curlp = curl;
    if(cstat != CURLE_OK) {
        nclog(NCLOGERR, "curl error: %s", curl_easy_strerror(cstat));
        ret = curlerrtoncerr(cstat);
    }
    return THROW(ret);
}

void
NCD4_curlclose(CURL* curl)
{
    if (curl != NULL)
        curl_easy_cleanup(curl);
}

int
NCD4_fetchlastmodified(CURL* curl, char* url, long* filetime)
{
    int ret = NC_NOERR;
    CURLcode cstat = CURLE_OK;

    /* Set the URL */
    cstat = curl_easy_setopt(curl, CURLOPT_URL, (void*)url);
    if (cstat != CURLE_OK)
        goto fail;

    /* Ask for head */
    cstat = curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30); /* 30sec timeout*/
    cstat = curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 2);
    cstat = curl_easy_setopt(curl, CURLOPT_HEADER, 1);
    cstat = curl_easy_setopt(curl, CURLOPT_NOBODY, 1);
    cstat = curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1);
    cstat = curl_easy_setopt(curl, CURLOPT_FILETIME, (long)1);

    cstat = curl_easy_perform(curl);
    if(cstat != CURLE_OK) goto fail;
    if(filetime != NULL)
        cstat = curl_easy_getinfo(curl,CURLINFO_FILETIME,filetime);
    if(cstat != CURLE_OK) goto fail;
    return THROW(ret);

fail:
    if(cstat != CURLE_OK) {
        nclog(NCLOGERR, "curl error: %s", curl_easy_strerror(cstat));
        ret = curlerrtoncerr(cstat);
    }
    return THROW(ret);
}

int
NCD4_ping(const char* url)
{
    int ret = NC_NOERR;
    CURLcode cstat = CURLE_OK;
    CURL* curl = NULL;
    NCbytes* buf = NULL;

    /* Create a CURL instance */
    ret = NCD4_curlopen(&curl);
    if(ret != NC_NOERR) return THROW(ret);

    /* Use redirects */
    cstat = curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 10L);
    if (cstat != CURLE_OK)
        goto done;
    cstat = curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    if (cstat != CURLE_OK)
        goto done;

    /* use a very short timeout: 10 seconds */
    cstat = curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)5);
    if (cstat != CURLE_OK)
        goto done;

    /* fail on HTTP 400 code errors */
    cstat = curl_easy_setopt(curl, CURLOPT_FAILONERROR, (long)1);
    if (cstat != CURLE_OK)
        goto done;

    /* Try to get the file */
    buf = ncbytesnew();
    ret = NCD4_fetchurl(curl,url,buf,NULL,NULL);
    if(ret == NC_NOERR) {
        /* Don't trust curl to return an error when request gets 404 */
        long http_code = 0;
        cstat = curl_easy_getinfo(curl,CURLINFO_RESPONSE_CODE, &http_code);
        if (cstat != CURLE_OK)
            goto done;
        if(http_code >= 400) {
            cstat = CURLE_HTTP_RETURNED_ERROR;
            goto done;
        }
    } else
        goto done;

done:
    ncbytesfree(buf);
    NCD4_curlclose(curl);
    if(cstat != CURLE_OK) {
        nclog(NCLOGERR, "curl error: %s", curl_easy_strerror(cstat));
        ret = curlerrtoncerr(cstat);
    }
    return THROW(ret);
}

static int
curlerrtoncerr(CURLcode cstat)
{
    switch (cstat) {
    case CURLE_OK: return THROW(NC_NOERR);
    case CURLE_URL_MALFORMAT:
	return THROW(NC_EURL);
    case CURLE_COULDNT_RESOLVE_HOST:
    case CURLE_COULDNT_CONNECT:
    case CURLE_REMOTE_ACCESS_DENIED:
    case CURLE_TOO_MANY_REDIRECTS:
	return THROW(NC_EDAPSVC);
    case CURLE_OUT_OF_MEMORY:
	return THROW(NC_ENOMEM);
    /* Eventually would be nice to convert these */
    case CURLE_SSL_CONNECT_ERROR:
    case CURLE_READ_ERROR:
    case CURLE_OPERATION_TIMEDOUT:
    case CURLE_SSL_CERTPROBLEM:
    case CURLE_FILESIZE_EXCEEDED:
    case CURLE_SSL_CACERT_BADFILE:
    default: break;
    }
    return THROW(NC_ECURL);
}
