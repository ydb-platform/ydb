#pragma once

#include "exthttpcodes.h"

#include <library/cpp/mime/types/mime.h>

#include <util/system/defaults.h>
#include <util/system/compat.h>
#include <util/generic/string.h>
#include <util/generic/ylimits.h>
#include <util/system/maxlen.h>

#include <ctime>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>

// This is ugly solution but here a lot of work to do it the right way.
#define FETCHER_URL_MAX 8192

extern const i64 DEFAULT_RETRY_AFTER;       /// == -1
extern const i64 DEFAULT_IF_MODIFIED_SINCE; /// == -1
extern const i32 DEFAULT_MAX_AGE;           /// == -1
extern const i8 DEFAULT_REQUEST_PRIORITY;   /// == -1
extern const i32 DEFAULT_RESPONSE_TIMEOUT;  /// == -1

#define HTTP_PREFIX "http://"
#define MAX_LANGREGION_LEN 4
#define MAXWORD_LEN 55

enum HTTP_COMPRESSION {
    HTTP_COMPRESSION_UNSET = 0,
    HTTP_COMPRESSION_ERROR = 1,
    HTTP_COMPRESSION_IDENTITY = 2,
    HTTP_COMPRESSION_GZIP = 3,
    HTTP_COMPRESSION_DEFLATE = 4,
    HTTP_COMPRESSION_COMPRESS = 5,
    HTTP_COMPRESSION_MAX = 6
};

enum HTTP_METHOD {
    HTTP_METHOD_UNDEFINED = -1,
    HTTP_METHOD_OPTIONS,
    HTTP_METHOD_GET,
    HTTP_METHOD_HEAD,
    HTTP_METHOD_POST,
    HTTP_METHOD_PUT,
    HTTP_METHOD_DELETE,
    HTTP_METHOD_TRACE,
    HTTP_METHOD_CONNECT,
    HTTP_METHOD_EXTENSION
};

enum HTTP_CONNECTION {
    HTTP_CONNECTION_UNDEFINED = -1,
    HTTP_CONNECTION_KEEP_ALIVE = 0,
    HTTP_CONNECTION_CLOSE = 1
};

/// Class represents general http header fields.
struct THttpBaseHeader {
public:
    i16 error;
    i32 header_size;
    i32 entity_size;
    i64 content_length;
    i64 http_time;                   // seconds since epoch
    i64 content_range_start;         // Content-Range: first-byte-pos
    i64 content_range_end;           // Content-Range: last-byte-pos
    i64 content_range_entity_length; // Content-Range: entity-length
    i8 http_minor;
    i8 mime_type;
    i8 charset;
    i8 compression_method;
    i8 transfer_chunked;
    i8 connection_closed;
    TString base;

public:
    void Init() {
        error = 0;
        header_size = 0;
        entity_size = 0;
        content_length = -1;
        http_time = -1;
        http_minor = -1;
        mime_type = -1;
        charset = -1;
        compression_method = HTTP_COMPRESSION_UNSET;
        transfer_chunked = -1;
        connection_closed = HTTP_CONNECTION_UNDEFINED;
        content_range_start = -1;
        content_range_end = -1;
        content_range_entity_length = -1;
        base.clear();
    }

    void Print() const {
        printf("content_length: %" PRIi64 "\n", content_length);
        printf("http_time: %" PRIi64 "\n", http_time);
        printf("http_minor: %" PRIi8 "\n", http_minor);
        printf("mime_type: %" PRIi8 "\n", mime_type);
        printf("charset: %" PRIi8 "\n", charset);
        printf("compression_method: %" PRIi8 "\n", compression_method);
        printf("transfer_chunked: %" PRIi8 "\n", transfer_chunked);
        printf("connection_closed: %" PRIi8 "\n", connection_closed);
        printf("content_range_start: %" PRIi64 "\n", content_range_start);
        printf("content_range_end: %" PRIi64 "\n", content_range_end);
        printf("content_range_entity_length: %" PRIi64 "\n", content_range_entity_length);
        printf("base: \"%s\"\n", base.c_str());
        printf("error: %" PRIi16 "\n", error);
    }

    int SetBase(const char* path,
                const char* hostNamePtr = nullptr,
                int hostNameLength = 0) {
        if (*path == '/') {
            base = "http://";
            base += TStringBuf(hostNamePtr, hostNameLength);
            base += path;
        } else {
            base = path;
        }
        return error;
    }
};

enum { HREFLANG_MAX = FETCHER_URL_MAX * 2 };
/// Class represents Http Response Header.
struct THttpHeader: public THttpBaseHeader {
public:
    i8 accept_ranges;
    i8 squid_error;
    i8 x_robots_tag; // deprecated, use x_robots_state instead
    i16 http_status;
    TString location;
    TString rel_canonical;
    char hreflangs[HREFLANG_MAX];
    i64 retry_after;
    TString x_robots_state; // 'xxxxx' format, see `library/html/zoneconf/parsefunc.cpp`

public:
    void Init() {
        THttpBaseHeader::Init();
        accept_ranges = -1;
        squid_error = 0;
        x_robots_tag = 0;
        rel_canonical.clear();
        http_status = -1;
        location.clear();
        hreflangs[0] = 0;
        retry_after = DEFAULT_RETRY_AFTER;
        x_robots_state = "xxxxx";
    }

    void Print() const {
        THttpBaseHeader::Print();
        printf("http_status: %" PRIi16 "\n", http_status);
        printf("squid_error: %" PRIi8 "\n", squid_error);
        printf("accept_ranges: %" PRIi8 "\n", accept_ranges);
        printf("location: \"%s\"\n", location.c_str());
        printf("retry_after: %" PRIi64 "\n", retry_after);
    }
};

struct THttpRequestHeader: public THttpBaseHeader {
public:
    TString request_uri;
    char host[HOST_MAX];
    char from[MAXWORD_LEN];
    char user_agent[MAXWORD_LEN];
    char x_yandex_langregion[MAX_LANGREGION_LEN];
    char x_yandex_sourcename[MAXWORD_LEN];
    char x_yandex_requesttype[MAXWORD_LEN];
    char x_yandex_fetchoptions[MAXWORD_LEN];
    i8 http_method;
    i8 x_yandex_request_priority;
    i32 x_yandex_response_timeout;
    i32 max_age;
    i64 if_modified_since;

public:
    THttpRequestHeader() {
        Init();
    }

    void Init() {
        request_uri.clear();
        host[0] = 0;
        from[0] = 0;
        user_agent[0] = 0;
        x_yandex_langregion[0] = 0;
        x_yandex_sourcename[0] = 0;
        x_yandex_requesttype[0] = 0;
        x_yandex_fetchoptions[0] = 0;
        http_method = HTTP_METHOD_UNDEFINED;
        x_yandex_request_priority = DEFAULT_REQUEST_PRIORITY;
        x_yandex_response_timeout = DEFAULT_RESPONSE_TIMEOUT;
        max_age = DEFAULT_MAX_AGE;
        if_modified_since = DEFAULT_IF_MODIFIED_SINCE;
        THttpBaseHeader::Init();
    }

    void Print() const {
        THttpBaseHeader::Print();
        printf("request_uri: \"%s\"\n", request_uri.c_str());
        printf("host: \"%s\"\n", host);
        printf("from: \"%s\"\n", from);
        printf("user_agent: \"%s\"\n", user_agent);
        printf("http_method: %" PRIi8 "\n", http_method);
        printf("response_timeout: %" PRIi32 "\n", x_yandex_response_timeout);
        printf("max_age: %" PRIi32 "\n", max_age);
        printf("if_modified_since: %" PRIi64 "\n", if_modified_since);
    }

    /// It doesn't care about errors in request or headers, where
    /// request_uri equals to '*'.
    /// This returns copy of the string, which you have to delete.
    TString GetUrl() {
        TString url;
        if (host[0] == 0 || !strcmp(host, "")) {
            url = request_uri;
        } else {
            url = HTTP_PREFIX;
            url += host;
            url += request_uri;
        }
        return url;
    }

    char* GetUrl(char* buffer, size_t size) {
        if (host[0] == 0 || !strcmp(host, "")) {
            strlcpy(buffer, request_uri.c_str(), size);
        } else {
            snprintf(buffer, size, "http://%s%s", host, request_uri.c_str());
        }
        return buffer;
    }
};

class THttpAuthHeader: public THttpHeader {
public:
    char* realm;
    char* nonce;
    char* opaque;
    bool stale;
    int algorithm;
    bool qop_auth;
    bool use_auth;

    //we do not provide auth-int variant as too heavy
    //bool  qop_auth_int;

    THttpAuthHeader()
        : realm(nullptr)
        , nonce(nullptr)
        , opaque(nullptr)
        , stale(false)
        , algorithm(0)
        , qop_auth(false)
        , use_auth(true)
    {
        THttpHeader::Init();
    }

    ~THttpAuthHeader() {
        free(realm);
        free(nonce);
        free(opaque);
    }

    void Print() {
        THttpHeader::Print();
        if (use_auth) {
            if (realm)
                printf("realm: \"%s\"\n", realm);
            if (nonce)
                printf("nonce: \"%s\"\n", nonce);
            if (opaque)
                printf("opaque: \"%s\"\n", opaque);
            printf("stale: %d\n", stale);
            printf("algorithm: %d\n", algorithm);
            printf("qop_auth: %d\n", qop_auth);
        }
    }
};
