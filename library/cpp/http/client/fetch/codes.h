#pragma once

namespace NHttpFetcher {
    const int FETCH_SUCCESS_CODE = 200;
    const int SERVICE_UNAVAILABLE = 503;
    const int ZORA_TIMEOUT_CODE = 5000;
    const int URL_FILTER_CODE = 6000;
    const int WRONG_HTTP_HEADER_CODE = 6001;
    const int FETCH_LARGE_FILE = 6002;
    const int FETCH_CANNOT_PARSE = 6003;
    const int HOSTS_QUEUE_TIMEOUT = 6004;
    const int WRONG_HTTP_RESPONSE = 6005;
    const int UNKNOWN_ERROR = 6006;
    const int FETCHER_QUEUE_TIMEOUT = 6007;
    const int FETCH_IGNORE = 6008;
    const int FETCH_CANCELLED = 6009;

    inline bool IsRedirectCode(int code) {
        return 301 == code || 302 == code || 303 == code ||
               305 == code || 307 == code || 308 == code;
    }

    inline bool IsSuccessCode(int code) {
        return code >= 200 && code < 300;
    }

    inline bool NoRefetch(int code) {
        return code == 415 || // Unsupported media type
               code == 601 || // Large file
               (code >= 400 && code < 500) ||
               code == 1003 || // disallowed by robots.txt
               code == 1006 || // not found by dns server
               code == 6008;   // once ignored, always ignored
    }

}
