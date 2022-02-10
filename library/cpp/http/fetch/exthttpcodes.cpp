#include "exthttpcodes.h"

#include <cstring>

const ui16 CrazyServer = ShouldDelete | MarkSuspect;

struct http_flag {
    ui16 http;
    ui16 flag;
};
static http_flag HTTP_FLAG[] = {
    {HTTP_CONTINUE, MarkSuspect},            // 100
    {HTTP_SWITCHING_PROTOCOLS, CrazyServer}, // 101
    {HTTP_PROCESSING, CrazyServer},          // 102

    {HTTP_OK, ShouldReindex},                            // 200
    {HTTP_CREATED, CrazyServer},                         // 201
    {HTTP_ACCEPTED, ShouldDelete},                       // 202
    {HTTP_NON_AUTHORITATIVE_INFORMATION, ShouldReindex}, // 203
    {HTTP_NO_CONTENT, ShouldDelete},                     // 204
    {HTTP_RESET_CONTENT, ShouldDelete},                  // 205
    {HTTP_PARTIAL_CONTENT, ShouldReindex},               // 206
    {HTTP_MULTI_STATUS, CrazyServer},                    // 207
    {HTTP_ALREADY_REPORTED, CrazyServer},                // 208
    {HTTP_IM_USED, CrazyServer},                         // 226

    {HTTP_MULTIPLE_CHOICES, CheckLinks | ShouldDelete},                  // 300
    {HTTP_MOVED_PERMANENTLY, CheckLocation | ShouldDelete | MoveRedir},  // 301
    {HTTP_FOUND, CheckLocation | ShouldDelete | MoveRedir},              // 302
    {HTTP_SEE_OTHER, CheckLocation | ShouldDelete | MoveRedir},          // 303
    {HTTP_NOT_MODIFIED, 0},                                              // 304
    {HTTP_USE_PROXY, ShouldDelete},                                      // 305
    {HTTP_TEMPORARY_REDIRECT, CheckLocation | ShouldDelete | MoveRedir}, // 307
    {HTTP_PERMANENT_REDIRECT, CheckLocation | ShouldDelete | MoveRedir}, // 308

    {HTTP_BAD_REQUEST, CrazyServer},                                       // 400
    {HTTP_UNAUTHORIZED, ShouldDelete},                                     // 401
    {HTTP_PAYMENT_REQUIRED, ShouldDelete},                                 // 402
    {HTTP_FORBIDDEN, ShouldDelete},                                        // 403
    {HTTP_NOT_FOUND, ShouldDelete},                                        // 404
    {HTTP_METHOD_NOT_ALLOWED, ShouldDelete},                               // 405
    {HTTP_NOT_ACCEPTABLE, ShouldDelete},                                   // 406
    {HTTP_PROXY_AUTHENTICATION_REQUIRED, CrazyServer},                     // 407
    {HTTP_REQUEST_TIME_OUT, ShouldDisconnect | ShouldRetry | MarkSuspect}, // 408
    {HTTP_CONFLICT, MarkSuspect},                                          // 409
    {HTTP_GONE, ShouldDelete},                                             // 410
    {HTTP_LENGTH_REQUIRED, CrazyServer},                                   // 411
    {HTTP_PRECONDITION_FAILED, CrazyServer},                               // 412
    {HTTP_REQUEST_ENTITY_TOO_LARGE, CrazyServer},                          // 413
    {HTTP_REQUEST_URI_TOO_LARGE, ShouldDelete},                            // 414
    {HTTP_UNSUPPORTED_MEDIA_TYPE, CrazyServer},                            // 415
    {HTTP_REQUESTED_RANGE_NOT_SATISFIABLE, CrazyServer},                   // 416
    {HTTP_EXPECTATION_FAILED, ShouldDelete},                               // 417
    {HTTP_I_AM_A_TEAPOT, CrazyServer},                                     // 418
    {HTTP_AUTHENTICATION_TIMEOUT, ShouldDelete},                           // 419

    {HTTP_MISDIRECTED_REQUEST, CrazyServer},                                // 421
    {HTTP_UNPROCESSABLE_ENTITY, CrazyServer},                               // 422
    {HTTP_LOCKED, ShouldDelete},                                            // 423
    {HTTP_FAILED_DEPENDENCY, CrazyServer},                                  // 424
    {HTTP_UPGRADE_REQUIRED, ShouldDelete},                                  // 426
    {HTTP_PRECONDITION_REQUIRED, ShouldDelete},                             // 428
    {HTTP_TOO_MANY_REQUESTS, ShouldDisconnect | ShouldRetry | MarkSuspect}, // 429
    {HTTP_UNAVAILABLE_FOR_LEGAL_REASONS, ShouldDelete},                     // 451

    {HTTP_INTERNAL_SERVER_ERROR, MarkSuspect},                                // 500
    {HTTP_NOT_IMPLEMENTED, ShouldDelete | ShouldDisconnect},                  // 501
    {HTTP_BAD_GATEWAY, MarkSuspect},                                          // 502
    {HTTP_SERVICE_UNAVAILABLE, ShouldDisconnect | ShouldRetry | MarkSuspect}, // 503
    {HTTP_GATEWAY_TIME_OUT, ShouldDisconnect | ShouldRetry | MarkSuspect},    // 504
    {HTTP_HTTP_VERSION_NOT_SUPPORTED, CrazyServer | ShouldDisconnect},        // 505

    {HTTP_VARIANT_ALSO_NEGOTIATES, CrazyServer | ShouldDisconnect},                // 506
    {HTTP_INSUFFICIENT_STORAGE, CrazyServer | ShouldDisconnect},                   // 507
    {HTTP_LOOP_DETECTED, CrazyServer | ShouldDisconnect},                          // 508
    {HTTP_BANDWIDTH_LIMIT_EXCEEDED, ShouldDisconnect | ShouldRetry | MarkSuspect}, // 509
    {HTTP_NOT_EXTENDED, ShouldDelete},                                             // 510
    {HTTP_NETWORK_AUTHENTICATION_REQUIRED, ShouldDelete},                          // 511

    // custom
    {HTTP_BAD_RESPONSE_HEADER, CrazyServer},                             // 1000
    {HTTP_CONNECTION_LOST, ShouldRetry},                                 // 1001
    {HTTP_BODY_TOO_LARGE, ShouldDelete | CanBeFake},                     // 1002
    {HTTP_ROBOTS_TXT_DISALLOW, ShouldDelete},                            // 1003
    {HTTP_BAD_URL, ShouldDelete},                                        // 1004
    {HTTP_BAD_MIME, ShouldDelete},                                       // 1005
    {HTTP_DNS_FAILURE, ShouldDisconnect | MarkSuspect},                  // 1006
    {HTTP_BAD_STATUS_CODE, CrazyServer},                                 // 1007
    {HTTP_BAD_HEADER_STRING, CrazyServer},                               // 1008
    {HTTP_BAD_CHUNK, CrazyServer},                                       // 1009
    {HTTP_CONNECT_FAILED, ShouldDisconnect | ShouldRetry | MarkSuspect}, // 1010
    {HTTP_FILTER_DISALLOW, ShouldDelete},                                // 1011
    {HTTP_LOCAL_EIO, ShouldRetry},                                       // 1012
    {HTTP_BAD_CONTENT_LENGTH, ShouldDelete},                             // 1013
    {HTTP_BAD_ENCODING, ShouldDelete},                                   // 1014
    {HTTP_LENGTH_UNKNOWN, ShouldDelete},                                 // 1015
    {HTTP_HEADER_EOF, ShouldRetry | CanBeFake},                          // 1016
    {HTTP_MESSAGE_EOF, ShouldRetry | CanBeFake},                         // 1017
    {HTTP_CHUNK_EOF, ShouldRetry | CanBeFake},                           // 1018
    {HTTP_PAST_EOF, ShouldRetry | ShouldDelete | CanBeFake},             // 1019
    {HTTP_HEADER_TOO_LARGE, ShouldDelete},                               // 1020
    {HTTP_URL_TOO_LARGE, ShouldDelete},                                  // 1021
    {HTTP_INTERRUPTED, 0},                                               // 1022
    {HTTP_CUSTOM_NOT_MODIFIED, 0},                                       // 1023
    {HTTP_BAD_CONTENT_ENCODING, ShouldDelete},                           // 1024
    {HTTP_PROXY_UNKNOWN, 0},                                             // 1030
    {HTTP_PROXY_REQUEST_TIME_OUT, 0},                                    // 1031
    {HTTP_PROXY_INTERNAL_ERROR, 0},                                      // 1032
    {HTTP_PROXY_CONNECT_FAILED, 0},                                      // 1033
    {HTTP_PROXY_CONNECTION_LOST, 0},                                     // 1034
    {HTTP_PROXY_NO_PROXY, 0},                                            // 1035
    {HTTP_PROXY_ERROR, 0},                                               // 1036
    {HTTP_SSL_ERROR, 0},                                                 // 1037
    {HTTP_CACHED_COPY_NOT_FOUND, 0},                                     // 1038
    {HTTP_TIMEDOUT_WHILE_BYTES_RECEIVING, ShouldRetry},                  // 1039
    {HTTP_FETCHER_BAD_RESPONSE, 0},                                      // 1040
    {HTTP_FETCHER_MB_ERROR, 0},                                          // 1041
    {HTTP_SSL_CERT_ERROR, 0},                                            // 1042

    // Custom (replace HTTP 200/304)
    {EXT_HTTP_MIRRMOVE, 0},                                          // 2000
    {EXT_HTTP_MANUAL_DELETE, ShouldDelete},                          // 2001
    {EXT_HTTP_NOTUSED2, ShouldDelete},                               // 2002
    {EXT_HTTP_NOTUSED3, ShouldDelete},                               // 2003
    {EXT_HTTP_REFRESH, ShouldDelete | CheckLinks | MoveRedir},       // 2004
    {EXT_HTTP_NOINDEX, ShouldDelete | CheckLinks},                   // 2005
    {EXT_HTTP_BADCODES, ShouldDelete},                               // 2006
    {EXT_HTTP_SITESTAT, ShouldDelete},                               // 2007
    {EXT_HTTP_IOERROR, ShouldDelete},                                // 2008
    {EXT_HTTP_BASEERROR, ShouldDelete},                              // 2009
    {EXT_HTTP_PARSERROR, ShouldDelete | CanBeFake},                  // 2010
    {EXT_HTTP_BAD_CHARSET, ShouldDelete | CheckLinks},               // 2011
    {EXT_HTTP_BAD_LANGUAGE, ShouldDelete | CheckLinks},              // 2012
    {EXT_HTTP_NUMERERROR, ShouldDelete},                             // 2013
    {EXT_HTTP_EMPTYDOC, ShouldDelete | CheckLinks},                  // 2014
    {EXT_HTTP_HUGEDOC, ShouldDelete},                                // 2015
    {EXT_HTTP_LINKGARBAGE, ShouldDelete},                            // 2016
    {EXT_HTTP_PARSERFAIL, ShouldDelete},                             // 2019
    {EXT_HTTP_GZIPERROR, ShouldDelete},                              // 2020
    {EXT_HTTP_MANUAL_DELETE_URL, ShouldDelete},                      // 2022
    {EXT_HTTP_CUSTOM_PARTIAL_CONTENT, ShouldReindex},                // 2023
    {EXT_HTTP_EMPTY_RESPONSE, ShouldDelete},                         // 2024
    {EXT_HTTP_REL_CANONICAL, ShouldDelete | CheckLinks | MoveRedir}, // 2025
    {0, 0}};

static ui16* prepare_flags(http_flag* arg) {
    static ui16 flags[EXT_HTTP_CODE_MAX];
    http_flag* ptr;
    size_t i;

    // устанавливаем значение по умолчанию для кодов не перечисленных в таблице выше
    for (i = 0; i < EXT_HTTP_CODE_MAX; ++i)
        flags[i] = CrazyServer;

    // устанавливаем флаги для перечисленных кодов
    for (ptr = arg; ptr->http; ++ptr)
        flags[ptr->http & (EXT_HTTP_CODE_MAX - 1)] = ptr->flag;

    // для стандартных кодов ошибок берем флаги из первого кода каждой группы и проставляем их
    // всем кодам не перечисленным в таблице выше
    for (size_t group = 0; group < 1000; group += 100)
        for (size_t j = group + 1; j < group + 100; ++j)
            flags[j] = flags[group];

    // предыдущий цикл затер некоторые флаги перечисленные в таблице выше
    // восстанавливаем их
    for (ptr = arg; ptr->http; ++ptr)
        flags[ptr->http & (EXT_HTTP_CODE_MAX - 1)] = ptr->flag;

    return flags;
}

ui16* http2status = prepare_flags(HTTP_FLAG);

TStringBuf ExtHttpCodeStr(int code) noexcept {
    if (code < HTTP_CODE_MAX) {
        return HttpCodeStr(code);
    }
    switch (code) {
        case HTTP_BAD_RESPONSE_HEADER:
            return TStringBuf("Bad response header");
        case HTTP_CONNECTION_LOST:
            return TStringBuf("Connection lost");
        case HTTP_BODY_TOO_LARGE:
            return TStringBuf("Body too large");
        case HTTP_ROBOTS_TXT_DISALLOW:
            return TStringBuf("robots.txt disallow");
        case HTTP_BAD_URL:
            return TStringBuf("Bad url");
        case HTTP_BAD_MIME:
            return TStringBuf("Bad mime type");
        case HTTP_DNS_FAILURE:
            return TStringBuf("Dns failure");
        case HTTP_BAD_STATUS_CODE:
            return TStringBuf("Bad status code");
        case HTTP_BAD_HEADER_STRING:
            return TStringBuf("Bad header string");
        case HTTP_BAD_CHUNK:
            return TStringBuf("Bad chunk");
        case HTTP_CONNECT_FAILED:
            return TStringBuf("Connect failed");
        case HTTP_FILTER_DISALLOW:
            return TStringBuf("Filter disallow");
        case HTTP_LOCAL_EIO:
            return TStringBuf("Local eio");
        case HTTP_BAD_CONTENT_LENGTH:
            return TStringBuf("Bad content length");
        case HTTP_BAD_ENCODING:
            return TStringBuf("Bad encoding");
        case HTTP_LENGTH_UNKNOWN:
            return TStringBuf("Length unknown");
        case HTTP_HEADER_EOF:
            return TStringBuf("Header EOF");
        case HTTP_MESSAGE_EOF:
            return TStringBuf("Message EOF");
        case HTTP_CHUNK_EOF:
            return TStringBuf("Chunk EOF");
        case HTTP_PAST_EOF:
            return TStringBuf("Past EOF");
        case HTTP_HEADER_TOO_LARGE:
            return TStringBuf("Header is too large");
        case HTTP_URL_TOO_LARGE:
            return TStringBuf("Url is too large");
        case HTTP_INTERRUPTED:
            return TStringBuf("Interrupted");
        case HTTP_CUSTOM_NOT_MODIFIED:
            return TStringBuf("Signature detector thinks that doc is not modified");
        case HTTP_BAD_CONTENT_ENCODING:
            return TStringBuf("Bad content encoding");
        case HTTP_NO_RESOURCES:
            return TStringBuf("No resources");
        case HTTP_FETCHER_SHUTDOWN:
            return TStringBuf("Fetcher shutdown");
        case HTTP_CHUNK_TOO_LARGE:
            return TStringBuf("Chunk size is too big");
        case HTTP_SERVER_BUSY:
            return TStringBuf("Server is busy");
        case HTTP_SERVICE_UNKNOWN:
            return TStringBuf("Service is unknown");
        case HTTP_PROXY_UNKNOWN:
            return TStringBuf("Zora: unknown error");
        case HTTP_PROXY_REQUEST_TIME_OUT:
            return TStringBuf("Zora: request time out");
        case HTTP_PROXY_INTERNAL_ERROR:
            return TStringBuf("Zora: internal server error");
        case HTTP_PROXY_CONNECT_FAILED:
            return TStringBuf("Spider proxy connect failed");
        case HTTP_PROXY_CONNECTION_LOST:
            return TStringBuf("Spider proxy connection lost");
        case HTTP_PROXY_NO_PROXY:
            return TStringBuf("Spider proxy no proxy alive in region");
        case HTTP_PROXY_ERROR:
            return TStringBuf("Spider proxy returned custom error");
        case HTTP_SSL_ERROR:
            return TStringBuf("Ssl library returned error");
        case HTTP_CACHED_COPY_NOT_FOUND:
            return TStringBuf("Cached copy for the url is not available");
        case HTTP_TIMEDOUT_WHILE_BYTES_RECEIVING:
            return TStringBuf("Timed out while bytes receiving");

            // TODO: messages for >2000 codes

        default:
            return TStringBuf("Unknown HTTP code");
    }
}
