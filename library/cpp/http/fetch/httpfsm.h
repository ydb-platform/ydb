#pragma once

#include "httpheader.h"

#include <util/system/maxlen.h>
#include <util/datetime/parser.h>

#include <time.h>

struct THttpHeaderParser {
    static constexpr int ErrFirstlineTypeMismatch = -3;
    static constexpr int ErrHeader = -2;
    static constexpr int Err = -1;
    static constexpr int Final = 0;
    static constexpr int NeedMore = 1;
    static constexpr int Accepted = 2;

    int Execute(const void* inBuf, size_t len) {
        return execute((unsigned char*)inBuf, (int)len);
    }

    int Execute(TStringBuf str) {
        return Execute(str.data(), str.size());
    }

    int Init(THttpHeader* h) {
        int ret = Init((THttpBaseHeader*)(h));
        hd = h;
        hd->Init();
        hreflangpos = hd->hreflangs;
        hreflangspace = HREFLANG_MAX;
        return ret;
    }

    int Init(THttpAuthHeader* h) {
        int ret = Init((THttpHeader*)(h));
        auth_hd = h;
        return ret;
    }
    int Init(THttpRequestHeader* h) {
        int ret = Init((THttpBaseHeader*)(h));
        request_hd = h;
        request_hd->Init();
        return ret;
    }

    THttpHeader* hd;
    long I;
    int Dc;
    TDateTimeFieldsDeprecated DateTimeFields;
    char buf[FETCHER_URL_MAX];
    size_t buflen;
    char* lastchar;

    const unsigned char* langstart;
    size_t langlen;

    char* hreflangpos;
    size_t hreflangspace;

    bool AcceptingXRobots;

    THttpAuthHeader* auth_hd;
    THttpRequestHeader* request_hd;

private:
    THttpBaseHeader* base_hd;
    int cs;

private:
    int Init(THttpBaseHeader* header) {
        base_hd = header;
        auth_hd = nullptr;
        request_hd = nullptr;
        hd = nullptr;
        init();
        return 0;
    }

    int execute(unsigned char* inBuf, int len);
    void init();
};

struct THttpChunkParser {
    int Execute(const void* inBuf, int len) {
        return execute((unsigned char*)inBuf, len);
    }

    int Init() {
        init();
        return 0;
    }

    int chunk_length;
    char* lastchar;
    long I;
    int Dc;
    i64 cnt64;

private:
    int cs;
    int execute(unsigned char* inBuf, int len);
    void init();
};
