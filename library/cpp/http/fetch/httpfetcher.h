#pragma once

#ifdef _MSC_VER
#include <io.h>
#endif

#include <library/cpp/http/misc/httpdate.h>

#include "httpagent.h"
#include "httpparser.h"

struct TFakeBackup {
    int Write(void* /*buf*/, size_t /*size*/) {
        return 0;
    }
};

template <size_t bufsize = 5000>
struct TFakeAlloc {
    void Shrink(void* /*buf*/, size_t /*size*/) {
    }
    void* Grab(size_t /*min*/, size_t* real) {
        *real = bufsize;
        return buf;
    }
    char buf[bufsize];
};

template <typename TAlloc = TFakeAlloc<>,
          typename TCheck = TFakeCheck<>,
          typename TWriter = TFakeBackup,
          typename TAgent = THttpAgent<>>
class THttpFetcher: public THttpParser<TCheck>, public TAlloc, public TWriter, public TAgent {
public:
    static const size_t TCP_MIN = 1500;
    static int TerminateNow;

    THttpFetcher()
        : THttpParser<TCheck>()
        , TAlloc()
        , TWriter()
        , TAgent()
    {
    }

    virtual ~THttpFetcher() {
    }

    int Fetch(THttpHeader* header, const char* path, const char* const* headers, int persistent, bool head_request = false) {
        int ret = 0;
        int fetcherr = 0;

        THttpParser<TCheck>::Init(header, head_request);
        const char* scheme = HttpUrlSchemeKindToString((THttpURL::TSchemeKind)TAgent::GetScheme());
        size_t schemelen = strlen(scheme);
        if (*path == '/') {
            header->base = TStringBuf(scheme, schemelen);
            header->base += TStringBuf("://", 3);
            header->base += TStringBuf(TAgent::pHostBeg, TAgent::pHostEnd - TAgent::pHostBeg);
            header->base += path;
        } else {
            if (strlen(path) >= FETCHER_URL_MAX) {
                header->error = HTTP_URL_TOO_LARGE;
                return 0;
            }
            header->base = path;
        }

        if ((ret = TAgent::RequestGet(path, headers, persistent, head_request))) {
            header->error = (i16)ret;
            return 0;
        }

        bool inheader = 1;
        void *bufptr = nullptr, *buf = nullptr, *parsebuf = nullptr;
        ssize_t got;
        size_t buffree = 0, bufsize = 0, buflen = 0;
        size_t maxsize = TCheck::GetMaxHeaderSize();
        do {
            if (buffree < TCP_MIN) {
                if (buf) {
                    TAlloc::Shrink(buf, buflen - buffree);
                    if (TWriter::Write(buf, buflen - buffree) < 0) {
                        buf = nullptr;
                        ret = EIO;
                        break;
                    }
                }
                if (!(buf = TAlloc::Grab(TCP_MIN, &buflen))) {
                    ret = ENOMEM;
                    break;
                }
                bufptr = buf;
                buffree = buflen;
            }
            if ((got = TAgent::read(bufptr, buffree)) < 0) {
                fetcherr = errno;
                if (errno == EINTR)
                    header->error = HTTP_INTERRUPTED;
                else if (errno == ETIMEDOUT)
                    header->error = HTTP_TIMEDOUT_WHILE_BYTES_RECEIVING;
                else
                    header->error = HTTP_CONNECTION_LOST;

                break;
            }

            parsebuf = bufptr;
            bufptr = (char*)bufptr + got;
            bufsize += got;
            buffree -= got;

            THttpParser<TCheck>::Parse(parsebuf, got);

            if (header->error)
                break; //if ANY error ocurred we will stop download that file or will have unprognosed stream position until MAX size reached

            if (inheader && THttpParser<TCheck>::GetState() != THttpParser<TCheck>::hp_in_header) {
                inheader = 0;
                if (TCheck::Check(header))
                    break;
                if (header->header_size > (long)maxsize) {
                    header->error = HTTP_HEADER_TOO_LARGE;
                    break;
                }
            }
            if (!inheader) {
                maxsize = TCheck::GetMaxBodySize(header);
            }
            if (header->http_status >= HTTP_EXTENDED)
                break;
            if (bufsize > maxsize) {
                header->error = inheader ? HTTP_HEADER_TOO_LARGE : HTTP_BODY_TOO_LARGE;
                break;
            }
            if (TerminateNow) {
                header->error = HTTP_INTERRUPTED;
                break;
            }
        } while (THttpParser<TCheck>::GetState() > THttpParser<TCheck>::hp_eof);

        i64 Adjustment = 0;
        if (!header->error) {
            if (header->transfer_chunked) {
                Adjustment = header->header_size + header->entity_size - bufsize - 1;
            } else if (header->content_length >= 0) {
                Adjustment = header->header_size + header->content_length - bufsize;
            }
            if (Adjustment > 0)
                Adjustment = 0;
        }

        if (buf) {
            TAlloc::Shrink(buf, buflen - buffree + Adjustment);

            if (TWriter::Write(buf, buflen - buffree) < 0)
                ret = EIO;
        }
        TCheck::CheckEndDoc(header);
        if (ret || header->error || header->http_status >= HTTP_EXTENDED || header->connection_closed) {
            TAgent::Disconnect();
            if (!fetcherr)
                fetcherr = errno;
        }
        errno = fetcherr;
        return ret;
    }
};

template <typename TAlloc, typename TCheck, typename TWriter, typename TAgent>
int THttpFetcher<TAlloc, TCheck, TWriter, TAgent>::TerminateNow = 0;
