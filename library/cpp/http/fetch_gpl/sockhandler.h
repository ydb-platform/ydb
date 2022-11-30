#pragma once

#include <library/cpp/http/fetch/sockhandler.h>
#include <contrib/libs/matrixssl/matrixSsl.h>

class TSslSocketBase {
public:
    static bool StaticInit(const char* caFile = nullptr);
    static bool StaticInit(unsigned char* caBuff, int caLen);
    static void StaticTerm();
    static int CertChecker(sslCertInfo_t* cert, void* arg);
    enum ECertErrors {
        SSL_CERT_UNTRUSTED = 0x01,
        SSL_CERT_BAD_CHAIN = 0x02,
        SSL_CERT_HOSTNAME_MISMATCH = 0x04,
        SSL_CERT_EXPIRED = 0x08
    };

    struct TSessIdDestructor {
        static void Destroy(sslSessionId_t* id) {
            matrixSslFreeSessionId(id);
        }
    };

protected:
    enum ESslStatus {
        SSLSOCKET_EOF = 0x1,
        SSLSOCKET_CLOSE_NOTIFY = 0x2
    };
    struct TSocketCtx {
        ui16 SslError;
        ui16 CertErrors;
        const char* Host;
        size_t HostLen;
        TSocketCtx()
            : SslError(0)
            , CertErrors(0)
            , Host(nullptr)
            , HostLen(0)
        {
        }
    };

protected:
    class TBufferAllocator {
        class TChunk;
        typedef TIntrusiveSListItem<TChunk> TChunkBase;

        class TChunk: public TChunkBase {
        public:
            inline unsigned char* ToPointer() {
                //shut up clang warning
                (void)Buf;

                return (unsigned char*)this;

                static_assert(sizeof(TChunk) >= SSL_MAX_BUF_SIZE, "expect sizeof(TChunk) >= SSL_MAX_BUF_SIZE");
            }
            static inline TChunk* FromPointer(unsigned char* ptr) {
                return (TChunk*)ptr;
            }

        private:
            unsigned char Buf[SSL_MAX_BUF_SIZE - sizeof(TChunkBase)];
        };

    public:
        TBufferAllocator()
            : NFree(0)
            , NAllocated(0)
        {
            static_assert(InitialItems > 0 && A1 > 0 && A2 > 0 && A1 >= A2, "expect InitialItems > 0 && A1 > 0 && A2 > 0 && A1 >= A2");
            ResizeList(InitialItems);
        }

        ~TBufferAllocator() {
            Y_VERIFY(!NAllocated, "Ssl bufferAllocator: %" PRISZT " blocks lost!", NAllocated);
            ResizeList(0);
        }

        unsigned char* Alloc() {
            TGuard<TMutex> guard(Lock);
            if (Free_.Empty())
                ResizeList(A2 * NAllocated);

            NAllocated++;
            NFree--;
            return Free_.PopFront()->ToPointer();
        }

        void Free(unsigned char* p) {
            if (!p)
                return;
            TGuard<TMutex> guard(Lock);
            Y_VERIFY(NAllocated, "Ssl bufferAllocator: multiple frees?");
            TChunk* ch = TChunk::FromPointer(p);
            Free_.PushFront(ch);
            NFree++;
            NAllocated--;

            // destroy some items if NFree/NAllocated increased too much
            size_t newSize = A2 * NAllocated;
            if (NAllocated + newSize >= InitialItems && NFree >= A1 * NAllocated)
                ResizeList(newSize);
        }

    private:
        inline void ResizeList(size_t newSize) {
            while (NFree < newSize) {
                Free_.PushFront(new TChunk);
                NFree++;
            }
            while (NFree > newSize) {
                TChunk* ch = Free_.PopFront();
                Y_VERIFY(ch, "Ssl bufferAllocator: internal error");
                delete ch;
                NFree--;
            }
        }

        static const size_t InitialItems = 100;
        static const unsigned A1 = 3; // maximum reserved/allocated ratio
        static const unsigned A2 = 1; // if ratio A1 is reached, decrease by A2

        TIntrusiveSList<TChunk> Free_;
        size_t NFree;
        size_t NAllocated;
        TMutex Lock;
    };

    static bool Initialized;
    static sslKeys_t* Keys;
    static THolder<TBufferAllocator> BufAlloc;

public:
    class TFakeLogger {
    public:
        static void Write(const char* /*format*/, ...) {
        }
    };
};

template <class TSocketHandler = TSimpleSocketHandler, class TErrorLogger = TSslSocketBase::TFakeLogger>
class TSslSocketHandler: public TSslSocketBase, protected TSocketHandler, TNonCopyable {
public:
    struct TSocketCtx: public TSslSocketBase::TSocketCtx {
        sslBuf_t InBuf;
        sslBuf_t InSock;
        THolder<sslSessionId_t, TSessIdDestructor> CachedSession;
        TSocketCtx() {
            Zero(InBuf);
            Zero(InSock);
        }
        void AllocBuffers() {
            Y_ASSERT(InBuf.size == 0);
            InBuf.size = SSL_MAX_BUF_SIZE;
            InBuf.start = InBuf.end = InBuf.buf = BufAlloc->Alloc();

            Y_ASSERT(InSock.size == 0);
            InSock.size = SSL_MAX_BUF_SIZE;
            InSock.start = InSock.end = InSock.buf = BufAlloc->Alloc();
        }
        void FreeBuffers() {
            if (InBuf.buf) {
                if (InBuf.end - InBuf.start) {
                    // We had some data read and decrypted. Too sad, nobody needs it now :(
                    TErrorLogger::Write("TSocketCtx::FreeBuffers: %i bytes of data lost in InBuf (%s)\n", (int)(InBuf.end - InBuf.start), TString(Host, HostLen).data());
                }
                BufAlloc->Free(InBuf.buf);
                Zero(InBuf);
            }
            if (InSock.buf) {
                if (InSock.end - InSock.start) {
                    // We had some data read and waiting for decryption. Most likely we disconnected before server's "bye".
                    TErrorLogger::Write("TSocketCtx::FreeBuffers: %i bytes of data lost in InSock (%s)\n", (int)(InSock.end - InSock.start), TString(Host, HostLen).data());
                }
                BufAlloc->Free(InSock.buf);
                Zero(InSock);
            }
        }
        void ResetBuffers() {
            InBuf.start = InBuf.end = InBuf.buf;
            InSock.start = InSock.end = InSock.buf;
        }
    };

    TSslSocketHandler()
        : TSocketHandler()
        , Ssl(nullptr)
    {
        Y_VERIFY(TSslSocketBase::Initialized, "Ssl library isn't initialized. Call TSslSocketBase::StaticInit() first");
    }

    virtual ~TSslSocketHandler() {
        Y_ASSERT(Initialized);
        Disconnect();
    }

    int Good() const {
        return TSocketHandler::Good();
    }
    bool HasSsl() const {
        return Ssl;
    }

    // set reconnect "true" to try to recover from cached session id
    int Connect(TSocketCtx* ctx, const TAddrList& addrs, TDuration timeout, bool isHttps, bool reconnect = false);

    // for debug "file" socket
    bool open(const char* name) {
        Y_ASSERT(Initialized);
        Disconnect();
        return TSocketHandler::open(name);
    }

    void Disconnect(TSocketCtx* ctx = nullptr) { // if ctx is non-NULL, cache session id in it.
        Y_ASSERT(Initialized);
        if (Ssl) {
            if (ctx) {
                sslSessionId_t* cs;
                if (matrixSslGetSessionId(Ssl, &cs) < 0) {
                    cs = nullptr;
                    TErrorLogger::Write("TSslSocketHandler::Disconnect: failed to create session id for host %s\n", TString(ctx->Host, ctx->HostLen).data());
                }
                ctx->CachedSession.Reset(cs);
            }
            matrixSslDeleteSession(Ssl);
            Ssl = nullptr;
        }
        TSocketHandler::Disconnect();
    }

    void shutdown() {
        TSocketHandler::shutdown();
    }

    ssize_t send(TSocketCtx* ctx, const void* message, size_t messlen) {
        Y_ASSERT(TSocketHandler::Good());
        if (!Ssl)
            return TSocketHandler::send(message, messlen);
        int status;
        int rc = SslWrite(ctx, static_cast<const char*>(message), (int)messlen, &status);
        if (rc < 0) {
            errno = status;
            ctx->ResetBuffers();
            Disconnect();
            return false;
        }
        Y_ASSERT((size_t)rc == messlen);
        return true;
    }

    bool peek(TSocketCtx* ctx) {
        if (!Ssl)
            return TSocketHandler::peek();
        int rc;
        int status;
        while (true) {
            rc = SslRead(ctx, nullptr, 0, &status);
            if (rc < 0) {
                errno = status;
                ctx->ResetBuffers();
                Disconnect();
                return false;
            } else if (rc > 0) {
                return true;
            }
            // else if (rc == 0)
            if (status) {
                errno = status;
                ctx->ResetBuffers();
                Disconnect();
                return false;
            }
        }
    }

    ssize_t read(TSocketCtx* ctx, void* message, size_t messlen) {
        if (!Ssl)
            return TSocketHandler::read(message, messlen);
        int rc;
        int status;
        if (!messlen)
            return 0;
        while (true) {
            rc = SslRead(ctx, static_cast<char*>(message), (int)messlen, &status);
            if (rc < 0) {
                errno = status;
                ctx->ResetBuffers();
                Disconnect();
                return rc;
            } else if (rc > 0)
                return rc;
            // else if (rc == 0)
            if (status) {
                errno = status;
                ctx->ResetBuffers();
                Disconnect();
                return 0;
            }
        }
    }

private:
    int SslRead(TSocketCtx* ctx, char* buf, int buflen, int* status);
    int SslWrite(TSocketCtx* ctx, const char* buf, int len, int* status);

    ssl_t* Ssl;
};

template <typename TSocketHandler, typename TErrorLogger>
int TSslSocketHandler<TSocketHandler, TErrorLogger>::Connect(TSocketCtx* ctx, const TAddrList& addrs, TDuration timeout, bool isHttps, bool reconnect) {
    Y_ASSERT(Initialized);
    ctx->SslError = 0;
    ctx->ResetBuffers();
    Disconnect();
    int res = TSocketHandler::Connect(addrs, timeout);
    if (!isHttps || res != 0) {
        ctx->CachedSession.Destroy();
        return res;
    }

    // create ssl session
    if ((res = matrixSslNewSession(&Ssl, Keys, reconnect ? ctx->CachedSession.Get() : nullptr, 0)) < 0) {
        ctx->SslError = 1;
        ctx->ResetBuffers();
        Disconnect();
        return res;
    }
    ctx->CachedSession.Destroy();

    matrixSslSetCertValidator(Ssl, CertChecker, ctx);

    // now it's time to perform handshake
    sslBuf_t outsock;
    outsock.buf = outsock.start = outsock.end = BufAlloc->Alloc();
    outsock.size = SSL_MAX_BUF_SIZE;

    res = matrixSslEncodeClientHello(Ssl, &outsock, 0);
    if (res) {
        TErrorLogger::Write("TSslSocketHandler::Connect: internal error %i\n", res);
        BufAlloc->Free(outsock.buf);
        ctx->SslError = 1;
        ctx->ResetBuffers();
        Disconnect();
        return -1;
    }

    if (!TSocketHandler::send(outsock.start, outsock.end - outsock.start)) {
        BufAlloc->Free(outsock.buf);
        ctx->SslError = 1;
        ctx->ResetBuffers();
        Disconnect();
        return -1;
    }
    BufAlloc->Free(outsock.buf);

    // SslRead will handle handshake and is suppozed to return 0
    int status, rc;
    int ncalls = 10; // FIXME: maybe it's better to check time
    while (true) {
        rc = SslRead(ctx, nullptr, 0, &status);
        if (rc == 0) {
            if (status == SSLSOCKET_EOF || status == SSLSOCKET_CLOSE_NOTIFY) {
                ctx->SslError = 1;
                ctx->ResetBuffers();
                Disconnect();
                return -1;
            }
            if (matrixSslHandshakeIsComplete(Ssl))
                break;
            if (--ncalls <= 0) {
                TErrorLogger::Write("TSslSocketHandler::Connect: handshake too long (server wants multiple handshakes maybe)\n");
                ctx->SslError = 1;
                ctx->ResetBuffers();
                Disconnect();
                return -1;
            }
            continue;
        } else if (rc > 0) {
            TErrorLogger::Write("TSslSocketHandler::Connect: server sent data instead of a handshake\n");
            ctx->SslError = 1;
            ctx->ResetBuffers();
            Disconnect();
            return -1;
        } else { // rc < 0
            //this is an error
            ctx->SslError = 1;
            ctx->ResetBuffers();
            Disconnect();
            return -1;
        }
    }

    return 0;
}

template <typename TSocketHandler, typename TErrorLogger>
int TSslSocketHandler<TSocketHandler, TErrorLogger>::SslRead(TSocketCtx* ctx, char* buf, int buflen, int* status) {
    Y_ASSERT(Initialized);
    int remaining, bytes, rc;

    *status = 0;
    if (Ssl == nullptr || buflen < 0)
        return -1;

    // Return data if we still have cached
    if (ctx->InBuf.start < ctx->InBuf.end) {
        remaining = (int)(ctx->InBuf.end - ctx->InBuf.start);
        if (!buflen) // polling
            return remaining;
        bytes = Min(buflen, remaining);
        memcpy(buf, ctx->InBuf.start, bytes);
        ctx->InBuf.start += bytes;
        return bytes;
    }

    // Pack buffered socket data (if any) so that start is at zero.
    if (ctx->InSock.buf < ctx->InSock.start) {
        if (ctx->InSock.start == ctx->InSock.end) {
            ctx->InSock.start = ctx->InSock.end = ctx->InSock.buf;
        } else {
            memmove(ctx->InSock.buf, ctx->InSock.start, ctx->InSock.end - ctx->InSock.start);
            ctx->InSock.end -= (ctx->InSock.start - ctx->InSock.buf);
            ctx->InSock.start = ctx->InSock.buf;
        }
    }

    bool performRead = false;
    bool dontPerformRead = false;
    unsigned char error;
    unsigned char alertLevel;
    unsigned char alertDescription;

    while (true) {
        // Read data from socket
        if (!dontPerformRead && (performRead || ctx->InSock.end == ctx->InSock.start)) {
            performRead = true;
            bytes = TSocketHandler::read((void*)ctx->InSock.end, (ctx->InSock.buf + ctx->InSock.size) - ctx->InSock.end);
            if (bytes == SOCKET_ERROR) {
                *status = errno;
                return -1;
            }
            if (bytes == 0) {
                *status = SSLSOCKET_EOF;
                return 0;
            }
            ctx->InSock.end += bytes;
        }
        dontPerformRead = false;

        error = 0;
        alertLevel = 0;
        alertDescription = 0;

        ctx->InBuf.start = ctx->InBuf.end = ctx->InBuf.buf;
        rc = matrixSslDecode(Ssl, &ctx->InSock, &ctx->InBuf, &error, &alertLevel, &alertDescription);

        switch (rc) {
            // Successfully decoded a record that did not return data or require a response.
            case SSL_SUCCESS:
                return 0;

            case SSL_PROCESS_DATA:
                rc = (int)(ctx->InBuf.end - ctx->InBuf.start);
                if (!buflen)
                    return rc;
                rc = Min(rc, buflen);
                memcpy(buf, ctx->InBuf.start, rc);
                ctx->InBuf.start += rc;
                return rc;

            case SSL_SEND_RESPONSE:
                if (!TSocketHandler::send(ctx->InBuf.start, ctx->InBuf.end - ctx->InBuf.start)) {
                    *status = errno;
                    return -1;
                }
                ctx->InBuf.start = ctx->InBuf.end = ctx->InBuf.buf;
                return 0;

            case SSL_ERROR:
                if (ctx->InBuf.start < ctx->InBuf.end)
                    TSocketHandler::send(ctx->InBuf.start, ctx->InBuf.end - ctx->InBuf.start);
                ctx->InBuf.start = ctx->InBuf.end = ctx->InBuf.buf;
                ctx->SslError = 1;
                return -1;

            case SSL_ALERT:
                if (alertDescription == SSL_ALERT_CLOSE_NOTIFY) {
                    *status = SSLSOCKET_CLOSE_NOTIFY;
                    ctx->InBuf.start = ctx->InBuf.end = ctx->InBuf.buf;
                    return 0;
                }
                ctx->InBuf.start = ctx->InBuf.end = ctx->InBuf.buf;
                ctx->SslError = 1;
                return -1;

            case SSL_PARTIAL:
                if (ctx->InSock.start == ctx->InSock.buf && ctx->InSock.end == (ctx->InSock.buf + ctx->InSock.size)) {
                    ctx->InSock.start = ctx->InSock.end = ctx->InSock.buf;
                    ctx->SslError = 1;
                    return -1;
                }
                if (!performRead) {
                    performRead = 1;
                    ctx->InBuf.start = ctx->InBuf.end = ctx->InBuf.buf;
                    continue;
                } else {
                    ctx->InBuf.start = ctx->InBuf.end = ctx->InBuf.buf;
                    return 0;
                }

            case SSL_FULL:
                ctx->InBuf.start = ctx->InBuf.end = ctx->InBuf.buf;
                ctx->SslError = 1;
                return -1;
        }
    }

    return 0;
}

template <typename TSocketHandler, typename TErrorLogger>
int TSslSocketHandler<TSocketHandler, TErrorLogger>::SslWrite(TSocketCtx* ctx, const char* buf, int len, int* status) {
    Y_ASSERT(Initialized);
    if (len <= 0)
        return len ? -1 : 0;
    int rc;
    *status = 0;

    sslBuf_t outsock;
    outsock.size = SSL_MAX_BUF_SIZE;
    outsock.start = outsock.end = outsock.buf = BufAlloc->Alloc();

    size_t remaining = len;
    while (remaining) {
        size_t l = Min<size_t>(remaining, SSL_MAX_PLAINTEXT_LEN);
        rc = matrixSslEncode(Ssl, (const unsigned char*)buf, l, &outsock);
        if (rc <= 0) {
            TErrorLogger::Write("TSslSocketHandler::SslWrite: internal error: %u\n", rc);
            BufAlloc->Free(outsock.buf);
            ctx->SslError = 1;
            return -1;
        }
        rc = TSocketHandler::send(outsock.start, outsock.end - outsock.start);
        if (!rc) {
            *status = errno;
            BufAlloc->Free(outsock.buf);
            return -1;
        }
        remaining -= l;
        buf += l;
        outsock.start = outsock.end = outsock.buf;
    }
    BufAlloc->Free(outsock.buf);
    return len;
}
