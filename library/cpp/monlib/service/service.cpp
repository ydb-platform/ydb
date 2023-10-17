#include "service.h"

#include <library/cpp/coroutine/engine/sockpool.h>
#include <library/cpp/http/io/stream.h>
#include <library/cpp/http/fetch/httpheader.h>
#include <library/cpp/http/fetch/httpfsm.h>
#include <library/cpp/uri/http_url.h>

#include <util/generic/buffer.h>
#include <util/stream/str.h>
#include <util/stream/buffer.h>
#include <util/stream/zerocopy.h>
#include <util/string/vector.h>

namespace NMonitoring {
    class THttpClient: public IHttpRequest {
    public:
        void ServeRequest(THttpInput& in, IOutputStream& out, const NAddr::IRemoteAddr* remoteAddr, const THandler& Handler) {
            try {
                try {
                    RemoteAddr = remoteAddr;
                    THttpHeaderParser parser;
                    parser.Init(&Header);
                    if (parser.Execute(in.FirstLine().data(), in.FirstLine().size()) < 0) {
                        out << "HTTP/1.1 400 Bad request\r\nConnection: Close\r\n\r\n";
                        return;
                    }
                    if (Url.Parse(Header.GetUrl().data()) != THttpURL::ParsedOK) {
                        out << "HTTP/1.1 400 Invalid url\r\nConnection: Close\r\n\r\n";
                        return;
                    }
                    TString path = GetPath();
                    if (!path.StartsWith('/')) {
                        out << "HTTP/1.1 400 Bad request\r\nConnection: Close\r\n\r\n";
                        return;
                    }
                    Headers = &in.Headers();
                    CgiParams.Scan(Url.Get(THttpURL::FieldQuery));
                } catch (...) {
                    out << "HTTP/1.1 500 Internal server error\r\nConnection: Close\r\n\r\n";
                    YSYSLOG(TLOG_ERR, "THttpClient: internal error while serving monitoring request: %s", CurrentExceptionMessage().data());
                }

                if (Header.http_method == HTTP_METHOD_POST)
                    TransferData(&in, &PostContent);

                Handler(out, *this);
                out.Finish();
            } catch (...) {
                auto msg = CurrentExceptionMessage();
                out << "HTTP/1.1 500 Internal server error\r\nConnection: Close\r\n\r\n" << msg;
                out.Finish();
                YSYSLOG(TLOG_ERR, "THttpClient: error while serving monitoring request: %s", msg.data());
            }
        }

        const char* GetURI() const override {
            return Header.request_uri.c_str();
        }
        const char* GetPath() const override {
            return Url.Get(THttpURL::FieldPath);
        }
        const TCgiParameters& GetParams() const override {
            return CgiParams;
        }
        const TCgiParameters& GetPostParams() const override {
            if (PostParams.empty() && !PostContent.Buffer().Empty())
                const_cast<THttpClient*>(this)->ScanPostParams();
            return PostParams;
        }
        TStringBuf GetPostContent() const override {
            return TStringBuf(PostContent.Buffer().Data(), PostContent.Buffer().Size());
        }
        HTTP_METHOD GetMethod() const override {
            return (HTTP_METHOD)Header.http_method;
        }
        void ScanPostParams() {
            PostParams.Scan(TStringBuf(PostContent.Buffer().data(), PostContent.Buffer().size()));
        }

        const THttpHeaders& GetHeaders() const override {
            if (Headers != nullptr) {
                return *Headers;
            }
            static THttpHeaders defaultHeaders;
            return defaultHeaders;
        }

        TString GetRemoteAddr() const override {
            return RemoteAddr ? NAddr::PrintHostAndPort(*RemoteAddr) : TString();
        }

    private:
        THttpRequestHeader Header;
        const THttpHeaders* Headers = nullptr;
        THttpURL Url;
        TCgiParameters CgiParams;
        TCgiParameters PostParams;
        TBufferOutput PostContent;
        const NAddr::IRemoteAddr* RemoteAddr = nullptr;
    };

    /* TCoHttpServer */

    class TCoHttpServer::TConnection: public THttpClient {
    public:
        TConnection(const TCoHttpServer::TAcceptFull& acc, const TCoHttpServer& parent)
            : Socket(acc.S->Release())
            , RemoteAddr(acc.Remote)
            , Parent(parent)
        {
        }

        void operator()(TCont* c) {
            try {
                THolder<TConnection> me(this);
                TContIO io(Socket, c);
                THttpInput in(&io);
                THttpOutput out(&io, &in);
                // buffer reply so there will be ne context switching
                TStringStream s;
                ServeRequest(in, s, RemoteAddr, Parent.Handler);
                out << s.Str();
                out.Finish();
            } catch (...) {
                YSYSLOG(TLOG_WARNING, "TCoHttpServer::TConnection: error: %s\n", CurrentExceptionMessage().data());
            }
        }

    private:
        TSocketHolder Socket;
        const NAddr::IRemoteAddr* RemoteAddr;
        const TCoHttpServer& Parent;
    };

    TCoHttpServer::TCoHttpServer(TContExecutor& executor, const TString& bindAddr, TIpPort port, THandler handler)
        : Executor(executor)
        , Listener(this, &executor)
        , Handler(std::move(handler))
        , BindAddr(bindAddr)
        , Port(port)
    {
        try {
            Listener.Bind(TIpAddress(bindAddr, port));
        } catch (yexception e) {
            Y_ABORT("TCoHttpServer::TCoHttpServer: couldn't bind to %s:%d\n", bindAddr.data(), port);
        }
    }

    void TCoHttpServer::Start() {
        Listener.Listen();
    }

    void TCoHttpServer::Stop() {
        Listener.Stop();
    }

    void TCoHttpServer::OnAcceptFull(const TAcceptFull& acc) {
        THolder<TConnection> conn(new TConnection(acc, *this));
        Executor.Create(*conn, "client");
        Y_UNUSED(conn.Release());
    }

    void TCoHttpServer::OnError() {
        throw; // just rethrow
    }

    void TCoHttpServer::ProcessRequest(IOutputStream& out, const IHttpRequest& request) {
        try {
            TNetworkAddress addr(BindAddr, Port);
            TSocket sock(addr);
            TSocketOutput sock_out(sock);
            TSocketInput sock_in(sock);
            sock_out << "GET " << request.GetURI() << " HTTP/1.0\r\n\r\n";
            THttpInput http_in(&sock_in);
            try {
                out << "HTTP/1.1 200 Ok\nConnection: Close\n\n";
                TransferData(&http_in, &out);
            } catch (...) {
                YSYSLOG(TLOG_DEBUG, "TCoHttpServer: while getting data from backend: %s", CurrentExceptionMessage().data());
            }
        } catch (const yexception& /*e*/) {
            out << "HTTP/1.1 500 Internal server error\nConnection: Close\n\n";
            YSYSLOG(TLOG_DEBUG, "TCoHttpServer: while getting data from backend: %s", CurrentExceptionMessage().data());
        }
    }

    /* TMtHttpServer */

    class TMtHttpServer::TConnection: public TClientRequest, public THttpClient {
    public:
        TConnection(const TMtHttpServer& parent)
            : Parent(parent)
        {
        }

        bool Reply(void*) override {
            ServeRequest(Input(), Output(), NAddr::GetPeerAddr(Socket()).Get(), Parent.Handler);
            return true;
        }

    private:
        const TMtHttpServer& Parent;
    };

    TMtHttpServer::TMtHttpServer(const TOptions& options, THandler handler, IThreadFactory* pool)
        : THttpServer(this, options, pool)
        , Handler(std::move(handler))
    {
    }

    TMtHttpServer::TMtHttpServer(const TOptions& options, THandler handler, TSimpleSharedPtr<IThreadPool> pool)
        : THttpServer(this, /* mainWorkers = */pool, /* failWorkers = */pool, options)
        , Handler(std::move(handler))
    {
    }

    bool TMtHttpServer::Start() {
        return THttpServer::Start();
    }

    void TMtHttpServer::StartOrThrow() {
        if (!Start()) {
            const auto& opts = THttpServer::Options();
            TNetworkAddress addr = opts.Host
                                       ? TNetworkAddress(opts.Host, opts.Port)
                                       : TNetworkAddress(opts.Port);
            ythrow TSystemError(GetErrorCode()) << addr;
        }
    }

    void TMtHttpServer::Stop() {
        THttpServer::Stop();
    }

    TClientRequest* TMtHttpServer::CreateClient() {
        return new TConnection(*this);
    }

    /* TService */

    TMonService::TMonService(TContExecutor& executor, TIpPort internalPort, TIpPort externalPort,
                             THandler coHandler, THandler mtHandler)
        : CoServer(executor, "127.0.0.1", internalPort, std::move(coHandler))
        , MtServer(THttpServerOptions(externalPort), std::bind(&TMonService::DispatchRequest, this, std::placeholders::_1, std::placeholders::_2))
        , MtHandler(std::move(mtHandler))
    {
    }

    void TMonService::Start() {
        MtServer.Start();
        CoServer.Start();
    }

    void TMonService::Stop() {
        MtServer.Stop();
        CoServer.Stop();
    }

    void TMonService::DispatchRequest(IOutputStream& out, const IHttpRequest& request) {
        if (strcmp(request.GetPath(), "/") == 0) {
            out << "HTTP/1.1 200 Ok\nConnection: Close\n\n";
            MtHandler(out, request);
        } else
            CoServer.ProcessRequest(out, request);
    }

}
