#pragma once

#include <library/cpp/coroutine/engine/impl.h>
#include <library/cpp/coroutine/listener/listen.h>
#include <library/cpp/http/fetch/httpheader.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/logger/all.h>

#include <util/network/ip.h>
#include <library/cpp/cgiparam/cgiparam.h>

#include <functional>

struct TMonitor;

namespace NMonitoring {
    struct IHttpRequest {
        virtual ~IHttpRequest() {
        }
        virtual const char* GetURI() const = 0;
        virtual const char* GetPath() const = 0;
        virtual const TCgiParameters& GetParams() const = 0;
        virtual const TCgiParameters& GetPostParams() const = 0;
        virtual TStringBuf GetPostContent() const = 0;
        virtual HTTP_METHOD GetMethod() const = 0;
        virtual const THttpHeaders& GetHeaders() const = 0;
        virtual TString GetRemoteAddr() const = 0;
    };
    // first param - output stream to write result to
    // second param - URL of request
    typedef std::function<void(IOutputStream&, const IHttpRequest&)> THandler;

    class TCoHttpServer: private TContListener::ICallBack {
    public:
        // initialize and schedule coroutines for execution
        TCoHttpServer(TContExecutor& executor, const TString& bindAddr, TIpPort port, THandler handler);
        void Start();
        void Stop();

        // this function implements THandler interface
        // by forwarding it to the httpserver
        // @note this call may be blocking; don't use inside coroutines
        // @throws may throw in case of connection error, etc
        void ProcessRequest(IOutputStream&, const IHttpRequest&);

    private:
        class TConnection;

        // ICallBack implementation
        void OnAcceptFull(const TAcceptFull& a) override;
        void OnError() override;

    private:
        TContExecutor& Executor;
        TContListener Listener;
        THandler Handler;
        TString BindAddr;
        TIpPort Port;
    };

    class TMtHttpServer: public THttpServer, private THttpServer::ICallBack {
    public:
        TMtHttpServer(const TOptions& options, THandler handler, IThreadFactory* pool = nullptr);
        TMtHttpServer(const TOptions& options, THandler handler, TSimpleSharedPtr<IThreadPool> pool);

        ~TMtHttpServer() override {
            Stop();
        }

        /**
         * This will cause the server start to accept incoming connections.
         *
         * @return true if the port binding was successfull,
         *         false otherwise.
         */
        bool Start();

        /**
         * Same as Start() member-function, but will throw TSystemError if
         * there were some errors.
         */
        void StartOrThrow();

        /**
         * Stops the server from accepting new connections.
         */
        void Stop();

    private:
        class TConnection;
        TClientRequest* CreateClient() override;

        THandler Handler;
    };

    // this class implements hybrid coroutine and threaded approach
    // requests for main page which holds counters and simple tables are served in a thread
    // requests for other pages which include access with inter-thread synchonization
    // will be served in a coroutine context
    class TMonService {
    public:
        TMonService(TContExecutor& executor, TIpPort internalPort, TIpPort externalPort,
                    THandler coHandler, THandler mtHandler);
        void Start();
        void Stop();

    protected:
        void DispatchRequest(IOutputStream& out, const IHttpRequest&);

    private:
        TCoHttpServer CoServer;
        TMtHttpServer MtServer;
        THandler MtHandler;
    };

}
