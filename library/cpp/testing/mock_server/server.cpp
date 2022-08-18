#include "server.h"

#include <util/system/thread.h>

namespace NMock {
    class TMockServer::TCallBack: public THttpServer::ICallBack {
        TGenerator Generator_;

        TClientRequest* CreateClient() override {
            return Generator_();
        }

    public:
        void SetGenerator(TGenerator generator) {
            Generator_ = generator;
        }

        TCallBack(TGenerator& generator)
            : Generator_(generator)
        {
        }
    };

    static THttpServerOptions createDefaultOptions(ui16 port) {
        THttpServerOptions o;
        o.AddBindAddress("localhost", port);
        o.SetThreads(1);
        o.SetMaxConnections(300);
        o.SetMaxQueueSize(30);
        return o;
    }

    TMockServer::TMockServer(ui16 port, TGenerator generator)
        : TMockServer(createDefaultOptions(port), generator)
    {
    }

    TMockServer::TMockServer(const THttpServerOptions& options, TGenerator generator)
        : Cb_(MakeHolder<TCallBack>(generator))
        , Server_(MakeHolder<THttpServer>(Cb_.Get(), options))
        , Thread_(MakeHolder<TThread>(Worker, this))
        , Ev_(MakeHolder<TAutoEvent>())
    {
        Thread_->Start();
        Ev_->Wait();
    }

    TMockServer::~TMockServer() {
        Server_->Stop();
        Thread_->Join();
    }

    size_t TMockServer::GetClientCount() const {
        return Server_->GetClientCount();
    }

    void TMockServer::SetGenerator(TMockServer::TGenerator generator) {
        Cb_->SetGenerator(generator);
    }

    void* TMockServer::Worker(void* arg) {
        TMockServer& this_ = *static_cast<TMockServer*>(arg);

        this_.Server_->Start();
        this_.Ev_->Signal();
        this_.Server_->Wait();

        return nullptr;
    }
}
