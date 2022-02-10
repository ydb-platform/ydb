#pragma once

#include <library/cpp/messagebus/ybus.h>
#include <library/cpp/messagebus/oldmodule/module.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <library/cpp/cgiparam/cgiparam.h>

namespace NBus {
    class TBusWww: public TAtomicRefCount<TBusWww> {
    public:
        struct TLink {
            TString Title;
            TString Href;
        };

        struct TOptionalParams {
            TVector<TLink> ParentLinks;
        };

        TBusWww();
        ~TBusWww();

        void RegisterClientSession(TBusClientSessionPtr);
        void RegisterServerSession(TBusServerSessionPtr);
        void RegisterQueue(TBusMessageQueuePtr);
        void RegisterModule(TBusModule*);

        void ServeHttp(IOutputStream& httpOutputStream, const TCgiParameters& queryArgs, const TOptionalParams& params = TOptionalParams());

        struct TImpl;
        THolder<TImpl> Impl;
    };

    class TBusWwwHttpServer {
    public:
        TBusWwwHttpServer(TIntrusivePtr<TBusWww> www, unsigned port);
        ~TBusWwwHttpServer();

        struct TImpl;
        THolder<TImpl> Impl;
    };

}
