#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/mailbox.h>
#include <ydb/library/actors/http/http.h>
#include <util/generic/string.h>

namespace {

class TActorContextGuard {
public:
    TActorContextGuard()
        : Prev(NActors::TlsActivationContext)
    {
        NActors::TlsActivationContext = &GetContext();
    }

    ~TActorContextGuard() {
        NActors::TlsActivationContext = Prev;
    }

private:
    static NActors::TActorContext& GetContext() {
        static THolder<NActors::TActorSystemSetup> setup(new NActors::TActorSystemSetup());
        static NActors::TActorSystem actorSystem(setup);
        static NActors::TExecutorThread executorThread(0, &actorSystem, nullptr, "http-request-parse-fuzz");
        static NActors::TMailbox mailbox;
        static NActors::TActorContext context(mailbox, executorThread, 0, NActors::TActorId());
        return context;
    }

private:
    NActors::TActivationContext* Prev = nullptr;
};

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TActorContextGuard actorContext;
    TStringBuf input((const char*)data, size);
    try {
        NHttp::THttpParser<NHttp::THttpRequest> parser(input);
    } catch (...) {}
    return 0;
}
