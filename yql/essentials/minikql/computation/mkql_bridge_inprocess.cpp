#include "mkql_bridge_inprocess.h"

#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/stream/pipe.h>
#include <util/system/pipe.h>
#include <util/system/thread.h>

namespace NKikimr::NMiniKQL {

namespace {

void RunBridgeWorker(const IFunctionRegistry& functionRegistry, THolder<TPipedInput> in, THolder<TPipedOutput> out, TBridgeNamespaceId workerNamespace, NYql::TRuntimeSettings::TConstPtr runtimeSettings) {
    try {
        const auto setup = CreateBridgeWorkerSetup(functionRegistry);
        TIntrusivePtr<TBridgeChannel> channel = new TBridgeChannel(*in, *out, *setup.HolderFactory, setup.ValueBuilder.Get(), workerNamespace, HostBridgeNamespace, &functionRegistry, setup.Env.Get(), std::move(runtimeSettings));
        channel->ServeForever();
    } catch (...) {
        const TString message = CurrentExceptionMessage();
        Cerr << "Bridge worker thread failed: " << message << Endl;
        try {
            WriteFrameHeader(*out, EBridgeFrameKind::Error);
            WriteErrorMessage(*out, message);
            out->Flush();
        } catch (...) {
            Cerr << "Bridge: failed to report the above error to the client: " << CurrentExceptionMessage() << Endl;
        }
    }
}

struct TInProcessPipes {
    TInProcessPipes() {
        TPipeHandle clientToWorkerR;
        TPipeHandle clientToWorkerW;
        TPipeHandle::Pipe(clientToWorkerR, clientToWorkerW);
        TPipeHandle workerToClientR;
        TPipeHandle workerToClientW;
        TPipeHandle::Pipe(workerToClientR, workerToClientW);

        ClientOut = MakeHolder<TPipedOutput>(clientToWorkerW.Release());
        WorkerIn = MakeHolder<TPipedInput>(clientToWorkerR.Release());
        WorkerOut = MakeHolder<TPipedOutput>(workerToClientW.Release());
        ClientIn = MakeHolder<TPipedInput>(workerToClientR.Release());
    }

    THolder<TPipedOutput> ClientOut;
    THolder<TPipedInput> ClientIn;
    THolder<TPipedInput> WorkerIn;
    THolder<TPipedOutput> WorkerOut;
};

class TInProcessBridgeChannel: public TBridgeChannel {
public:
    // `pipes` is constructed by the caller (see CreateInProcessBridgeChannel
    // below) and handed over already-live: its ClientIn/ClientOut streams must
    // exist before TBridgeChannel's own constructor runs (it binds them into
    // reference members), which a plain field constructed after the base class
    // cannot guarantee -- passing it in as an already-constructed parameter
    // sidesteps that ordering problem without resorting to inheriting
    // TInProcessPipes.
    TInProcessBridgeChannel(THolder<TInProcessPipes> pipes, const IFunctionRegistry& functionRegistry,
                            const THolderFactory& holderFactory, const NUdf::IValueBuilder* valueBuilder,
                            TBridgeNamespaceId workerNamespace, NYql::TRuntimeSettings::TConstPtr runtimeSettings)
        : TBridgeChannel(*pipes->ClientIn, *pipes->ClientOut, holderFactory, valueBuilder, HostBridgeNamespace, workerNamespace,
                         /*workerFunctionRegistry=*/nullptr, /*workerEnv=*/nullptr, /*workerRuntimeSettings=*/nullptr)
        , Pipes_(std::move(pipes))
        , Thread_([&functionRegistry, workerNamespace, runtimeSettings,
                   in = std::move(Pipes_->WorkerIn), out = std::move(Pipes_->WorkerOut)]() mutable {
            RunBridgeWorker(functionRegistry, std::move(in), std::move(out), workerNamespace, std::move(runtimeSettings));
        })
    {
        Thread_.Start();
    }

    ~TInProcessBridgeChannel() override {
        // Close our end of the request pipe first so the worker's blocking
        // read unblocks with EOF and its serve loop returns -- only then is
        // it safe to join without risking a deadlock against an in-flight
        // request/response exchange.
        Pipes_->ClientOut.Reset();
        Pipes_->ClientIn.Reset();
        try {
            Thread_.Join();
        } catch (...) {
            Cerr << "Bridge: failed to join worker thread: " << CurrentExceptionMessage() << Endl;
        }
    }

private:
    THolder<TInProcessPipes> Pipes_;
    TThread Thread_;
};

} // namespace

TIntrusivePtr<TBridgeChannel> CreateInProcessBridgeChannel(
    const IFunctionRegistry& functionRegistry,
    const THolderFactory& holderFactory,
    const NUdf::IValueBuilder* valueBuilder,
    TBridgeNamespaceId workerNamespace,
    NYql::TRuntimeSettings::TConstPtr runtimeSettings) {
    return new TInProcessBridgeChannel(MakeHolder<TInProcessPipes>(), functionRegistry, holderFactory, valueBuilder, workerNamespace, std::move(runtimeSettings));
}

} // namespace NKikimr::NMiniKQL
