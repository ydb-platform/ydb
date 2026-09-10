#include "mkql_bridge_outprocess.h"

#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/list.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/system/file.h>
#include <util/system/shellcommand.h>

namespace NKikimr::NMiniKQL {

namespace {

class TFileHandleOutput: public IOutputStream {
public:
    explicit TFileHandleOutput(TFileHandle& handle)
        : Handle_(handle)
    {
    }

private:
    void DoWrite(const void* buf, size_t len) override {
        Handle_.Write(buf, len);
    }

    TFileHandle& Handle_;
};

class TFileHandleInput: public IInputStream {
public:
    explicit TFileHandleInput(TFileHandle& handle)
        : Handle_(handle)
    {
    }

private:
    size_t DoRead(void* buf, size_t len) override {
        return Handle_.Read(buf, len);
    }

    TFileHandle& Handle_;
};

struct TOutProcessTransport {
    TOutProcessTransport(const TString& bridgeBinaryPath, const TList<TString>& args) {
        TShellCommandOptions options;
        options.SetUseShell(false)
            .SetDetachSession(true)
            .SetAsync(true)
            .SetCloseInput(false)
            .PipeInput()
            .PipeOutput()
            .PipeError();
        Shell = MakeHolder<TShellCommand>(bridgeBinaryPath, args, options);
        Shell->Run();
        Out = MakeHolder<TFileHandleOutput>(Shell->GetInputHandle());
        In = MakeHolder<TFileHandleInput>(Shell->GetOutputHandle());
    }

    THolder<TShellCommand> Shell;
    THolder<TFileHandleOutput> Out;
    THolder<TFileHandleInput> In;
};

class TOutProcessBridgeChannel: public TBridgeChannel {
public:
    // `transport` is constructed by the caller (see CreateOutProcessBridgeChannel
    // below) and handed over already-live: its In/Out streams must exist before
    // TBridgeChannel's own constructor runs (it binds them into reference
    // members), which a plain field constructed after the base class cannot
    // guarantee -- passing it in as an already-constructed parameter sidesteps
    // that ordering problem without resorting to inheriting TOutProcessTransport.
    TOutProcessBridgeChannel(THolder<TOutProcessTransport> transport,
                             const THolderFactory& holderFactory, const NUdf::IValueBuilder* valueBuilder,
                             TBridgeNamespaceId workerNamespace)
        : TBridgeChannel(*transport->In, *transport->Out, holderFactory, valueBuilder, HostBridgeNamespace, workerNamespace,
                         /*workerFunctionRegistry=*/nullptr, /*workerEnv=*/nullptr, /*workerRuntimeSettings=*/nullptr)
        , Transport_(std::move(transport))
    {
    }

    ~TOutProcessBridgeChannel() override {
        // Closing stdin (to let the child's serve loop exit on EOF, mirroring
        // the in-process worker's shutdown) isn't reliable here: TShellCommand
        // keeps its own internal duplicate of the pipe descriptors alive for
        // its async I/O pump, so the child never actually observes EOF.
        // Terminate it directly instead.
        try {
            Transport_->Shell->Terminate();
            Transport_->Shell->Wait();
        } catch (...) {
            Cerr << "Bridge: failed to terminate out-of-process worker: " << CurrentExceptionMessage() << Endl;
        }
    }

private:
    THolder<TOutProcessTransport> Transport_;
};

} // namespace

TIntrusivePtr<TBridgeChannel> CreateOutProcessBridgeChannel(
    const TString& bridgeBinaryPath,
    const TString& udfModulePath,
    const THolderFactory& holderFactory,
    const NUdf::IValueBuilder* valueBuilder,
    TBridgeNamespaceId workerNamespace,
    NYql::TRuntimeSettings::TConstPtr runtimeSettings) {
    Y_ENSURE(bridgeBinaryPath, "Bridge: OutProcess mode requires the bridge worker binary path to be configured (e.g. via --udf-bridge)");

    TList<TString> args;
    args.push_back("--module-path");
    args.push_back(udfModulePath);
    args.push_back("--namespace");
    args.push_back(ToString(workerNamespace));
    args.push_back("--runtime-settings");
    // Base64-encoded: the serialized proto can contain raw NUL bytes, which
    // would silently truncate a plain argv[] string.
    args.push_back(Base64Encode(NYql::SerializeRuntimeSettingsToString(*runtimeSettings)));

    return new TOutProcessBridgeChannel(MakeHolder<TOutProcessTransport>(bridgeBinaryPath, args), holderFactory, valueBuilder, workerNamespace);
}

} // namespace NKikimr::NMiniKQL
