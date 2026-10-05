#include <yql/essentials/minikql/computation/mkql_bridge.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_utils.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>
#include <yql/essentials/utils/backtrace/backtrace.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/yexception.h>
#include <util/stream/output.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

namespace {

int RunBridgeWorker(int argc, char** argv) {
    TString modulePath;
    // Plain ui64 here (not TBridgeNamespaceId) since NLastGetopt::StoreResult
    // needs FromString<T> support, which the strong alias deliberately
    // doesn't provide -- wrapped below, once parsed.
    ui64 workerNamespaceId = 0;
    TString encodedRuntimeSettings;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption("module-path", "Path to the UDF module (.so) to load")
        .Required()
        .RequiredArgument("PATH")
        .StoreResult(&modulePath);
    opts.AddLongOption("namespace", "This worker's own id for node references on the wire (see TBridgeNamespaceId)")
        .Required()
        .RequiredArgument("ID")
        .StoreResult(&workerNamespaceId);
    opts.AddLongOption("runtime-settings", "Base64-encoded serialized NYql::TRuntimeSettings, as resolved by the host for this query")
        .Required()
        .RequiredArgument("DATA")
        .StoreResult(&encodedRuntimeSettings);
    opts.SetFreeArgsNum(0);

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr())->Clone();
    functionRegistry->LoadUdfs(modulePath, {});

    const NYql::TRuntimeSettings::TConstPtr runtimeSettings =
        NYql::CreateRuntimeSettingsFromString(Base64Decode(encodedRuntimeSettings));

    const auto setup = CreateBridgeWorkerSetup(*functionRegistry);
    TIntrusivePtr<TBridgeChannel> channel = new TBridgeChannel(Cin, Cout, *setup.HolderFactory, setup.ValueBuilder.Get(), TBridgeNamespaceId(workerNamespaceId), HostBridgeNamespace, functionRegistry.Get(), setup.Env.Get(), runtimeSettings);
    channel->ServeForever();
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    try {
        return RunBridgeWorker(argc, argv);
    } catch (...) {
        const TString message = CurrentExceptionMessage();
        Cerr << message << Endl;
        try {
            WriteFrameHeader(Cout, EBridgeFrameKind::Error);
            WriteErrorMessage(Cout, message);
            Cout.Flush();
        } catch (...) {
            Cerr << "Bridge: failed to report the above error to the client: " << CurrentExceptionMessage() << Endl;
        }
        return 1;
    }
}
