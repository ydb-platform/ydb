#include <yql/essentials/public/udf/udf_helpers.h>

#include "udf_init.hpp"
#include "udf_load.hpp"
#include "wasm_directory_state.hpp"

#include <util/generic/string.h>
#include <util/stream/output.h>

#include <unistd.h>

#include <cstdlib>
#include <exception>

namespace NWasm::NYQL {

class TWasmModule: public IUdfModule
{
public:
    TStringRef Name() const
    {
        static auto name = TStringRef::Of("Wasm");
        return name;
    }

    void CleanupOnTerminate() const final
    {
    }

    void GetAllFunctions(IFunctionsSink& sink) const final
    {
        sink.Add(TInit::Name());
        sink.Add(TLoadUdfs::Name());
    }

    void BuildFunctionTypeInfo(
        const TStringRef& name,
        TType*,
        const TStringRef&,
        ui32 flags,
        IFunctionTypeInfoBuilder& builder) const final
    {
        try {
            const auto flagsOnly = (flags & TFlags::TypesOnly);
            if (TInit::Name() == name) {
                TInit::Register(builder, flagsOnly);
            } else if (TLoadUdfs::Name() == name) {
                TLoadUdfs::Register(builder, flagsOnly);
            }
        } catch (const std::exception& e) {
            builder.SetError(CurrentExceptionMessage());
        }
    }
};

} // namespace NWasm::NYQL

extern "C" YQL_UDF_API void Register(NYql::NUdf::IRegistrator& registrator, ui32 /*flags*/)
{
    Cerr << "[wasm-udf] Register() called (pid=" << ::getpid() << ")" << Endl;
    registrator.AddModule(
        ::NWasm::NYQL::TWasmModule().Name(),
        new ::NWasm::NYQL::TWasmModule());

    const char* envPath = std::getenv("YQL_WASM_UDF_REGISTRY_PATH");
    if (envPath == nullptr || *envPath == '\0') {
        Cerr << "[wasm-udf] YQL_WASM_UDF_REGISTRY_PATH is empty, skipping env registry" << Endl;
        return;
    }
    Cerr << "[wasm-udf] YQL_WASM_UDF_REGISTRY_PATH=" << envPath << Endl;

    try {
        auto modules = ::NWasm::NYQL::BuildSoModulesFromRegistry(envPath);
        Cerr << "[wasm-udf] BuildSoModulesFromRegistry returned " << modules.size() << " modules" << Endl;
        for (auto& builtModule : modules) {
            Cerr << "[wasm-udf]   registering module: " << builtModule.ModuleName << Endl;
            registrator.AddModule(
                NYql::NUdf::TStringRef(builtModule.ModuleName),
                std::move(builtModule.Module));
        }
        Cerr << "[wasm-udf] Register() finished registering env modules" << Endl;
    } catch (const std::exception& ex) {
        Cerr << "Failed to scan wasm UDF env registry " << envPath
             << ": " << ex.what() << Endl;
    }
}

extern "C" YQL_UDF_API ui32 AbiVersion()
{
    return NYql::NUdf::CurrentAbiVersion();
}

extern "C" YQL_UDF_API void SetBackTraceCallback(NYql::NUdf::TBackTraceCallback callback)
{
    NYql::NUdf::SetBackTraceCallbackImpl(callback);
}
