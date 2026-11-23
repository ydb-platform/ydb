#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_mem_info.h>
#include <yql/essentials/minikql/mkql_terminator.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_utils.h>
#include <yql/essentials/public/udf/udf_static_library.h>

#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/system/compiler.h>

#include <array>

#ifdef YQL_UDF_STATIC_ENTRY_POINT
extern "C" const NYql::NUdf::TYqlUdfStaticLibrary* YQL_UDF_STATIC_ENTRY_POINT();
#endif

namespace {

namespace NMiniKQL = NKikimr::NMiniKQL;
namespace NUdf = NYql::NUdf;

NMiniKQL::TFunctionTypeInfo ResolveFunction(const NMiniKQL::IFunctionRegistry& registry,
                                            const NMiniKQL::TTypeEnvironment& environment, TStringBuf name) {
    NMiniKQL::TFunctionTypeInfo function;
    const NYql::TRuntimeSettings runtimeSettings;
    const auto status = registry.FindFunctionTypeInfo(
        NYql::UnknownLangVersion, runtimeSettings, environment, new NMiniKQL::TTypeInfoHelper,
        /*countersProvider=*/nullptr, name, /*userType=*/nullptr, {}, 0, NUdf::TSourcePosition{},
        /*secureParamsProvider=*/nullptr, /*logProvider=*/nullptr, &function);
    Y_ENSURE(status.IsOk(), status.GetError());
    Y_ENSURE(function.Implementation, "Missing implementation of " << name);
    return function;
}

void CheckUdfTerminate(const NMiniKQL::IFunctionRegistry& registry) {
    NMiniKQL::TScopedAlloc allocator(__LOCATION__);
    NMiniKQL::TTypeEnvironment environment(allocator);
    NMiniKQL::TMemoryUsageInfo memoryUsage("static_host");
    NMiniKQL::THolderFactory holderFactory(allocator.Ref(), memoryUsage, &registry);
    const NUdf::TSourcePosition sourcePosition;
    const NUdf::TSourcePosition* calleePosition = &sourcePosition;
    NMiniKQL::TDefaultValueBuilder valueBuilder(holderFactory, NUdf::EValidatePolicy::Exception);
    valueBuilder.SetCalleePositionHolder(calleePosition);
    const auto function = ResolveFunction(registry, environment, "String.LeftPad");
    const std::array arguments = {
        NUdf::TUnboxedValuePod::Embedded(NUdf::TStringRef::Of("a")),
        NUdf::TUnboxedValuePod(ui64{3}),
        NUdf::TUnboxedValuePod::Embedded(NUdf::TStringRef::Of("xx")),
    };
    NMiniKQL::TThrowingBindTerminator terminator;
    bool caught = false;
    try {
        NUdf::TBoxedValueAccessor::Run(*function.Implementation, &valueBuilder, arguments.data());
    } catch (const NMiniKQL::TTerminateException& exception) {
        Y_ENSURE(TStringBuf(exception.what()).Contains("Not 1 symbol in paddingSymbol"), exception.what());
        caught = true;
    }
    Y_ENSURE(caught, "String.LeftPad did not throw through UdfTerminate");
}

} // namespace

int main() {
    try {
#ifdef YQL_UDF_STATIC_ENTRY_POINT
        Y_ENSURE(YQL_UDF_STATIC_ENTRY_POINT(), "Missing cached String UDF descriptor");
#endif
        auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry({})->Clone();
        NKikimr::NMiniKQL::FillStaticModules(*registry);
        Y_ENSURE(registry->GetModuleFunctions("String").contains("HexEncode"), "String::HexEncode was not registered");
        CheckUdfTerminate(*registry);
        Cout << "Static String UDF: registration and UdfTerminate checks passed" << Endl;
        return 0;
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
