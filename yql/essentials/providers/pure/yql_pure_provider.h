#pragma once
#include <yql/essentials/core/yql_data_provider.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NYql {

struct TTypeAnnotationContext;

struct TPureProviderSettings {
    THashMap<TString, TString> SecureParams;
};

struct TPureState: public TThrRefBase {
    using TPtr = TIntrusivePtr<TPureState>;

    TTypeAnnotationContext* Types;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    TPureProviderSettings Setting;
};

TIntrusivePtr<IDataProvider> CreatePureProvider(const TPureState::TPtr& state);

TDataProviderInitializer GetPureDataProviderInitializer(TPureProviderSettings settings = {});

} // namespace NYql
