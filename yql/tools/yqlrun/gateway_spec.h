#pragma once

#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/core/yql_data_provider.h>

void ExtProviderSpecific(const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry,
        TVector<NYql::TDataProviderInitializer>& dataProvidersInit,
        const THashMap<std::pair<TString, TString>, TVector<std::pair<TString, TString>>>& rtmrTableAttributes = {});
