#pragma once

#include "mkql_bridge.h"

namespace NKikimr::NMiniKQL {

TIntrusivePtr<TBridgeChannel> CreateInProcessBridgeChannel(
    const IFunctionRegistry& functionRegistry,
    const THolderFactory& holderFactory,
    const NUdf::IValueBuilder* valueBuilder,
    TBridgeNamespaceId workerNamespace,
    NYql::TRuntimeSettings::TConstPtr runtimeSettings);

} // namespace NKikimr::NMiniKQL
