#pragma once

#include "mkql_bridge.h"

namespace NKikimr::NMiniKQL {

TIntrusivePtr<TBridgeChannel> CreateOutProcessBridgeChannel(
    const TString& bridgeBinaryPath,
    const TString& udfModulePath,
    const THolderFactory& holderFactory,
    const NUdf::IValueBuilder* valueBuilder,
    TBridgeNamespaceId workerNamespace,
    NYql::TRuntimeSettings::TConstPtr runtimeSettings);

} // namespace NKikimr::NMiniKQL
