#pragma once
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>

namespace NYql::NUdf {

#define UDF_BRIDGE_MODE(XX) \
    XX(None, 0)             \
    XX(InProcess, 1)        \
    XX(OutProcess, 2)       \
    XX(Max, 3)

enum class EBridgeMode: ui8 {
    UDF_BRIDGE_MODE(ENUM_VALUE_GEN)
};

TString BridgeModeAvailables();
TStringBuf BridgeModeAsStr(EBridgeMode bridgeMode);
EBridgeMode BridgeModeByStr(const TString& bridgeModeStr);

} // namespace NYql::NUdf
