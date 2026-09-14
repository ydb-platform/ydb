#include "mkql_bridge_mode.h"
#include <util/string/join.h>
#include <util/generic/yexception.h>

namespace NYql::NUdf {

#define SWITCH_ENUM_TYPE_TO_STR(name, val) \
    case val:                              \
        return TStringBuf(#name);

TString BridgeModeAvailables() {
    return Join(", ",
                BridgeModeAsStr(EBridgeMode::None),
                BridgeModeAsStr(EBridgeMode::InProcess),
                BridgeModeAsStr(EBridgeMode::OutProcess));
}

TStringBuf BridgeModeAsStr(EBridgeMode bridgeMode) {
    switch (static_cast<int>(bridgeMode)) {
        UDF_BRIDGE_MODE(SWITCH_ENUM_TYPE_TO_STR)
    }

    return TStringBuf("unknown");
}

EBridgeMode BridgeModeByStr(const TString& bridgeModeStr) {
    const TString lowerBridgeModeStr = to_lower(bridgeModeStr);
    for (auto val = EBridgeMode::None; val < EBridgeMode::Max; val = static_cast<EBridgeMode>(static_cast<ui8>(val) + 1)) {
        if (lowerBridgeModeStr == to_lower(TString(BridgeModeAsStr(val)))) {
            return val;
        }
    }
    ythrow yexception() << "Unknown udf bridge mode: " << bridgeModeStr;
}

} // namespace NYql::NUdf
