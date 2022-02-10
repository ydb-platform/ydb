#pragma once

#include <util/datetime/base.h>

#include <optional> 
 
namespace NKikimr {
namespace NKqp {

struct TKqpForceNewEngineState { 
    ui32 ForceNewEnginePercent = 0; 
    ui32 ForceNewEngineLevel = 0; 
    std::optional<bool> ForcedNewEngine; 
}; 
 
struct TKqpTransactionInfo {
    enum class EKind {
        Pure,
        ReadOnly,
        WriteOnly,
        ReadWrite
    };

    enum class EStatus {
        Active,
        Committed,
        Aborted
    };

    enum class EEngine { 
        OldEngine, 
        NewEngine 
    }; 
 
public:
    EStatus Status;
    EKind Kind;
    std::optional<EEngine> TxEngine; 
    TDuration TotalDuration;
    TDuration ServerDuration;
    ui32 QueriesCount = 0;
    TKqpForceNewEngineState ForceNewEngineState; 
};

} // namespace NKqp
} // namespace NKikimr

