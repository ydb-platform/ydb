#pragma once

#include <util/datetime/base.h>

#include <optional>

namespace NKikimr {
namespace NKqp {

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
};

} // namespace NKqp
} // namespace NKikimr

