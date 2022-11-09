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

public:
    EStatus Status;
    EKind Kind;
    TDuration TotalDuration;
    TDuration ServerDuration;
    ui32 QueriesCount = 0;
};

} // namespace NKqp
} // namespace NKikimr

