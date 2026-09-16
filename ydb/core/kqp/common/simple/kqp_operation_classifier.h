#pragma once

#include <util/generic/string.h>

namespace NKikimr::NKqp {

enum class EKqpOperationKind {
    Unknown,
    Join,
    Filter,
    Aggregate,
    Sort,
};

inline bool IsKqpAggregateFinalizer(TStringBuf callableName) {
    return callableName == "BlockMergeFinalizeHashed" || callableName == "BlockMergeManyFinalizeHashed";
}

inline EKqpOperationKind ClassifyKqpOperation(TStringBuf callableName) {
    if (callableName.Contains("Join")) {
        return EKqpOperationKind::Join;
    }
    if (callableName.Contains("Combine") || callableName.Contains("Aggregate")
            || callableName == "Condense1" || callableName == "Condense"
            || IsKqpAggregateFinalizer(callableName)) {
        return EKqpOperationKind::Aggregate;
    }
    if (callableName.Contains("Filter")) {
        return EKqpOperationKind::Filter;
    }
    if (callableName == "Sort" || callableName == "Top" || callableName == "TopSort"
            || callableName.StartsWith("WideSort") || callableName.StartsWith("WideTop")
            || callableName.StartsWith("BlockSort") || callableName.StartsWith("BlockTop")) {
        return EKqpOperationKind::Sort;
    }
    return EKqpOperationKind::Unknown;
}

inline constexpr const char* KqpOperationName(EKqpOperationKind kind) {
    switch (kind) {
        case EKqpOperationKind::Join:
            return "Join";
        case EKqpOperationKind::Filter:
            return "Filter";
        case EKqpOperationKind::Aggregate:
            return "Aggregate";
        case EKqpOperationKind::Sort:
            return "Sort";
        case EKqpOperationKind::Unknown:
            return "Compute";
    }
    return "Compute";
}

} // namespace NKikimr::NKqp
