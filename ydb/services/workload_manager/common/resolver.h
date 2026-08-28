#pragma once

#include <util/generic/string.h>

namespace NKikimr::NWorkloadManager {

///
/// Explains how a query was routed to its resource pool. Feeds the
/// `WmClassifiedBy` sys view column and error/log messages built by the
/// classifier.
///
struct TResolver {
    enum class EType {
        Direct,          // user set PoolId in the request
        Classifier,      // a classifier rule matched
        Default,         // no rule matched, fell through to default pool
        SharedReading,   // system-forced pool for shared-reading streaming queries
    };

    EType Type;
    TString Name;  // classifier name for EType::Classifier; empty otherwise

    static TResolver Direct()              { return {EType::Direct, {}}; }
    static TResolver Default()             { return {EType::Default, {}}; }
    static TResolver SharedReading()       { return {EType::SharedReading, {}}; }
    static TResolver Classifier(TString n) { return {EType::Classifier, std::move(n)}; }

    // Value emitted into the `WmClassifiedBy` sys view column.
    TString ToSysViewString() const {
        switch (Type) {
            case EType::Direct:        return "USER";
            case EType::Classifier:    return TString("CLASSIFIER: ") + Name;
            case EType::Default:       return "NONE";
            case EType::SharedReading: return "SHARED_READING";
        }
    }

    // Human-readable label for error messages and logs. Kept stable so existing
    // classifier tests and user-visible error text do not change.
    TString ToLogString() const {
        switch (Type) {
            case EType::Direct:        return "User request";
            case EType::Classifier:    return TString("Classifier: ") + Name;
            case EType::Default:       return "Default";
            case EType::SharedReading: return "ResourcePoolForSharedReading";
        }
    }
};

} // namespace NKikimr::NWorkloadManager
