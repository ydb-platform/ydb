#pragma once

#include "defs.h"

namespace NKikimr::NCms {

class TReason {  
public:
    // Must be sync with proto enum
    enum class EType {
        Unspecified,
        TooManyUnavailableVDisks,
        TooManyUnavailableStateStorageRings,
        DisabledNodesLimitReached,
        TenantDisabledNodesLimitReached,
        SysTabletsNodeLimitReached,
    };

    TReason(const TString &message, EType type = EType::Unspecified)
        : Message(message)
        , Type(type)
    {}

    TReason(const char* message, EType type = EType::Unspecified)
        : Message(message)
        , Type(type)
    {}

    TReason() = default;

    operator TString() const {
        return Message;
    }

    TString GetMessage() const {
        return Message;
    }

    EType GetType() const {
        return Type;
    }

private:
    TString Message;
    EType Type = EType::Unspecified;
}; 

struct TErrorInfo {
    NKikimrCms::TStatus::ECode Code = NKikimrCms::TStatus::ALLOW;
    TReason Reason;
    TInstant Deadline;
    ui64 RollbackPoint = 0;
};

} // namespace NKikimr::NCms
