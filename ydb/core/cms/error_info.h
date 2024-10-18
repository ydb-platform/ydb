#pragma once

#include "defs.h"

namespace NKikimr::NCms {

class TReason {  
public:
    // Must be sync with proto enum
    enum class EType {
        Generic,
        TooManyUnavailableVDisks,
        TooManyUnavailableStateStorageRings,
        DisabledNodesLimitReached,
        TenantDisabledNodesLimitReached,
        SysTabletsNodeLimitReached,
    };

    TReason(const TString &message, EType type = EType::Generic)
        : Message(message)
        , Type(type)
    {}

    TReason(const char* message, EType type = EType::Generic)
        : Message(message)
        , Type(type)
    {}

    TReason() = default;

    operator TString() const {
        return Message;
    }

    const TString& GetMessage() const {
        return Message;
    }

    EType GetType() const {
        return Type;
    }

private:
    TString Message;
    EType Type = EType::Generic;
}; 

struct TErrorInfo {
    NKikimrCms::TStatus::ECode Code = NKikimrCms::TStatus::ALLOW;
    TReason Reason;
    TInstant Deadline;
    ui64 RollbackPoint = 0;
};

} // namespace NKikimr::NCms

Y_DECLARE_OUT_SPEC(inline, NKikimr::NCms::TReason, stream, value) {
    stream << value.GetMessage();
}
