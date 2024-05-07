#pragma once

#include "defs.h"

namespace NKikimr::NCms {

class TReason {  
public:
    enum class EType {
        UNSPECIFIED = NKikimrCms::TAction::STATUS_UNSPECIFIED,
        TOO_MANY_UNAVAILABLE_VDISKS = NKikimrCms::TAction::TOO_MANY_UNAVAILABLE_VDISKS,
        TOO_MANY_UNAVAILABLE_STATE_STORAGE_RINGS = NKikimrCms::TAction::TOO_MANY_UNAVAILABLE_STATE_STORAGE_RINGS,
        DISABLED_NODES_LIMIT_REACHED = NKikimrCms::TAction::DISABLED_NODES_LIMIT_REACHED,
        TENANT_DISABLED_NODES_LIMIT_REACHED = NKikimrCms::TAction::TENANT_DISABLED_NODES_LIMIT_REACHED,
        SYS_TABLETS_NODE_LIMIT_REACHED = NKikimrCms::TAction::SYS_TABLETS_NODE_LIMIT_REACHED,
    };

    explicit TReason(const TString &message, EType type = EType::UNSPECIFIED)
    : Message(message)
    , Type(type)
    {}

    TReason() = default;

    TString GetMessage() const {
        return Message;
    }

    EType GetType() const {
        return Type;
    }

private:
    TString Message;
    EType Type = EType::UNSPECIFIED;
}; 

struct TErrorInfo {
    NKikimrCms::TStatus::ECode Code = NKikimrCms::TStatus::ALLOW;
    TReason Reason;
    TInstant Deadline;
    ui64 RollbackPoint = 0;
};

} // namespace NKikimr::NCms
