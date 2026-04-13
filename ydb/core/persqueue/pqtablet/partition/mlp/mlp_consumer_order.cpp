#include "mlp_consumer_order.h"

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/pqdata_mlp.pb.h>
#include <ydb/core/persqueue/events/internal.h>

#include <util/generic/serialized_enum.h>
#include <util/stream/format.h>
#include <util/string/builder.h>

#include <ranges>

namespace NKikimr::NPQ::NMLP {

    bool TChildPartitionsOrderManager::TChildrenPartitionWithKeepOrder::NeedSendFullState() const {
        if (LastSendReasons.Defined() && LastSendReasons->Reasons == ESendReasons::ParentDone && SendReasons.Reasons == ESendReasons::ParentDone) {
            return false;
        }
        return SendReasons.Reasons != ESendReasons::None;
    }

    void TChildPartitionsOrderManager::TChildrenPartitionWithKeepOrder::MarkAsSent() {
        LastSendReasons = std::exchange(SendReasons, TFullState{ESendReasons::None, SendReasons.InflightMessagesCount});
    }

    bool TChildPartitionsOrderManager::Empty() const {
        return ChildrenPartitionWithKeepOrder.empty();
    }

    bool TChildPartitionsOrderManager::TChildrenPartitionWithKeepOrder::AddSendFullStateReason(ESendReasons reason, ui64 inflightMessagesCount) {
        if (reason == ESendReasons::Commit) {
            if (!EnableSendFullBlacklist) {
                return false;
            }
            if (LastSendReasons.Defined() && LastSendReasons->Reasons == ESendReasons::Commit) {
                if (inflightMessagesCount * 2 > LastSendReasons->InflightMessagesCount) {
                    return false;
                }
            }
        }

        ESendReasons n = static_cast<ESendReasons>(static_cast<ui32>(SendReasons.Reasons) | static_cast<ui32>(reason));
        TFullState newState{n, inflightMessagesCount};
        std::swap(SendReasons, newState);
        return SendReasons.Reasons != newState.Reasons;
    }

    bool TChildPartitionsOrderManager::TChildrenPartitionWithKeepOrder::AddSendFullStateReason(ESendReasons reason) {
        Y_ASSERT(reason != ESendReasons::Commit);
        return AddSendFullStateReason(reason, SendReasons.InflightMessagesCount);
    }

    bool TChildPartitionsOrderManager::SetSendFullStateToAll(ESendReasons reason, ui64 inflightMessagesCount) {
        Y_ASSERT(reason != ESendReasons::None);
        bool update = false;
        for (auto& [_, state] : ChildrenPartitionWithKeepOrder) {
            if (state.AddSendFullStateReason(reason, inflightMessagesCount)) {
                update = true;
            }
        }
        return update;
    }

    bool TChildPartitionsOrderManager::SetSendFullStateByCookie(ui32 cookie, ESendReasons reason) {
        bool update = false;
        for (auto& [childPartitionId, state] : ChildrenPartitionWithKeepOrder) {
            if (state.Cookie == cookie) {
                state.AddSendFullStateReason(reason);
                update = true;
            }
        }
        return update;
    }

    TString TChildPartitionsOrderManager::TChildrenPartitionWithKeepOrder::SendFullStateReasonsAsString() const {
        return SendReasonsToString(SendReasons.Reasons);
    }

    TString TChildPartitionsOrderManager::SendReasonsToString(const ESendReasons reasons) {
        if (reasons == ESendReasons::None) {
            return ToString(ESendReasons::None);
        }
        TStringBuilder ss;
        ui32 uReasons = static_cast<ui32>(reasons);
        for (const auto p : GetEnumAllValues<ESendReasons>()) {
            const ui32 uCheck = static_cast<ui32>(p);
            if (uCheck == 0) {
                continue;
            }
            if ((uReasons & uCheck) != uCheck) {
                continue;
            }
            ss << p << '|';
            uReasons &= ~uCheck;
        }
        if (uReasons != 0) {
            ss << Hex(uReasons, HF_ADDX);
        }
        if (ss.EndsWith('|')) {
            ss.pop_back();
        }
        return ss;
    }

    TString ShortDebugString(const NKikimrPQ::TEvMLPUpdateExternalLockedMessageGroupsId& ev) {
        return ev.ShortUtf8DebugString();
    }

    TString ShortDebugString(const NKikimrPQ::TExternalLockedMessageGroupsId& update) {
        return update.ShortUtf8DebugString();
    }
} // namespace NKikimr::NPQ::NMLP
