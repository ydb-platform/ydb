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
        if (LastSendFullStateReasons == ESendReasons::ParentDone && SendFullStateReasons == ESendReasons::ParentDone) {
            return false;
        }
        return SendFullStateReasons != ESendReasons::None;
    }

    void TChildPartitionsOrderManager::TChildrenPartitionWithKeepOrder::MarkAsSent() {
        LastSendFullStateReasons = std::exchange(SendFullStateReasons, ESendReasons::None);
    }

    bool TChildPartitionsOrderManager::Empty() const {
        return ChildrenPartitionWithKeepOrder.empty();
    }

    bool TChildPartitionsOrderManager::TChildrenPartitionWithKeepOrder::AddSendFullStateReason(ESendReasons reason) {
        ESendReasons n = static_cast<ESendReasons>(static_cast<ui32>(SendFullStateReasons) | static_cast<ui32>(reason));
        std::swap(SendFullStateReasons, n);
        return SendFullStateReasons != n;
    }

    bool TChildPartitionsOrderManager::SetSendFullStateToAll(ESendReasons reason) {
        Y_ASSERT(reason != ESendReasons::None);
        bool update = false;
        for (auto& [_, state] : ChildrenPartitionWithKeepOrder) {
            if (state.AddSendFullStateReason(reason)) {
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
        return SendReasonsToString(SendFullStateReasons);
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
