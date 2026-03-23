#include "mlp_consumer_order.h"

#include <util/generic/serialized_enum.h>
#include <util/stream/format.h>
#include <util/string/builder.h>

#include <ranges>

namespace NKikimr::NPQ::NMLP {

    bool TChildPartitionsOrderManager::TChildrenPartitionWithKeepOrder::NeedSendFullState() const {
        return SendFullStateReasons != ESendReasons::None;
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

    TString TChildPartitionsOrderManager::SendReasonsToString(const ESendReasons reasons) {
        if (reasons == ESendReasons::None) {
            return ToString(ESendReasons::None);
        }
        TStringBuilder ss;
        ui32 uReasons = static_cast<ui32>(reasons);
        for (const auto p : GetEnumAllValues<ESendReasons>()) {
            const ui32 uCheck = static_cast<ui32>(p);
            if ((uReasons & uCheck) != uCheck || uCheck == 0) {
                continue;
            }
            ss << p << '|';
            uReasons &= ~uCheck;
        }
        if (uReasons != 0) {
            ss << Hex(uReasons, {});
        }
        if (ss.EndsWith('|')) {
            ss.pop_back();
        }
        return ss;
    }

} // namespace NKikimr::NPQ::NMLP
