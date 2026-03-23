#pragma once

#include <ydb/core/util/backoff.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/datetime/base.h>

namespace NKikimr::NPQ::NMLP {

    struct TChildPartitionsOrderManager {
    public:
        enum class ESendReasons: ui8 {
            None = 0,
            Initial = 1 << 0,
            DeliveryProblem = 1 << 1,
            Commit = 1 << 2,
            ParentDone = 1 << 3,
        };

        struct TChildrenPartitionWithKeepOrder {
            ui64 TabletId = 0;
            ui32 Cookie = 0;
            ESendReasons SendFullStateReasons = ESendReasons::None;

            bool AddSendFullStateReason(ESendReasons reason);
            bool NeedSendFullState() const;
        };

        bool Empty() const;
        bool SetSendFullStateToAll(ESendReasons reason);
        bool SetSendFullStateByCookie(ui32 cookie, ESendReasons reason);

        static TString SendReasonsToString(ESendReasons reason);

    public:
        ui64 ConsumerStep = 0;
        TMap<ui32, TChildrenPartitionWithKeepOrder> ChildrenPartitionWithKeepOrder;
        TBackoff UpdateChildPartitionsBackoff{TDuration::Seconds(1), TDuration::Seconds(10)};
    };
}
