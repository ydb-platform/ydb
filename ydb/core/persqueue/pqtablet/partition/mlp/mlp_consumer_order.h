#pragma once

#include <ydb/core/util/backoff.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/datetime/base.h>

namespace NKikimrPQ {
    class TEvMLPUpdateExternalLockedMessageGroupsId;
    class TExternalLockedMessageGroupsId;
}

namespace NKikimr::NPQ::NMLP {

    struct TChildPartitionsOrderManager {
    public:
        static constexpr bool EnableSendFullBlacklist = true;

        enum class ESendReasons: ui8 {
            None = 0,
            Initial = 1 << 0,
            DeliveryProblem = 1 << 1,
            Commit = 1 << 2,
            Done = 1 << 3, // current partition has been read to the end
            ParentChange = 1 << 4, // propagate change from (grand-)parent partition
        };

        struct TChildrenPartitionWithKeepOrder {
            ui64 TabletId = 0;
            ui32 Cookie = 0;
            struct TFullState {
                ESendReasons Reasons = ESendReasons::None;
                ui64 GroupsCount = Max<ui64>();
            };

            TFullState SendReasons;
            TMaybe<TFullState> LastSendReasons;

            bool AddSendFullStateReason(ESendReasons reason, ui64 inflightMessagesCount);
            bool AddSendFullStateReason(ESendReasons reason);
            bool NeedSendFullState() const;
            void MarkAsSent(); // stores last sent reasons and reset current ones
            TString SendFullStateReasonsAsString() const;
        };

        bool Empty() const;
        bool SetSendFullStateToAll(ESendReasons reason, ui64 inflightMessagesCount);
        bool SetSendFullStateByCookie(ui32 cookie, ESendReasons reason);

        static TString SendReasonsToString(ESendReasons reason);

    public:
        ui64 ConsumerStep = 0;
        TMap<ui32, TChildrenPartitionWithKeepOrder> ChildrenPartitionWithKeepOrder;
        TBackoff UpdateChildPartitionsBackoff{TDuration::Seconds(1), TDuration::Seconds(10)};
    };

    TString ShortDebugString(const NKikimrPQ::TEvMLPUpdateExternalLockedMessageGroupsId&);
    TString ShortDebugString(const NKikimrPQ::TExternalLockedMessageGroupsId&);
}
