#pragma once

#include "defs.h"
#include "change_record.h"

#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <util/generic/vector.h>

namespace NKikimr {
namespace NDataShard {

class TDataShard;

struct TEvChangeExchange {
    enum EEv {
        /// Network exchange protocol
        // Handshake between sender & receiver
        EvHandshake = EventSpaceBegin(TKikimrEvents::ES_CHANGE_EXCHANGE),
        // Apply change record(s) on receiver
        EvApplyRecords,
        // Handshake & application status
        EvStatus,
        // Activation
        EvActivateSender,
        EvActivateSenderAck,

        /// Local exchange (mostly using change's id)
        // Enqueue for sending
        EvEnqueueRecords,
        // Request change record(s) by id
        EvRequestRecords,
        // Change record(s)
        EvRecords,
        // Remove change record(s) from local database
        EvRemoveRecords,

        // Add new change sender
        EvAddSender,
        // Remove existing change sender
        EvRemoveSender,

        // Already removed records that the sender should forget about
        EvForgetRecods,

        // Split/merge
        EvSplitAck,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_CHANGE_EXCHANGE));

    /// Network events
    struct TEvHandshake: public TEventPB<TEvHandshake, NKikimrChangeExchange::TEvHandshake, EvHandshake> {};
    struct TEvApplyRecords: public TEventPB<TEvApplyRecords, NKikimrChangeExchange::TEvApplyRecords, EvApplyRecords> {};
    struct TEvStatus: public TEventPB<TEvStatus, NKikimrChangeExchange::TEvStatus, EvStatus> {};
    struct TEvActivateSender: public TEventPB<TEvActivateSender, NKikimrChangeExchange::TEvActivateSender, EvActivateSender> {};
    struct TEvActivateSenderAck: public TEventPB<TEvActivateSenderAck, NKikimrChangeExchange::TEvActivateSenderAck, EvActivateSenderAck> {};

    /// Local events
    struct TEvEnqueueRecords: public TEventLocal<TEvEnqueueRecords, EvEnqueueRecords> {
        struct TRecordInfo {
            ui64 Order;
            TPathId PathId;
            ui64 BodySize;

            TRecordInfo(ui64 order, const TPathId& pathId, ui64 bodySize);

            void Out(IOutputStream& out) const;
        };

        TVector<TRecordInfo> Records;

        explicit TEvEnqueueRecords(const TVector<TRecordInfo>& records);
        explicit TEvEnqueueRecords(TVector<TRecordInfo>&& records);
        TString ToString() const override;
    };

    struct TEvRequestRecords: public TEventLocal<TEvRequestRecords, EvRequestRecords> {
        struct TRecordInfo {
            ui64 Order;
            ui64 BodySize;

            TRecordInfo(ui64 order, ui64 bodySize = 0);

            bool operator<(const TRecordInfo& rhs) const;
            void Out(IOutputStream& out) const;
        };

        TVector<TRecordInfo> Records;

        explicit TEvRequestRecords(const TVector<TRecordInfo>& records);
        explicit TEvRequestRecords(TVector<TRecordInfo>&& records);
        TString ToString() const override;
    };

    struct TEvRemoveRecords: public TEventLocal<TEvRemoveRecords, EvRemoveRecords> {
        TVector<ui64> Records;

        explicit TEvRemoveRecords(const TVector<ui64>& records);
        explicit TEvRemoveRecords(TVector<ui64>&& records);
        TString ToString() const override;
    };

    struct TEvRecords: public TEventLocal<TEvRecords, EvRecords> {
        TVector<TChangeRecord> Records;

        explicit TEvRecords(const TVector<TChangeRecord>& records);
        explicit TEvRecords(TVector<TChangeRecord>&& records);
        TString ToString() const override;
    };

    struct TEvForgetRecords: public TEventLocal<TEvForgetRecords, EvForgetRecods> {
        TVector<ui64> Records;

        explicit TEvForgetRecords(const TVector<ui64>& records);
        explicit TEvForgetRecords(TVector<ui64>&& records);
        TString ToString() const override;
    };

    enum class ESenderType {
        AsyncIndex,
        CdcStream,
    };

    struct TEvAddSender: public TEventLocal<TEvAddSender, EvAddSender> {
        TTableId UserTableId;
        ESenderType Type;
        TPathId PathId;

        explicit TEvAddSender(const TTableId& userTableId, ESenderType type, const TPathId& pathId);
        TString ToString() const override;
    };

    struct TEvRemoveSender: public TEventLocal<TEvRemoveSender, EvRemoveSender> {
        TPathId PathId;

        explicit TEvRemoveSender(const TPathId& pathId);
        TString ToString() const override;
    };

    struct TEvSplitAck: public TEventLocal<TEvSplitAck, EvSplitAck> {
    };

}; // TEvChangeExchange

IActor* CreateChangeSender(const TDataShard* self);
IActor* CreateChangeExchangeSplit(const TDataShard* self, const TVector<ui64>& dstDataShards);

} // NDataShard
} // NKikimr

Y_DECLARE_OUT_SPEC(inline, NKikimr::NDataShard::TEvChangeExchange::TEvEnqueueRecords::TRecordInfo, o, x) {
    return x.Out(o);
}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NDataShard::TEvChangeExchange::TEvRequestRecords::TRecordInfo, o, x) {
    return x.Out(o);
}
