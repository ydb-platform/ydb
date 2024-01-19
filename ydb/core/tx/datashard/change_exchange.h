#pragma once

#include "defs.h"

#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <util/generic/vector.h>

namespace NKikimr::NDataShard {

class TDataShard;

struct TEvChangeExchange {
    enum EEv {
        /// Network exchange protocol
        // Handshake between sender & receiver
        EvHandshake = EventSpaceBegin(TKikimrEvents::ES_CHANGE_EXCHANGE_DATASHARD),
        // Apply change record(s) on receiver
        EvApplyRecords,
        // Handshake & application status
        EvStatus,
        // Activation
        EvActivateSender,
        EvActivateSenderAck,

        /// Local exchange (mostly using change's id)
        // Add new change sender
        EvAddSender,
        // Remove existing change sender
        EvRemoveSender,
        // Split/merge
        EvSplitAck,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_CHANGE_EXCHANGE_DATASHARD));

    /// Network events
    struct TEvHandshake: public TEventPB<TEvHandshake, NKikimrChangeExchange::TEvHandshake, EvHandshake> {};
    struct TEvApplyRecords: public TEventPB<TEvApplyRecords, NKikimrChangeExchange::TEvApplyRecords, EvApplyRecords> {};
    struct TEvStatus: public TEventPB<TEvStatus, NKikimrChangeExchange::TEvStatus, EvStatus> {};
    struct TEvActivateSender: public TEventPB<TEvActivateSender, NKikimrChangeExchange::TEvActivateSender, EvActivateSender> {};
    struct TEvActivateSenderAck: public TEventPB<TEvActivateSenderAck, NKikimrChangeExchange::TEvActivateSenderAck, EvActivateSenderAck> {};

    /// Local events
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

}
