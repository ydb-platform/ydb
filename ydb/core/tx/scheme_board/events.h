#pragma once

#include "defs.h"

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/scheme_board.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NKikimr {

struct TSchemeBoardEvents {
    using TDescribeSchemeResult = NKikimrScheme::TEvDescribeSchemeResult;

    enum EEv {
        // populator events
        EvRequestDescribe = EventSpaceBegin(TKikimrEvents::ES_SCHEME_BOARD),
        EvDescribeResult,
        EvRequestUpdate,

        // replica <--> populator events
        EvHandshakeRequest = EvRequestDescribe + 512, // <-
        EvHandshakeResponse, // ->
        EvUpdate, // <-
        EvUpdateAck, // ->
        EvCommitRequest, // <-
        EvCommitResponse, // ->

        // subscriber <--> replica events
        EvSubscribe = EvRequestDescribe + 2 * 512, // ->
        EvUnsubscribe, // ->
        EvNotify, // <-
        EvSyncVersionRequest, // ->
        EvSyncVersionResponse, // <-
        EvNotifyAck, // ->

        // cache <--> subscriber events
        EvNotifyUpdate = EvRequestDescribe + 3 * 512, // <-
        EvNotifyDelete, // <-
        EvSyncRequest, // ->
        EvSyncResponse, // ->

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_SCHEME_BOARD), "expect End < EventSpaceEnd(ES_SCHEME_BOARD)");

    // cache <--> subscriber events
    struct TEvNotifyUpdate: public TEventLocal<TEvNotifyUpdate, EvNotifyUpdate> {
        TString Path;
        TPathId PathId;
        TDescribeSchemeResult DescribeSchemeResult;

        TEvNotifyUpdate() = default;

        explicit TEvNotifyUpdate(
            const TString& path,
            TDescribeSchemeResult&& describeSchemeResult)
            : Path(path)
            , DescribeSchemeResult(std::move(describeSchemeResult))
        {
        }

        explicit TEvNotifyUpdate(
            const TPathId& pathId,
            TDescribeSchemeResult&& describeSchemeResult)
            : PathId(pathId)
            , DescribeSchemeResult(std::move(describeSchemeResult))
        {
        }

        explicit TEvNotifyUpdate(
            const TString& path,
            const TPathId& pathId,
            TDescribeSchemeResult&& describeSchemeResult)
            : Path(path)
            , PathId(pathId)
            , DescribeSchemeResult(std::move(describeSchemeResult))
        {
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " Path: " << Path
                << " PathId: " << PathId
                << " DescribeSchemeResult: " << DescribeSchemeResult.ShortDebugString()
            << " }";
        }
    };

    struct TEvNotifyDelete: public TEventLocal<TEvNotifyDelete, EvNotifyDelete> {
        TString Path;
        TPathId PathId;
        bool Strong;

        TEvNotifyDelete() = default;

        explicit TEvNotifyDelete(const TString& path, bool strong)
            : Path(path)
            , Strong(strong)
        {
        }

        explicit TEvNotifyDelete(const TPathId& pathId, bool strong)
            : PathId(pathId)
            , Strong(strong)
        {
        }

        explicit TEvNotifyDelete(const TString& path, const TPathId& pathId, bool strong)
            : Path(path)
            , PathId(pathId)
            , Strong(strong)
        {
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " Path: " << Path
                << " PathId: " << PathId
                << " Strong: " << Strong
            << " }";
        }
    };

}; // TSchemeBoardEvents

} // NKikimr
