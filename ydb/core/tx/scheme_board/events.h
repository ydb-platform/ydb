#pragma once

#include "defs.h"
#include "helpers.h"
#include "opaque_path_description.h"

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/scheme_board.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NKikimr {

using namespace NSchemeBoard;

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

    template <typename T>
    static TStringBuilder& PrintOwnerGeneration(TStringBuilder& out, const T& record) {
        return out
            << " Owner: " << record.GetOwner()
            << " Generation: " << record.GetGeneration();
    }

    template <typename T>
    static TString PrintOwnerGeneration(const IEventBase* ev, const T& record) {
        auto out = TStringBuilder() << ev->ToStringHeader() << " {";
        PrintOwnerGeneration(out, record);
        return out << " }";
    }

    template <typename T>
    static TStringBuilder& PrintPath(TStringBuilder& out, const T& record) {
        if (record.HasPath() && record.HasPathOwnerId() && record.HasLocalPathId()) {
            out << " Path: " << record.GetPath()
                << " PathId: " << TPathId(record.GetPathOwnerId(), record.GetLocalPathId());
        } else if (record.HasPath()) {
            out << " Path: " << record.GetPath();
        } else if (record.HasPathOwnerId() && record.HasLocalPathId()) {
            out << " PathId: " << TPathId(record.GetPathOwnerId(), record.GetLocalPathId());
        } else {
            out << " Path: <empty>"
                << " PathId: <empty>";
        }

        return out;
    }

    template <typename T>
    static TString PrintPath(const IEventBase* ev, const T& record) {
        auto out = TStringBuilder() << ev->ToStringHeader() << " {";
        PrintPath(out, record);
        return out << " }";
    }

    // populator events
    struct TEvRequestDescribe: public TEventLocal<TEvRequestDescribe, EvRequestDescribe> {
        const TPathId PathId;
        const TActorId Replica;

        TEvRequestDescribe() = default;

        explicit TEvRequestDescribe(const TPathId pathId, const TActorId& replica)
            : PathId(pathId)
            , Replica(replica)
        {
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " PathId: " << PathId
                << " Replica: " << Replica
            << " }";
        }
    };

    struct TEvDescribeResult: public TEventLocal<TEvDescribeResult, EvDescribeResult> {
        const bool Commit = false;
        const TLocalPathId DeletedPathBegin = 0; // The points are inclusive
        const TLocalPathId DeletedPathEnd = 0; // [DeletedPathBegin; DeletedPathEnd]
        const TLocalPathId MigratedPathId = InvalidLocalPathId;
        TOpaquePathDescription Description;

        TEvDescribeResult() = default;

        explicit TEvDescribeResult(const bool commit)
            : Commit(commit)
        {
        }

        explicit TEvDescribeResult(TLocalPathId deletedPathBegin, TLocalPathId deletedPathEnd)
            : Commit(false)
            , DeletedPathBegin(deletedPathBegin)
            , DeletedPathEnd(deletedPathEnd)
        {
        }

        explicit TEvDescribeResult(
                TLocalPathId deletedPathBegin, TLocalPathId deletedPathEnd,
                const TOpaquePathDescription& description)
            : Commit(false)
            , DeletedPathBegin(deletedPathBegin)
            , DeletedPathEnd(deletedPathEnd)
            , Description(description)
        {
        }

        explicit TEvDescribeResult(
                TLocalPathId deletedPathBegin, TLocalPathId deletedPathEnd,
                TLocalPathId migratedPathId)
            : Commit(false)
            , DeletedPathBegin(deletedPathBegin)
            , DeletedPathEnd(deletedPathEnd)
            , MigratedPathId(migratedPathId)
        {
        }

        bool HasDeletedLocalPathIds() const {
            return DeletedPathBegin != 0;
        }

        bool HasMigratedPath() const {
            return MigratedPathId != InvalidLocalPathId;
        }

        bool HasDescription() const {
            return !Description.IsEmpty();
        }

        TString ToString() const override {
            auto builder = TStringBuilder() << ToStringHeader() << " {"
                << " Commit: " << (Commit ? "true" : "false")
                << " DeletedPathBegin: " << DeletedPathBegin
                << " DeletedPathEnd: " << DeletedPathEnd
            ;
            if (HasDescription()) {
                builder << " { Path: " << Description.Path
                    << " PathId: " << Description.PathId
                    << " PathVersion: " << Description.PathVersion
                    << " }"
                ;
            }
            builder << " }";
            return builder;
        }
    };

    struct TEvRequestUpdate: public TEventLocal<TEvRequestUpdate, EvRequestUpdate> {
        const TPathId PathId;

        TEvRequestUpdate() = default;

        explicit TEvRequestUpdate(const TPathId pathId)
            : PathId(pathId)
        {
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " PathId: " << PathId
            << " }";
        }
    };

    // replica <--> populator events
    struct TEvHandshakeRequest: public TEventPB<TEvHandshakeRequest, NKikimrSchemeBoard::TEvHandshake, EvHandshakeRequest> {
        TEvHandshakeRequest() = default;

        explicit TEvHandshakeRequest(const ui64 owner, const ui64 generation) {
            Record.SetOwner(owner);
            Record.SetGeneration(generation);
        }

        TString ToString() const override {
            return PrintOwnerGeneration(this, Record);
        }
    };

    struct TEvHandshakeResponse: public TEventPB<TEvHandshakeResponse, NKikimrSchemeBoard::TEvHandshake, EvHandshakeResponse> {
        TEvHandshakeResponse() = default;

        explicit TEvHandshakeResponse(const ui64 owner, const ui64 generation) {
            Record.SetOwner(owner);
            Record.SetGeneration(generation);
        }

        TString ToString() const override {
            return PrintOwnerGeneration(this, Record);
        }
    };

    struct TEvUpdate: public TEventPreSerializedPB<TEvUpdate, NKikimrSchemeBoard::TEvUpdate, EvUpdate> {
        TEvUpdate() = default;

        TString GetPath() const {
            return Record.GetPath();
        }
        TPathId GetPathId() const {
            if (!Record.HasLocalPathId()) {
                return TPathId();
            }

            return TPathId(
                Record.HasPathOwnerId() ? Record.GetPathOwnerId() : Record.GetOwner(),
                Record.GetLocalPathId()
            );
        }

        TOpaquePathDescription ExtractPathDescription();

        TString ToString() const override {
            return PrintOwnerGeneration(this, Record);
        }
    };

    struct TEvUpdateBuilder: public TEvUpdate {
        using TBase::Record;

        TEvUpdateBuilder() = default;

        explicit TEvUpdateBuilder(const ui64 owner, const ui64 generation);
        explicit TEvUpdateBuilder(const ui64 owner, const ui64 generation, const TPathId& pathId);
        explicit TEvUpdateBuilder(
            const ui64 owner,
            const ui64 generation,
            const TOpaquePathDescription& pathDescription,
            const bool isDeletion = false
        );

        void SetDescribeSchemeResultSerialized(const TString& serialized);
        void SetDescribeSchemeResultSerialized(TString&& serialized);
    };

    struct TEvUpdateAck: public TEventPB<TEvUpdateAck, NKikimrSchemeBoard::TEvUpdateAck, EvUpdateAck> {
        TEvUpdateAck() = default;

        explicit TEvUpdateAck(
            const ui64 owner,
            const ui64 generation,
            const TPathId pathId,
            const ui64 version
        ) {
            Record.SetOwner(owner);
            Record.SetGeneration(generation);
            Record.SetLocalPathId(pathId.LocalPathId);
            Record.SetVersion(version);
            Record.SetPathOwnerId(pathId.OwnerId);
        }

        TPathId GetPathId() const {
            return TPathId(
                Record.HasPathOwnerId() ? Record.GetPathOwnerId() : Record.GetOwner(),
                Record.GetLocalPathId()
            );
        }

        TString ToString() const override {
            auto out = TStringBuilder() << ToStringHeader() << " {";
            PrintOwnerGeneration(out, Record);
            return out
                << " PathId: " << GetPathId()
                << " Version: " << Record.GetVersion()
            << " }";
        }
    };

    struct TEvCommitRequest: public TEventPB<TEvCommitRequest, NKikimrSchemeBoard::TEvCommitGeneration, EvCommitRequest> {
        TEvCommitRequest() = default;

        explicit TEvCommitRequest(const ui64 owner, const ui64 generation) {
            Record.SetOwner(owner);
            Record.SetGeneration(generation);
        }

        TString ToString() const override {
            return PrintOwnerGeneration(this, Record);
        }
    };

    struct TEvCommitResponse: public TEventPB<TEvCommitResponse, NKikimrSchemeBoard::TEvCommitGeneration, EvCommitResponse> {
        TEvCommitResponse() = default;

        explicit TEvCommitResponse(const ui64 owner, const ui64 generation) {
            Record.SetOwner(owner);
            Record.SetGeneration(generation);
        }

        TString ToString() const override {
            return PrintOwnerGeneration(this, Record);
        }
    };

    // subscriber <--> replica events
    struct TEvSubscribe: public TEventPB<TEvSubscribe, NKikimrSchemeBoard::TEvSubscribe, EvSubscribe> {
        TEvSubscribe() = default;

        explicit TEvSubscribe(const TString& path, const ui64 domainOwnerId) {
            Record.SetPath(path);
            Record.SetDomainOwnerId(domainOwnerId);
            FillCapabilities(Record);
        }

        explicit TEvSubscribe(const TPathId& pathId, const ui64 domainOwnerId) {
            Record.SetPathOwnerId(pathId.OwnerId);
            Record.SetLocalPathId(pathId.LocalPathId);
            Record.SetDomainOwnerId(domainOwnerId);
            FillCapabilities(Record);
        }

        TString ToString() const override {
            auto out = TStringBuilder() << ToStringHeader() << " {";
            PrintPath(out, Record);
            return out << " DomainOwnerId: " << Record.GetDomainOwnerId() << " }";
        }

        static void FillCapabilities(NKikimrSchemeBoard::TEvSubscribe& record) {
            record.MutableCapabilities()->SetAckNotifications(true);
        }
    };

    struct TEvUnsubscribe: public TEventPB<TEvUnsubscribe, NKikimrSchemeBoard::TEvUnsubscribe, EvUnsubscribe> {
        TEvUnsubscribe() = default;

        explicit TEvUnsubscribe(const TString& path) {
            Record.SetPath(path);
        }

        explicit TEvUnsubscribe(const TPathId& pathId) {
            Record.SetPathOwnerId(pathId.OwnerId);
            Record.SetLocalPathId(pathId.LocalPathId);
        }

        TString ToString() const override {
            return PrintPath(this, Record);
        }
    };

    template <typename T>
    static TStringBuilder& PrintPathVersion(TStringBuilder& out, const T& record) {
        return out
            << " Version: " << NSchemeBoard::GetPathVersion(record)
        ;
    }

    template <>
    TString PrintPath(const IEventBase* ev, const NKikimrSchemeBoard::TEvNotify& record) {
        auto out = TStringBuilder() << ev->ToStringHeader() << " {";
        PrintPath(out, record);
        PrintPathVersion(out, record);
        return out << " }";
    }

    struct TEvNotify: public TEventPreSerializedPB<TEvNotify, NKikimrSchemeBoard::TEvNotify, EvNotify> {
        TEvNotify() = default;

        TString ToString() const override {
            return PrintPath(this, Record);
        }
    };

    struct TEvNotifyBuilder: public TEvNotify {
        using TBase::Record;

        TEvNotifyBuilder() = default;

        explicit TEvNotifyBuilder(const TString& path, const bool isDeletion = false);
        explicit TEvNotifyBuilder(const TPathId& pathId, const bool isDeletion = false);
        explicit TEvNotifyBuilder(const TString& path, const TPathId& pathId, const bool isDeletion = false);

        void SetPathDescription(const TOpaquePathDescription& pathDescription);
    };

    struct TEvNotifyAck: public TEventPB<TEvNotifyAck, NKikimrSchemeBoard::TEvNotifyAck, EvNotifyAck> {
        TEvNotifyAck() = default;

        explicit TEvNotifyAck(ui64 version) {
            Record.SetVersion(version);
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " Version: " << Record.GetVersion()
            << " }";
        }
    };

    struct TEvSyncVersionRequest: public TEventPB<TEvSyncVersionRequest, NKikimrSchemeBoard::TEvSyncVersionRequest, EvSyncVersionRequest> {
        TEvSyncVersionRequest() = default;

        explicit TEvSyncVersionRequest(const TString& path) {
            Record.SetPath(path);
        }

        explicit TEvSyncVersionRequest(const TPathId& pathId) {
            Record.SetPathOwnerId(pathId.OwnerId);
            Record.SetLocalPathId(pathId.LocalPathId);
        }

        TString ToString() const override {
            return PrintPath(this, Record);
        }
    };

    struct TEvSyncVersionResponse: public TEventPB<TEvSyncVersionResponse, NKikimrSchemeBoard::TEvSyncVersionResponse, EvSyncVersionResponse> {
        TEvSyncVersionResponse() = default;

        explicit TEvSyncVersionResponse(const ui64 version, const bool partial = false) {
            Record.SetVersion(version);
            Record.SetPartial(partial);
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " Version: " << Record.GetVersion()
                << " Partial: " << Record.GetPartial()
            << " }";
        }
    };

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

    struct TEvSyncRequest: public TEventLocal<TEvSyncRequest, EvSyncRequest> {
    };

    struct TEvSyncResponse: public TEventLocal<TEvSyncResponse, EvSyncResponse> {
        TString Path;
        TPathId PathId;
        bool Partial;

        TEvSyncResponse() = default;

        explicit TEvSyncResponse(const TString& path, const bool partial = false)
            : Path(path)
            , Partial(partial)
        {
        }

        explicit TEvSyncResponse(const TPathId& pathId, const bool partial = false)
            : PathId(pathId)
            , Partial(partial)
        {
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " Path: " << Path
                << " PathId: " << PathId
                << " Partial: " << Partial
            << " }";
        }
    };

}; // TSchemeBoardEvents

} // NKikimr
