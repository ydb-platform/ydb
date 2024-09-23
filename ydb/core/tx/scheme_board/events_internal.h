#pragma once

#include "defs.h"
#include "opaque_path_description.h"
#include "helpers.h"
#include "events.h"

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/scheme_board.pb.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NKikimr::NSchemeBoard {

namespace NInternalEvents {

template <typename T>
TStringBuilder& PrintOwnerGeneration(TStringBuilder& out, const T& record) {
    return out
        << " Owner: " << record.GetOwner()
        << " Generation: " << record.GetGeneration();
}

template <typename T>
TString PrintOwnerGeneration(const IEventBase* ev, const T& record) {
    auto out = TStringBuilder() << ev->ToStringHeader() << " {";
    PrintOwnerGeneration(out, record);
    return out << " }";
}

template <typename T>
TStringBuilder& PrintPath(TStringBuilder& out, const T& record) {
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
TString PrintPath(const IEventBase* ev, const T& record) {
    auto out = TStringBuilder() << ev->ToStringHeader() << " {";
    PrintPath(out, record);
    return out << " }";
}

// populator events
struct TEvRequestDescribe: public TEventLocal<TEvRequestDescribe, TSchemeBoardEvents::EvRequestDescribe> {
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

struct TEvDescribeResult: public TEventLocal<TEvDescribeResult, TSchemeBoardEvents::EvDescribeResult> {
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

struct TEvRequestUpdate: public TEventLocal<TEvRequestUpdate, TSchemeBoardEvents::EvRequestUpdate> {
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
struct TEvHandshakeRequest: public TEventPB<TEvHandshakeRequest, NKikimrSchemeBoard::TEvHandshake, TSchemeBoardEvents::EvHandshakeRequest> {
    TEvHandshakeRequest() = default;

    explicit TEvHandshakeRequest(const ui64 owner, const ui64 generation) {
        Record.SetOwner(owner);
        Record.SetGeneration(generation);
    }

    TString ToString() const override {
        return PrintOwnerGeneration(this, Record);
    }
};

struct TEvHandshakeResponse: public TEventPB<TEvHandshakeResponse, NKikimrSchemeBoard::TEvHandshake, TSchemeBoardEvents::EvHandshakeResponse> {
    TEvHandshakeResponse() = default;

    explicit TEvHandshakeResponse(const ui64 owner, const ui64 generation) {
        Record.SetOwner(owner);
        Record.SetGeneration(generation);
    }

    TString ToString() const override {
        return PrintOwnerGeneration(this, Record);
    }
};

struct TEvUpdate: public TEventPreSerializedPB<TEvUpdate, NKikimrSchemeBoard::TEvUpdate, TSchemeBoardEvents::EvUpdate> {
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

// Defined in schemeshard interface events_schemeshard.h:
// struct TEvUpdateAck

struct TEvCommitRequest: public TEventPB<TEvCommitRequest, NKikimrSchemeBoard::TEvCommitGeneration, TSchemeBoardEvents::EvCommitRequest> {
    TEvCommitRequest() = default;

    explicit TEvCommitRequest(const ui64 owner, const ui64 generation) {
        Record.SetOwner(owner);
        Record.SetGeneration(generation);
    }

    TString ToString() const override {
        return PrintOwnerGeneration(this, Record);
    }
};

struct TEvCommitResponse: public TEventPB<TEvCommitResponse, NKikimrSchemeBoard::TEvCommitGeneration, TSchemeBoardEvents::EvCommitResponse> {
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
struct TEvSubscribe: public TEventPB<TEvSubscribe, NKikimrSchemeBoard::TEvSubscribe, TSchemeBoardEvents::EvSubscribe> {
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

struct TEvUnsubscribe: public TEventPB<TEvUnsubscribe, NKikimrSchemeBoard::TEvUnsubscribe, TSchemeBoardEvents::EvUnsubscribe> {
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

template <>
TString PrintPath(const IEventBase* ev, const NKikimrSchemeBoard::TEvNotify& record);

struct TEvNotify: public TEventPreSerializedPB<TEvNotify, NKikimrSchemeBoard::TEvNotify, TSchemeBoardEvents::EvNotify> {
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

struct TEvNotifyAck: public TEventPB<TEvNotifyAck, NKikimrSchemeBoard::TEvNotifyAck, TSchemeBoardEvents::EvNotifyAck> {
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

struct TEvSyncVersionRequest: public TEventPB<TEvSyncVersionRequest, NKikimrSchemeBoard::TEvSyncVersionRequest, TSchemeBoardEvents::EvSyncVersionRequest> {
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

struct TEvSyncVersionResponse: public TEventPB<TEvSyncVersionResponse, NKikimrSchemeBoard::TEvSyncVersionResponse, TSchemeBoardEvents::EvSyncVersionResponse> {
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

// Defined in public interface events.h:
// struct TEvNotifyUpdate
// struct TEvNotifyDelete

struct TEvSyncRequest: public TEventLocal<TEvSyncRequest, TSchemeBoardEvents::EvSyncRequest> {
};

struct TEvSyncResponse: public TEventLocal<TEvSyncResponse, TSchemeBoardEvents::EvSyncResponse> {
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

}  // NInternalEvents

}  // NKikimr::NSchemeBoard
