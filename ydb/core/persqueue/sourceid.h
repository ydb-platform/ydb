#pragma once

#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/heartbeat.h>
#include <ydb/core/persqueue/key.h>
#include <ydb/core/persqueue/ownerinfo.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <util/generic/set.h>

namespace NKikimr::NPQ {

enum class ESourceIdFormat: ui8 {
    Raw = 0,
    Proto = 1,
};

struct TSourceIdInfo {
    enum class EState {
        Unknown,
        Registered,
        PendingRegistration,
    };

    ui64 SeqNo = 0;
    ui64 Offset = 0;
    TInstant WriteTimestamp;
    TInstant CreateTimestamp;
    bool Explicit = false;
    TMaybe<TPartitionKeyRange> KeyRange;
    TMaybe<THeartbeat> LastHeartbeat;
    EState State = EState::Registered;

    TSourceIdInfo() = default;
    TSourceIdInfo(ui64 seqNo, ui64 offset, TInstant createTs);
    TSourceIdInfo(ui64 seqNo, ui64 offset, TInstant createTs, THeartbeat&& heartbeat);
    TSourceIdInfo(ui64 seqNo, ui64 offset, TInstant createTs, TMaybe<TPartitionKeyRange>&& keyRange, bool isInSplit = false);

    TSourceIdInfo Updated(ui64 seqNo, ui64 offset, TInstant writeTs) const;
    TSourceIdInfo Updated(ui64 seqNo, ui64 offset, TInstant writeTs, THeartbeat&& heartbeat) const;

    static EState ConvertState(NKikimrPQ::TMessageGroupInfo::EState value);
    static NKikimrPQ::TMessageGroupInfo::EState ConvertState(EState value);

    // Raw format
    static TSourceIdInfo Parse(const TString& data, TInstant now);
    void Serialize(TBuffer& data) const;

    // Proto format
    static TSourceIdInfo Parse(const NKikimrPQ::TMessageGroupInfo& proto);
    void Serialize(NKikimrPQ::TMessageGroupInfo& proto) const;

    bool operator==(const TSourceIdInfo& rhs) const;
    void Out(IOutputStream& out) const;

    bool IsExpired(TDuration ttl, TInstant now) const;

}; // TSourceIdInfo

class THeartbeatProcessor {
protected:
    using TSourceIdsByHeartbeat = TMap<TRowVersion, THashSet<TString>>;

public:
    void ApplyHeartbeat(const TString& sourceId, const TRowVersion& version);
    void ForgetHeartbeat(const TString& sourceId, const TRowVersion& version);
    void ForgetSourceId(const TString& sourceId);

protected:
    THashSet<TString> SourceIdsWithHeartbeat;
    TSourceIdsByHeartbeat SourceIdsByHeartbeat;

}; // THeartbeatProcessor

using TSourceIdMap = THashMap<TString, TSourceIdInfo>;
class THeartbeatEmitter;

class TSourceIdStorage: private THeartbeatProcessor {
    friend class THeartbeatEmitter;

public:
    const TSourceIdMap& GetInMemorySourceIds() const {
        return InMemorySourceIds;
    }

    template <typename... Args>
    void RegisterSourceId(const TString& sourceId, Args&&... args) {
        RegisterSourceIdInfo(sourceId, TSourceIdInfo(std::forward<Args>(args)...), false);
    }

    void DeregisterSourceId(const TString& sourceId);

    void LoadSourceIdInfo(const TString& key, const TString& data, TInstant now);
    bool DropOldSourceIds(TEvKeyValue::TEvRequest* request, TInstant now, ui64 startOffset, ui32 partition, const NKikimrPQ::TPartitionConfig& config);

    void RegisterSourceIdOwner(const TString& sourceId, const TStringBuf& ownerCookie);
    void MarkOwnersForDeletedSourceId(THashMap<TString, TOwnerInfo>& owners);

    TInstant MinAvailableTimestamp(TInstant now) const;

private:
    void LoadRawSourceIdInfo(const TString& key, const TString& data, TInstant now);
    void LoadProtoSourceIdInfo(const TString& key, const TString& data);
    void RegisterSourceIdInfo(const TString& sourceId, TSourceIdInfo&& sourceIdInfo, bool load);

private:
    TSourceIdMap InMemorySourceIds;
    THashMap<TString, TString> SourceIdOwners;
    TVector<TString> OwnersToDrop;
    TSet<std::pair<ui64, TString>> SourceIdsByOffset[2];
    // used to track heartbeats
    THashSet<TString> ExplicitSourceIds;

}; // TSourceIdStorage

class TSourceIdWriter {
public:
    explicit TSourceIdWriter(ESourceIdFormat format);

    const TSourceIdMap& GetSourceIdsToWrite() const {
        return Registrations;
    }

    const THashSet<TString>& GetSourceIdsToDelete() const {
        return Deregistrations;
    }

    template <typename... Args>
    void RegisterSourceId(const TString& sourceId, Args&&... args) {
        Registrations[sourceId] = TSourceIdInfo(std::forward<Args>(args)...);
    }

    void DeregisterSourceId(const TString& sourceId);
    void Clear();

    void FillRequest(TEvKeyValue::TEvRequest* request, ui32 partition);
    static void FillKeyAndData(ESourceIdFormat format, const TString& sourceId, const TSourceIdInfo& sourceIdInfo, TKeyPrefix& key, TBuffer& data);

private:
    static void FillRawData(const TSourceIdInfo& sourceIdInfo, TBuffer& data);
    static void FillProtoData(const TSourceIdInfo& sourceIdInfo, TBuffer& data);
    static TKeyPrefix::EMark FormatToMark(ESourceIdFormat format);

private:
    const ESourceIdFormat Format;
    TSourceIdMap Registrations;
    THashSet<TString> Deregistrations;

}; // TSourceIdWriter

class THeartbeatEmitter: private THeartbeatProcessor {
public:
    explicit THeartbeatEmitter(const TSourceIdStorage& storage);

    void Process(const TString& sourceId, THeartbeat&& heartbeat);
    TMaybe<THeartbeat> CanEmit() const;

private:
    TMaybe<THeartbeat> GetFromStorage(TSourceIdsByHeartbeat::const_iterator it) const;
    TMaybe<THeartbeat> GetFromDiff(TSourceIdsByHeartbeat::const_iterator it) const;

private:
    const TSourceIdStorage& Storage;
    THashSet<TString> NewSourceIdsWithHeartbeat;
    THashMap<TString, THeartbeat> Heartbeats;

}; // THeartbeatEmitter

}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NPQ::TSourceIdInfo, out, value) {
    return value.Out(out);
}
