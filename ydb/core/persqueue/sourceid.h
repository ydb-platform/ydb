#pragma once

#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/heartbeat.h>
#include <ydb/core/persqueue/sourceid_info.h>
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

class THeartbeatEmitter;

class TSourceIdStorage: private THeartbeatProcessor {
    friend class THeartbeatEmitter;

public:
    const TSourceIdMap& GetInMemorySourceIds() const {
        return InMemorySourceIds;
    }
    TSourceIdMap ExtractInMemorySourceIds() {
        auto ret = std::move(InMemorySourceIds);
        InMemorySourceIds = {};
        return ret;
    }

    template <typename... Args>
    void RegisterSourceId(const TString& sourceId, Args&&... args) {
        RegisterSourceIdInfo(sourceId, TSourceIdInfo(std::forward<Args>(args)...), false);
    }

    void DeregisterSourceId(const TString& sourceId);

    void LoadSourceIdInfo(const TString& key, const TString& data, TInstant now);
    bool DropOldSourceIds(TEvKeyValue::TEvRequest* request, TInstant now, ui64 startOffset, const TPartitionId& partition, const NKikimrPQ::TPartitionConfig& config);

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

    void FillRequest(TEvKeyValue::TEvRequest* request, const TPartitionId& partition);
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
