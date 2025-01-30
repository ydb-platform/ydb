#pragma once

#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/heartbeat.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ {

struct TSourceIdInfo {
    enum class EState {
        Unknown,
        Registered,
        PendingRegistration,
    };

    ui64 SeqNo = 0;
    ui64 MinSeqNo = 0;
    ui64 Offset = 0;
    TInstant WriteTimestamp;
    TInstant CreateTimestamp;
    bool Explicit = false;
    bool TxModified = false;
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

using TSourceIdMap = THashMap<TString, TSourceIdInfo>;

} //namespace NKikimr::NPQ
