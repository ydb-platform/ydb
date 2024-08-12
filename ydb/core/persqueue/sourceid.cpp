#include "sourceid.h"
#include "ownerinfo.h"

#include <ydb/core/persqueue/partition.h>

#include <util/generic/size_literals.h>

#include <algorithm>

namespace NKikimr::NPQ {

static constexpr ui64 MAX_DELETE_COMMAND_SIZE = 10_MB;
static constexpr ui64 MAX_DELETE_COMMAND_COUNT = 1000;

template <typename T>
T ReadAs(const TString& data, ui32& pos) {
    auto result = ReadUnaligned<T>(data.c_str() + pos);
    pos += sizeof(T);
    return result;
}

template <typename T>
T ReadAs(const TString& data, ui32& pos, const T& default_) {
    if (pos + sizeof(T) <= data.size()) {
        return ReadAs<T>(data, pos);
    } else {
        return default_;
    }
}

template <typename T>
void Write(T value, TBuffer& data, ui32& pos) {
    memcpy(data.Data() + pos, &value, sizeof(T));
    pos += sizeof(T);
}

TSourceIdInfo::EState TSourceIdInfo::ConvertState(NKikimrPQ::TMessageGroupInfo::EState value) {
    switch (value) {
    case NKikimrPQ::TMessageGroupInfo::STATE_REGISTERED:
        return TSourceIdInfo::EState::Registered;
    case NKikimrPQ::TMessageGroupInfo::STATE_PENDING_REGISTRATION:
        return TSourceIdInfo::EState::PendingRegistration;
    default:
        return TSourceIdInfo::EState::Unknown;
    }
}

NKikimrPQ::TMessageGroupInfo::EState TSourceIdInfo::ConvertState(TSourceIdInfo::EState value) {
    switch (value) {
    case TSourceIdInfo::EState::Registered:
        return NKikimrPQ::TMessageGroupInfo::STATE_REGISTERED;
    case TSourceIdInfo::EState::PendingRegistration:
        return NKikimrPQ::TMessageGroupInfo::STATE_PENDING_REGISTRATION;
    case TSourceIdInfo::EState::Unknown:
        return NKikimrPQ::TMessageGroupInfo::STATE_UNKNOWN;
    }
}

void FillWrite(const TKeyPrefix& key, const TBuffer& data, NKikimrClient::TKeyValueRequest::TCmdWrite& cmd) {
    cmd.SetKey(key.Data(), key.Size());
    cmd.SetValue(data.Data(), data.Size());
    cmd.SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
}

void FillDelete(const TKeyPrefix& key, NKikimrClient::TKeyValueRequest::TCmdDeleteRange& cmd) {
    auto& range = *cmd.MutableRange();
    range.SetFrom(key.Data(), key.Size());
    range.SetIncludeFrom(true);
    range.SetTo(key.Data(), key.Size());
    range.SetIncludeTo(true);
}

void FillDelete(const TPartitionId& partition, const TString& sourceId, TKeyPrefix::EMark mark, NKikimrClient::TKeyValueRequest::TCmdDeleteRange& cmd) {
    TKeyPrefix key(TKeyPrefix::TypeInfo, partition, mark);
    key.Append(sourceId.c_str(), sourceId.size());
    FillDelete(key, cmd);
}

void FillDelete(const TPartitionId& partition, const TString& sourceId, NKikimrClient::TKeyValueRequest::TCmdDeleteRange& cmd) {
    FillDelete(partition, sourceId, TKeyPrefix::MarkProtoSourceId, cmd);
}

void THeartbeatProcessor::ApplyHeartbeat(const TString& sourceId, const TRowVersion& version) {
    SourceIdsWithHeartbeat.insert(sourceId);
    SourceIdsByHeartbeat[version].insert(sourceId);
}

void THeartbeatProcessor::ForgetHeartbeat(const TString& sourceId, const TRowVersion& version) {
    auto it = SourceIdsByHeartbeat.find(version);
    if (it == SourceIdsByHeartbeat.end()) {
        return;
    }

    it->second.erase(sourceId);
    if (it->second.empty()) {
        SourceIdsByHeartbeat.erase(it);
    }
}

void THeartbeatProcessor::ForgetSourceId(const TString& sourceId) {
    SourceIdsWithHeartbeat.erase(sourceId);
}

TSourceIdInfo::TSourceIdInfo(ui64 seqNo, ui64 offset, TInstant createTs)
    : SeqNo(seqNo)
    , MinSeqNo(seqNo)
    , Offset(offset)
    , WriteTimestamp(createTs)
    , CreateTimestamp(createTs)
{
}

TSourceIdInfo::TSourceIdInfo(ui64 seqNo, ui64 offset, TInstant createTs, THeartbeat&& heartbeat)
    : SeqNo(seqNo)
    , MinSeqNo(seqNo)
    , Offset(offset)
    , WriteTimestamp(createTs)
    , CreateTimestamp(createTs)
    , LastHeartbeat(std::move(heartbeat))
{
}

TSourceIdInfo::TSourceIdInfo(ui64 seqNo, ui64 offset, TInstant createTs, TMaybe<TPartitionKeyRange>&& keyRange, bool isInSplit)
    : SeqNo(seqNo)
    , MinSeqNo(seqNo)
    , Offset(offset)
    , CreateTimestamp(createTs)
    , Explicit(true)
    , KeyRange(std::move(keyRange))
{
    if (isInSplit) {
        State = EState::PendingRegistration;
    }
}

TSourceIdInfo TSourceIdInfo::Updated(ui64 seqNo, ui64 offset, TInstant writeTs) const {
    auto copy = *this;
    copy.SeqNo = seqNo;
    if (copy.MinSeqNo == 0) {
        copy.MinSeqNo = seqNo;
    }
    copy.Offset = offset;
    copy.WriteTimestamp = writeTs;

    return copy;
}

TSourceIdInfo TSourceIdInfo::Updated(ui64 seqNo, ui64 offset, TInstant writeTs, THeartbeat&& heartbeat) const {
    auto copy = Updated(seqNo, offset, writeTs);
    copy.LastHeartbeat = std::move(heartbeat);

    return copy;
}

TSourceIdInfo TSourceIdInfo::Parse(const TString& data, TInstant now) {
    Y_ABORT_UNLESS(data.size() >= 2 * sizeof(ui64), "Data must contain at least SeqNo & Offset");
    ui32 pos = 0;

    TSourceIdInfo result;
    result.SeqNo = ReadAs<ui64>(data, pos);
    result.Offset = ReadAs<ui64>(data, pos);
    result.WriteTimestamp = TInstant::MilliSeconds(ReadAs<ui64>(data, pos, now.MilliSeconds()));
    result.CreateTimestamp = TInstant::MilliSeconds(ReadAs<ui64>(data, pos, now.MilliSeconds()));

    Y_ABORT_UNLESS(result.SeqNo <= (ui64)Max<i64>(), "SeqNo is too big: %" PRIu64, result.SeqNo);
    Y_ABORT_UNLESS(result.Offset <= (ui64)Max<i64>(), "Offset is too big: %" PRIu64, result.Offset);

    return result;
}

void TSourceIdInfo::Serialize(TBuffer& data) const {
    Y_ABORT_UNLESS(!Explicit);
    Y_ABORT_UNLESS(!KeyRange);

    data.Resize(4 * sizeof(ui64));
    ui32 pos = 0;

    Write<ui64>(SeqNo, data, pos);
    Write<ui64>(Offset, data, pos);
    Write<ui64>(WriteTimestamp.MilliSeconds(), data, pos);
    Write<ui64>(CreateTimestamp.MilliSeconds(), data, pos);
}

TSourceIdInfo TSourceIdInfo::Parse(const NKikimrPQ::TMessageGroupInfo& proto) {
    TSourceIdInfo result;
    result.SeqNo = proto.GetSeqNo();
    result.MinSeqNo = proto.GetMinSeqNo();
    result.Offset = proto.GetOffset();
    result.WriteTimestamp = TInstant::FromValue(proto.GetWriteTimestamp());
    result.CreateTimestamp = TInstant::FromValue(proto.GetCreateTimestamp());
    result.Explicit = proto.GetExplicit();
    if (result.Explicit) {
        result.State = ConvertState(proto.GetState());
    }
    if (proto.HasKeyRange()) {
        result.KeyRange = TPartitionKeyRange::Parse(proto.GetKeyRange());
    }
    if (proto.HasLastHeartbeat()) {
        result.LastHeartbeat = THeartbeat::Parse(proto.GetLastHeartbeat());
    }

    return result;
}

void TSourceIdInfo::Serialize(NKikimrPQ::TMessageGroupInfo& proto) const {
    proto.SetSeqNo(SeqNo);
    proto.SetMinSeqNo(MinSeqNo);
    proto.SetOffset(Offset);
    proto.SetWriteTimestamp(WriteTimestamp.GetValue());
    proto.SetCreateTimestamp(CreateTimestamp.GetValue());
    proto.SetExplicit(Explicit);
    if (Explicit) {
        proto.SetState(ConvertState(State));
    }
    if (KeyRange) {
        KeyRange->Serialize(*proto.MutableKeyRange());
    }
    if (LastHeartbeat) {
        LastHeartbeat->Serialize(*proto.MutableLastHeartbeat());
    }
}

bool TSourceIdInfo::operator==(const TSourceIdInfo& rhs) const {
    return SeqNo == rhs.SeqNo
        && Offset == rhs.Offset
        && WriteTimestamp == rhs.WriteTimestamp
        && CreateTimestamp == rhs.CreateTimestamp
        && Explicit == rhs.Explicit
        && State == rhs.State;
}

void TSourceIdInfo::Out(IOutputStream& out) const {
    out << "{"
        << " SeqNo: " << SeqNo
        << " Offset: " << Offset
        << " WriteTimestamp: " << WriteTimestamp.GetValue()
        << " CreateTimestamp: " << CreateTimestamp.GetValue()
        << " Explicit: " << (Explicit ? "true" : "false")
        << " State: " << State
    << " }";
}

bool TSourceIdInfo::IsExpired(TDuration ttl, TInstant now) const {
    Y_DEBUG_ABORT_UNLESS(!Explicit);
    return !Explicit && ((WriteTimestamp + ttl) <= now);
}

/// TSourceIdStorage
void TSourceIdStorage::DeregisterSourceId(const TString& sourceId) {
    auto it = InMemorySourceIds.find(sourceId);
    if (it == InMemorySourceIds.end()) {
        return;
    }

    ForgetSourceId(sourceId);
    if (const auto& heartbeat = it->second.LastHeartbeat) {
        ForgetHeartbeat(sourceId, heartbeat->Version);
    }

    if (it->second.Explicit) {
        ExplicitSourceIds.erase(sourceId);
    }

    SourceIdsByOffset[it->second.Explicit].erase(std::make_pair(it->second.Offset, sourceId));
    InMemorySourceIds.erase(it);

    auto jt = SourceIdOwners.find(sourceId);
    if (jt != SourceIdOwners.end()) {
        OwnersToDrop.push_back(jt->second);
        SourceIdOwners.erase(jt);
    }
}

bool TSourceIdStorage::DropOldSourceIds(TEvKeyValue::TEvRequest* request, TInstant now, ui64 startOffset, const TPartitionId& partition, const NKikimrPQ::TPartitionConfig& config) {
    TVector<std::pair<ui64, TString>> toDelOffsets;

    ui64 maxDeleteSourceIds = 0;
    if (InMemorySourceIds.size() > config.GetSourceIdMaxCounts()) {
        maxDeleteSourceIds = InMemorySourceIds.size() - config.GetSourceIdMaxCounts();
        maxDeleteSourceIds = Min(maxDeleteSourceIds, MAX_DELETE_COMMAND_COUNT);
    }

    Y_ABORT_UNLESS(maxDeleteSourceIds <= InMemorySourceIds.size());

    const auto ttl = TDuration::Seconds(config.GetSourceIdLifetimeSeconds());
    ui32 size = request->Record.ByteSize();

    for (const auto& [offset, sourceId] : SourceIdsByOffset[0]) {
        if (offset >= startOffset && toDelOffsets.size() >= maxDeleteSourceIds) {
            break;
        }

        auto it = InMemorySourceIds.find(sourceId);
        Y_ABORT_UNLESS(it != InMemorySourceIds.end());
        if (it->second.Explicit) {
            continue;
        }

        if (toDelOffsets.size() < maxDeleteSourceIds || it->second.IsExpired(ttl, now)) {
            toDelOffsets.emplace_back(offset, sourceId);

            bool reachedLimit = false;
            for (const auto mark : {TKeyPrefix::MarkSourceId, TKeyPrefix::MarkProtoSourceId}) {
                auto& cmd = *request->Record.AddCmdDeleteRange();
                FillDelete(partition, sourceId, mark, cmd);

                size += cmd.ByteSize();
                if (size >= MAX_DELETE_COMMAND_SIZE || toDelOffsets.size() >= MAX_DELETE_COMMAND_COUNT) {
                    LOG_INFO_S(*TlsActivationContext, NKikimrServices::PERSQUEUE, "DropOldSourceIds reached proto size limit"
                        << ": size# " << size
                        << ", count# " << toDelOffsets.size());
                    reachedLimit = true;
                    break;
                }
            }

            if (reachedLimit) {
                break; // Check here size to prevent crashing in protobuf verify -max size is 25Mb's and no more then 100K operations
                       // but even 10Mbs sounds too big here.
                       // Rest of SourceIds will be deleted in next DropOldSourceIds calls, whitch will be made
                       // right after complete of current deletion.
            }
        } else {
            // there are no space for new sourcID to delete, stop check sourceIds
            break;
        }
    }

    for (auto& t : toDelOffsets) {
        // delete sourceId from memory
        size_t res = InMemorySourceIds.erase(t.second);
        Y_ABORT_UNLESS(res == 1);
        // delete sourceID from offsets
        res = SourceIdsByOffset[0].erase(t);
        Y_ABORT_UNLESS(res == 1);
        // save owners to drop and delete records from map
        auto it = SourceIdOwners.find(t.second);
        if (it != SourceIdOwners.end()) {
            OwnersToDrop.push_back(it->second);
            SourceIdOwners.erase(it);
        }
    }

    return !toDelOffsets.empty();
}

void TSourceIdStorage::LoadSourceIdInfo(const TString& key, const TString& data, TInstant now) {
    Y_ABORT_UNLESS(key.size() >= TKeyPrefix::MarkedSize());

    const auto mark = TKeyPrefix::EMark(key[TKeyPrefix::MarkPosition()]);
    switch (mark) {
    case TKeyPrefix::MarkSourceId:
        return LoadRawSourceIdInfo(key, data, now);
    case TKeyPrefix::MarkProtoSourceId:
        return LoadProtoSourceIdInfo(key, data);
    default:
        Y_FAIL_S("Unexpected mark: " << (char)mark);
    }
}

void TSourceIdStorage::LoadRawSourceIdInfo(const TString& key, const TString& data, TInstant now) {
    Y_ABORT_UNLESS(key.size() >= TKeyPrefix::MarkedSize());
    Y_ABORT_UNLESS(key[TKeyPrefix::MarkPosition()] == TKeyPrefix::MarkSourceId);

    RegisterSourceIdInfo(key.substr(TKeyPrefix::MarkedSize()), TSourceIdInfo::Parse(data, now), true);
}

void TSourceIdStorage::LoadProtoSourceIdInfo(const TString& key, const TString& data) {
    Y_ABORT_UNLESS(key.size() >= TKeyPrefix::MarkedSize());
    Y_ABORT_UNLESS(key[TKeyPrefix::MarkPosition()] == TKeyPrefix::MarkProtoSourceId);

    NKikimrPQ::TMessageGroupInfo proto;
    const bool ok = proto.ParseFromString(data);
    Y_ABORT_UNLESS(ok);

    RegisterSourceIdInfo(key.substr(TKeyPrefix::MarkedSize()), TSourceIdInfo::Parse(proto), true);
}

void TSourceIdStorage::RegisterSourceIdInfo(const TString& sourceId, TSourceIdInfo&& sourceIdInfo, bool load) {
    auto it = InMemorySourceIds.find(sourceId);
    if (it != InMemorySourceIds.end()) {
        if (!load || it->second.Offset < sourceIdInfo.Offset) {
            const auto res = SourceIdsByOffset[sourceIdInfo.Explicit].erase(std::make_pair(it->second.Offset, sourceId));
            Y_ABORT_UNLESS(res == 1);
        } else {
            return;
        }
    }

    const bool res = SourceIdsByOffset[sourceIdInfo.Explicit].emplace(sourceIdInfo.Offset, sourceId).second;
    Y_ABORT_UNLESS(res);

    if (sourceIdInfo.Explicit) {
        ExplicitSourceIds.insert(sourceId);
    }

    if (const auto& heartbeat = sourceIdInfo.LastHeartbeat) {
        Y_ABORT_UNLESS(sourceIdInfo.Explicit);

        if (it != InMemorySourceIds.end() && it->second.LastHeartbeat) {
            ForgetHeartbeat(sourceId, it->second.LastHeartbeat->Version);
        }

        ApplyHeartbeat(sourceId, heartbeat->Version);
    }

    InMemorySourceIds[sourceId] = std::move(sourceIdInfo);
}

void TSourceIdStorage::RegisterSourceIdOwner(const TString& sourceId, const TStringBuf& ownerCookie) {
    if (ownerCookie == "default") {
        // cookie for legacy http protocol - skip it, we use one cookie for all write sessions
        return;
    }
    // owner cookie could be deleted in main object - so we should copy it
    SourceIdOwners[sourceId] = ownerCookie;
}

void TSourceIdStorage::MarkOwnersForDeletedSourceId(THashMap<TString, TOwnerInfo>& owners) {
    for (auto& sourceid : OwnersToDrop) {
        auto it = owners.find(sourceid);
        if (it != owners.end()) {
             it->second.SourceIdDeleted = true;
        }
    }

    OwnersToDrop.clear();
}

TInstant TSourceIdStorage::MinAvailableTimestamp(TInstant now) const {
    TInstant ds = now;
    for (ui32 i = 0 ; i < 2; ++i) {
        if (!SourceIdsByOffset[i].empty()) {
            auto it = InMemorySourceIds.find(SourceIdsByOffset[i].begin()->second);
            Y_ABORT_UNLESS(it != InMemorySourceIds.end());
            ds = Min(ds, it->second.WriteTimestamp);
        }
    }

    return ds;
}

/// TSourceIdWriter
TSourceIdWriter::TSourceIdWriter(ESourceIdFormat format)
    : Format(format)
{
}

void TSourceIdWriter::DeregisterSourceId(const TString& sourceId) {
    Deregistrations.insert(sourceId);
}

void TSourceIdWriter::Clear() {
    Registrations.clear();
    Deregistrations.clear();
}

void TSourceIdWriter::FillKeyAndData(ESourceIdFormat format, const TString& sourceId, const TSourceIdInfo& sourceIdInfo, TKeyPrefix& key, TBuffer& data) {
    key.Resize(TKeyPrefix::MarkedSize());
    key.Append(sourceId.c_str(), sourceId.size());

    switch (format) {
    case ESourceIdFormat::Raw:
        return FillRawData(sourceIdInfo, data);
    case ESourceIdFormat::Proto:
        return FillProtoData(sourceIdInfo, data);
    }
}

void TSourceIdWriter::FillRawData(const TSourceIdInfo& sourceIdInfo, TBuffer& data) {
    sourceIdInfo.Serialize(data);
}

void TSourceIdWriter::FillProtoData(const TSourceIdInfo& sourceIdInfo, TBuffer& data) {
    NKikimrPQ::TMessageGroupInfo proto;
    sourceIdInfo.Serialize(proto);

    const TString serialized = proto.SerializeAsString();
    data.Assign(serialized.c_str(), serialized.size());
}

TKeyPrefix::EMark TSourceIdWriter::FormatToMark(ESourceIdFormat format) {
    switch (format) {
    case ESourceIdFormat::Raw:
        return TKeyPrefix::MarkSourceId;
    case ESourceIdFormat::Proto:
        return TKeyPrefix::MarkProtoSourceId;
    }
}

void TSourceIdWriter::FillRequest(TEvKeyValue::TEvRequest* request, const TPartitionId& partition) {
    TKeyPrefix key(TKeyPrefix::TypeInfo, partition, FormatToMark(Format));
    TBuffer data;

    for (const auto& [sourceId, sourceIdInfo] : Registrations) {
        FillKeyAndData(Format, sourceId, sourceIdInfo, key, data);
        FillWrite(key, data, *request->Record.AddCmdWrite());
    }

    for (const auto& sourceId : Deregistrations) {
        FillDelete(partition, sourceId, *request->Record.AddCmdDeleteRange());
    }
}

/// THeartbeatEmitter
THeartbeatEmitter::THeartbeatEmitter(const TSourceIdStorage& storage)
    : Storage(storage)
{
}

void THeartbeatEmitter::Process(const TString& sourceId, THeartbeat&& heartbeat) {
    auto it = Storage.InMemorySourceIds.find(sourceId);
    if (it != Storage.InMemorySourceIds.end() && it->second.LastHeartbeat) {
        if (heartbeat.Version <= it->second.LastHeartbeat->Version) {
            return;
        }
    }

    if (!Storage.SourceIdsWithHeartbeat.contains(sourceId)) {
        NewSourceIdsWithHeartbeat.insert(sourceId);
    }

    if (Heartbeats.contains(sourceId)) {
        ForgetHeartbeat(sourceId, Heartbeats.at(sourceId).Version);
    }

    ApplyHeartbeat(sourceId, heartbeat.Version);
    Heartbeats[sourceId] = std::move(heartbeat);
}

TMaybe<THeartbeat> THeartbeatEmitter::CanEmit() const {
    if (Storage.ExplicitSourceIds.size() != (Storage.SourceIdsWithHeartbeat.size() + NewSourceIdsWithHeartbeat.size())) {
        return Nothing();
    }

    if (SourceIdsByHeartbeat.empty()) {
        return Nothing();
    }

    if (!NewSourceIdsWithHeartbeat.empty()) { // just got quorum
        if (!Storage.SourceIdsByHeartbeat.empty() && Storage.SourceIdsByHeartbeat.begin()->first < SourceIdsByHeartbeat.begin()->first) {
            return GetFromStorage(Storage.SourceIdsByHeartbeat.begin());
        } else {
            return GetFromDiff(SourceIdsByHeartbeat.begin());
        }
    } else if (SourceIdsByHeartbeat.begin()->first > Storage.SourceIdsByHeartbeat.begin()->first) {
        auto storage = Storage.SourceIdsByHeartbeat.begin();
        auto diff = SourceIdsByHeartbeat.begin();

        TMaybe<TRowVersion> newVersion;
        while (storage != Storage.SourceIdsByHeartbeat.end()) {
            const auto& [version, sourceIds] = *storage;

            auto rest = sourceIds.size();
            for (const auto& sourceId : sourceIds) {
                auto it = Heartbeats.find(sourceId);
                if (it != Heartbeats.end() && it->second.Version > version && version <= diff->first) {
                    --rest;
                } else {
                    break;
                }
            }

            if (!rest) {
                if (++storage != Storage.SourceIdsByHeartbeat.end()) {
                    newVersion = storage->first;
                } else {
                    newVersion = diff->first;
                }
            } else {
                break;
            }
        }

        if (newVersion) {
            storage = Storage.SourceIdsByHeartbeat.find(*newVersion);
            if (storage != Storage.SourceIdsByHeartbeat.end()) {
                return GetFromStorage(storage);
            } else {
                return GetFromDiff(diff);
            }
        }
    }

    return Nothing();
}

TMaybe<THeartbeat> THeartbeatEmitter::GetFromStorage(TSourceIdsByHeartbeat::const_iterator it) const {
    Y_ABORT_UNLESS(!it->second.empty());
    const auto& someSourceId = *it->second.begin();

    Y_ABORT_UNLESS(Storage.InMemorySourceIds.contains(someSourceId));
    return Storage.InMemorySourceIds.at(someSourceId).LastHeartbeat;
}

TMaybe<THeartbeat> THeartbeatEmitter::GetFromDiff(TSourceIdsByHeartbeat::const_iterator it) const {
    Y_ABORT_UNLESS(!it->second.empty());
    const auto& someSourceId = *it->second.begin();

    Y_ABORT_UNLESS(Heartbeats.contains(someSourceId));
    return Heartbeats.at(someSourceId);
}

}
