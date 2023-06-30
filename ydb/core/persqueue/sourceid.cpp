#include "sourceid.h"
#include "ownerinfo.h"

#include <ydb/core/persqueue/partition.h>

#include <util/generic/size_literals.h>

#include <algorithm>

namespace NKikimr {
namespace NPQ {

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

void FillDelete(ui32 partition, const TString& sourceId, TKeyPrefix::EMark mark, NKikimrClient::TKeyValueRequest::TCmdDeleteRange& cmd) {
    TKeyPrefix key(TKeyPrefix::TypeInfo, partition, mark);
    key.Append(sourceId.c_str(), sourceId.size());
    FillDelete(key, cmd);
}

void FillDelete(ui32 partition, const TString& sourceId, NKikimrClient::TKeyValueRequest::TCmdDeleteRange& cmd) {
    FillDelete(partition, sourceId, TKeyPrefix::MarkProtoSourceId, cmd);
}

TSourceIdInfo::TSourceIdInfo(ui64 seqNo, ui64 offset, TInstant createTs)
    : SeqNo(seqNo)
    , Offset(offset)
    , WriteTimestamp(createTs)
    , CreateTimestamp(createTs)
{
}

TSourceIdInfo::TSourceIdInfo(ui64 seqNo, ui64 offset, TInstant createTs, TMaybe<TPartitionKeyRange>&& keyRange, bool isInSplit)
    : SeqNo(seqNo)
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
    copy.Offset = offset;
    copy.WriteTimestamp = writeTs;

    return copy;
}

TSourceIdInfo TSourceIdInfo::Parse(const TString& data, TInstant now) {
    Y_VERIFY(data.size() >= 2 * sizeof(ui64), "Data must contain at least SeqNo & Offset");
    ui32 pos = 0;

    TSourceIdInfo result;
    result.SeqNo = ReadAs<ui64>(data, pos);
    result.Offset = ReadAs<ui64>(data, pos);
    result.WriteTimestamp = TInstant::MilliSeconds(ReadAs<ui64>(data, pos, now.MilliSeconds()));
    result.CreateTimestamp = TInstant::MilliSeconds(ReadAs<ui64>(data, pos, now.MilliSeconds()));

    Y_VERIFY(result.SeqNo <= (ui64)Max<i64>(), "SeqNo is too big: %" PRIu64, result.SeqNo);
    Y_VERIFY(result.Offset <= (ui64)Max<i64>(), "Offset is too big: %" PRIu64, result.Offset);

    return result;
}

void TSourceIdInfo::Serialize(TBuffer& data) const {
    Y_VERIFY(!Explicit);
    Y_VERIFY(!KeyRange);

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

    return result;
}

void TSourceIdInfo::Serialize(NKikimrPQ::TMessageGroupInfo& proto) const {
    proto.SetSeqNo(SeqNo);
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
    Y_VERIFY_DEBUG(!Explicit);
    return !Explicit && ((WriteTimestamp + ttl) <= now);
}

/// TSourceIdStorage
void TSourceIdStorage::DeregisterSourceId(const TString& sourceId) {
    auto it = InMemorySourceIds.find(sourceId);
    if (it == InMemorySourceIds.end()) {
        return;
    }

    SourceIdsByOffset.erase(std::make_pair(it->second.Offset, sourceId));
    InMemorySourceIds.erase(it);

    auto jt = SourceIdOwners.find(sourceId);
    if (jt != SourceIdOwners.end()) {
        OwnersToDrop.push_back(jt->second);
        SourceIdOwners.erase(jt);
    }
}

bool TSourceIdStorage::DropOldSourceIds(TEvKeyValue::TEvRequest* request, TInstant now, ui64 startOffset, ui32 partition, const NKikimrPQ::TPartitionConfig& config) {
    TVector<std::pair<ui64, TString>> toDelOffsets;

    ui64 maxDeleteSourceIds = 0;
    if (InMemorySourceIds.size() > config.GetSourceIdMaxCounts()) {
        maxDeleteSourceIds = InMemorySourceIds.size() - config.GetSourceIdMaxCounts();
        maxDeleteSourceIds = Min(maxDeleteSourceIds, MAX_DELETE_COMMAND_COUNT);
    }

    Y_VERIFY(maxDeleteSourceIds <= InMemorySourceIds.size());

    const auto ttl = TDuration::Seconds(config.GetSourceIdLifetimeSeconds());
    ui32 size = request->Record.ByteSize();

    for (const auto& [offset, sourceId] : SourceIdsByOffset) {
        if (offset >= startOffset && toDelOffsets.size() >= maxDeleteSourceIds) {
            break;
        }

        auto it = InMemorySourceIds.find(sourceId);
        Y_VERIFY(it != InMemorySourceIds.end());
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
        Y_VERIFY(res == 1);
        // delete sourceID from offsets
        res = SourceIdsByOffset.erase(t);
        Y_VERIFY(res == 1);
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
    Y_VERIFY(key.size() >= TKeyPrefix::MarkedSize());

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
    Y_VERIFY(key.size() >= TKeyPrefix::MarkedSize());
    Y_VERIFY(key[TKeyPrefix::MarkPosition()] == TKeyPrefix::MarkSourceId);

    RegisterSourceIdInfo(key.substr(TKeyPrefix::MarkedSize()), TSourceIdInfo::Parse(data, now), true);
}

void TSourceIdStorage::LoadProtoSourceIdInfo(const TString& key, const TString& data) {
    Y_VERIFY(key.size() >= TKeyPrefix::MarkedSize());
    Y_VERIFY(key[TKeyPrefix::MarkPosition()] == TKeyPrefix::MarkProtoSourceId);

    NKikimrPQ::TMessageGroupInfo proto;
    const bool ok = proto.ParseFromString(data);
    Y_VERIFY(ok);

    RegisterSourceIdInfo(key.substr(TKeyPrefix::MarkedSize()), TSourceIdInfo::Parse(proto), true);
}

void TSourceIdStorage::RegisterSourceIdInfo(const TString& sourceId, TSourceIdInfo&& sourceIdInfo, bool load) {
    auto it = InMemorySourceIds.find(sourceId);
    if (it != InMemorySourceIds.end()) {
        if (!load || it->second.Offset < sourceIdInfo.Offset) {
            const auto res = SourceIdsByOffset.erase(std::make_pair(it->second.Offset, sourceId));
            Y_VERIFY(res == 1);
        } else {
            return;
        }
    }

    const auto offset = sourceIdInfo.Offset;
    InMemorySourceIds[sourceId] = std::move(sourceIdInfo);

    const bool res = SourceIdsByOffset.emplace(offset, sourceId).second;
    Y_VERIFY(res);
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
    if (!SourceIdsByOffset.empty()) {
        auto it = InMemorySourceIds.find(SourceIdsByOffset.begin()->second);
        Y_VERIFY(it != InMemorySourceIds.end());
        ds = Min(ds, it->second.WriteTimestamp);
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

void TSourceIdWriter::FillRequest(TEvKeyValue::TEvRequest* request, ui32 partition) {
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

} // NPQ
} // NKikimr
