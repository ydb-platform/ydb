#include "partition.h"

#include <ydb/core/base/tablet_pipe.h>

namespace NKikimr::NPQ {

//
// TPartitionSourceManager
//

TPartitionSourceManager::TPartitionSourceManager(TPartition& partition)
    : Partition(partition) {
}


const TPartitionSourceManager::TSourceInfo TPartitionSourceManager::Get(const TString& sourceId) const {
    auto& knownSourceIds = GetSourceIdStorage().GetInMemorySourceIds();
    auto itk = knownSourceIds.find(sourceId);
    if (itk != knownSourceIds.end()) {
        auto& value = itk->second;

        TSourceInfo result;
        result.State = value.State;
        result.SeqNo = value.SeqNo;
        result.Offset = value.Offset;
        result.Explicit = value.Explicit;
        result.WriteTimestamp = value.WriteTimestamp;

        return result;
    }

    return TSourceInfo{};
}


TPartitionSourceManager::TModificationBatch TPartitionSourceManager::CreateModificationBatch(const TActorContext& ctx) {
    const auto format = AppData(ctx)->PQConfig.GetEnableProtoSourceIdInfo()
        ? ESourceIdFormat::Proto
        : ESourceIdFormat::Raw;
    return TModificationBatch(*this, format);
}

const TPartitionSourceManager::TPartitionNode* TPartitionSourceManager::GetPartitionNode() const {
    return Partition.PartitionGraph.GetPartition(Partition.Partition.OriginalPartitionId);
}

TSourceIdStorage& TPartitionSourceManager::GetSourceIdStorage() const {
    return Partition.SourceIdStorage;
}

bool TPartitionSourceManager::HasParents() const {
    auto node = GetPartitionNode();
    return node && !node->Parents.empty();
}


//
// TPartitionSourceManager::TModificationBatch
//

TPartitionSourceManager::TModificationBatch::TModificationBatch(TPartitionSourceManager& manager, ESourceIdFormat format)
    : Manager(manager)
    , Node(Manager.GetPartitionNode())
    , SourceIdWriter(format)
    , HeartbeatEmitter(Manager.Partition.SourceIdStorage) {
}

TPartitionSourceManager::TModificationBatch::~TModificationBatch() {
}

TMaybe<THeartbeat> TPartitionSourceManager::TModificationBatch::CanEmitHeartbeat() const {
    return HeartbeatEmitter.CanEmit();
}

TPartitionSourceManager::TSourceManager TPartitionSourceManager::TModificationBatch::GetSource(const TString& id) {
    return TPartitionSourceManager::TSourceManager(*this, id);
}

void TPartitionSourceManager::TModificationBatch::Cancel() {
    return SourceIdWriter.Clear();
}

bool TPartitionSourceManager::TModificationBatch::HasModifications() const {
    return !SourceIdWriter.GetSourceIdsToWrite().empty()
        || !SourceIdWriter.GetSourceIdsToDelete().empty();
}

void TPartitionSourceManager::TModificationBatch::FillRequest(TEvKeyValue::TEvRequest* request) {
    SourceIdWriter.FillRequest(request, Manager.Partition.Partition);
}

void TPartitionSourceManager::TModificationBatch::DeregisterSourceId(const TString& sourceId) {
    SourceIdWriter.DeregisterSourceId(sourceId);
}

TPartitionSourceManager& TPartitionSourceManager::TModificationBatch::GetManager() const {
    return Manager;
}


//
// TPartitionSourceManager::TSourceManager
//

TPartitionSourceManager::TSourceInfo Convert(TSourceIdInfo value) {
    TPartitionSourceManager::TSourceInfo result(value.State);
    result.SeqNo = value.SeqNo;
    result.MinSeqNo = value.MinSeqNo;
    result.Offset = value.Offset;
    result.Explicit = value.Explicit;
    result.WriteTimestamp = value.WriteTimestamp;
    return result;
}

TPartitionSourceManager::TSourceManager::TSourceManager(TModificationBatch& batch, const TString& id)
    : Batch(batch)
    , SourceId(id) {
    auto& memory = MemoryStorage();
    auto& writer = WriteStorage();

    InMemory = memory.find(id);
    InWriter = writer.find(id);

    if (InWriter != writer.end()) {
        Info = Convert(InWriter->second);
        return;
    }
    if (InMemory != memory.end()) {
        Info = Convert(InMemory->second);
        return;
    }
}

std::optional<ui64> TPartitionSourceManager::TSourceManager::SeqNo() const {
    return Info.State == TSourceIdInfo::EState::Unknown ? std::nullopt : std::optional(Info.SeqNo);
}

bool TPartitionSourceManager::TSourceManager::Explicit() const {
    return Info.Explicit;
}

std::optional<ui64> TPartitionSourceManager::TSourceManager::CommittedSeqNo() const {
    return InMemory == MemoryStorage().end() ? std::nullopt : std::optional(InMemory->second.SeqNo);
}

std::optional<ui64> TPartitionSourceManager::TSourceManager::UpdatedSeqNo() const {
    return InWriter == WriteStorage().end() ? std::nullopt : std::optional(InWriter->second.SeqNo);
}

void TPartitionSourceManager::TSourceManager::Update(ui64 seqNo, ui64 offset, TInstant timestamp) {
    if (InMemory == MemoryStorage().end()) {
        Batch.SourceIdWriter.RegisterSourceId(SourceId, seqNo, offset, timestamp);
    } else {
        Batch.SourceIdWriter.RegisterSourceId(SourceId, InMemory->second.Updated(seqNo, offset, timestamp));
    }
}

void TPartitionSourceManager::TSourceManager::Update(THeartbeat&& heartbeat) {
    Batch.HeartbeatEmitter.Process(SourceId, std::move(heartbeat));
}


const TSourceIdMap& TPartitionSourceManager::TSourceManager::MemoryStorage() const {
    return Batch.GetManager().GetSourceIdStorage().GetInMemorySourceIds();
}

const TSourceIdMap& TPartitionSourceManager::TSourceManager::WriteStorage() const {
    return Batch.SourceIdWriter.GetSourceIdsToWrite();
}


//
// TSourceInfo
//

TPartitionSourceManager::TSourceInfo::TSourceInfo(TSourceIdInfo::EState state)
    : State(state) {
}

} // namespace NKikimr::NPQ
