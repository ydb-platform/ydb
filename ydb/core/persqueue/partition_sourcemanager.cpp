#include "partition.h"
#include "partition_log.h"

#include <unordered_map>

#include <ydb/core/base/tablet_pipe.h>

namespace NKikimr::NPQ {

IActor* CreateRequester(TActorId parent, ui32 partition, ui64 tabletId, TPartitionSourceManager::TSourceIdsPtr& sourceIds, ui64 cookie);
bool IsResearchRequires(std::optional<const TPartitionGraph::Node*> node);

//
// TPartitionSourceManager
//

TPartitionSourceManager::TPartitionSourceManager(TPartition* partition)
    : Partition(partition) {
}

void TPartitionSourceManager::EnsureSource(const TActorContext& /*ctx*/) {
    if (WaitSources()) {
        return;
    }

    PendingCookies.clear();
    Responses.clear();

    auto node = GetPartitionNode();
    if (!IsResearchRequires(node)) {
        return;
    }

    auto unknowSourceIds = BuildUnknownSourceIds();
    if (unknowSourceIds->empty()) {
        return;
    }

    for(const auto* parent : node.value()->HierarhicalParents) {
        PendingCookies.insert(++Cookie);
        Partition->RegisterWithSameMailbox(CreateRequester(Partition->SelfId(),
                    parent->Id,
                    parent->TabletId,
                    unknowSourceIds,
                    Cookie));
    }
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

    auto its = Sources.find(sourceId);
    if (its != Sources.end()) {
        return its->second;
    }

    TSourceInfo result;
    result.Pending = IsResearchRequires(GetPartitionNode());
    return result;
}

bool TPartitionSourceManager::WaitSources() const {
    return !PendingCookies.empty();
}

TPartitionSourceManager::TModificationBatch TPartitionSourceManager::CreateModificationBatch(const TActorContext& ctx) {
    const auto format = AppData(ctx)->PQConfig.GetEnableProtoSourceIdInfo()
        ? ESourceIdFormat::Proto
        : ESourceIdFormat::Raw;
    return TModificationBatch(this, format);
}

void TPartitionSourceManager::Handle(TEvPQ::TEvSourceIdResponse::TPtr& ev, const TActorContext& ctx) {
    auto it = PendingCookies.find(ev->Cookie);
    if (it == PendingCookies.end()) {
        PQ_LOG_D("Received TEvSourceIdResponse with unknown cookie: " << ev->Cookie);
        return;
    }
    PendingCookies.erase(it);

    if (ev->Get()->Record.HasError()) {
        PQ_LOG_D("Error on request SourceId: " << ev->Get()->Record.GetError());
        Responses.clear();

        EnsureSource(ctx);
        return;
    }

    Responses.push_back(ev);

    if (PendingCookies.empty()) {
        FinishBatch(ctx);
    }
}

TPartitionSourceManager::TPartitionNode TPartitionSourceManager::GetPartitionNode() const {
    return Partition->PartitionGraph.GetPartition(Partition->Partition);
}

TPartitionSourceManager::TSourceIdsPtr TPartitionSourceManager::BuildUnknownSourceIds() const {
    auto& knownSourceIds = GetSourceIdStorage().GetInMemorySourceIds();
    auto unknownSourceIds = std::make_shared<std::set<const TString*>>();

    auto predicate = [&](const TString& sourceId) {
        return !Sources.contains(sourceId) && !knownSourceIds.contains(sourceId);
    };

    for(const auto& msg : Partition->Requests) {
        if (msg.IsWrite()) {
            const TString& sourceId = msg.GetWrite().Msg.SourceId;
            if (predicate(sourceId)) {
                unknownSourceIds->insert(&sourceId);
            }
        }
    }

    for(const auto& ev : Partition->MaxSeqNoRequests) {
        for (const auto& sourceId : ev->Get()->SourceIds) {
            if (predicate(sourceId)) {
                unknownSourceIds->insert(&sourceId);
            }
        }
    }

    return unknownSourceIds;
}

void TPartitionSourceManager::FinishBatch(const TActorContext& ctx) {
    for(const auto& ev : Responses) {
        const auto& record = ev->Get()->Record;
        for(const auto& s : record.GetSource()) {
            auto& value = Sources[s.GetId()];
            if (s.GetState() == NKikimrPQ::TEvSourceIdResponse::EState::TEvSourceIdResponse_EState_Unknown) {
                continue;
            }
            PQ_LOG_T("Received SourceId " << s.GetId() << " SeqNo=" << s.GetSeqNo());
            if (value.State == TSourceIdInfo::EState::Unknown || value.SeqNo < s.GetSeqNo()) {
                value.State = Convert(s.GetState());
                value.SeqNo = s.GetSeqNo();
                value.Offset = s.GetOffset();
                value.Explicit = s.GetExplicit();
                value.WriteTimestamp.FromValue(s.GetWriteTimestamp());
            }
        }
    }

    Responses.clear();

    if (Partition->CurrentStateFunc() == &TPartition::StateIdle) {
        Partition->HandleWrites(ctx);
    }
    Partition->ProcessMaxSeqNoRequest(ctx);
}

TSourceIdStorage& TPartitionSourceManager::GetSourceIdStorage() const {
    return Partition->SourceIdStorage;
}

bool TPartitionSourceManager::HasParents() const {
    auto node = Partition->PartitionGraph.GetPartition(Partition->Partition);
    return node && !node.value()->Parents.empty();
}


//
// TPartitionSourceManager::TModificationBatch
//

TPartitionSourceManager::TModificationBatch::TModificationBatch(TPartitionSourceManager* manager, ESourceIdFormat format)
    : Manager(manager)
    , Node(manager->GetPartitionNode()) 
    , SourceIdWriter(format)
    , HeartbeatEmitter(manager->Partition->SourceIdStorage) {
}

TPartitionSourceManager::TModificationBatch::~TModificationBatch() {
    for(auto& [k, _] : SourceIdWriter.GetSourceIdsToWrite()) {
        Manager->Sources.erase(k);
    }
}

TMaybe<THeartbeat> TPartitionSourceManager::TModificationBatch::CanEmit() const {
    return HeartbeatEmitter.CanEmit();
}

TPartitionSourceManager::TSourceManager TPartitionSourceManager::TModificationBatch::GetSource(const TString& id) {
    return TPartitionSourceManager::TSourceManager(this, id);
}

void TPartitionSourceManager::TModificationBatch::Cancel() {
    return SourceIdWriter.Clear();
}

bool TPartitionSourceManager::TModificationBatch::HasModifications() const {
    return !SourceIdWriter.GetSourceIdsToWrite().empty();
}

void TPartitionSourceManager::TModificationBatch::FillRequest(TEvKeyValue::TEvRequest* request) {
    SourceIdWriter.FillRequest(request, Manager->Partition->Partition);
}

void TPartitionSourceManager::TModificationBatch::DeregisterSourceId(const TString& sourceId) {
    SourceIdWriter.DeregisterSourceId(sourceId);
}

TPartitionSourceManager* TPartitionSourceManager::TModificationBatch::GetManager() const {
    return Manager;
}


//
// TPartitionSourceManager::TSourceManager
//

TPartitionSourceManager::TSourceInfo Convert(TSourceIdInfo value) {
    TPartitionSourceManager::TSourceInfo result(value.State);
    result.SeqNo = value.SeqNo;
    result.Offset = value.Offset;
    result.Explicit = value.Explicit;
    result.WriteTimestamp = value.WriteTimestamp;
    return result;
}

TPartitionSourceManager::TSourceManager::TSourceManager(TModificationBatch* batch, const TString& id)
    : Batch(batch)
    , SourceId(id) {
    auto& memory = MemoryStorage();
    auto& writer = WriteStorage();
    auto& sources = batch->GetManager()->Sources;

    InMemory = memory.find(id);
    InWriter = writer.find(id);
    InSources = sources.end();

    if (InWriter != writer.end()) {
        Info = Convert(InWriter->second);
        return;
    }
    if (InMemory != memory.end()) {
        Info = Convert(InMemory->second);
        return;
    }

    InSources = sources.find(id);
    if (InSources != sources.end()) {
        Info = InSources->second;
        return;
    }

    Info.Pending = IsResearchRequires(batch->Node);
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
        Batch->SourceIdWriter.RegisterSourceId(SourceId, seqNo, offset, timestamp);
    } else {
        Batch->SourceIdWriter.RegisterSourceId(SourceId, InMemory->second.Updated(seqNo, offset, timestamp));
    }
}

void TPartitionSourceManager::TSourceManager::Update(ui64 seqNo, ui64 offset, TInstant timestamp, THeartbeat&& heartbeat) {
    Batch->HeartbeatEmitter.Process(SourceId, heartbeat);
    if (InMemory == MemoryStorage().end()) {
        Batch->SourceIdWriter.RegisterSourceId(SourceId, seqNo, offset, timestamp, heartbeat);
    } else {
        Batch->SourceIdWriter.RegisterSourceId(SourceId, InMemory->second.Updated(seqNo, offset, timestamp, std::move(heartbeat)));
    }
}

TPartitionSourceManager::TSourceManager::operator bool() const {
    return Info;
}

const TSourceIdMap& TPartitionSourceManager::TSourceManager::MemoryStorage() const {
    return Batch->GetManager()->GetSourceIdStorage().GetInMemorySourceIds();
}

const TSourceIdMap& TPartitionSourceManager::TSourceManager::WriteStorage() const {
    return Batch->SourceIdWriter.GetSourceIdsToWrite();
}


//
// TSourceInfo
//

TPartitionSourceManager::TSourceInfo::TSourceInfo(TSourceIdInfo::EState state)
    : State(state) {
}

TPartitionSourceManager::TSourceInfo::operator bool() const {
    return !Pending;
}


//
// TSourceIdRequester
//

class TSourceIdRequester : public TActorBootstrapped<TSourceIdRequester> {
    static constexpr TDuration RetryDelay = TDuration::MilliSeconds(100);
public:
    TSourceIdRequester(TActorId parent, ui32 partition, ui64 tabletId, TPartitionSourceManager::TSourceIdsPtr& sourceIds, ui64 cookie)
        : Parent(parent)
        , Partition(partition)
        , TabletId(tabletId)
        , SourceIds(sourceIds)
        , Cookie(cookie) {
    }

    void Bootstrap(const TActorContext&) {
        Become(&TThis::StateWork);
        MakePipe();
    }

    void PassAway() override {
        if (PipeClient) {
            NTabletPipe::CloseAndForgetClient(SelfId(), PipeClient);
        }
        TActorBootstrapped::PassAway();
    }

private:
    void MakePipe() {
        NTabletPipe::TClientConfig config;
        config.RetryPolicy = {
            .RetryLimitCount = 6,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(200),
            .BackoffMultiplier = 2,
            .DoFirstRetryInstantly = true
        };
        PipeClient = RegisterWithSameMailbox(NTabletPipe::CreateClient(SelfId(), TabletId, config));
    }

    std::unique_ptr<TEvPQ::TEvSourceIdRequest> CreateRequest() {
        auto request = std::make_unique<TEvPQ::TEvSourceIdRequest>();
        auto& record = request->Record;
        record.SetPartition(Partition);
        for(const auto& sourceId : *SourceIds) {
            record.AddSourceId(*sourceId);
        }
        return request;
    }

    void Handle(TEvPQ::TEvSourceIdResponse::TPtr& ev, const TActorContext& /*ctx*/) {
        auto msg = std::make_unique<TEvPQ::TEvSourceIdResponse>();
        msg->Record = std::move(ev->Get()->Record);

        Reply(msg);
        PassAway();
    }

    void DoRequest() {
        NTabletPipe::SendData(SelfId(), PipeClient, CreateRequest().release());
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status == NKikimrProto::EReplyStatus::OK) {
            DoRequest();
        } else {
            ReplyWithError("Error connecting to PQ");
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->TabletId == TabletId) {
            ReplyWithError("Pipe destroyed");
        }
    }

    void ReplyWithError(const TString& error) {
        auto msg = std::make_unique<TEvPQ::TEvSourceIdResponse>();
        msg->Record.SetError(error);

        Reply(msg);

        PassAway();
    }

    void Reply(std::unique_ptr<TEvPQ::TEvSourceIdResponse>& msg) {
        Send(Parent, msg.release(), 0, Cookie);
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPQ::TEvSourceIdResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    TActorId Parent;
    ui32 Partition;
    ui64 TabletId;
    TPartitionSourceManager::TSourceIdsPtr SourceIds;
    ui64 Cookie;

    TActorId PipeClient;
};

IActor* CreateRequester(TActorId parent, ui32 partition, ui64 tabletId, TPartitionSourceManager::TSourceIdsPtr& sourceIds, ui64 cookie) {
    return new TSourceIdRequester(parent, partition, tabletId, sourceIds, cookie);
}

bool IsResearchRequires(std::optional<const TPartitionGraph::Node*> node)  {
    return node && !node.value()->Parents.empty();
}

NKikimrPQ::TEvSourceIdResponse::EState Convert(TSourceIdInfo::EState value) {
    switch(value) {
        case TSourceIdInfo::EState::Unknown:
            return NKikimrPQ::TEvSourceIdResponse::EState::TEvSourceIdResponse_EState_Unknown;
        case TSourceIdInfo::EState::Registered:
            return NKikimrPQ::TEvSourceIdResponse::EState::TEvSourceIdResponse_EState_Registered;
        case TSourceIdInfo::EState::PendingRegistration:
            return NKikimrPQ::TEvSourceIdResponse::EState::TEvSourceIdResponse_EState_PendingRegistration;
    }
}

TSourceIdInfo::EState Convert(NKikimrPQ::TEvSourceIdResponse::EState value) {
    switch(value) {
        case NKikimrPQ::TEvSourceIdResponse::EState::TEvSourceIdResponse_EState_Unknown:
            return TSourceIdInfo::EState::Unknown;
        case NKikimrPQ::TEvSourceIdResponse::EState::TEvSourceIdResponse_EState_Registered:
            return TSourceIdInfo::EState::Registered;
        case NKikimrPQ::TEvSourceIdResponse::EState::TEvSourceIdResponse_EState_PendingRegistration:
            return TSourceIdInfo::EState::PendingRegistration;
    }
}



} // namespace NKikimr::NPQ
