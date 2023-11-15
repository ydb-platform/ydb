#include "partition.h"
#include "partition_log.h"

#include <unordered_map>

#include <ydb/core/base/tablet_pipe.h>

namespace NKikimr::NPQ {

IActor* CreateRequester(TActorId parent, TPartitionSourceManager::TPartitionId partition, ui64 tabletId);
bool IsResearchRequires(std::optional<const TPartitionGraph::Node*> node);

//
// TPartitionSourceManager
//

TPartitionSourceManager::TPartitionSourceManager(TPartition* partition)
    : Partition(partition) {
}

void TPartitionSourceManager::ScheduleBatch() {
    if (WaitSources()) {
        return;
    }

    PendingCookies.clear();
    Responses.clear();

    if (UnknownSourceIds.empty()) {
        return;
    }

    auto node = GetPartitionNode();
    if (!IsResearchRequires(node)) {
        return;
    }

    PendingSourceIds = std::move(UnknownSourceIds);

    for(const auto* parent : node.value()->HierarhicalParents) {
        PendingCookies.insert(++Cookie);

        TActorId actorId = PartitionRequester(parent->Id, parent->TabletId);
        Partition->Send(actorId, CreateRequest(parent->Id).release(), 0, Cookie);
    }
}

void TPartitionSourceManager::EnsureSourceId(const TString& sourceId) {
    if (!IsResearchRequires(GetPartitionNode())) {
        return;
    }

    if (RequireEnqueue(sourceId)) {
        UnknownSourceIds.insert(sourceId);
    }

    ScheduleBatch();
}

void TPartitionSourceManager::EnsureSourceIds(const TVector<TString>& sourceIds) {
    if (!IsResearchRequires(GetPartitionNode())) {
        return;
    }

    for(const auto& sourceId : sourceIds) {
        if (RequireEnqueue(sourceId)) {
            UnknownSourceIds.insert(sourceId);
        }
    }

    ScheduleBatch();
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
        RequesterActors.erase(ev->Get()->Record.GetPartition());

        if (UnknownSourceIds.empty()) {
            UnknownSourceIds = std::move(PendingSourceIds);
        } else {
            UnknownSourceIds.insert(PendingSourceIds.begin(), PendingSourceIds.end());
            PendingSourceIds.clear();
        }

        ScheduleBatch();
        return;
    }

    Responses.push_back(ev);

    if (PendingCookies.empty()) {
        FinishBatch(ctx);
        ScheduleBatch();
    }
}

TPartitionSourceManager::TPartitionNode TPartitionSourceManager::GetPartitionNode() const {
    return Partition->PartitionGraph.GetPartition(Partition->Partition);
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
    PendingSourceIds.clear();

    if (Partition->CurrentStateFunc() == &TPartition::StateIdle) {
        Partition->HandleWrites(ctx);
    }
    Partition->ProcessMaxSeqNoRequest(ctx);
}

bool TPartitionSourceManager::RequireEnqueue(const TString& sourceId) {
    auto& knownSourceIds = GetSourceIdStorage().GetInMemorySourceIds();
    return !Sources.contains(sourceId) && !knownSourceIds.contains(sourceId) 
        && !PendingSourceIds.contains(sourceId);
}

TSourceIdStorage& TPartitionSourceManager::GetSourceIdStorage() const {
    return Partition->SourceIdStorage;
}

bool TPartitionSourceManager::HasParents() const {
    auto node = Partition->PartitionGraph.GetPartition(Partition->Partition);
    return node && !node.value()->Parents.empty();
}

TActorId TPartitionSourceManager::PartitionRequester(TPartitionId id, ui64 tabletId) {
    auto it = RequesterActors.find(id);
    if (it != RequesterActors.end()) {
        return it->second;
    }

    TActorId actorId = Partition->RegisterWithSameMailbox(CreateRequester(Partition->SelfId(),
                    id,
                    tabletId));
    RequesterActors[id] = actorId;
    return actorId;
}

std::unique_ptr<TEvPQ::TEvSourceIdRequest> TPartitionSourceManager::CreateRequest(TPartitionSourceManager::TPartitionId id) const {
    auto request = std::make_unique<TEvPQ::TEvSourceIdRequest>();
    auto& record = request->Record;
    record.SetPartition(id);
    for(const auto& sourceId : PendingSourceIds) {
        record.AddSourceId(sourceId);
    }
    return request;
}

void TPartitionSourceManager::PassAway() {
    for(const auto [_, actorId] : RequesterActors) {
        Partition->Send(actorId, new TEvents::TEvPoison());
    }
    RequesterActors.clear();
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
    TSourceIdRequester(TActorId parent, TPartitionSourceManager::TPartitionId partition, ui64 tabletId)
        : Parent(parent)
        , Partition(partition)
        , TabletId(tabletId) {
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

    void Handle(TEvPQ::TEvSourceIdRequest::TPtr& ev, const TActorContext& /*ctx*/) {
        Cookie = ev->Cookie;
        PendingRequest = ev;

        DoRequest();
    }

    void Handle(TEvPQ::TEvSourceIdResponse::TPtr& ev, const TActorContext& /*ctx*/) {
        auto msg = std::make_unique<TEvPQ::TEvSourceIdResponse>();
        msg->Record = std::move(ev->Get()->Record);
        bool hasError = msg->Record.HasError();

        Reply(msg);

        if (hasError) {
            PassAway();
        }
    }

    void DoRequest() {
        if (PendingRequest && PipeConnected) {
            auto msg = std::make_unique<TEvPQ::TEvSourceIdRequest>();
            msg->Record = std::move(PendingRequest->Get()->Record);
            PendingRequest.Reset();

            NTabletPipe::SendData(SelfId(), PipeClient, msg.release());
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status == NKikimrProto::EReplyStatus::OK) {
            PipeConnected = true;
            DoRequest();
        } else {
            ReplyWithError("Error connecting to PQ");
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->TabletId == TabletId) {
            PipeConnected = false;
            ReplyWithError("Pipe destroyed");
        }
    }

    void ReplyWithError(const TString& error) {
        auto msg = std::make_unique<TEvPQ::TEvSourceIdResponse>();
        msg->Record.SetPartition(Partition);
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
            HFunc(TEvPQ::TEvSourceIdRequest, Handle);
            HFunc(TEvPQ::TEvSourceIdResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    TActorId Parent;
    TPartitionSourceManager::TPartitionId Partition;
    ui64 TabletId;
    TEvPQ::TEvSourceIdRequest::TPtr PendingRequest;
    ui64 Cookie;

    TActorId PipeClient;
    bool PipeConnected = false;
};

IActor* CreateRequester(TActorId parent, TPartitionSourceManager::TPartitionId partition, ui64 tabletId) {
    return new TSourceIdRequester(parent, partition, tabletId);
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
