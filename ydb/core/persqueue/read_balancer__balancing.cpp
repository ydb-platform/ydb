#include "read_balancer.h"


namespace NKikimr::NPQ {

//
// TPartitionFamilty
//

TPersQueueReadBalancer::TPartitionFamilty::TPartitionFamilty(TBalancingConsumerInfo& consumerInfo, size_t id, std::vector<ui32>&& partitions)
    : ConsumerInfo(consumerInfo)
    , Id(id)
    , Status(EStatus::Free)
    , Partitions(std::move(partitions))
    , Session(nullptr)
{
    auto [activePartitionCount, inactivePartitionCount] = ClassifyPartitions(Partitions);
    ActivePartitionCount = activePartitionCount;
    InactivePartitionCount = inactivePartitionCount;

    UpdatePartitionMapping(Partitions);
}

const TString& TPersQueueReadBalancer::TPartitionFamilty::Topic() const {
    return ConsumerInfo.Topic();
}

const TString& TPersQueueReadBalancer::TPartitionFamilty::Path() const {
    return ConsumerInfo.Path();
}

ui32 TPersQueueReadBalancer::TPartitionFamilty::TabletGeneration() const {
    return ConsumerInfo.TabletGeneration();
}

const TPersQueueReadBalancer::TPartitionInfo& TPersQueueReadBalancer::TPartitionFamilty::GetPartitionInfo(ui32 partitionId) const {
    return ConsumerInfo.GetPartitionInfo(partitionId);
}

ui32 TPersQueueReadBalancer::TPartitionFamilty::NextStep() {
    return ConsumerInfo.NextStep();
}


void TPersQueueReadBalancer::TPartitionFamilty::Release(const TActorContext& ctx) {
    if (Status != EStatus::Active) {
        // TODO error. должны освобождать только активные семейства
        return;
    }

    if (!Session) {
        // TODO error. Не должно быть заблоченных партиции
        return;
    }

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "client " << Session->ClientId << " release partitions [" << JoinRange(", ", LockedPartitions.begin(), LockedPartitions.end())
            << "] for pipe " << Session->Sender << " session " << Session->Session);

    Status = EStatus::Releasing;

    Session->ActivePartitionCount -= ActivePartitionCount;
    Session->InactivePartitionCount -= InactivePartitionCount;

    for (auto partitionId : LockedPartitions) {
        ctx.Send(Session->Sender, MakeEvReleasePartition(partitionId).release());
    }

}

bool TPersQueueReadBalancer::TPartitionFamilty::Unlock(const TActorId& sender, ui32 partitionId, const TActorContext&) {
    if (Status != EStatus::Releasing) {
        // TODO error.
        return false;
    }

    if (!Session || Session->Sender != sender) {
        // TODO error. Не должно быть заблоченных партиции
        return false;
    }

    if (!LockedPartitions.erase(partitionId)) {
        // TODO освободили ранее не залоченную партицию
        return false;
    }

    if (!LockedPartitions.empty()) {
        return false;
    }

    Status = EStatus::Free;
    Session = nullptr;

    return true;
}

void TPersQueueReadBalancer::TPartitionFamilty::StartReading(TPersQueueReadBalancer::TReadingSession& session, const TActorContext& ctx) {
    if (Status != EStatus::Free) {
        // TODO error.
        return;
    }

    Status = EStatus::Active;
    Session = &session;

    Session->ActivePartitionCount += ActivePartitionCount;
    Session->InactivePartitionCount += InactivePartitionCount;

    for (auto partitionId : Partitions) {
        ctx.Send(Session->Sender, MakeEvLockPartition(partitionId, NextStep()).release());
    }

    LockedPartitions.insert(Partitions.begin(), Partitions.end());
}

void TPersQueueReadBalancer::TPartitionFamilty::AddPartitions(const std::vector<ui32>& partitions, const TActorContext& ctx) {
    auto [activePartitionCount, inactivePartitionCount] = ClassifyPartitions(partitions);

    ActivePartitionCount += activePartitionCount;
    InactivePartitionCount += inactivePartitionCount;

    Partitions.insert(Partitions.end(), partitions.begin(), partitions.end());
    UpdatePartitionMapping(partitions);

    if (Status == EStatus::Active) {
        if (!Session->AllPartitionsReadable(Partitions)) {
            // TODO не надо добавлятьпартиции если текущая сессия не может читать новое семейство. Ждем коммита.
            Release(ctx);
            return;
        }

        Session->ActivePartitionCount += activePartitionCount;
        Session->InactivePartitionCount += inactivePartitionCount;

        for (auto partitionId : partitions) {
            ctx.Send(Session->Sender, MakeEvLockPartition(partitionId, NextStep()).release());
        }

        LockedPartitions.insert(partitions.begin(), partitions.end());
    }

    for (auto it = SpecialSessions.begin(); it != SpecialSessions.end();) {
        auto& session = it->second;
        if (session->AllPartitionsReadable(Partitions)) {
            ++it;
        } else {
            it = SpecialSessions.erase(it);
        }
    }
}

std::pair<size_t, size_t> TPersQueueReadBalancer::TPartitionFamilty::ClassifyPartitions(const std::vector<ui32>& partitions) {
    size_t activePartitionCount = 0;
    size_t inactivePartitionCount = 0;

    for (auto partitionId : partitions) {
        auto* partitionStatus = GetPartitionStatus(partitionId);
        if (IsReadeable(partitionId)) {
            if (partitionStatus && partitionStatus->IsFinished()) {
                ++inactivePartitionCount;
            } else {
                ++activePartitionCount;
            }
        } else {
            // TODO Family with unreadable partition
        }
    }

    return {activePartitionCount, inactivePartitionCount};
}

void TPersQueueReadBalancer::TPartitionFamilty::UpdatePartitionMapping(const std::vector<ui32>& partitions) {
    for (auto partitionId: partitions) {
        ConsumerInfo.PartitionMapping[partitionId] = this;
    }
}

std::unique_ptr<TEvPersQueue::TEvReleasePartition> TPersQueueReadBalancer::TPartitionFamilty::MakeEvReleasePartition(ui32 partitionId) const {
    auto res = std::make_unique<TEvPersQueue::TEvReleasePartition>();
    auto& r = res->Record;

    r.SetSession(Session->Session);
    r.SetTopic(Topic());
    r.SetPath(Path());
    r.SetGeneration(TabletGeneration());
    r.SetClientId(Session->ClientId);
    //if (count) { TODO always 1 or 0
    //    r.SetCount(1);
    //}
    r.SetGroup(partitionId + 1);
    ActorIdToProto(Session->Sender, r.MutablePipeClient());

    return res;
}

std::unique_ptr<TEvPersQueue::TEvLockPartition> TPersQueueReadBalancer::TPartitionFamilty::MakeEvLockPartition(ui32 partitionId, ui32 step) const {
    auto res = std::make_unique<TEvPersQueue::TEvLockPartition>();
    auto& r = res->Record;

    r.SetSession(Session->Session);
    r.SetPartition(partitionId);
    r.SetTopic(Topic());
    r.SetPath(Path());
    r.SetGeneration(TabletGeneration());
    r.SetStep(step);
    r.SetClientId(Session->ClientId);
    ActorIdToProto(Session->Sender, res->Record.MutablePipeClient());
    r.SetTabletId(GetPartitionInfo(partitionId).TabletId);

    return res;
}


//
// TBalancingConsumerInfo
//

TPersQueueReadBalancer::TBalancingConsumerInfo::TBalancingConsumerInfo(TPersQueueReadBalancer& balancer)
    : Balancer(balancer)
    , NextFamilyId(0)
    , Step(0)
{}

const TString& TPersQueueReadBalancer::TBalancingConsumerInfo::Topic() const {
    return Balancer.Topic;
}

const TString& TPersQueueReadBalancer::TBalancingConsumerInfo::Path() const {
    return Balancer.Path;
}

ui32 TPersQueueReadBalancer::TBalancingConsumerInfo::TabletGeneration() const {
    return Balancer.Generation;
}

const TPersQueueReadBalancer::TPartitionInfo& TPersQueueReadBalancer::TBalancingConsumerInfo::GetPartitionInfo(ui32 partitionId) const {
    auto it = Balancer.PartitionsInfo.find(partitionId);
    if (it == Balancer.PartitionsInfo.end()) {
        return ; // TODO
    }
    return it->second;
}

TPersQueueReadBalancer::TReadingPartitionStatus* TPersQueueReadBalancer::TBalancingConsumerInfo::GetPartitionStatus(ui32 partitionId) {
    auto it = Partitions.find(partitionId);
    if (it == Partitions.end()) {
        return nullptr;
    }
    return &it->second;
}

ui32 TPersQueueReadBalancer::TBalancingConsumerInfo::NextStep() {
    return ++Step;
}

void TPersQueueReadBalancer::TBalancingConsumerInfo::CreateFamily(std::vector<ui32>&& partitions) {
    auto family = std::make_unique<TPersQueueReadBalancer::TPartitionFamilty>(*this, ++NextFamilyId, std::move(partitions));

    for (auto& [_, readingSession] : ReadingSessions) {
        if (readingSession->WithGroups() && readingSession->AllPartitionsReadable(family->Partitions)) {
            family->SpecialSessions[readingSession->Sender] = readingSession;
        }
    }

    Families[family->Id] = std::move(family);
}

TPersQueueReadBalancer::TReadingPartitionStatus* TPersQueueReadBalancer::TBalancingConsumerInfo::GetPartitionStatus(ui32 partitionId) {
    auto it = Partitions.find(partitionId);
    if (it == Partitions.end()) {
        return nullptr;
    }
    return &it->second;
}

bool TPersQueueReadBalancer::TBalancingConsumerInfo::IsReadeable(ui32 partitionId) const {
    if (!ScalingSupport()) {
        return true;
    }

    auto* node = Balancer.PartitionGraph.GetPartition(partitionId);
    if (!node) {
        return false;
    }

    if (Partitions.empty()) {
        return node->Parents.empty();
    }

    for(auto* parent : node->HierarhicalParents) {
        if (!IsFinished(parent->Id)) {
            return false;
        }
    }

    return true;
}


//
// TReadingSession
//

TPersQueueReadBalancer::TReadingSession::TReadingSession()
            : ServerActors(0)
            , ActivePartitionCount(0)
            , InactivePartitionCount(0)
        {}

void TPersQueueReadBalancer::TReadingSession::Init(const TString& clientId, const TString& session, const TActorId& sender, const std::vector<ui32>& groups) {
    ClientId = clientId;
    Session = session;
    Sender = sender;
    Groups.insert(groups.begin(), groups.end());
}

bool TPersQueueReadBalancer::TReadingSession::WithGroups() const { return !Groups.empty(); }

bool TPersQueueReadBalancer::TReadingSession::AllPartitionsReadable(const std::vector<ui32>& partitions) const {
    if (WithGroups()) {
        for (auto p : partitions) {
            if (!Groups.contains(p + 1)) {
                return false;
            }
        }
    }

    return true;
}

}
