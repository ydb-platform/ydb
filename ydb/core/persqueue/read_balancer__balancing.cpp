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
    UpdateSpecialSessions();
}

const TString& TPersQueueReadBalancer::TPartitionFamilty::Topic() const {
    return ConsumerInfo.Topic();
}

const TString& TPersQueueReadBalancer::TPartitionFamilty::TopicPath() const {
    return ConsumerInfo.TopicPath();
}

ui32 TPersQueueReadBalancer::TPartitionFamilty::TabletGeneration() const {
    return ConsumerInfo.TabletGeneration();
}

const TPersQueueReadBalancer::TPartitionInfo& TPersQueueReadBalancer::TPartitionFamilty::GetPartitionInfo(ui32 partitionId) const {
    return ConsumerInfo.GetPartitionInfo(partitionId);
}
bool TPersQueueReadBalancer::TPartitionFamilty::IsReadable(ui32 partitionId) const {
    return ConsumerInfo.IsReadable(partitionId);
}

ui32 TPersQueueReadBalancer::TPartitionFamilty::NextStep() {
    return ConsumerInfo.NextStep();
}

TString TPersQueueReadBalancer::TPartitionFamilty::GetPrefix() const {
    return TStringBuilder() << "partitions family " << Id << " ";
}


void TPersQueueReadBalancer::TPartitionFamilty::Release(const TActorContext& ctx, EStatus targetStatus) {
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
    TargetStatus = targetStatus;

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

    Reset();

    return true;
}

void TPersQueueReadBalancer::TPartitionFamilty::Reset() {
    Status = TargetStatus;

    Session->Families.erase(this);
    Session = nullptr;

    if (Status == EStatus::Destroyed) {
        ConsumerInfo.Families.erase(Id);
        return;
    }

    if (!AttachedPartitions.empty()) {
        auto [activePartitionCount, inactivePartitionCount] = ClassifyPartitions(AttachedPartitions);
        ActivePartitionCount -= activePartitionCount;
        InactivePartitionCount -= inactivePartitionCount;

        // The attached partitions are always at the end of the list.
        Partitions.resize(Partitions.size() - AttachedPartitions.size());
        for (auto partitionId : AttachedPartitions) {
            ConsumerInfo.PartitionMapping.erase(partitionId);
        }
        AttachedPartitions.clear();

        // After reducing the number of partitions in the family, the list of reading sessions that can read this family may expand.
        UpdateSpecialSessions();
    }
}

void TPersQueueReadBalancer::TPartitionFamilty::StartReading(TPersQueueReadBalancer::TReadingSession& session, const TActorContext& ctx) {
    if (Status != EStatus::Free) {
        // TODO error.
        return;
    }

    Status = EStatus::Active;

    Session = &session;
    Session->Families.insert(this);

    Session->ActivePartitionCount += ActivePartitionCount;
    Session->InactivePartitionCount += InactivePartitionCount;

    for (auto partitionId : Partitions) {
        LockPartition(partitionId, ctx);
    }

    LockedPartitions.insert(Partitions.begin(), Partitions.end());
}

void TPersQueueReadBalancer::TPartitionFamilty::AttachePartitions(const std::vector<ui32>& partitions, const TActorContext& ctx) {
    auto [activePartitionCount, inactivePartitionCount] = ClassifyPartitions(partitions);

    if (Session) {
        Session->Families.erase(this);
    }

    ActivePartitionCount += activePartitionCount;
    InactivePartitionCount += inactivePartitionCount;

    if (Session) {
        // Reordering Session->Families
        Session->Families.insert(this);
    }

    Partitions.insert(Partitions.end(), partitions.begin(), partitions.end());
    UpdatePartitionMapping(partitions);

    AttachedPartitions.insert(partitions.begin(), partitions.end());

    if (Status == EStatus::Active) {
        if (!Session->AllPartitionsReadable(Partitions)) {
            // TODO не надо добавлятьпартиции если текущая сессия не может читать новое семейство. Ждем коммита.
            Release(ctx);
            return;
        }

        Session->ActivePartitionCount += activePartitionCount;
        Session->InactivePartitionCount += inactivePartitionCount;

        for (auto partitionId : partitions) {
            LockPartition(partitionId, ctx);
        }

        LockedPartitions.insert(partitions.begin(), partitions.end());
    }

    // Removing sessions wich can't read the family now
    for (auto it = SpecialSessions.begin(); it != SpecialSessions.end();) {
        auto& session = it->second;
        if (session->AllPartitionsReadable(partitions)) {
            ++it;
        } else {
            it = SpecialSessions.erase(it);
        }
    }
}

void TPersQueueReadBalancer::TPartitionFamilty::ActivatePartition(ui32 partitionId) {
    Y_UNUSED(partitionId);

    ++ActivePartitionCount;
    --InactivePartitionCount;

    if (Status == EStatus::Active) {
        ++Session->ActivePartitionCount;
        --Session->InactivePartitionCount;
    }
}

void TPersQueueReadBalancer::TPartitionFamilty::InactivatePartition(ui32 partitionId) {
    Y_UNUSED(partitionId);

    --ActivePartitionCount;
    ++InactivePartitionCount;

    if (Status == EStatus::Active) {
        --Session->ActivePartitionCount;
        ++Session->InactivePartitionCount;
    }
}

TString TPersQueueReadBalancer::TPartitionFamilty::DebugStr() const {
    return TStringBuilder() << "family=" << Id << " (Status=" << Status << ", Partitions=[" << JoinRange(", ", Partitions.begin(), Partitions.end()) << "])";
}


TPersQueueReadBalancer::TReadingPartitionStatus* TPersQueueReadBalancer::TPartitionFamilty::GetPartitionStatus(ui32 partitionId) {
    return ConsumerInfo.GetPartitionStatus(partitionId);
}

template<typename TPartitions>
std::pair<size_t, size_t> TPersQueueReadBalancer::TPartitionFamilty::ClassifyPartitions(const TPartitions& partitions) {
    size_t activePartitionCount = 0;
    size_t inactivePartitionCount = 0;

    for (auto partitionId : partitions) {
        auto* partitionStatus = GetPartitionStatus(partitionId);
        if (IsReadable(partitionId)) {
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

template
std::pair<size_t, size_t> TPersQueueReadBalancer::TPartitionFamilty::ClassifyPartitions(const std::set<ui32>& partitions);

template
std::pair<size_t, size_t> TPersQueueReadBalancer::TPartitionFamilty::ClassifyPartitions(const std::vector<ui32>& partitions);

void TPersQueueReadBalancer::TPartitionFamilty::UpdatePartitionMapping(const std::vector<ui32>& partitions) {
    for (auto partitionId: partitions) {
        ConsumerInfo.PartitionMapping[partitionId] = this;
    }
}

void TPersQueueReadBalancer::TPartitionFamilty::UpdateSpecialSessions() {
    for (auto& [_, readingSession] : ConsumerInfo.ReadingSessions) {
        if (readingSession->WithGroups() && readingSession->AllPartitionsReadable(Partitions)) {
            SpecialSessions[readingSession->Sender] = readingSession;
        }
    }
}

void TPersQueueReadBalancer::TPartitionFamilty::LockPartition(ui32 partitionId, const TActorContext& ctx) {
    auto step = NextStep();

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "lock partition for " << Session->Sender
            << " session " << Session->Session << " partition " << partitionId
            << " generation " << TabletGeneration() << " step " << step);

    ctx.Send(Session->Sender, MakeEvLockPartition(partitionId, step).release());
}

std::unique_ptr<TEvPersQueue::TEvReleasePartition> TPersQueueReadBalancer::TPartitionFamilty::MakeEvReleasePartition(ui32 partitionId) const {
    auto res = std::make_unique<TEvPersQueue::TEvReleasePartition>();
    auto& r = res->Record;

    r.SetSession(Session->Session);
    r.SetTopic(Topic());
    r.SetPath(TopicPath());
    r.SetGeneration(TabletGeneration());
    r.SetClientId(Session->ClientId);
    //if (count) { TODO always 1 or 0
    //    r.SetCount(1);
    //}
    r.SetGroup(partitionId + 1);
    ActorIdToProto(Session->PipeClient, r.MutablePipeClient());

    return res;
}

std::unique_ptr<TEvPersQueue::TEvLockPartition> TPersQueueReadBalancer::TPartitionFamilty::MakeEvLockPartition(ui32 partitionId, ui32 step) const {
    auto res = std::make_unique<TEvPersQueue::TEvLockPartition>();
    auto& r = res->Record;

    r.SetSession(Session->Session);
    r.SetPartition(partitionId);
    r.SetTopic(Topic());
    r.SetPath(TopicPath());
    r.SetGeneration(TabletGeneration());
    r.SetStep(step);
    r.SetClientId(Session->ClientId);
    ActorIdToProto(Session->PipeClient, res->Record.MutablePipeClient());
    r.SetTabletId(GetPartitionInfo(partitionId).TabletId);

    return res;
}


//
// TBalancingConsumerInfo
//

TPersQueueReadBalancer::TBalancingConsumerInfo::TBalancingConsumerInfo(TPersQueueReadBalancer& balancer, const TString& consumerName)
    : Balancer(balancer)
    , ConsumerName(consumerName)
    , NextFamilyId(0)
    , Step(0)
{
}

const TString& TPersQueueReadBalancer::TBalancingConsumerInfo::Topic() const {
    return Balancer.Topic;
}

const TString& TPersQueueReadBalancer::TBalancingConsumerInfo::TopicPath() const {
    return Balancer.Path;
}

ui32 TPersQueueReadBalancer::TBalancingConsumerInfo::TabletGeneration() const {
    return Balancer.Generation;
}

const TPersQueueReadBalancer::TPartitionInfo& TPersQueueReadBalancer::TBalancingConsumerInfo::GetPartitionInfo(ui32 partitionId) const {
    return Balancer.PartitionsInfo[partitionId];
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

void TPersQueueReadBalancer::TBalancingConsumerInfo::RegisterPartition(ui32 partitionId, const TActorContext& ctx) {
    auto [_, inserted] = Partitions.emplace(partitionId, TReadingPartitionStatus());
    if (inserted && IsReadable(partitionId)) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "register readable partition " << partitionId);

        CreateFamily({partitionId}, ctx);
    }
}

void TPersQueueReadBalancer::TBalancingConsumerInfo::UnregisterPartition(ui32 partitionId) {
    Partitions.erase(partitionId); // TODO аккуратно почистить в families
}

void  TPersQueueReadBalancer::TBalancingConsumerInfo::InitPartitions(const TActorContext& ctx) {
    for (auto& [partitionId,_] : Balancer.PartitionsInfo) {
        RegisterPartition(partitionId, ctx);
    }
}

void TPersQueueReadBalancer::TBalancingConsumerInfo::CreateFamily(std::vector<ui32>&& partitions, const TActorContext& ctx) {
    auto id = ++NextFamilyId;
    auto [it, _] = Families.emplace(id, std::make_unique<TPersQueueReadBalancer::TPartitionFamilty>(*this, id, std::move(partitions)));
    auto* family = it->second.get();

    UnreadableFamilies.emplace(id, family);

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
        GetPrefix() << "family created " << family->DebugStr());
}

TPersQueueReadBalancer::TPartitionFamilty* TPersQueueReadBalancer::TBalancingConsumerInfo::FindFamily(ui32 partitionId) {
    auto it = PartitionMapping.find(partitionId);
    if (it == PartitionMapping.end()) {
        return nullptr;
    }
    return it->second;
}

void TPersQueueReadBalancer::TBalancingConsumerInfo::RegisterReadingSession(TPersQueueReadBalancer::TReadingSession* session, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "register reading session " << session->DebugStr());

    ReadingSessions[session->Sender] = session;

    if (session->WithGroups()) {
        for (auto& [_, family] : Families) {
            if (session->AllPartitionsReadable(family->Partitions)) {
                family->SpecialSessions[session->Sender] = session;
            }
        }
    }
}

void TPersQueueReadBalancer::TBalancingConsumerInfo::UnregisterReadingSession(TPersQueueReadBalancer::TReadingSession* session) {
    ReadingSessions.erase(session->Sender);

    if (session->WithGroups()) {
        for (auto& [_, family] : Families) {
            family->SpecialSessions.erase(session->Sender);
        }
    }

    for (auto& [_, family] : Families) {
        if (session == family->Session) {
            family->Reset();
            UnreadableFamilies[family->Id] = family.get();
        }
    }
}

bool TPersQueueReadBalancer::TBalancingConsumerInfo::Unlock(const TActorId& sender, ui32 partitionId, const TActorContext& ctx) {
    auto* family = FindFamily(partitionId);
    if (!family) {
        // TODO Messages
        return false;
    }

    return family->Unlock(sender, partitionId, ctx);
}

bool TPersQueueReadBalancer::TBalancingConsumerInfo::IsReadable(ui32 partitionId) {
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

bool TPersQueueReadBalancer::TBalancingConsumerInfo::IsFinished(ui32 partitionId) {
    auto* partition = GetPartitionStatus(partitionId);
    if (partition) {
        return partition->IsFinished();
    }
    return false;
}

bool TPersQueueReadBalancer::TBalancingConsumerInfo::ScalingSupport() const {
    return SplitMergeEnabled(Balancer.TabletConfig);
}

TString TPersQueueReadBalancer::TBalancingConsumerInfo::GetPrefix() const {
    return TStringBuilder() << "Consumer=" << ConsumerName << " ";
}

bool TPersQueueReadBalancer::TBalancingConsumerInfo::SetCommittedState(ui32 partitionId, ui32 generation, ui64 cookie) {
    return Partitions[partitionId].SetCommittedState(generation, cookie);
}

bool TPersQueueReadBalancer::TBalancingConsumerInfo::ProccessReadingFinished(ui32 partitionId, const TActorContext& ctx) {
    if (!ScalingSupport()) {
        return false;
    }

    auto& partition = Partitions[partitionId];

    auto* family = FindFamily(partitionId);
    if (!family) {
        return false; // TODO is it correct?
    }
    family->InactivatePartition(partitionId);

    std::vector<ui32> newPartitions;
    Balancer.PartitionGraph.Travers(partitionId, [&](ui32 id) {
        if (!IsReadable(id)) {
            return false;
        }

        newPartitions.push_back(id);
        return true;
    });

    if (partition.NeedReleaseChildren()) {
        if (family->Status == TPartitionFamilty::EStatus::Active && !family->Session->AllPartitionsReadable(newPartitions)) {
            // TODO тут надо найти сессию, которая сможет читать все партиции
        }
        family->AttachePartitions(newPartitions, ctx);
    } else {
        for (auto p : newPartitions) {
            auto* f = FindFamily(p);
            if (f) {
                if (f->Status == TPartitionFamilty::EStatus::Releasing) {
                    f->TargetStatus = TPartitionFamilty::EStatus::Free;
                }
            } else {
                CreateFamily({p}, ctx);
            }
        }
    }

    return !newPartitions.empty();

}

void TPersQueueReadBalancer::TBalancingConsumerInfo::StartReading(ui32 partitionId, const TActorContext& ctx) {
    auto* status = GetPartitionStatus(partitionId);

    if (status->StartReading()) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                "Reading of the partition " << partitionId << " was started by " << ConsumerName << ". We stop reading from child partitions.");

        auto* family = FindFamily(partitionId);
        if (family) {
            family->ActivatePartition(partitionId);
        }

        // We releasing all children's partitions because we don't start reading the partition from EndOffset
        Balancer.PartitionGraph.Travers(partitionId, [&](ui32 partitionId) {
            // TODO несколько партиции в одном family
            auto* status = GetPartitionStatus(partitionId);
            auto* family = FindFamily(partitionId);

            if (family) {
                if (status->Reset()) {
                    family->ActivatePartition(partitionId);
                }
                family->Release(ctx, TPartitionFamilty::EStatus::Destroyed);
            }

            return true;
        });
    } else {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                "Reading of the partition " << partitionId << " was started by " << ConsumerName << ".");

    }
}

TString GetSdkDebugString0(bool scaleAwareSDK) {
    return scaleAwareSDK ? "ScaleAwareSDK" : "old SDK";
}

void TPersQueueReadBalancer::TBalancingConsumerInfo::FinishReading(TEvPersQueue::TEvReadingPartitionFinishedRequest::TPtr& ev, const TActorContext& ctx) {
    auto& r = ev->Get()->Record;
    auto partitionId = r.GetPartitionId();

    auto* status = GetPartitionStatus(partitionId);

    if (!IsReadable(partitionId)) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    "Reading of the partition " << partitionId << " was finished by " << ConsumerName
                    << " but the partition isn't readable");
        return;
    }

    if (status->SetFinishedState(r.GetScaleAwareSDK(), r.GetStartedReadingFromEndOffset())) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    "Reading of the partition " << partitionId << " was finished by " << r.GetConsumer()
                    << ", firstMessage=" << r.GetStartedReadingFromEndOffset() << ", " << GetSdkDebugString0(r.GetScaleAwareSDK()));

        if (ProccessReadingFinished(partitionId, ctx)) {
            Balance(ctx);
        }
    } else if (!status->IsFinished()) {
        auto delay = std::min<size_t>(1ul << status->Iteration, Balancer.TabletConfig.GetPartitionConfig().GetLifetimeSeconds()); // TODO Учесть время закрытия партиции на запись

        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    "Reading of the partition " << partitionId << " was finished by " << r.GetConsumer()
                    << ". Scheduled release of the partition for re-reading. Delay=" << delay << " seconds,"
                    << " firstMessage=" << r.GetStartedReadingFromEndOffset() << ", " << GetSdkDebugString0(r.GetScaleAwareSDK()));

        status->LastPipe = ev->Sender;
        ctx.Schedule(TDuration::Seconds(delay), new TEvPQ::TEvWakeupReleasePartition(ConsumerName, partitionId, status->Cookie));
    }
}

struct SessionComparator {
    bool operator()(const TPersQueueReadBalancer::TReadingSession* lhs, const TPersQueueReadBalancer::TReadingSession* rhs) const {
        if (lhs->ActivePartitionCount != rhs->ActivePartitionCount) {
            return lhs->ActivePartitionCount < rhs->ActivePartitionCount;
        }
        if (lhs->InactivePartitionCount != rhs->InactivePartitionCount) {
            return lhs->InactivePartitionCount < rhs->InactivePartitionCount;
        }
        return (lhs->Session < rhs->Session);
    }
};

using TOrderedSessions = std::set<TPersQueueReadBalancer::TReadingSession*, SessionComparator>;

TOrderedSessions OrderSessions(
    const std::unordered_map<TActorId, TPersQueueReadBalancer::TReadingSession*>& values,
    std::function<bool (const TPersQueueReadBalancer::TReadingSession*)> predicate = [](const TPersQueueReadBalancer::TReadingSession*) { return true; }
) {
    TOrderedSessions result;
    for (auto& [_, v] : values) {
        if (predicate(v)) {
            result.insert(v);
        }
    }

    return result;
}

TString DebugStr(const std::unordered_map<size_t, TPersQueueReadBalancer::TPartitionFamilty*>& values) {
    TStringBuilder sb;
    for (auto& [id, family] : values) {
        sb << id << " (" << JoinRange(", ", family->Partitions.begin(), family->Partitions.end()) << "), ";
    }
    return sb;
}

TString DebugStr(const TPersQueueReadBalancer::TOrderedTPartitionFamilies& values) {
    TStringBuilder sb;
    for (auto* family : values) {
        sb << family->DebugStr() << ", ";
    }
    return sb;
}

TPersQueueReadBalancer::TOrderedTPartitionFamilies OrderFamilies(
    const std::unordered_map<size_t, TPersQueueReadBalancer::TPartitionFamilty*>& values
) {
    TPersQueueReadBalancer::TOrderedTPartitionFamilies result;
    for (auto& [_, v] : values) {
        result.insert(v);
    }

    return result;
}

std::tuple<size_t, size_t, size_t> GetStatistics(
    const std::unordered_map<size_t, const std::unique_ptr<TPersQueueReadBalancer::TPartitionFamilty>>& values,
    std::function<bool (const TPersQueueReadBalancer::TPartitionFamilty*)> predicate = [](const TPersQueueReadBalancer::TPartitionFamilty*) { return true; }
) {
    size_t activePartitionCount = 0;
    size_t inactivePartitionCount = 0;
    size_t maxSize = 1;

    for (auto& [_, family] : values) {
        if (predicate(family.get())) {
            activePartitionCount += family->ActivePartitionCount;
            inactivePartitionCount += family->InactivePartitionCount;
            if (maxSize < family->Partitions.size()) {
                maxSize = family->Partitions.size();
            }
        }
    }

    return {activePartitionCount, inactivePartitionCount, maxSize};
}

size_t GetMaxFamilySize(const std::unordered_map<size_t, const std::unique_ptr<TPersQueueReadBalancer::TPartitionFamilty>>& values) {
    size_t result = 1;
    for (auto& [_, v] : values)  {
        result = std::max(result, v->ActivePartitionCount);
    }
    return result;
}

size_t SessionWithoutGroupsCount(const std::unordered_map<TActorId, TPersQueueReadBalancer::TReadingSession*>& values) {
    size_t result = 0;
    for (auto [_, session] : values) {
        if (!session->WithGroups()) {
            ++result;
        }
    }
    return result;
}

void TPersQueueReadBalancer::TBalancingConsumerInfo::Balance(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "balancing. ReadingSessions=" << ReadingSessions.size() << ", Families=" << Families.size()
            << ", UnradableFamilies=" << UnreadableFamilies.size() << " [" << DebugStr(UnreadableFamilies) << "]");

    if (ReadingSessions.empty()) {
        return;
    }

    TOrderedSessions commonSessions = OrderSessions(ReadingSessions, [](const TPersQueueReadBalancer::TReadingSession* s) {
        return !s->WithGroups();
    });
    auto families = OrderFamilies(UnreadableFamilies);

    for (auto it = families.rbegin(); it != families.rend(); ++it) {
        auto* family = *it;
        TOrderedSessions specialSessions;
        auto& sessions = (family->SpecialSessions.empty()) ? commonSessions : (specialSessions = OrderSessions(family->SpecialSessions));

        auto sit = sessions.begin();
        if (sit == sessions.end()) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    GetPrefix() << "balancing of the " << family->DebugStr() << " failed because there are no suitable reading sessions.");
            continue;
        }
        auto* session = *sit;
        sessions.erase(sit);

        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "balancing partitions " << family->DebugStr() << " for " << session->DebugStr());
        family->StartReading(*session, ctx);

        // Reorder sessions
        sessions.insert(session);

        UnreadableFamilies.erase(family->Id);
    }

    // We try to balance the partitions by sessions that clearly want to read them, even if the distribution is not uniform.
    for (auto& [_, family] : Families) {
        if (family->Status != TPartitionFamilty::EStatus::Active || family->SpecialSessions.empty()) {
            continue;
        }
        if (!family->SpecialSessions.contains(family->Session->Sender)) {
            Release(family.get(), ctx);
        }
    }

/*
    auto sessionWithoutGroupsCount = SessionWithoutGroupsCount(ReadingSessions);
    if (sessionWithoutGroupsCount) {
        auto [activePartitionCount, inactivePartitionCount, maxSize] = GetStatistics(Families, [](auto* family) {
            return family->SpecialSessions.empty();
        });
        auto desiredPartitionCount = activePartitionCount / sessionWithoutGroupsCount + maxSize;

        for (auto [_, session] : ReadingSessions) {
            if (session->WithGroups()) {
                continue;
            }
            if (session->ActivePartitionCount > desiredPartitionCount && session->Families.size() > 1) {
                for (auto family = session->Families.begin(); family != session->Families.end() &&
                                                            session->ActivePartitionCount > desiredPartitionCount &&
                                                            (*family)->ActivePartitionCount < desiredPartitionCount; ++family) {
                    Release(family.get(), ctx);
                }
            }
        }
    }*/
}

void TPersQueueReadBalancer::TBalancingConsumerInfo::Release(ui32 partitionId, const TActorContext& ctx) {
    auto* family = FindFamily(partitionId);
    if (!family) {
        return;
    }

    Release(family, ctx);
}

void TPersQueueReadBalancer::TBalancingConsumerInfo::Release(TPartitionFamilty* family, const TActorContext& ctx) {
    bool releaseChildren = false;
    for (auto partitionId : family->LockedPartitions) {
        auto* status = GetPartitionStatus(partitionId);
        if (status->NeedReleaseChildren()) {
            releaseChildren = true;
            break;
        }
    }

    Cerr << ">>>>> releaseChildren=" << releaseChildren << Endl;

    family->Release(ctx);

    if (releaseChildren) {
        for (auto partitionId : family->LockedPartitions) {
            Balancer.PartitionGraph.Travers(partitionId, [&](auto id) {
                auto* f = FindFamily(id);
                if (f && f->Status == TPartitionFamilty::EStatus::Active) {
                    f->Release(ctx, TPartitionFamilty::EStatus::Destroyed);
                }
                return true;
            });
        }
    }

}

//
// TReadingSession
//

TPersQueueReadBalancer::TReadingSession::TReadingSession(const TActorId& pipeClient)
            : PipeClient(pipeClient)
            , ServerActors(0)
            , ActivePartitionCount(0)
            , InactivePartitionCount(0)
        {}

void TPersQueueReadBalancer::TReadingSession::Init(const TString& clientId, const TString& session, const TActorId& sender, const std::vector<ui32>& partitions) {
    ClientId = clientId;
    Session = session;
    Sender = sender;
    Partitions.insert(partitions.begin(), partitions.end());
}

bool TPersQueueReadBalancer::TReadingSession::WithGroups() const { return !Partitions.empty(); }

bool TPersQueueReadBalancer::TReadingSession::AllPartitionsReadable(const std::vector<ui32>& partitions) const {
    if (WithGroups()) {
        for (auto p : partitions) {
            if (!Partitions.contains(p)) {
                return false;
            }
        }
    }

    return true;
}

TString TPersQueueReadBalancer::TReadingSession::DebugStr() const {
    return TStringBuilder() << "ReadingSession \"" << Session << "\" (Sender=" << Sender <<
            ", Partitions=[" << JoinRange(", ", Partitions.begin(), Partitions.end()) << "])";
}

}
