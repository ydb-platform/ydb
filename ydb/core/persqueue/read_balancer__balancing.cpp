#include "read_balancer__balancing.h"

#define DEBUG(message)


namespace NKikimr::NPQ::NBalancing {


//
// TPartition
//

bool TPartition::IsFinished() const {
    return Commited || (ReadingFinished && (StartedReadingFromEndOffset || ScaleAwareSDK));
}

bool TPartition::NeedReleaseChildren() const {
     return !(Commited || (ReadingFinished && !ScaleAwareSDK));
}

bool TPartition::BalanceToOtherPipe() const {
    return LastPipe && !Commited && ReadingFinished && !ScaleAwareSDK;
}

bool TPartition::StartReading() {
    return std::exchange(ReadingFinished, false);
}

bool TPartition::StopReading() {
    ReadingFinished = false;
    ++Cookie;
    return NeedReleaseChildren();
}

bool TPartition::SetCommittedState(ui32 generation, ui64 cookie) {
    if (PartitionGeneration < generation || (PartitionGeneration == generation && PartitionCookie < cookie)) {
        Iteration = 0;
        PartitionGeneration = generation;
        PartitionCookie = cookie;

        return !std::exchange(Commited, true);
    }

    return false;
}

bool TPartition::SetFinishedState(bool scaleAwareSDK, bool startedReadingFromEndOffset) {
    bool previousStatus = IsFinished();

    ScaleAwareSDK = scaleAwareSDK;
    StartedReadingFromEndOffset = startedReadingFromEndOffset;
    ReadingFinished = true;
    ++Cookie;

    bool currentStatus = IsFinished();
    if (currentStatus) {
        Iteration = 0;
    } else {
        ++Iteration;
    }
    if (scaleAwareSDK || currentStatus) {
        LastPipe = TActorId();
    }
    return currentStatus && !previousStatus;
}

bool TPartition::Reset() {
    bool result = IsFinished();

    ScaleAwareSDK = false;
    ReadingFinished = false;
    Commited = false;
    ++Cookie;
    LastPipe = TActorId();

    return result;
};


//
// TPartitionFamily
//

TPartitionFamily::TPartitionFamily(TConsumer& consumerInfo, size_t id, std::vector<ui32>&& partitions)
    : Consumer(consumerInfo)
    , Id(id)
    , Status(EStatus::Free)
    , Partitions(std::move(partitions))
    , Session(nullptr)
{
    ClassifyPartitions();
    UpdatePartitionMapping(Partitions);
    UpdateSpecialSessions();
}

bool TPartitionFamily::IsLonely() const {
    return Partitions.size() == 1;
}

const TString& TPartitionFamily::Topic() const {
    return Consumer.Topic();
}

const TString& TPartitionFamily::TopicPath() const {
    return Consumer.TopicPath();
}

ui32 TPartitionFamily::TabletGeneration() const {
    return Consumer.TabletGeneration();
}

const TPartitionInfo* TPartitionFamily::GetPartitionInfo(ui32 partitionId) const {
    return Consumer.GetPartitionInfo(partitionId);
}
bool TPartitionFamily::IsReadable(ui32 partitionId) const {
    return Consumer.IsReadable(partitionId);
}

ui32 TPartitionFamily::NextStep() {
    return Consumer.NextStep();
}

TString TPartitionFamily::GetPrefix() const {
    TStringBuilder sb;
    sb << Consumer.GetPrefix() << " family " << Id << " status " << Status << " ";
    if (Session) {
        sb << " session \"" << Session->Session << "\" sender " << Session->Sender;
    }
    return TStringBuilder() ;
}


void TPartitionFamily::Release(const TActorContext& ctx, EStatus targetStatus) {
    if (Status != EStatus::Active) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "releasing the family " << DebugStr() << " that isn't active");
        return;
    }

    if (!Session) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "releasing the family " << DebugStr() << " that does not have a session");
        return;
    }

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << " release partitions [" << JoinRange(", ", LockedPartitions.begin(), LockedPartitions.end())
            << "]. Target status " << targetStatus);

    Status = EStatus::Releasing;
    TargetStatus = targetStatus;

    Session->ActivePartitionCount -= ActivePartitionCount;
    Session->InactivePartitionCount -= InactivePartitionCount;

    for (auto partitionId : LockedPartitions) {
        ctx.Send(Session->Sender, MakeEvReleasePartition(partitionId).release());
    }

}

bool TPartitionFamily::Unlock(const TActorId& sender, ui32 partitionId, const TActorContext& ctx) {
    if (!Session || Session->PipeClient != sender) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "try unlock the partition " << partitionId << " from other sender");
        return false;
    }

    if (Status != EStatus::Releasing) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "try unlock partition " << partitionId << " but family status is " << Status);
        return false;
    }

    if (!LockedPartitions.erase(partitionId)) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "try unlock partition " << partitionId << " but partition isn't locked."
                << " Locked partitions are [" << JoinRange(", ", LockedPartitions.begin(), LockedPartitions.end()) << "]");
        return false;
    }

    if (!LockedPartitions.empty()) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "partition " << partitionId << " was unlocked but wait else [" << JoinRange(", ", LockedPartitions.begin(), LockedPartitions.end()) << "]");
        return false;
    }

    Reset(ctx);

    return true;
}

bool TPartitionFamily::Reset(const TActorContext& ctx) {
    Status = TargetStatus;

    Session->Families.erase(this);
    Session = nullptr;

    if (Status == EStatus::Destroyed) {
        Destroy(ctx);
        return false;
    } else if (Status == EStatus::Free) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << " is free.");

        Consumer.UnreadableFamilies[Id] = this;
    }

    if (!AttachedPartitions.empty()) {

        auto [activePartitionCount, inactivePartitionCount] = ClassifyPartitions(AttachedPartitions);
        ActivePartitionCount -= activePartitionCount;
        InactivePartitionCount -= inactivePartitionCount;

        // The attached partitions are always at the end of the list.
        Partitions.resize(Partitions.size() - AttachedPartitions.size());
        for (auto partitionId : AttachedPartitions) {
            Consumer.PartitionMapping.erase(partitionId);
        }
        AttachedPartitions.clear();

        // After reducing the number of partitions in the family, the list of reading sessions that can read this family may expand.
        UpdateSpecialSessions();
    }

    return true;
}

void TPartitionFamily::Destroy(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << " destroyed.");

    for (auto partitionId : Partitions) {
        Consumer.PartitionMapping.erase(partitionId);
    }
    Consumer.UnreadableFamilies.erase(Id);
    Consumer.Families.erase(Id);
}

void TPartitionFamily::StartReading(TSession& session, const TActorContext& ctx) {
    if (Status != EStatus::Free) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "try start reading but the family status is " << Status);
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "start reading");

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

void TPartitionFamily::AttachePartitions(const std::vector<ui32>& partitions, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "attaching partitions [" << JoinRange(", ", partitions.begin(), partitions.end()) << "]");

    auto [activePartitionCount, inactivePartitionCount] = ClassifyPartitions(partitions);

    if (Session) {
        // Reordering Session->Families
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
            // TODO не надо добавлять партиции если текущая сессия не может читать новое семейство. Ждем коммита.
            //Release(ctx);
            //return;
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

void TPartitionFamily::ActivatePartition(ui32 partitionId) {
    ALOG_DEBUG(NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "activating partition " << partitionId);

    ++ActivePartitionCount;
    --InactivePartitionCount;

    if (Status == EStatus::Active) {
        ++Session->ActivePartitionCount;
        --Session->InactivePartitionCount;
    }
}

void TPartitionFamily::InactivatePartition(ui32 partitionId) {
    ALOG_DEBUG(NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "inactivating partition " << partitionId);

    --ActivePartitionCount;
    ++InactivePartitionCount;

    if (Status == EStatus::Active) {
        --Session->ActivePartitionCount;
        ++Session->InactivePartitionCount;
    }
}

TString TPartitionFamily::DebugStr() const {
    return TStringBuilder() << "family=" << Id << " (Status=" << Status
            << ", Partitions=[" << JoinRange(", ", Partitions.begin(), Partitions.end()) << "])";
}

TPartition* TPartitionFamily::GetPartitionStatus(ui32 partitionId) {
    return Consumer.GetPartitionStatus(partitionId);
}

void TPartitionFamily::ClassifyPartitions() {
    auto [activePartitionCount, inactivePartitionCount] = ClassifyPartitions(Partitions);
    ActivePartitionCount = activePartitionCount;
    InactivePartitionCount = inactivePartitionCount;
}

template<typename TPartitions>
std::pair<size_t, size_t> TPartitionFamily::ClassifyPartitions(const TPartitions& partitions) {
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
        }
    }

    return {activePartitionCount, inactivePartitionCount};
}

template
std::pair<size_t, size_t> TPartitionFamily::ClassifyPartitions(const std::set<ui32>& partitions);

template
std::pair<size_t, size_t> TPartitionFamily::ClassifyPartitions(const std::vector<ui32>& partitions);

void TPartitionFamily::UpdatePartitionMapping(const std::vector<ui32>& partitions) {
    for (auto partitionId: partitions) {
        Consumer.PartitionMapping[partitionId] = this;
    }
}

void TPartitionFamily::UpdateSpecialSessions() {
    for (auto& [_, session] : Consumer.Session) {
        if (session->WithGroups() && session->AllPartitionsReadable(Partitions)) {
            SpecialSessions[session->Sender] = session;
        }
    }
}

void TPartitionFamily::LockPartition(ui32 partitionId, const TActorContext& ctx) {
    auto step = NextStep();

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "lock partition " << partitionId << " for " << Session->DebugStr()
            << " generation " << TabletGeneration() << " step " << step);

    ctx.Send(Session->Sender, MakeEvLockPartition(partitionId, step).release());
}

std::unique_ptr<TEvPersQueue::TEvReleasePartition> TPartitionFamily::MakeEvReleasePartition(ui32 partitionId) const {
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

std::unique_ptr<TEvPersQueue::TEvLockPartition> TPartitionFamily::MakeEvLockPartition(ui32 partitionId, ui32 step) const {
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

    auto* partitionInfo = GetPartitionInfo(partitionId);
    if (partitionInfo) {
        r.SetTabletId(partitionInfo->TabletId);
    }

    return res;
}


//
// TConsumer
//

TConsumer::TConsumer(TBalancer& balancer, const TString& consumerName)
    : Balancer(balancer)
    , ConsumerName(consumerName)
    , NextFamilyId(0)
    , Step(0)
{
}

const TString& TConsumer::Topic() const {
    return Balancer.Topic();
}

const TString& TConsumer::TopicPath() const {
    return Balancer.TopicPath();
}

ui32 TConsumer::TabletGeneration() const {
    return Balancer.TabletGeneration();
}

const TPartitionInfo* TConsumer::GetPartitionInfo(ui32 partitionId) const {
    return Balancer.GetPartitionInfo(partitionId);
}

TPartition* TConsumer::GetPartitionStatus(ui32 partitionId) {
    auto it = Partitions.find(partitionId);
    if (it == Partitions.end()) {
        return nullptr;
    }
    return &it->second;
}

ui32 TConsumer::NextStep() {
    return ++Step;
}

void TConsumer::RegisterPartition(ui32 partitionId, const TActorContext& ctx) {
    auto [_, inserted] = Partitions.try_emplace(partitionId, TPartition());
    if (inserted && IsReadable(partitionId)) {
        // TODO to existed family?
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "register readable partition " << partitionId);

        CreateFamily({partitionId}, ctx);
    }
}

bool Contains(const std::vector<ui32>& values, ui32 value) {
    for (auto v : values) {
        if (v == value) {
            return true;
        }
    }
    return false;
}

void TConsumer::UnregisterPartition(ui32 partitionId, const TActorContext& ctx) {
    for (auto& [_, family] : Families) {
        if (Contains(family->Partitions, partitionId)) {
            if (family->IsLonely()) {
                if (family->Status == TPartitionFamily::EStatus::Active) {
                    family->Release(ctx, TPartitionFamily::EStatus::Destroyed);
                } else if (family->Status == TPartitionFamily::EStatus::Releasing) {
                    family->TargetStatus = TPartitionFamily::EStatus::Destroyed;
                } else {
                    // Free
                    family->Status = TPartitionFamily::EStatus::Releasing;
                    family->TargetStatus = TPartitionFamily::EStatus::Destroyed;
                    family->Reset(ctx);
                }
            } else {
                for (auto id : family->Partitions) {
                    if (id == partitionId) {
                        continue;
                    }

                    auto* node = Balancer.GetPartitionGraph().GetPartition(id);
                    if (node->IsRoot()) {
                        std::vector<ui32> members;
                        Balancer.GetPartitionGraph().Travers(id, [&](auto childId) {
                            if (!Contains(family->Partitions, childId)) {
                                return false;
                            }
                            members.push_back(childId);
                            return true;
                        });

                        auto* f = CreateFamily(std::move(members), family->Status, ctx);
                        f->TargetStatus = family->TargetStatus;
                        f->Session = family->Session;
                        f->LockedPartitions = family->LockedPartitions; // TODO intercept with members
                        f->AttachedPartitions = family->AttachedPartitions;
                        if (f->Session) {
                            f->Session->Families.insert(f);
                        }
                    }
                }

                family->Partitions.clear();
                family->LockedPartitions.clear();
                family->AttachedPartitions.clear();
                family->Status = TPartitionFamily::EStatus::Releasing;
                family->TargetStatus = TPartitionFamily::EStatus::Destroyed;
                family->Reset(ctx);
            }
        }
    }
    Partitions.erase(partitionId); // TODO аккуратно почистить в families
}

void  TConsumer::InitPartitions(const TActorContext& ctx) {
    for (auto& [partitionId,_] : Balancer.GetPartitionsInfo()) {
        RegisterPartition(partitionId, ctx);
    }
}

TPartitionFamily* TConsumer::CreateFamily(std::vector<ui32>&& partitions, const TActorContext& ctx) {
    return CreateFamily(std::move(partitions), TPartitionFamily::EStatus::Free, ctx);
}

TPartitionFamily* TConsumer::CreateFamily(std::vector<ui32>&& partitions, TPartitionFamily::EStatus status, const TActorContext& ctx) {
    auto id = ++NextFamilyId;
    auto [it, _] = Families.emplace(id, std::make_unique<TPartitionFamily>(*this, id, std::move(partitions)));
    auto* family = it->second.get();

    if (status == TPartitionFamily::EStatus::Free) {
        UnreadableFamilies.emplace(id, family);
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "family created " << family->DebugStr());

    return family;
}

TPartitionFamily* TConsumer::FindFamily(ui32 partitionId) {
    auto it = PartitionMapping.find(partitionId);
    if (it == PartitionMapping.end()) {
        return nullptr;
    }
    return it->second;
}

void TConsumer::RegisterReadingSession(TSession* session, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "register reading session " << session->DebugStr());

    Session[session->Sender] = session;

    if (session->WithGroups()) {
        for (auto& [_, family] : Families) {
            if (session->AllPartitionsReadable(family->Partitions)) {
                family->SpecialSessions[session->Sender] = session;
            }
        }
    }
}


std::vector<TPartitionFamily*> Snapshot(const std::unordered_map<size_t, const std::unique_ptr<TPartitionFamily>>& families) {
    std::vector<TPartitionFamily*> result;
    result.reserve(families.size());

    for (auto& [_, family] : families) {
        result.push_back(family.get());
    }

    return result;
}

void TConsumer::UnregisterReadingSession(TSession* session, const TActorContext& ctx) {
    if (session->WithGroups()) {
        for (auto& [_, family] : Families) {
            family->SpecialSessions.erase(session->Sender);
        }
    }

    for (auto* family : Snapshot(Families)) {
        if (session == family->Session) {
            if (family->Reset(ctx)) {
                UnreadableFamilies[family->Id] = family;
            }
        }
    }

    Session.erase(session->Sender);
}

bool TConsumer::Unlock(const TActorId& sender, ui32 partitionId, const TActorContext& ctx) {
    auto* family = FindFamily(partitionId);
    if (!family) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "unlocking the partition " << partitionId << " from unknown family.");
        return false;
    }

    return family->Unlock(sender, partitionId, ctx);
}

bool TConsumer::IsReadable(ui32 partitionId) {
    if (!ScalingSupport()) {
        return true;
    }

    auto* node = Balancer.GetPartitionGraph().GetPartition(partitionId);
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

bool TConsumer::IsFinished(ui32 partitionId) {
    auto* partition = GetPartitionStatus(partitionId);
    if (partition) {
        return partition->IsFinished();
    }
    return false;
}

bool TConsumer::ScalingSupport() const {
    return Balancer.ScalingSupport();
}

TString TConsumer::GetPrefix() const {
    return TStringBuilder() << Balancer.GetPrefix() << "consumer " << ConsumerName << " ";
}

bool TConsumer::SetCommittedState(ui32 partitionId, ui32 generation, ui64 cookie) {
    return Partitions[partitionId].SetCommittedState(generation, cookie);
}

bool TConsumer::ProccessReadingFinished(ui32 partitionId, const TActorContext& ctx) {
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
    Balancer.GetPartitionGraph().Travers(partitionId, [&](ui32 id) {
        if (!IsReadable(id)) {
            return false;
        }

        newPartitions.push_back(id);
        return true;
    });

    if (partition.NeedReleaseChildren()) {
        if (family->Status == TPartitionFamily::EStatus::Active && !family->Session->AllPartitionsReadable(newPartitions)) {
            // TODO тут надо найти сессию, которая сможет читать все партиции
        }
        family->AttachePartitions(newPartitions, ctx);
    } else {
        for (auto p : newPartitions) {
            auto* f = FindFamily(p);
            if (f) {
                if (f->Status == TPartitionFamily::EStatus::Releasing) {
                    f->TargetStatus = TPartitionFamily::EStatus::Free;
                }
            } else {
                CreateFamily({p}, ctx);
            }
        }
    }

    return !newPartitions.empty();

}

void TConsumer::StartReading(ui32 partitionId, const TActorContext& ctx) {
    if (!GetPartitionInfo(partitionId)) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "start reading for deleted partition " << partitionId);
        return;
    }

    auto* status = GetPartitionStatus(partitionId);

    if (status && status->StartReading()) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                "Reading of the partition " << partitionId << " was started by " << ConsumerName << ". We stop reading from child partitions.");

        auto* family = FindFamily(partitionId);
        if (family) {
            family->ActivatePartition(partitionId);
        }

        // We releasing all children's partitions because we don't start reading the partition from EndOffset
        Balancer.GetPartitionGraph().Travers(partitionId, [&](ui32 partitionId) {
            // TODO несколько партиции в одном family
            auto* status = GetPartitionStatus(partitionId);
            auto* family = FindFamily(partitionId);

            if (family) {
                if (status && status->Reset()) {
                    family->ActivatePartition(partitionId);
                }
                family->Release(ctx, TPartitionFamily::EStatus::Destroyed);
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

void TConsumer::FinishReading(TEvPersQueue::TEvReadingPartitionFinishedRequest::TPtr& ev, const TActorContext& ctx) {
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
        auto delay = std::min<size_t>(1ul << status->Iteration, Balancer.GetLifetimeSeconds()); // TODO Учесть время закрытия партиции на запись

        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    "Reading of the partition " << partitionId << " was finished by " << r.GetConsumer()
                    << ". Scheduled release of the partition for re-reading. Delay=" << delay << " seconds,"
                    << " firstMessage=" << r.GetStartedReadingFromEndOffset() << ", " << GetSdkDebugString0(r.GetScaleAwareSDK()));

        status->LastPipe = ev->Sender;
        ctx.Schedule(TDuration::Seconds(delay), new TEvPQ::TEvWakeupReleasePartition(ConsumerName, partitionId, status->Cookie));
    }
}

struct SessionComparator {
    bool operator()(const TSession* lhs, const TSession* rhs) const {
        if (lhs->ActivePartitionCount != rhs->ActivePartitionCount) {
            return lhs->ActivePartitionCount < rhs->ActivePartitionCount;
        }
        if (lhs->InactivePartitionCount != rhs->InactivePartitionCount) {
            return lhs->InactivePartitionCount < rhs->InactivePartitionCount;
        }
        return (lhs->Session < rhs->Session);
    }
};

using TOrderedSessions = std::set<TSession*, SessionComparator>;

TOrderedSessions OrderSessions(
    const std::unordered_map<TActorId, TSession*>& values,
    std::function<bool (const TSession*)> predicate = [](const TSession*) { return true; }
) {
    TOrderedSessions result;
    for (auto& [_, v] : values) {
        if (predicate(v)) {
            result.insert(v);
        }
    }

    return result;
}

TString DebugStr(const std::unordered_map<size_t, TPartitionFamily*>& values) {
    TStringBuilder sb;
    for (auto& [id, family] : values) {
        sb << id << " (" << JoinRange(", ", family->Partitions.begin(), family->Partitions.end()) << "), ";
    }
    return sb;
}

TString DebugStr(const TOrderedPartitionFamilies& values) {
    TStringBuilder sb;
    for (auto* family : values) {
        sb << family->DebugStr() << ", ";
    }
    return sb;
}

TOrderedPartitionFamilies OrderFamilies(
    const std::unordered_map<size_t, TPartitionFamily*>& values
) {
    TOrderedPartitionFamilies result;
    for (auto& [_, v] : values) {
        result.insert(v);
    }

    return result;
}

std::tuple<size_t, size_t, size_t> GetStatistics(
    const std::unordered_map<size_t, const std::unique_ptr<TPartitionFamily>>& values,
    std::function<bool (const TPartitionFamily*)> predicate = [](const TPartitionFamily*) { return true; }
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

size_t GetMaxFamilySize(const std::unordered_map<size_t, const std::unique_ptr<TPartitionFamily>>& values) {
    size_t result = 1;
    for (auto& [_, v] : values)  {
        result = std::max(result, v->ActivePartitionCount);
    }
    return result;
}

size_t SessionWithoutGroupsCount(const std::unordered_map<TActorId, TSession*>& values) {
    size_t result = 0;
    for (auto [_, session] : values) {
        if (!session->WithGroups()) {
            ++result;
        }
    }
    return result;
}

void TConsumer::Balance(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "balancing. Sessions=" << Session.size() << ", Families=" << Families.size()
            << ", UnradableFamilies=" << UnreadableFamilies.size() << " [" << DebugStr(UnreadableFamilies) << "]");

    if (Session.empty()) {
        return;
    }

    TOrderedSessions commonSessions = OrderSessions(Session, [](const TSession* s) {
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
                GetPrefix() << "balancing " << family->DebugStr() << " for " << session->DebugStr());
        family->StartReading(*session, ctx);

        // Reorder sessions
        sessions.insert(session);

        UnreadableFamilies.erase(family->Id);
    }

    // We try to balance the partitions by sessions that clearly want to read them, even if the distribution is not uniform.
    for (auto& [_, family] : Families) {
        if (family->Status != TPartitionFamily::EStatus::Active || family->SpecialSessions.empty()) {
            continue;
        }
        if (!family->SpecialSessions.contains(family->Session->Sender)) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "rebalance " << family->DebugStr() << " because exists the special session for it");
            family->Release(ctx);
        }
    }

/*
    auto sessionWithoutGroupsCount = SessionWithoutGroupsCount(Session);
    if (sessionWithoutGroupsCount) {
        auto [activePartitionCount, inactivePartitionCount, maxSize] = GetStatistics(Families, [](auto* family) {
            return family->SpecialSessions.empty();
        });
        auto desiredPartitionCount = activePartitionCount / sessionWithoutGroupsCount + maxSize;

        for (auto [_, session] : Session) {
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

void TConsumer::Release(ui32 partitionId, const TActorContext& ctx) {
    auto* family = FindFamily(partitionId);
    if (!family) {
        return;
    }

    family->Release(ctx);
}


//
// TSession
//

TSession::TSession(const TActorId& pipeClient)
            : PipeClient(pipeClient)
            , ServerActors(0)
            , ActivePartitionCount(0)
            , InactivePartitionCount(0)
        {}

void TSession::Init(const TString& clientId, const TString& session, const TActorId& sender, const std::vector<ui32>& partitions) {
    ClientId = clientId;
    Session = session;
    Sender = sender;
    Partitions.insert(partitions.begin(), partitions.end());
}

bool TSession::WithGroups() const { return !Partitions.empty(); }

bool TSession::AllPartitionsReadable(const std::vector<ui32>& partitions) const {
    if (WithGroups()) {
        for (auto p : partitions) {
            if (!Partitions.contains(p)) {
                return false;
            }
        }
    }

    return true;
}

TString TSession::DebugStr() const {
    return TStringBuilder() << "ReadingSession \"" << Session << "\" (Sender=" << Sender <<
            ", Partitions=[" << JoinRange(", ", Partitions.begin(), Partitions.end()) << "])";
}


//
// TBalancer
//

TBalancer::TBalancer(TPersQueueReadBalancer& topicActor)
    : TopicActor(topicActor) {
}

const TString& TBalancer::Topic() const {
    return TopicActor.Topic;
}

const TString& TBalancer::TopicPath() const {
    return TopicActor.Path;
}

ui32 TBalancer::TabletGeneration() const {
    return TopicActor.Generation;
}

const TPartitionInfo* TBalancer::GetPartitionInfo(ui32 partitionId) const {
    auto it = GetPartitionsInfo().find(partitionId);
    if (it == GetPartitionsInfo().end()) {
        return nullptr;
    }
    return &it->second;
}

const std::unordered_map<ui32, TPartitionInfo>& TBalancer::GetPartitionsInfo() const {
    return TopicActor.PartitionsInfo;
}

const TPartitionGraph& TBalancer::GetPartitionGraph() const {
    return TopicActor.PartitionGraph;
}

bool TBalancer::ScalingSupport() const {
    return SplitMergeEnabled(TopicActor.TabletConfig);
}

i32 TBalancer::GetLifetimeSeconds() const {
    return TopicActor.TabletConfig.GetPartitionConfig().GetLifetimeSeconds();
}

TConsumer* TBalancer::GetConsumer(const TString& consumerName) {
    auto it = Consumers.find(consumerName);
    if (it == Consumers.end()) {
        return nullptr;
    }
    return it->second.get();
}

const TStatistics TBalancer::GetStatistics() const {
    TStatistics result;

    result.Consumers.reserve(Consumers.size());
    for (auto& [_, consumer] : Consumers) {
        result.Consumers.push_back(TStatistics::TConsumerStatistics());
        auto& c = result.Consumers.back();

        c.ConsumerName = consumer->ConsumerName;
        c.Partitions.reserve(GetPartitionsInfo().size());
        for (auto [partitionId, partitionInfo] : GetPartitionsInfo()) {
            c.Partitions.push_back(TStatistics::TConsumerStatistics::TPartitionStatistics());
            auto& p = c.Partitions.back();
            p.PartitionId = partitionId;
            p.TabletId = partitionInfo.TabletId;

            auto* family = consumer->FindFamily(partitionId);
            if (family && family->Session && family->LockedPartitions.contains(partitionId)) {
                p.Session = family->Session->Session;
                p.State = 1;
            }
        }
    }

    size_t readablePartitionCount = 0;

    result.Sessions.reserve(Sessions.size());
    for (auto& [_, session] : Sessions) {
        result.Sessions.push_back(TStatistics::TSessionStatistics());
        auto& s = result.Sessions.back();
        s.Session = session->Session;
        s.ActivePartitionCount = session->ActivePartitionCount;
        s.InactivePartitionCount = session->InactivePartitionCount;
        s.SuspendedPartitionCount = 0; // TODO
        s.TotalPartitionCount = s.ActivePartitionCount + s.InactivePartitionCount;

        readablePartitionCount += s.TotalPartitionCount;
    }

    result.FreePartitions = GetPartitionsInfo().size() - readablePartitionCount;

    return result;
}

void TBalancer::UpdateConfig(std::vector<ui32> addedPartitions, std::vector<ui32> deletedPartitions, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "updating configuration. Deleted partitions [" << JoinRange(", ", deletedPartitions.begin(), deletedPartitions.end())
            << "]. Added partitions [" << JoinRange(", ", addedPartitions.begin(), addedPartitions.end()) << "]");

    for (auto partitionId : deletedPartitions) {
        for (auto& [_, consumer] : Consumers) {
            consumer->UnregisterPartition(partitionId, ctx);
        }
    }

    for (auto& partitionId : addedPartitions) {
        for (auto& [_, balancingConsumer] : Consumers) {
            balancingConsumer->RegisterPartition(partitionId, ctx);
        }
    }

    for (auto& [_, consumer] : Consumers) {
        consumer->Balance(ctx);
    }
}

bool TBalancer::SetCommittedState(const TString& consumerName, ui32 partitionId, ui32 generation, ui64 cookie, const TActorContext& ctx) {
    auto* consumer = GetConsumer(consumerName);
    if (!consumer) {
        return false;
    }

    if (!consumer->IsReadable(partitionId)) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "The offset of the partition " << partitionId << " was commited by " << consumerName
                << " but the partition isn't readable");
        return false;
    }

    if (consumer->SetCommittedState(partitionId, generation, cookie)) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "The offset of the partition " << partitionId << " was commited by " << consumerName);

        if (consumer->ProccessReadingFinished(partitionId, ctx)) {
            consumer->Balance(ctx);
        }

        return true;
    }

    return false;
}

void TBalancer::Handle(TEvPQ::TEvReadingPartitionStatusRequest::TPtr& ev, const TActorContext& ctx) {
    auto& r = ev->Get()->Record;

    SetCommittedState(r.GetConsumer(), r.GetPartitionId(), r.GetGeneration(), r.GetCookie(), ctx);
}

void TBalancer::Handle(TEvPersQueue::TEvReadingPartitionStartedRequest::TPtr& ev, const TActorContext& ctx) {
    auto& r = ev->Get()->Record;
    auto partitionId = r.GetPartitionId();

    auto consumer = GetConsumer(r.GetConsumer());
    if (!consumer) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "Received TEvReadingPartitionStartedRequest from unknown consumer " << r.GetConsumer());
        return;
    }

    consumer->StartReading(partitionId, ctx);
}

void TBalancer::Handle(TEvPersQueue::TEvReadingPartitionFinishedRequest::TPtr& ev, const TActorContext& ctx) {
    auto& r = ev->Get()->Record;

    auto consumer = GetConsumer(r.GetConsumer());
    if (!consumer) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "Received TEvReadingPartitionFinishedRequest from unknown consumer " << r.GetConsumer());
        return;
    }

    consumer->FinishReading(ev, ctx);
}

void TBalancer::Handle(TEvPersQueue::TEvPartitionReleased::TPtr& ev, const TActorContext& ctx) {
    const auto& r = ev->Get()->Record;
    const TString& consumerName = r.GetClientId();
    auto partitionId = r.GetPartition();
    TActorId sender = ActorIdFromProto(r.GetPipeClient());

    auto* partitionInfo = GetPartitionInfo(partitionId);
    if (!partitionInfo) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "client " << r.GetClientId() << " pipe " << sender << " got deleted partition " << r);
        return;
    }

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "client " << r.GetClientId() << " released partition from pipe " << sender
            << " session " << r.GetSession() << " partition " << partitionId);

    auto* consumer = GetConsumer(consumerName);
    if (!consumer) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "client " << r.GetClientId() << " pipe " << sender
                << " is not connected and got release partitions request for session " << r.GetSession());
        return;
    }

    if (consumer->Unlock(sender, partitionId, ctx)) {
        consumer->Balance(ctx);
    }
}

void TBalancer::Handle(TEvPQ::TEvWakeupReleasePartition::TPtr &ev, const TActorContext& ctx) {
    auto* msg = ev->Get();
    auto* consumer = GetConsumer(msg->Consumer);
    if (!consumer) {
        return;
    }

    auto* partition = consumer->GetPartitionStatus(msg->PartitionId);
    if (partition->Cookie != msg->Cookie) {
        return;
    }

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "releasing partition " << msg->PartitionId << " of consumer \"" << msg->Consumer << "\" by reading finished timeout");

    consumer->Release(msg->PartitionId, ctx);
}

void TBalancer::Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext& ctx) {
    const TActorId& sender = ev->Get()->ClientId;

    auto it = Sessions.find(sender);
    if (it == Sessions.end()) {
        auto [i, _] = Sessions.emplace(sender, std::make_unique<TSession>(sender));
        it = i;
    }
    auto& session = it->second;
    ++session->ServerActors;

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "pipe " << sender << " connected; active server actors: " << session->ServerActors);
}

void TBalancer::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext& ctx) {
    auto it = Sessions.find(ev->Get()->ClientId);

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "pipe " << ev->Get()->ClientId << " disconnected; active server actors: "
            << (it != Sessions.end() ? it->second->ServerActors : -1));

    if (it != Sessions.end()) {
        auto& session = it->second;
        if (--(session->ServerActors) > 0) {
            return;
        }
        if (!session->Session.empty()) {
            LOG_NOTICE_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    GetPrefix() << "pipe " << ev->Get()->ClientId << " client "
                    << session->ClientId << " disconnected session " << session->Session);

            auto cit = Consumers.find(session->ClientId);
            if (cit != Consumers.end()) {
                auto& consumer = cit->second;
                consumer->UnregisterReadingSession(session.get(), ctx);
                if (consumer->Session.empty()) {
                    Consumers.erase(cit);
                } else {
                    consumer->Balance(ctx);
                }
            }
        } else {
            LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    GetPrefix() << "pipe " << ev->Get()->ClientId << " disconnected no session");

            Sessions.erase(it);
        }
    }
}

void TBalancer::Handle(TEvPersQueue::TEvRegisterReadSession::TPtr& ev, const TActorContext& ctx) {
    const auto& r = ev->Get()->Record;
    auto& consumerName = r.GetClientId();

    TActorId pipe = ActorIdFromProto(r.GetPipeClient());
    LOG_NOTICE_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "consumer \"" << consumerName << "\" register session for pipe " << pipe << " session " << r.GetSession());

    if (consumerName.empty()) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "ignored the session registration with empty consumer name.");
        return;
    }

    if (r.GetSession().empty()) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "ignored the session registration with empty session name.");
        return;
    }

    if (!pipe) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "ignored the session registration with empty PipeClient.");
        return;
    }

    auto jt = Sessions.find(pipe);
    if (jt == Sessions.end()) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "client \"" << consumerName << "\" pipe " << pipe
                        << " is not connected and got register session request for session " << r.GetSession());
        return;
    }

    std::vector<ui32> partitions;
    partitions.reserve(r.GroupsSize());
    for (auto& group : r.GetGroups()) {
        auto partitionId = group - 1;
        if (group == 0 || !GetPartitionInfo(partitionId)) {
            THolder<TEvPersQueue::TEvError> response(new TEvPersQueue::TEvError);
            response->Record.SetCode(NPersQueue::NErrorCode::BAD_REQUEST);
            response->Record.SetDescription(TStringBuilder() << "no group " << group << " in topic " << Topic());
            ctx.Send(ev->Sender, response.Release());
            return;
        }
        partitions.push_back(partitionId);
    }

    auto* session = jt->second.get();
    session->Init(r.GetClientId(), r.GetSession(), ev->Sender, partitions);
    session->ClientNode = r.HasClientNode() ? r.GetClientNode() : "none";
    session->ProxyNodeId = ev->Sender.NodeId();
    session->CreateTimestamp = TAppData::TimeProvider->Now();

    auto it = Consumers.find(consumerName);
    if (it == Consumers.end()) {
        auto [i, _] = Consumers.emplace(consumerName, std::make_unique<TConsumer>(*this, consumerName));
        i->second->InitPartitions(ctx);
        it = i;
    }

    auto* consumer = it->second.get();
    consumer->RegisterReadingSession(session, ctx);
    consumer->Balance(ctx);
}

void TBalancer::Handle(TEvPersQueue::TEvGetReadSessionsInfo::TPtr& ev, const TActorContext& ctx) {
    const auto& r = ev->Get()->Record;

    std::unordered_set<ui32> partitionsRequested;
    partitionsRequested.insert(r.GetPartitions().begin(), r.GetPartitions().end());

    auto response = std::make_unique<TEvPersQueue::TEvReadSessionsInfoResponse>();
    response->Record.SetTabletId(TopicActor.TabletID());

    auto consumer = GetConsumer(r.GetClientId());
    if (consumer) {
        for (auto& [partitionId, _] : GetPartitionsInfo()) {
            if (!partitionsRequested.empty() && !partitionsRequested.contains(partitionId)) {
                continue;
            }

            auto pi = response->Record.AddPartitionInfo();
            pi->SetPartition(partitionId);

            auto* family = consumer->FindFamily(partitionId);
            if (family && family->LockedPartitions.contains(partitionId)) {
                auto* session = family->Session;

                Y_ABORT_UNLESS(session != nullptr);
                pi->SetClientNode(session->ClientNode);
                pi->SetProxyNodeId(session->ProxyNodeId);
                pi->SetSession(session->Session);
                pi->SetTimestamp(session->CreateTimestamp.Seconds());
                pi->SetTimestampMs(session->CreateTimestamp.MilliSeconds());
            } else {
                pi->SetClientNode("");
                pi->SetProxyNodeId(0);
                pi->SetSession("");
                pi->SetTimestamp(0);
                pi->SetTimestampMs(0);
            }
        }

        for (auto& [_, session] : consumer->Session) {
            auto si = response->Record.AddReadSessions();
            si->SetSession(session->Session);

            ActorIdToProto(session->Sender, si->MutableSessionActor());
        }
    }
    ctx.Send(ev->Sender, response.release());
}

TString TBalancer::GetPrefix() const {
    return TStringBuilder() << "balancer: tablet " << TopicActor.TabletID() << " topic " << Topic() << " ";
}

}
