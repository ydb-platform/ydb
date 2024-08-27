#include "read_balancer__balancing.h"

#define DEBUG(message)


namespace NKikimr::NPQ::NBalancing {


struct LowLoadSessionComparator {
    bool operator()(const TSession* lhs, const TSession* rhs) const;
};

using TLowLoadOrderedSessions = std::set<TSession*, LowLoadSessionComparator>;



//
// TPartition
//

bool TPartition::IsInactive() const {
    return Commited || (ReadingFinished && (StartedReadingFromEndOffset || ScaleAwareSDK));
}

bool TPartition::NeedReleaseChildren() const {
     return !(Commited || (ReadingFinished && !ScaleAwareSDK));
}

bool TPartition::BalanceToOtherPipe() const {
    return !Commited && ReadingFinished && !ScaleAwareSDK;
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
    bool previousStatus = IsInactive();

    ScaleAwareSDK = scaleAwareSDK;
    StartedReadingFromEndOffset = startedReadingFromEndOffset;
    ReadingFinished = true;
    ++Cookie;

    bool currentStatus = IsInactive();
    if (currentStatus) {
        Iteration = 0;
    } else {
        ++Iteration;
    }
    return currentStatus && !previousStatus;
}

bool TPartition::Reset() {
    bool result = IsInactive();

    ScaleAwareSDK = false;
    StartedReadingFromEndOffset = false;
    ReadingFinished = false;
    Commited = false;
    ++Cookie;

    return result;
};


//
// TPartitionFamily
//

TPartitionFamily::TPartitionFamily(TConsumer& consumerInfo, size_t id, std::vector<ui32>&& partitions)
    : Consumer(consumerInfo)
    , Id(id)
    , Status(EStatus::Free)
    , TargetStatus(ETargetStatus::Free)
    , RootPartitions(partitions)
    , Partitions(std::move(partitions))
    , Session(nullptr)
    , MergeTo(0)
{
    ClassifyPartitions();
    UpdatePartitionMapping(Partitions);
    UpdateSpecialSessions();
}

bool TPartitionFamily::IsActive() const {
    return Status == EStatus::Active;
}

bool TPartitionFamily::IsFree() const {
    return Status == EStatus::Free;
}

bool TPartitionFamily::IsRelesing() const {
    return Status == EStatus::Releasing;
}

bool TPartitionFamily::IsCommon() const {
    return SpecialSessions.empty();
}

bool TPartitionFamily::IsLonely() const {
    return Partitions.size() == 1;
}

bool TPartitionFamily::HasActivePartitions() const {
    return ActivePartitionCount;
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
    sb << Consumer.GetPrefix() << "family " << Id << " status " << Status
        << " partitions [" << JoinRange(", ", Partitions.begin(), Partitions.end()) << "] ";
    if (Session) {
        sb << "session \"" << Session->SessionName << "\" sender " << Session->Sender << " ";
    }
    return sb;
}


void TPartitionFamily::Release(const TActorContext& ctx, ETargetStatus targetStatus) {
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
    Session->ReleasingPartitionCount += LockedPartitions.size();

    --Session->ActiveFamilyCount;
    ++Session->ReleasingFamilyCount;

    for (auto partitionId : LockedPartitions) {
        ctx.Send(Session->Sender, MakeEvReleasePartition(partitionId).release());
    }
}

bool TPartitionFamily::Unlock(const TActorId& sender, ui32 partitionId, const TActorContext& ctx) {
    if (!Session || Session->Pipe != sender) {
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

    --Session->ReleasingPartitionCount;

    if (!LockedPartitions.empty()) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "partition " << partitionId << " was unlocked but wait else [" << JoinRange(", ", LockedPartitions.begin(), LockedPartitions.end()) << "]");
        return false;
    }

    --Session->ReleasingFamilyCount;

    Reset(ctx);

    return true;
}

bool TPartitionFamily::Reset(const TActorContext& ctx) {
    return Reset(TargetStatus, ctx);
}

bool TPartitionFamily::Reset(ETargetStatus targetStatus, const TActorContext& ctx) {
    Session->Families.erase(this->Id);
    Session = nullptr;

    TargetStatus = ETargetStatus::Free;

    switch (targetStatus) {
        case ETargetStatus::Destroy:
            Destroy(ctx);
            return false;

        case ETargetStatus::Free:
            LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    GetPrefix() << " is free.");

            Status = EStatus::Free;
            AfterRelease();

            return true;

        case ETargetStatus::Merge:
            Status = EStatus::Free;
            AfterRelease();

            auto it = Consumer.Families.find(MergeTo);
            if (it == Consumer.Families.end()) {
                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                        GetPrefix() << " has been released for merge but target family is not exists.");
                return true;
            }
            Consumer.MergeFamilies(it->second.get(), this, ctx);

            return true;
    }
}

void TPartitionFamily::Destroy(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << " destroyed.");

    if (Session) {
        Session->Families.erase(Id);
    }

    for (auto partitionId : Partitions) {
        Consumer.PartitionMapping.erase(partitionId);
    }
    Consumer.UnreadableFamilies.erase(Id);
    Consumer.FamiliesRequireBalancing.erase(Id);
    Consumer.Families.erase(Id);
}

void TPartitionFamily::AfterRelease() {
    Consumer.UnreadableFamilies[Id] = this;
    Consumer.FamiliesRequireBalancing.erase(Id);

    for (auto partitionId : Partitions) {
        Consumer.PartitionMapping.erase(partitionId);
    }

    Partitions.clear();
    Partitions.insert(Partitions.end(), RootPartitions.begin(), RootPartitions.end());

    LockedPartitions.clear();

    ClassifyPartitions();
    UpdatePartitionMapping(Partitions);
    // After reducing the number of partitions in the family, the list of reading sessions that can read this family may expand.
    UpdateSpecialSessions();
}

void TPartitionFamily::StartReading(TSession& session, const TActorContext& ctx) {
    if (Status != EStatus::Free) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "try start reading but the family status is " << Status);
        return;
    }

    LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "start reading");

    Status = EStatus::Active;

    Session = &session;
    Session->Families.try_emplace(this->Id, this);

    Session->ActivePartitionCount += ActivePartitionCount;
    Session->InactivePartitionCount += InactivePartitionCount;

    ++Session->ActiveFamilyCount;

    LastPipe = Session->Pipe;

    for (auto partitionId : Partitions) {
        LockPartition(partitionId, ctx);
    }

    LockedPartitions.insert(Partitions.begin(), Partitions.end());
}

void TPartitionFamily::AttachePartitions(const std::vector<ui32>& partitions, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "attaching partitions [" << JoinRange(", ", partitions.begin(), partitions.end()) << "]");

    std::unordered_set<ui32> existedPartitions;
    existedPartitions.insert(Partitions.begin(), Partitions.end());

    std::vector<ui32> newPartitions;
    newPartitions.reserve(partitions.size());
    for (auto partitionId : partitions) {
        if (existedPartitions.contains(partitionId)) {
            continue;
        }

        newPartitions.push_back(partitionId);
        existedPartitions.insert(partitionId);
    }

    auto [activePartitionCount, inactivePartitionCount] = ClassifyPartitions(newPartitions);
    ChangePartitionCounters(activePartitionCount, inactivePartitionCount);

    if (IsActive()) {
        if (!Session->AllPartitionsReadable(newPartitions)) {
            WantedPartitions.insert(newPartitions.begin(), newPartitions.end());
            UpdateSpecialSessions();
            Release(ctx);
            return;
        }

        for (auto partitionId : newPartitions) {
            LockPartition(partitionId, ctx);
            WantedPartitions.erase(partitionId);
        }

        Partitions.insert(Partitions.end(), newPartitions.begin(), newPartitions.end());
        UpdatePartitionMapping(newPartitions);

        LockedPartitions.insert(newPartitions.begin(), newPartitions.end());
    }

    // Removing sessions wich can't read the family now
    for (auto it = SpecialSessions.begin(); it != SpecialSessions.end();) {
        auto& session = it->second;
        if (session->AllPartitionsReadable(newPartitions)) {
            ++it;
        } else {
            it = SpecialSessions.erase(it);
        }
    }
}

void TPartitionFamily::ActivatePartition(ui32 partitionId) {
    ALOG_DEBUG(NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "activating partition " << partitionId);

    ChangePartitionCounters(1, -1);
}

void TPartitionFamily::InactivatePartition(ui32 partitionId) {
    ALOG_DEBUG(NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "inactivating partition " << partitionId);

    ChangePartitionCounters(-1, 1);
}

 void TPartitionFamily::ChangePartitionCounters(ssize_t active, ssize_t inactive) {
    ActivePartitionCount += active;
    InactivePartitionCount += inactive;

    if (IsActive() && Session) {
        Session->ActivePartitionCount += active;
        Session->InactivePartitionCount += inactive;
    }
 }

void TPartitionFamily::Merge(TPartitionFamily* other) {
    ALOG_DEBUG(NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "merge family with  " << other->DebugStr());

    Y_VERIFY(this != other);

    Partitions.insert(Partitions.end(), other->Partitions.begin(), other->Partitions.end());
    UpdatePartitionMapping(other->Partitions);
    other->Partitions.clear();

    RootPartitions.insert(RootPartitions.end(), other->RootPartitions.begin(), other->RootPartitions.end());
    other->RootPartitions.clear();

    WantedPartitions.insert(other->WantedPartitions.begin(), other->WantedPartitions.end());
    other->WantedPartitions.clear();

    LockedPartitions.insert(other->LockedPartitions.begin(), other->LockedPartitions.end());
    other->LockedPartitions.clear();

    ChangePartitionCounters(other->ActivePartitionCount, other->InactivePartitionCount);
    other->ChangePartitionCounters(-other->ActivePartitionCount, -other->InactivePartitionCount);

    UpdateSpecialSessions();

    if (other->IsActive()) {
        --other->Session->ActiveFamilyCount;
    }
}

TString TPartitionFamily::DebugStr() const {
    TStringBuilder sb;
    sb << "family=" << Id << " (Status=" << Status
            << ", Partitions=[" << JoinRange(", ", Partitions.begin(), Partitions.end()) << "]";
    if (!WantedPartitions.empty()) {
        sb << ", WantedPartitions=[" << JoinRange(", ", WantedPartitions.begin(), WantedPartitions.end()) << "]";
    }
    if (!SpecialSessions.empty()) {
        sb << ", SpecialSessions=" << SpecialSessions.size();
    }
    if (Session) {
        sb << ", Session=" << Session->DebugStr();
    }
    sb << ")";

    return sb;
}

TPartition* TPartitionFamily::GetPartition(ui32 partitionId) {
    return Consumer.GetPartition(partitionId);
}

bool TPartitionFamily::PossibleForBalance(TSession* session) {
    if (!IsLonely()) {
        return true;
    }

    auto partitionId = Partitions.front();
    auto* partition = GetPartition(partitionId);
    if (!partition) {
        return true;
    }

    if (!partition->BalanceToOtherPipe()) {
        return true;
    }

    return session->Pipe != LastPipe;
}


void TPartitionFamily::ClassifyPartitions() {
    auto [activePartitionCount, inactivePartitionCount] = ClassifyPartitions(Partitions);
    ChangePartitionCounters(activePartitionCount - ActivePartitionCount, inactivePartitionCount - InactivePartitionCount);
}

template<typename TPartitions>
std::pair<size_t, size_t> TPartitionFamily::ClassifyPartitions(const TPartitions& partitions) {
    size_t activePartitionCount = 0;
    size_t inactivePartitionCount = 0;

    for (auto partitionId : partitions) {
        auto* partition = GetPartition(partitionId);
        if (IsReadable(partitionId)) {
            if (partition && partition->IsInactive()) {
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
    bool hasChanges = false;

    for (auto& [_, session] : Consumer.Sessions) {
        if (session->WithGroups() && session->AllPartitionsReadable(Partitions) && session->AllPartitionsReadable(WantedPartitions)) {
            auto [_, inserted] = SpecialSessions.try_emplace(session->Pipe, session);
            if (inserted) {
                hasChanges = true;
            }
        }
    }

    if (hasChanges) {
        Consumer.FamiliesRequireBalancing[Id] = this;
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

    r.SetSession(Session->SessionName);
    r.SetTopic(Topic());
    r.SetPath(TopicPath());
    r.SetGeneration(TabletGeneration());
    r.SetClientId(Session->ClientId);
    r.SetCount(1);
    r.SetGroup(partitionId + 1);
    ActorIdToProto(Session->Pipe, r.MutablePipeClient());

    return res;
}

std::unique_ptr<TEvPersQueue::TEvLockPartition> TPartitionFamily::MakeEvLockPartition(ui32 partitionId, ui32 step) const {
    auto res = std::make_unique<TEvPersQueue::TEvLockPartition>();
    auto& r = res->Record;

    r.SetSession(Session->SessionName);
    r.SetPartition(partitionId);
    r.SetTopic(Topic());
    r.SetPath(TopicPath());
    r.SetGeneration(TabletGeneration());
    r.SetStep(step);
    r.SetClientId(Session->ClientId);
    ActorIdToProto(Session->Pipe, res->Record.MutablePipeClient());

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
    , BalanceScheduled(false)
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

TPartition* TConsumer::GetPartition(ui32 partitionId) {
    auto it = Partitions.find(partitionId);
    if (it == Partitions.end()) {
        return nullptr;
    }
    return &it->second;
}

const TPartitionGraph& TConsumer::GetPartitionGraph() const {
    return Balancer.GetPartitionGraph();
}

ui32 TConsumer::NextStep() {
    return Balancer.NextStep();
}

void TConsumer::RegisterPartition(ui32 partitionId, const TActorContext& ctx) {
    auto [_, inserted] = Partitions.try_emplace(partitionId, TPartition());
    if (inserted && IsReadable(partitionId)) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "register readable partition " << partitionId);

        CreateFamily({partitionId}, ctx);
    }
}

void TConsumer::UnregisterPartition(ui32 partitionId, const TActorContext& ctx) {
    BreakUpFamily(partitionId, true, ctx);
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

    family->Status = status;
    if (status == TPartitionFamily::EStatus::Free) {
        UnreadableFamilies[id] = family;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "family created " << family->DebugStr());

    return family;
}

std::unordered_set<ui32> Intercept(std::unordered_set<ui32> values, std::vector<ui32> members) {
    std::unordered_set<ui32> result;
    for (auto m : members) {
        if (values.contains(m)) {
            result.insert(m);
        }
    }
    return result;
}

bool IsRoot(const TPartitionGraph::Node* node, const std::unordered_set<ui32>& partitions) {
    if (node->IsRoot()) {
        return true;
    }
    for (auto* p : node->Parents) {
        if (partitions.contains(p->Id)) {
            return false;
        }
    }
    return true;
}

bool TConsumer::BreakUpFamily(ui32 partitionId, bool destroy, const TActorContext& ctx) {
    auto* family = FindFamily(partitionId);
    if (!family) {
        return false;
    }

    return BreakUpFamily(family, partitionId, destroy, ctx);
}

bool TConsumer::BreakUpFamily(TPartitionFamily* family, ui32 partitionId, bool destroy, const TActorContext& ctx) {
    std::vector<TPartitionFamily*> newFamilies;

    if (!family->IsLonely()) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "break up " << family->DebugStr() << " partition=" << partitionId);

        std::unordered_set<ui32> partitions;
        partitions.insert(family->Partitions.begin(), family->Partitions.end());

        if (IsRoot(GetPartitionGraph().GetPartition(partitionId), partitions)) {
            partitions.erase(partitionId);

            std::unordered_set<ui32> processedPartitions;
            // There are partitions that are contained in two families at once
            bool familiesIntersect = false;

            for (auto id : family->Partitions) {
                if (id == partitionId) {
                    continue;
                }

                if (!IsRoot(GetPartitionGraph().GetPartition(id), partitions)) {
                    continue;
                }

                std::vector<ui32> members;
                GetPartitionGraph().Travers(id, [&](auto childId) {
                    if (partitions.contains(childId)) {
                        auto [_, i] = processedPartitions.insert(childId);
                        if (!i) {
                            familiesIntersect = true;
                        } else {
                            members.push_back(childId);
                        }

                        return true;
                    }
                    return false;
                });

                bool locked = family->Session && (family->LockedPartitions.contains(id) ||
                        std::any_of(members.begin(), members.end(), [family](auto id) { return family->LockedPartitions.contains(id); }));
                auto* f = CreateFamily({id}, locked ? family->Status : TPartitionFamily::EStatus::Free, ctx);
                f->TargetStatus = family->TargetStatus;
                f->Partitions.insert(f->Partitions.end(), members.begin(), members.end());
                f->LastPipe = family->LastPipe;
                f->UpdatePartitionMapping(f->Partitions);
                f->ClassifyPartitions();
                if (locked) {
                    f->LockedPartitions = Intercept(family->LockedPartitions, f->Partitions);

                    f->Session = family->Session;
                    f->Session->Families.try_emplace(f->Id, f);
                    f->Session->ActivePartitionCount += f->ActivePartitionCount;
                    f->Session->InactivePartitionCount += f->InactivePartitionCount;
                    if (f->IsActive()) {
                        ++f->Session->ActiveFamilyCount;
                    } else if (f->IsRelesing()) {
                        ++f->Session->ReleasingFamilyCount;
                    }
                }

                newFamilies.push_back(f);
            }

            family->Partitions.clear();
            family->Partitions.push_back(partitionId);

            auto locked = family->LockedPartitions.contains(partitionId);
            family->LockedPartitions.clear();
            if (locked) {
                family->LockedPartitions.insert(partitionId);
            }

            family->ClassifyPartitions();

            if (familiesIntersect) {
                for (auto* f : newFamilies) {
                    if (f->IsActive()) {
                        f->Release(ctx);
                    }
                }
            }
        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    GetPrefix() << "can't break up " << family->DebugStr() << " because partition=" << partitionId << " is not root of family");
        }
    }

    family->WantedPartitions.clear();

    if (destroy) {
        DestroyFamily(family, ctx);
    } else {
        family->UpdateSpecialSessions();
    }

    return !newFamilies.empty();
}

std::pair<TPartitionFamily*, bool> TConsumer::MergeFamilies(TPartitionFamily* lhs, TPartitionFamily* rhs, const TActorContext& ctx) {
    Y_VERIFY(lhs != rhs);

    if (lhs->IsFree() && rhs->IsFree() ||
        lhs->IsActive() && rhs->IsActive() && lhs->Session == rhs->Session ||
        lhs->IsRelesing() && rhs->IsRelesing() && lhs->Session == rhs->Session && lhs->TargetStatus == rhs->TargetStatus) {

        lhs->Merge(rhs);
        rhs->Destroy(ctx);

        return {lhs, true};
    }

    if (lhs->IsFree() && (rhs->IsActive() || rhs->IsRelesing())) {
        std::swap(lhs, rhs);
    }
    if ((lhs->IsActive() || lhs->IsRelesing()) && rhs->IsFree()) {
        lhs->AttachePartitions(rhs->Partitions, ctx);
        lhs->RootPartitions.insert(lhs->RootPartitions.end(), rhs->Partitions.begin(), rhs->Partitions.end());

        rhs->Partitions.clear();
        rhs->Destroy(ctx);

        return {lhs, true};
    }

    if (lhs->IsActive() && rhs->IsActive()) { // lhs->Session != rhs->Session
        rhs->Release(ctx);
    }
    if (lhs->IsRelesing() && rhs->IsActive()) {
        std::swap(rhs, lhs);
    }
    if (lhs->IsActive() && rhs->IsRelesing() && rhs->TargetStatus == TPartitionFamily::ETargetStatus::Free) {
        rhs->TargetStatus = TPartitionFamily::ETargetStatus::Merge;
        rhs->MergeTo = lhs->Id;

        return {lhs, false};
    }

    // In this case, one of the families is either already being merged or is being destroyed. In any case, they cannot be merged.

    return {lhs, false};
}

void TConsumer::DestroyFamily(TPartitionFamily* family, const TActorContext& ctx) {
    switch(family->Status) {
        case TPartitionFamily::EStatus::Active:
            family->Release(ctx, TPartitionFamily::ETargetStatus::Destroy);
            break;
        case TPartitionFamily::EStatus::Releasing:
            family->TargetStatus = TPartitionFamily::ETargetStatus::Destroy;
            break;
        case TPartitionFamily::EStatus::Free:
            family->Reset(TPartitionFamily::ETargetStatus::Destroy, ctx);
            break;
    }
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

    Sessions[session->Pipe] = session;

    if (session->WithGroups()) {
        for (auto& [_, family] : Families) {
            if (session->AllPartitionsReadable(family->Partitions) && session->AllPartitionsReadable(family->WantedPartitions)) {
                family->SpecialSessions[session->Pipe] = session;
                FamiliesRequireBalancing[family->Id] = family.get();
            }
        }

        for (auto& partitionId : session->Partitions) {
            if (!FindFamily(partitionId)) {
                CreateFamily({partitionId}, ctx);
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
    auto pipe = session->Pipe;
    Sessions.erase(session->Pipe);

    for (auto* family : Snapshot(Families)) {
        auto special = family->SpecialSessions.erase(pipe);

        if (session == family->Session) {
            std::vector<ui32> roots;
            roots.reserve(family->RootPartitions.size());
            roots.insert(roots.end(), family->RootPartitions.begin(), family->RootPartitions.end());

            TPartitionFamily::ETargetStatus targetStatus = family->TargetStatus;
            if (special && family->SpecialSessions.empty()) {
                for (auto& r : roots) {
                    if (!IsReadable(r)) {
                        targetStatus = TPartitionFamily::ETargetStatus::Destroy;
                        break;
                    }
                }
            }
            if (family->Reset(targetStatus, ctx)) {
                UnreadableFamilies[family->Id] = family;
                FamiliesRequireBalancing.erase(family->Id);
            } else {
                for (auto& r : roots) {
                    if (IsReadable(r)) {
                        CreateFamily({r}, ctx);
                    }
                }
            }
        }
    }
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

    auto* node = GetPartitionGraph().GetPartition(partitionId);
    if (!node) {
        return false;
    }

    if (Partitions.empty()) {
        return node->Parents.empty();
    }

    for(auto* parent : node->HierarhicalParents) {
        if (!IsInactive(parent->Id)) {
            return false;
        }
    }

    return true;
}

bool TConsumer::IsInactive(ui32 partitionId) {
    auto* partition = GetPartition(partitionId);
    if (partition) {
        return partition->IsInactive();
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
        return false;
    }
    family->InactivatePartition(partitionId);

    if (!family->IsLonely() && partition.Commited) {
        if (BreakUpFamily(family, partitionId, false, ctx)) {
            return true;
        }
    }

    std::vector<ui32> newPartitions;
    GetPartitionGraph().Travers(partitionId, [&](ui32 id) {
        if (!IsReadable(id)) {
            return false;
        }

        newPartitions.push_back(id);
        return true;
    });

    if (partition.NeedReleaseChildren()) {
        for (auto id : newPartitions) {
            auto* node = GetPartitionGraph().GetPartition(id);
            bool allParentsMerged = true;
            if (node->Parents.size() > 1) {
                // The partition was obtained as a result of the merge.
                for (auto* c : node->Parents) {
                    auto* other = FindFamily(c->Id);
                    if (!other) {
                        allParentsMerged = false;
                        continue;
                    }

                    if (other != family) {
                        auto [f, v] = MergeFamilies(family, other, ctx);
                        allParentsMerged = v;
                        family = f;
                    }
                }
            }

            if (allParentsMerged) {
                auto* other = FindFamily(id);
                if (other && other != family) {
                    auto [f, _] = MergeFamilies(family, other, ctx);
                    family = f;
                } else {
                    family->AttachePartitions({id}, ctx);
                }
            }
        }
    } else {
        for (auto p : newPartitions) {
            auto* f = FindFamily(p);
            if (!f) {
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

    auto* partition = GetPartition(partitionId);

    if (partition && partition->StartReading()) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "Reading of the partition " << partitionId << " was started by " << ConsumerName << ". We stop reading from child partitions.");

        auto* family = FindFamily(partitionId);
        if (!family) {
            return;
        }

        if (!family->IsLonely()) {
            BreakUpFamily(family, partitionId, false, ctx);
            return;
        }

        family->ActivatePartition(partitionId);

        // We releasing all children's partitions because we don't start reading the partition from EndOffset
        GetPartitionGraph().Travers(partitionId, [&](ui32 partitionId) {
            auto* partition = GetPartition(partitionId);
            auto* f = FindFamily(partitionId);

            if (f) {
                if (partition && partition->Reset()) {
                    f->ActivatePartition(partitionId);
                }
                DestroyFamily(f, ctx);
            }

            return true;
        });
    } else {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "Reading of the partition " << partitionId << " was started by " << ConsumerName << ".");
    }
}

TString GetSdkDebugString0(bool scaleAwareSDK) {
    return scaleAwareSDK ? "ScaleAwareSDK" : "old SDK";
}

void TConsumer::FinishReading(TEvPersQueue::TEvReadingPartitionFinishedRequest::TPtr& ev, const TActorContext& ctx) {
    auto& r = ev->Get()->Record;
    auto partitionId = r.GetPartitionId();

    if (!IsReadable(partitionId)) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    GetPrefix() << "Reading of the partition " << partitionId << " was finished by " << ConsumerName
                    << " but the partition isn't readable");
        return;
    }

    auto* family = FindFamily(partitionId);
    if (!family) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    GetPrefix() << "Reading of the partition " << partitionId << " was finished by " << ConsumerName
                    << " but the partition hasn't family");
        return;
    }

    if (!family->Session) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    GetPrefix() << "Reading of the partition " << partitionId << " was finished by " << ConsumerName
                    << " but the partition hasn't reading session");
        return;
    }

    auto& partition = Partitions[partitionId];

    if (partition.SetFinishedState(r.GetScaleAwareSDK(), r.GetStartedReadingFromEndOffset())) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    GetPrefix() << "Reading of the partition " << partitionId << " was finished by " << r.GetConsumer()
                    << ", firstMessage=" << r.GetStartedReadingFromEndOffset() << ", " << GetSdkDebugString0(r.GetScaleAwareSDK()));

        if (ProccessReadingFinished(partitionId, ctx)) {
            ScheduleBalance(ctx);
        }
    } else if (!partition.IsInactive()) {
        auto delay = std::min<size_t>(1ul << partition.Iteration, Balancer.GetLifetimeSeconds()); // TODO use split/merge time

        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    GetPrefix() << "Reading of the partition " << partitionId << " was finished by " << r.GetConsumer()
                    << ". Scheduled release of the partition for re-reading. Delay=" << delay << " seconds,"
                    << " firstMessage=" << r.GetStartedReadingFromEndOffset() << ", " << GetSdkDebugString0(r.GetScaleAwareSDK()));

        ctx.Schedule(TDuration::Seconds(delay), new TEvPQ::TEvWakeupReleasePartition(ConsumerName, partitionId, partition.Cookie));
    }
}

void TConsumer::ScheduleBalance(const TActorContext& ctx) {
    if (BalanceScheduled) {
        LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "rebalancing already was scheduled");
        return;
    }

    BalanceScheduled = true;

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "rebalancing was scheduled");

    ctx.Send(Balancer.TopicActor.SelfId(), new TEvPQ::TEvBalanceConsumer(ConsumerName));
}

TLowLoadOrderedSessions OrderSessions(
    const std::unordered_map<TActorId, TSession*>& values,
    std::function<bool (const TSession*)> predicate = [](const TSession*) { return true; }
) {
    TLowLoadOrderedSessions result;
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

size_t GetStatistics(
    const std::unordered_map<size_t, const std::unique_ptr<TPartitionFamily>>& values,
    std::function<bool (const TPartitionFamily*)> predicate = [](const TPartitionFamily*) { return true; }
) {
    size_t count = 0;

    for (auto& [_, family] : values) {
        if (predicate(family.get())) {
            ++count;
        }
    }

    return count;
}

size_t GetMaxFamilySize(const std::unordered_map<size_t, const std::unique_ptr<TPartitionFamily>>& values) {
    size_t result = 1;
    for (auto& [_, v] : values)  {
        result = std::max(result, v->ActivePartitionCount);
    }
    return result;
}

void TConsumer::Balance(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "balancing. Sessions=" << Sessions.size() << ", Families=" << Families.size()
            << ", UnradableFamilies=" << UnreadableFamilies.size() << " [" << DebugStr(UnreadableFamilies)
            << "], RequireBalancing=" << FamiliesRequireBalancing.size() << " [" << DebugStr(FamiliesRequireBalancing) << "]");

    if (Sessions.empty()) {
        return;
    }

    auto startTime = TInstant::Now();

    // We try to balance the partitions by sessions that clearly want to read them, even if the distribution is not uniform.
    for (auto& [_, family] : Families) {
        if (family->Status != TPartitionFamily::EStatus::Active || family->IsCommon()) {
            continue;
        }
        if (!family->SpecialSessions.contains(family->Session->Pipe)) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "rebalance " << family->DebugStr() << " because exists the special session for it");
            family->Release(ctx);
        }
    }

    TLowLoadOrderedSessions commonSessions = OrderSessions(Sessions, [](auto* session) {
        return !session->WithGroups();
    });

    // Balance unredable families.
    if (!UnreadableFamilies.empty()) {
        auto families = OrderFamilies(UnreadableFamilies);
        for (auto it = families.rbegin(); it != families.rend(); ++it) {
            auto* family = *it;
            TLowLoadOrderedSessions specialSessions;
            auto& sessions = (family->IsCommon()) ? commonSessions : (specialSessions = OrderSessions(family->SpecialSessions));

            auto sit = sessions.begin();
            for (;sit != sessions.end() && sessions.size() > 1 && !family->PossibleForBalance(*sit); ++sit) {
                // Skip unpossible session. If there is only one session, then we always balance in it.
            }

            if (sit == sessions.end()) {
                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                        GetPrefix() << "balancing of the " << family->DebugStr() << " failed because there are no suitable reading sessions.");

                continue;
            }

            auto* session = *sit;

            // Reorder sessions
            sessions.erase(sit);

            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    GetPrefix() << "balancing " << family->DebugStr() << " for " << session->DebugStr());
            family->StartReading(*session, ctx);

            // Reorder sessions
            sessions.insert(session);

            UnreadableFamilies.erase(family->Id);
        }
    }

    // Rebalancing reading sessions with a large number of readable partitions.
    if (!commonSessions.empty()) {
        auto familyCount = GetStatistics(Families, [](auto* family) {
            return family->IsCommon();
        });

        auto desiredFamilyCount = familyCount / commonSessions.size();
        auto allowPlusOne = familyCount % commonSessions.size();

        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "start rebalancing. familyCount=" << familyCount << ", sessionCount=" << commonSessions.size()
                << ", desiredFamilyCount=" << desiredFamilyCount << ", allowPlusOne=" << allowPlusOne);

        TOrderedSessions orderedSession;
        orderedSession.insert(commonSessions.begin(), commonSessions.end());
        for (auto it = orderedSession.begin(); it != orderedSession.end(); ++it) {
            auto* session = *it;
            auto targerFamilyCount = desiredFamilyCount + (allowPlusOne ? 1 : 0);
            auto families = OrderFamilies(session->Families);
            for (auto it = session->Families.begin(); it != session->Families.end() && session->ActiveFamilyCount > targerFamilyCount; ++it) {
                auto* f = it->second;
                if (f->IsActive()) {
                    f->Release(ctx);
                }
            }

            if (allowPlusOne) {
                --allowPlusOne;
            }
        }
    }

    // Rebalancing special sessions
    if (!FamiliesRequireBalancing.empty()) {
        for (auto it = FamiliesRequireBalancing.begin(); it != FamiliesRequireBalancing.end();) {
            auto* family = it->second;

            if (!family->IsActive()) {
                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                        GetPrefix() << "skip balancing " << family->DebugStr() << " because it is not active.");

                it = FamiliesRequireBalancing.erase(it);
                continue;
            }

            if (!family->SpecialSessions.contains(family->Session->Pipe)) {
                family->Release(ctx);
                continue;
            }

            if (family->Session->ActiveFamilyCount == 1) {
                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                        GetPrefix() << "skip balancing " << family->DebugStr() << " because it is considered a session that does not read anything else.");

                it = FamiliesRequireBalancing.erase(it);
                continue;
            }

            if (family->SpecialSessions.size() <= 1) {
                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                        GetPrefix() << "skip balancing " << family->DebugStr() << " because there are no other suitable reading sessions.");

                it = FamiliesRequireBalancing.erase(it);
                continue;
            }

            bool hasGoodestSession = false;
            size_t targetPartitionCount = family->Session->ActiveFamilyCount - 1;
            for (auto [_, s] : family->SpecialSessions) {
                if (s == family->Session) {
                    continue;
                }
                if (s->ActivePartitionCount < targetPartitionCount) {
                    hasGoodestSession = true;
                    break;
                }
            }

            if (hasGoodestSession) {
                family->Release(ctx);
                it = FamiliesRequireBalancing.erase(it);
            } else {
                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                        GetPrefix() << "skip balancing " << family->DebugStr() << " because it is already being read by the best session.");
                ++it;
            }
        }
    }

    auto duration = TInstant::Now() - startTime;
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "balancing duration: " << duration);
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

TSession::TSession(const TActorId& pipe)
            : Pipe(pipe)
            , ServerActors(0)
            , ActivePartitionCount(0)
            , InactivePartitionCount(0)
            , ReleasingPartitionCount(0)
            , ActiveFamilyCount(0)
            , ReleasingFamilyCount(0)
            , Order(RandomNumber<size_t>()) {
}

bool TSession::WithGroups() const { return !Partitions.empty(); }

template<typename TCollection>
bool TSession::AllPartitionsReadable(const TCollection& partitions) const {
    if (WithGroups()) {
        for (auto p : partitions) {
            if (!Partitions.contains(p)) {
                return false;
            }
        }
    }

    return true;
}

template bool TSession::AllPartitionsReadable(const std::vector<ui32>& partitions) const;
template bool TSession::AllPartitionsReadable(const std::unordered_set<ui32>& partitions) const;

TString TSession::DebugStr() const {
    return TStringBuilder() << "ReadingSession \"" << SessionName << "\" (Sender=" << Sender << ", Pipe=" << Pipe
            << ", Partitions=[" << JoinRange(", ", Partitions.begin(), Partitions.end())
            << "], ActiveFamilyCount=" << ActiveFamilyCount << ")";
}


//
// TBalancer
//

TBalancer::TBalancer(TPersQueueReadBalancer& topicActor)
    : TopicActor(topicActor)
    , Step(0) {
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

const std::unordered_map<TString, std::unique_ptr<TConsumer>>& TBalancer::GetConsumers() const {
    return Consumers;
}

const std::unordered_map<TActorId, std::unique_ptr<TSession>>& TBalancer::GetSessions() const {
    return Sessions;
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
        consumer->ScheduleBalance(ctx);
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
            consumer->ScheduleBalance(ctx);
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
        consumer->ScheduleBalance(ctx);
    }
}

void TBalancer::Handle(TEvPQ::TEvWakeupReleasePartition::TPtr &ev, const TActorContext& ctx) {
    auto* msg = ev->Get();
    auto* consumer = GetConsumer(msg->Consumer);
    if (!consumer) {
        return;
    }

    auto* partition = consumer->GetPartition(msg->PartitionId);
    if (!partition || partition->Cookie != msg->Cookie) {
        return;
    }

    if (partition->Commited) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "skip releasing partition " << msg->PartitionId << " of consumer \"" << msg->Consumer << "\" by reading finished timeout because offset is commited");
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

    if (it == Sessions.end()) {
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "pipe " << ev->Get()->ClientId << " disconnected but there aren't sessions exists.");
        return;
    }

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "pipe " << ev->Get()->ClientId << " disconnected; active server actors: "
            << (it != Sessions.end() ? it->second->ServerActors : -1));

    auto& session = it->second;
    if (--(session->ServerActors) > 0) {
        return;
    }

    if (!session->SessionName.empty()) {
        LOG_NOTICE_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "pipe " << ev->Get()->ClientId << " client "
                << session->ClientId << " disconnected session " << session->SessionName);

        auto* consumer = GetConsumer(session->ClientId);
        if (consumer) {
            consumer->UnregisterReadingSession(session.get(), ctx);

            if (consumer->Sessions.empty()) {
                Consumers.erase(consumer->ConsumerName);
            } else {
                consumer->ScheduleBalance(ctx);
            }
        }

        Sessions.erase(it);
    } else {
        LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "pipe " << ev->Get()->ClientId << " disconnected no session");

        Sessions.erase(it);
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
            GetPrefix() << "ignored the session registration with empty Pipe.");
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
    session->ClientId = r.GetClientId();
    session->SessionName = r.GetSession();
    session->Sender = ev->Sender;
    session->Partitions.insert(partitions.begin(), partitions.end());
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
    consumer->ScheduleBalance(ctx);
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
            if (family && family->Session && family->LockedPartitions.contains(partitionId)) {
                auto* session = family->Session;

                pi->SetClientNode(session->ClientNode);
                pi->SetProxyNodeId(session->ProxyNodeId);
                pi->SetSession(session->SessionName);
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

        for (auto& [_, session] : consumer->Sessions) {
            auto si = response->Record.AddReadSessions();
            si->SetSession(session->SessionName);

            ActorIdToProto(session->Sender, si->MutableSessionActor());
        }
    }
    ctx.Send(ev->Sender, response.release());
}

void TBalancer::Handle(TEvPQ::TEvBalanceConsumer::TPtr& ev, const TActorContext& ctx) {
    auto* consumer = GetConsumer(ev->Get()->ConsumerName);
    if (consumer) {
        consumer->BalanceScheduled = false;
        consumer->Balance(ctx);
    }
}

void TBalancer::Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext&) {
    const auto& record = ev->Get()->Record;
    for (const auto& partResult : record.GetPartResult()) {
        for (const auto& consumerResult : partResult.GetConsumerResult()) {
            PendingUpdates[partResult.GetPartition()].push_back(TData{partResult.GetGeneration(), partResult.GetCookie(), consumerResult.GetConsumer(), consumerResult.GetReadingFinished()});
        }
    }
}

void TBalancer::ProcessPendingStats(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "ProcessPendingStats. PendingUpdates size " << PendingUpdates.size());

    GetPartitionGraph().Travers([&](ui32 id) {
        for (auto& d : PendingUpdates[id]) {
            if (d.Commited) {
                SetCommittedState(d.Consumer, id, d.Generation, d.Cookie, ctx);
            }
        }
        return true;
    });

    PendingUpdates.clear();
}

TString TBalancer::GetPrefix() const {
    return TStringBuilder() << "balancer: [" << TopicActor.TabletID() << "] topic " << Topic() << " ";
}

ui32 TBalancer::NextStep() {
    return ++Step;
}


bool TPartitionFamilyComparator::operator()(const TPartitionFamily* lhs, const TPartitionFamily* rhs) const {
    if (lhs->ActivePartitionCount != rhs->ActivePartitionCount) {
        return lhs->ActivePartitionCount < rhs->ActivePartitionCount;
    }
    if (lhs->InactivePartitionCount != rhs->InactivePartitionCount) {
        return lhs->InactivePartitionCount < rhs->InactivePartitionCount;
    }
    return lhs->Id < rhs->Id;
}

bool SessionComparator::operator()(const TSession* lhs, const TSession* rhs) const {
    if (lhs->Order != rhs->Order) {
        return lhs->Order < rhs->Order;
    }
    return lhs->SessionName < rhs->SessionName;
}


bool LowLoadSessionComparator::operator()(const TSession* lhs, const TSession* rhs) const {
    if (lhs->ActiveFamilyCount != rhs->ActiveFamilyCount) {
        return lhs->ActiveFamilyCount < rhs->ActiveFamilyCount;
    }
    if (lhs->ActivePartitionCount != rhs->ActivePartitionCount) {
        return lhs->ActivePartitionCount < rhs->ActivePartitionCount;
    }
    if (lhs->InactivePartitionCount != rhs->InactivePartitionCount) {
        return lhs->InactivePartitionCount < rhs->InactivePartitionCount;
    }
    if (lhs->Partitions.size() != rhs->Partitions.size()) {
        return lhs->Partitions.size() < rhs->Partitions.size();
    }
    if (lhs->Order != rhs->Order) {
        return lhs->Order < rhs->Order;
    }
    return lhs->SessionName < rhs->SessionName;
}

}
