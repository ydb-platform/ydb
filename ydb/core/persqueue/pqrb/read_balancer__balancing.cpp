#include "read_balancer__balancing.h"
#include "read_balancer_log.h"

#include <ydb/core/persqueue/public/utils.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/containers/absl/btree_set.h>

#include <util/system/yassert.h>

#include <algorithm>
#include <array>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PERSQUEUE_READ_BALANCER

namespace NKikimr::NPQ::NBalancing {


struct LowLoadSessionComparator {
    bool operator()(const TSession* lhs, const TSession* rhs) const;
};

using TLowLoadOrderedSessions = absl::btree_set<TSession*, LowLoadSessionComparator>;



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
    if (std::exchange(IgnoreNextStartReading, false)) {
        Y_DEBUG_ABORT_UNLESS(IsInactive(),
            "IgnoreNextStartReading on a partition that is not inactive");
        return false;
    }
    return std::exchange(ReadingFinished, false);
}

bool TPartition::StopReading() {
    ReadingFinished = false;
    IgnoreNextStartReading = false;
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
    IgnoreNextStartReading = false;
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

bool TPartitionFamily::IsReleasing() const {
    return Status == EStatus::Releasing;
}

bool TPartitionFamily::IsCommon() const {
    return SpecialSessions.empty();
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

TString TPartitionFamily::LogPrefix() const {
    TStringBuilder sb;
    sb << Consumer.LogPrefix() << "family " << Id << " status " << Status
        << " partitions [" << JoinRange(", ", Partitions.begin(), Partitions.end()) << "] ";
    if (Session) {
        sb << "session \"" << Session->SessionName << "\" sender " << Session->Sender << " ";
    }
    return sb;
}


void TPartitionFamily::Release(const TActorContext& ctx, ETargetStatus targetStatus) {
    Y_DEBUG_ABORT_UNLESS(IsActive(), "Releasing a family that is not active, family %lu", Id);
    Y_DEBUG_ABORT_UNLESS(Session, "Releasing a family without a session, family %lu", Id);
    if (Status != EStatus::Active) {
        YDB_LOG_CRIT("Releasing the family that isn't active",
            {"logPrefix", LogPrefix()},
            {"debugStr", DebugStr()});
        return;
    }

    if (!Session) {
        YDB_LOG_CRIT("Releasing the family that does not have a session",
            {"logPrefix", LogPrefix()},
            {"debugStr", DebugStr()});
        return;
    }

    YDB_LOG_INFO("Release partitions. Target status",
        {"logPrefix", LogPrefix()},
        {"lockedPartitions", JoinRange(", ", LockedPartitions.begin(), LockedPartitions.end())},
        {"targetStatus", targetStatus});

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

bool TPartitionFamily::Unlock(const TActorId& sender, ui32 partitionId) {
    if (!Session || Session->Pipe != sender) {
        YDB_LOG_DEBUG("Try unlock the partition from other sender",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId});
        return false;
    }

    if (Status != EStatus::Releasing) {
        YDB_LOG_CRIT("Try unlock partition but family status is",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId},
            {"status", Status});
        return false;
    }

    if (!LockedPartitions.erase(partitionId)) {
        YDB_LOG_CRIT("Try unlock partition but partition isn't locked. Locked partitions are",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId},
            {"lockedPartitions", JoinRange(", ", LockedPartitions.begin(), LockedPartitions.end())});
        return false;
    }

    --Session->ReleasingPartitionCount;

    if (!LockedPartitions.empty()) {
        YDB_LOG_DEBUG("Partition was unlocked, but wait",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId},
            {"lockedPartitions", JoinRange(", ", LockedPartitions.begin(), LockedPartitions.end())});
        return false;
    }

    --Session->ReleasingFamilyCount;

    return true;
}

bool TPartitionFamily::Reset(const TActorContext& ctx) {
    return Reset(TargetStatus, ctx);
}

bool TPartitionFamily::Reset(ETargetStatus targetStatus, const TActorContext& ctx) {
    if (Session) {
        Session->Families.erase(this->Id);
        Session = nullptr;
    }

    TargetStatus = ETargetStatus::Free;

    switch (targetStatus) {
        case ETargetStatus::Destroy:
            Destroy();
            return false;

        case ETargetStatus::Free:
            YDB_LOG_TRACE("Is free",
                {"logPrefix", LogPrefix()});

            Status = EStatus::Free;
            AfterRelease();
            AttachReadyWantedPartitions(ctx);
            AssertInvariants();

            return true;

        case ETargetStatus::Merge: {
            Status = EStatus::Free;
            AfterRelease();

            TPartitionFamily* attachTo = this;
            auto it = Consumer.Families.find(MergeTo);
            if (it == Consumer.Families.end()) {
                YDB_LOG_DEBUG("Has been released for merge but target family is not exists",
                    {"logPrefix", LogPrefix()});
            } else {
                auto* targetFamily = it->second.get();
                // Target may still be Active on a session that UnregisterReadingSession
                // already removed from Sessions (Balance re-locked it onto the
                // dying pipe). Detach before MergeFamilies/AttachePartitions
                // asserts ownership.
                if (targetFamily->Session &&
                    !Consumer.Sessions.contains(targetFamily->Session->Pipe))
                {
                    targetFamily->Reset(ETargetStatus::Free, ctx);
                    it = Consumer.Families.find(MergeTo);
                    if (it == Consumer.Families.end()) {
                        Consumer.AttachReadableDescendants(this, ctx);
                        return true;
                    }
                    targetFamily = it->second.get();
                }
                if (targetFamily->CanAttach(Partitions) && targetFamily->CanAttach(WantedPartitions)) {
                    attachTo = Consumer.MergeFamilies(targetFamily, this, ctx).first;
                } else {
                    WantedPartitions.clear();
                    attachTo = targetFamily;
                }
            }
            // Parent families are in one place now, or this family remains after the
            // target died with its session. Readable merge-children must be attached
            // here: ProccessReadingFinished could not attach them while the merge was
            // delayed on a release.
            Consumer.AttachReadableDescendants(attachTo, ctx);

            return true;
        }
    }
}

void TPartitionFamily::Destroy() {
    YDB_LOG_DEBUG("Destroyed",
        {"logPrefix", LogPrefix()});

    if (Session) {
        Session->Families.erase(Id);
    }

    for (auto partitionId : Partitions) {
        Consumer.PartitionMapping.erase(partitionId);
    }
    Consumer.UnreadableFamilies.erase(Id);
    Consumer.FamiliesRequireBalancing.erase(Id);

    auto it = Consumer.Families.find(Id);
    if (it == Consumer.Families.end()) {
        return;
    }

    Y_DEBUG_ABORT_UNLESS(Consumer.DoomedGuardDepth > 0,
        "Destroy family %zu without a doomed-family guard", Id);
    Consumer.DoomedFamilies.push_back(std::move(it->second));
    Consumer.Families.erase(it);
}

void TPartitionFamily::AfterRelease() {
    Consumer.UnreadableFamilies[Id] = this;
    Consumer.FamiliesRequireBalancing.erase(Id);

    for (auto partitionId : Partitions) {
        Consumer.PartitionMapping.erase(partitionId);
    }

    Partitions.clear();
    {
        absl::flat_hash_set<ui32> seen;
        std::vector<ui32> uniqueRoots;
        uniqueRoots.reserve(RootPartitions.size());
        for (auto partitionId : RootPartitions) {
            if (!seen.insert(partitionId).second) {
                continue;
            }
            // Nested reread leaves descendants unreadable while they may still
            // sit in RootPartitions. Restoring them would make the next Balance
            // StartReading lock an unreadable id.
            if (!IsReadable(partitionId)) {
                continue;
            }
            // Finish+Commit during Release CreateFamily's children while they
            // are only in RootPartitions. Remapping them here steals mapping
            // from those families and the next StartReading aborts.
            if (auto it = Consumer.PartitionMapping.find(partitionId);
                it != Consumer.PartitionMapping.end() && it->second != this)
            {
                continue;
            }
            uniqueRoots.push_back(partitionId);
        }
        RootPartitions = std::move(uniqueRoots);
    }
    Partitions.insert(Partitions.end(), RootPartitions.begin(), RootPartitions.end());

    LockedPartitions.clear();

    ClassifyPartitions();
    UpdatePartitionMapping(Partitions);
    for (auto it = WantedPartitions.begin(); it != WantedPartitions.end();) {
        auto mit = Consumer.PartitionMapping.find(*it);
        if (mit != Consumer.PartitionMapping.end() && mit->second != this) {
            WantedPartitions.erase(it++);
        } else {
            ++it;
        }
    }
    // After reducing the number of partitions in the family, the list of reading sessions that can read this family may expand.
    UpdateSpecialSessions();

    for (auto partitionId : Partitions) {
        Y_DEBUG_ABORT_UNLESS(IsReadable(partitionId),
            "AfterRelease restored unreadable partition %u, family %zu", partitionId, Id);
    }
}

void TPartitionFamily::StartReading(TSession& session, const TActorContext& ctx) {
    Y_DEBUG_ABORT_UNLESS(IsFree(), "StartReading requires a free family %lu", Id);
    Y_DEBUG_ABORT_UNLESS(Consumer.Sessions.contains(session.Pipe),
        "StartReading session is not registered, family %lu", Id);
    if (Status != EStatus::Free) {
        YDB_LOG_CRIT("Try start reading but the family status is",
            {"logPrefix", LogPrefix()},
            {"status", Status});
        return;
    }

    YDB_LOG_TRACE("Start reading",
        {"logPrefix", LogPrefix()});

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
    for (auto partitionId : Partitions) {
        if (IsReadable(partitionId)) {
            Y_DEBUG_ABORT_UNLESS(LockedPartitions.contains(partitionId),
                "StartReading did not lock readable partition %u, family %zu", partitionId, Id);
        } else {
            Y_DEBUG_ABORT_UNLESS(!LockedPartitions.contains(partitionId),
                "StartReading locked unreadable partition %u, family %zu", partitionId, Id);
        }
    }
    AssertInvariants();
}

void TPartitionFamily::AttachePartitions(const std::vector<ui32>& partitions, const TActorContext& ctx) {
    YDB_LOG_DEBUG("Attaching partitions",
        {"logPrefix", LogPrefix()},
        {"partitions", JoinRange(", ", partitions.begin(), partitions.end())});

    absl::flat_hash_set<ui32> existedPartitions;
    existedPartitions.insert(Partitions.begin(), Partitions.end());

    std::vector<ui32> newPartitions;
    newPartitions.reserve(partitions.size());
    for (auto partitionId : partitions) {
        if (existedPartitions.contains(partitionId)) {
            continue;
        }
        if (!IsReadable(partitionId)) {
            YDB_LOG_DEBUG("Skip attaching unreadable partition",
                {"logPrefix", LogPrefix()},
                {"partitionId", partitionId});
            continue;
        }

        newPartitions.push_back(partitionId);
        existedPartitions.insert(partitionId);
    }
    if (newPartitions.empty()) {
        return;
    }

    if (IsActive()) {
        Y_DEBUG_ABORT_UNLESS(Session,
            "Attaching partitions to an active family without a session, family %zu", Id);
        if (!Session) {
            YDB_LOG_CRIT("Attaching partitions to an active family without a session",
                {"logPrefix", LogPrefix()});
            return;
        }
        if (!Session->AllPartitionsReadable(newPartitions)) {
            WantedPartitions.insert(newPartitions.begin(), newPartitions.end());
            UpdateSpecialSessions();
            Release(ctx);
            return;
        }

        auto [activePartitionCount, inactivePartitionCount] = ClassifyPartitions(newPartitions);
        ChangePartitionCounters(activePartitionCount, inactivePartitionCount);
        Partitions.insert(Partitions.end(), newPartitions.begin(), newPartitions.end());
        UpdatePartitionMapping(newPartitions);
        AppendUniqueRoots(newPartitions);

        for (auto partitionId : newPartitions) {
            LockPartition(partitionId, ctx);
            WantedPartitions.erase(partitionId);
        }
        LockedPartitions.insert(newPartitions.begin(), newPartitions.end());
    } else if (IsFree()) {
        auto [activePartitionCount, inactivePartitionCount] = ClassifyPartitions(newPartitions);
        ChangePartitionCounters(activePartitionCount, inactivePartitionCount);
        Partitions.insert(Partitions.end(), newPartitions.begin(), newPartitions.end());
        UpdatePartitionMapping(newPartitions);
        AppendUniqueRoots(newPartitions);
        for (auto partitionId : newPartitions) {
            WantedPartitions.erase(partitionId);
        }
    } else if (IsReleasing()) {
        WantedPartitions.insert(newPartitions.begin(), newPartitions.end());
    }

    // Removing sessions wich can't read the family now
    for (auto it = SpecialSessions.begin(); it != SpecialSessions.end();) {
        auto& session = it->second;
        if (session->AllPartitionsReadable(newPartitions)) {
            ++it;
        } else {
            SpecialSessions.erase(it++);
        }
    }
    AssertInvariants();
}

void TPartitionFamily::AttachReadyWantedPartitions(const TActorContext& ctx) {
    if (WantedPartitions.empty()) {
        return;
    }

    std::vector<ui32> ready;
    ready.reserve(WantedPartitions.size());
    for (auto partitionId : WantedPartitions) {
        auto* node = Consumer.GetPartitionGraph().GetPartition(partitionId);
        if (!node) {
            continue;
        }
        bool allParentsInFamily = true;
        for (auto* parent : node->DirectParents) {
            if (!parent) {
                YDB_LOG_WARN("Partition graph has a null DirectParents pointer",
                    {"logPrefix", LogPrefix()},
                    {"partitionId", partitionId});
                allParentsInFamily = false;
                break;
            }
            if (Consumer.FindFamily(parent->Id) != this) {
                allParentsInFamily = false;
                break;
            }
        }
        if (allParentsInFamily) {
            ready.push_back(partitionId);
        }
    }

    if (!ready.empty()) {
        AttachePartitions(ready, ctx);
    }
}

void TPartitionFamily::ActivatePartition(ui32 partitionId) {
    YDB_LOG_DEBUG("Activating partition",
        {"logPrefix", LogPrefix()},
        {"partitionId", partitionId});

    ChangePartitionCounters(1, -1);
}

void TPartitionFamily::InactivatePartition(ui32 partitionId) {
    YDB_LOG_DEBUG("Inactivating partition",
        {"logPrefix", LogPrefix()},
        {"partitionId", partitionId});

    ChangePartitionCounters(-1, 1);
}

void TPartitionFamily::AppendUniqueRoots(const std::vector<ui32>& partitions) {
    absl::flat_hash_set<ui32> seen(RootPartitions.begin(), RootPartitions.end());
    for (auto partitionId : partitions) {
        if (seen.insert(partitionId).second) {
            RootPartitions.push_back(partitionId);
        }
    }
}

void TPartitionFamily::ChangePartitionCounters(ssize_t active, ssize_t inactive) {
    Y_DEBUG_ABORT_UNLESS((ssize_t)ActivePartitionCount + active >= 0,
        "ActivePartitionCount underflow: %zu, active: %ld, family %zu", ActivePartitionCount, (long)active, Id);
    Y_DEBUG_ABORT_UNLESS((ssize_t)InactivePartitionCount + inactive >= 0,
        "InactivePartitionCount underflow: %zu, inactive: %ld, family %zu", InactivePartitionCount, (long)inactive, Id);

    ActivePartitionCount += active;
    InactivePartitionCount += inactive;

    if (IsActive() && Session) {
        Session->ActivePartitionCount += active;
        Session->InactivePartitionCount += inactive;
    }
 }

void TPartitionFamily::Merge(TPartitionFamily* other) {
    YDB_LOG_DEBUG("Merge family with",
        {"logPrefix", LogPrefix()},
        {"debug", other->DebugStr()});

    AFL_ENSURE(this != other)
        ("this_id", Id)("other_id", other->Id)
        ("this_partitions", Partitions.size())("other_partitions", other->Partitions.size());

    Partitions.insert(Partitions.end(), other->Partitions.begin(), other->Partitions.end());
    UpdatePartitionMapping(other->Partitions);
    other->Partitions.clear();

    AppendUniqueRoots(other->RootPartitions);
    other->RootPartitions.clear();

    for (auto partitionId : Partitions) {
        other->WantedPartitions.erase(partitionId);
    }
    WantedPartitions.insert(other->WantedPartitions.begin(), other->WantedPartitions.end());
    other->WantedPartitions.clear();

    LockedPartitions.insert(other->LockedPartitions.begin(), other->LockedPartitions.end());
    other->LockedPartitions.clear();

    ChangePartitionCounters(other->ActivePartitionCount, other->InactivePartitionCount);
    other->ChangePartitionCounters(-other->ActivePartitionCount, -other->InactivePartitionCount);

    UpdateSpecialSessions();

    if (other->IsActive() && other->Session) {
        --other->Session->ActiveFamilyCount;
    }
    AssertInvariants();
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

void TPartitionFamily::AssertInvariants() const {
#ifndef NDEBUG
    auto familyIt = Consumer.Families.find(Id);
    Y_DEBUG_ABORT_UNLESS(familyIt != Consumer.Families.end() && familyIt->second.get() == this,
        "family %zu is not registered in the consumer", Id);

    Y_DEBUG_ABORT_UNLESS(!Partitions.empty(),
        "empty family %zu", Id);

    absl::flat_hash_set<ui32> uniquePartitions;
    uniquePartitions.reserve(Partitions.size());
    for (auto partitionId : Partitions) {
        Y_DEBUG_ABORT_UNLESS(uniquePartitions.insert(partitionId).second,
            "duplicate partition %u in family %zu", partitionId, Id);
        auto mit = Consumer.PartitionMapping.find(partitionId);
        Y_DEBUG_ABORT_UNLESS(mit != Consumer.PartitionMapping.end() && mit->second == this,
            "partition mapping mismatch for %u, family %zu", partitionId, Id);
    }

    absl::flat_hash_set<ui32> uniqueRoots;
    uniqueRoots.reserve(RootPartitions.size());
    for (auto partitionId : RootPartitions) {
        Y_DEBUG_ABORT_UNLESS(uniqueRoots.insert(partitionId).second,
            "duplicate root partition %u in family %zu", partitionId, Id);
        Y_DEBUG_ABORT_UNLESS(uniquePartitions.contains(partitionId) || WantedPartitions.contains(partitionId),
            "root partition %u is not in family %zu", partitionId, Id);
    }

    for (auto partitionId : LockedPartitions) {
        Y_DEBUG_ABORT_UNLESS(uniquePartitions.contains(partitionId),
            "locked partition %u is not in family %zu", partitionId, Id);
    }

    if (IsFree()) {
        Y_DEBUG_ABORT_UNLESS(!Session, "free family %zu has a session", Id);
        Y_DEBUG_ABORT_UNLESS(LockedPartitions.empty(), "free family %zu has locked partitions", Id);
        auto uit = Consumer.UnreadableFamilies.find(Id);
        Y_DEBUG_ABORT_UNLESS(uit != Consumer.UnreadableFamilies.end() && uit->second == this,
            "free family %zu is not in UnreadableFamilies", Id);
    } else {
        Y_DEBUG_ABORT_UNLESS(Session, "family %zu has no session", Id);
        if (Session) {
            auto sit = Consumer.Sessions.find(Session->Pipe);
            Y_DEBUG_ABORT_UNLESS(sit != Consumer.Sessions.end() && sit->second == Session,
                "family %zu session is not registered", Id);
            auto fit = Session->Families.find(Id);
            Y_DEBUG_ABORT_UNLESS(fit != Session->Families.end() && fit->second == this,
                "family %zu is not owned by its session", Id);
        }
    }
#endif
}

TPartition* TPartitionFamily::GetPartition(ui32 partitionId) {
    return Consumer.GetPartition(partitionId);
}

bool TPartitionFamily::PossibleForBalance(TSession* session) {
    if (!session) {
        return false;
    }
    if (Partitions.empty() || !IsLonely()) {
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

template<typename TCollection>
bool TPartitionFamily::CanAttach(const TCollection& partitionsIds) {
    if (partitionsIds.empty()) {
        return true;
    }

    if (Consumer.WithCommonSessions) {
        return true;
    }

    return AnyOf(SpecialSessions, [&](const auto& s) {
        return s.second->AllPartitionsReadable(partitionsIds);
    });
}

template bool TPartitionFamily::CanAttach(const absl::flat_hash_set<ui32>& partitionsIds);
template bool TPartitionFamily::CanAttach(const std::vector<ui32>& partitionsIds);
template bool TPartitionFamily::CanAttach(const std::array<ui32, 1>& partitionsIds);

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
    Y_DEBUG_ABORT_UNLESS(Session, "Lock partition %u without a session, family %lu", partitionId, Id);
    Y_DEBUG_ABORT_UNLESS(IsActive(), "Lock partition %u from a non-active family %lu", partitionId, Id);
    Y_DEBUG_ABORT_UNLESS(IsReadable(partitionId),
        "Lock unreadable partition %u, family %lu", partitionId, Id);
    if (!Session) {
        YDB_LOG_CRIT("Lock partition without a session",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId});
        return;
    }

    auto step = NextStep();

    if (auto* partition = GetPartition(partitionId); partition && partition->IsInactive()) {
        // The session will send ReadingPartitionStarted for this lock. That is
        // membership, not a reread: keep ReadingFinished so children stay readable.
        partition->IgnoreNextStartReading = true;
    }

    YDB_LOG_INFO("Lock partition for generation step",
        {"logPrefix", LogPrefix()},
        {"partitionId", partitionId},
        {"debug", Session->DebugStr()},
        {"tabletGeneration", TabletGeneration()},
        {"step", step});

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
    , WithCommonSessions(false)
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

void TConsumer::EnsurePartition(ui32 partitionId) {
    Partitions.try_emplace(partitionId, TPartition());
}

void TConsumer::RegisterPartition(ui32 partitionId, const TActorContext& ctx) {
    EnsurePartition(partitionId);
    bool pendingApplied = TryApplyPendingInactive(partitionId, ctx);
    if (auto* node = GetPartitionGraph().GetPartition(partitionId)) {
        for (auto* parent : node->DirectParents) {
            if (parent) {
                pendingApplied = TryApplyPendingInactive(parent->Id, ctx) || pendingApplied;
            }
        }
    }
    if (pendingApplied) {
        ScheduleBalance(ctx);
    }
    if (IsReadable(partitionId) && !FindFamily(partitionId)) {
        YDB_LOG_DEBUG("Register readable partition",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId});

        CreateFamily({partitionId}, ctx);
    }
}

void TConsumer::UnregisterPartition(ui32 partitionId, const TActorContext& ctx) {
    BreakUpFamily(partitionId, true, ctx);
}

void  TConsumer::InitPartitions(const TActorContext& ctx) {
    for (auto& [partitionId,_] : Balancer.GetPartitionsInfo()) {
        EnsurePartition(partitionId);
    }
    for (auto& [partitionId,_] : Balancer.GetPartitionsInfo()) {
        RegisterPartition(partitionId, ctx);
    }
}

TPartitionFamily* TConsumer::CreateFamily(std::vector<ui32>&& partitions, const TActorContext& ctx) {
    return CreateFamily(std::move(partitions), TPartitionFamily::EStatus::Free, ctx);
}

TPartitionFamily* TConsumer::CreateFamily(std::vector<ui32>&& partitions, TPartitionFamily::EStatus status, const TActorContext&) {
    Y_DEBUG_ABORT_UNLESS(!partitions.empty(), "Cannot create an empty family, consumer %s", ConsumerName.data());
    auto id = ++NextFamilyId;
    auto [it, _] = Families.emplace(id, std::make_unique<TPartitionFamily>(*this, id, std::move(partitions)));
    auto* family = it->second.get();

    family->Status = status;
    if (status == TPartitionFamily::EStatus::Free) {
        UnreadableFamilies[id] = family;
        family->AssertInvariants();
    }

    YDB_LOG_DEBUG("Family created",
        {"logPrefix", LogPrefix()},
        {"family", family->DebugStr()});

    return family;
}

absl::flat_hash_set<ui32> Intercept(const absl::flat_hash_set<ui32>& values, const std::vector<ui32>& members) {
    absl::flat_hash_set<ui32> result;
    result.reserve(members.size());
    for (auto m : members) {
        if (values.contains(m)) {
            result.insert(m);
        }
    }
    return result;
}

bool IsRoot(const TPartitionGraph::Node* node, const absl::flat_hash_set<ui32>& partitions) {
    if (!node) {
        return false;
    }
    if (node->IsRoot()) {
        return true;
    }
    for (auto* p : node->DirectParents) {
        if (p && partitions.contains(p->Id)) {
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
    TDoomedFamilyGuard doomed(*this);

    std::vector<TPartitionFamily*> newFamilies;

    if (!family->IsLonely()) {
        YDB_LOG_DEBUG("Break up",
            {"logPrefix", LogPrefix()},
            {"family", family->DebugStr()},
            {"partition", partitionId});

        absl::flat_hash_set<ui32> partitions;
        partitions.insert(family->Partitions.begin(), family->Partitions.end());

        if (IsRoot(GetPartitionGraph().GetPartition(partitionId), partitions)) {
            partitions.erase(partitionId);

            absl::flat_hash_set<ui32> processedPartitions;
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
                        if (i) {
                            members.push_back(childId);
                        } else {
                            familiesIntersect = true;
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
                f->RootPartitions.assign(f->Partitions.begin(), f->Partitions.end());
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
                    } else if (f->IsReleasing()) {
                        ++f->Session->ReleasingFamilyCount;
                    }
                }

                newFamilies.push_back(f);
            }

            family->Partitions.clear();
            family->Partitions.push_back(partitionId);
            family->RootPartitions = {partitionId};

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
            YDB_LOG_DEBUG("Can't break up because is not root of family",
                {"logPrefix", LogPrefix()},
                {"family", family->DebugStr()},
                {"partition", partitionId});
        }
    }

    family->WantedPartitions.clear();

    if (destroy) {
        DestroyFamily(family, ctx);
    } else {
        family->UpdateSpecialSessions();
        family->AssertInvariants();
    }
    for (auto* f : newFamilies) {
        if (Families.contains(f->Id)) {
            f->AssertInvariants();
        }
    }

    return !newFamilies.empty();
}

std::vector<TPartitionFamily*> TConsumer::ExtractDescendantsFromFamily(
    TPartitionFamily* family,
    ui32 parentId,
    const TActorContext& ctx)
{
    absl::flat_hash_set<ui32> inFamily(family->Partitions.begin(), family->Partitions.end());
    absl::flat_hash_set<ui32> descendants;
    GetPartitionGraph().Travers(parentId, [&](ui32 childId) {
        if (inFamily.contains(childId)) {
            descendants.insert(childId);
        }
        return true;
    });
    if (descendants.empty()) {
        return {};
    }

    std::vector<TPartitionFamily*> result;
    absl::flat_hash_set<ui32> processed;
    const auto partitions = family->Partitions;
    for (auto id : partitions) {
        if (!descendants.contains(id) || processed.contains(id)) {
            continue;
        }
        if (!IsRoot(GetPartitionGraph().GetPartition(id), descendants)) {
            continue;
        }

        std::vector<ui32> members;
        GetPartitionGraph().Travers(id, [&](auto childId) {
            if (!descendants.contains(childId)) {
                return false;
            }
            if (processed.insert(childId).second) {
                members.push_back(childId);
            }
            return true;
        });
        processed.insert(id);

        bool locked = family->Session && (family->LockedPartitions.contains(id) ||
            std::any_of(members.begin(), members.end(), [family](auto memberId) {
                return family->LockedPartitions.contains(memberId);
            }));
        auto* f = CreateFamily({id}, locked ? family->Status : TPartitionFamily::EStatus::Free, ctx);
        f->TargetStatus = family->TargetStatus;
        f->Partitions.insert(f->Partitions.end(), members.begin(), members.end());
        f->LastPipe = family->LastPipe;
        f->RootPartitions.assign(f->Partitions.begin(), f->Partitions.end());
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
            } else if (f->IsReleasing()) {
                ++f->Session->ReleasingFamilyCount;
            }
        }
        result.push_back(f);
    }

    auto dropDescendants = [&](std::vector<ui32>& ids) {
        ids.erase(std::remove_if(ids.begin(), ids.end(), [&](ui32 id) {
            return descendants.contains(id);
        }), ids.end());
    };
    dropDescendants(family->Partitions);
    // AfterRelease rebuilds Partitions from RootPartitions. Remaining members
    // must survive a session drop; otherwise nested reread + churn loses them.
    family->RootPartitions.assign(family->Partitions.begin(), family->Partitions.end());
    for (auto id : descendants) {
        family->LockedPartitions.erase(id);
        family->WantedPartitions.erase(id);
    }
    family->ClassifyPartitions();
    family->UpdateSpecialSessions();
    family->AssertInvariants();
    for (auto* f : result) {
        f->AssertInvariants();
    }

    return result;
}

std::pair<TPartitionFamily*, bool> TConsumer::MergeFamilies(TPartitionFamily* lhs, TPartitionFamily* rhs, const TActorContext& ctx) {
    Y_DEBUG_ABORT_UNLESS(lhs && rhs, "MergeFamilies with a null family");
    if (!lhs || !rhs || lhs == rhs) {
        return {lhs ? lhs : rhs, false};
    }

    if (lhs->IsFree() && rhs->IsFree() ||
        lhs->IsActive() && rhs->IsActive() && lhs->Session == rhs->Session ||
        lhs->IsReleasing() && rhs->IsReleasing() && lhs->Session == rhs->Session && lhs->TargetStatus == rhs->TargetStatus) {

        lhs->Merge(rhs);
        rhs->Destroy();
        lhs->AttachReadyWantedPartitions(ctx);

        return {lhs, true};
    }

    if (lhs->IsFree() && (rhs->IsActive() || rhs->IsReleasing())) {
        std::swap(lhs, rhs);
    }
    if ((lhs->IsActive() || lhs->IsReleasing()) && rhs->IsFree()) {
        lhs->AttachePartitions(rhs->Partitions, ctx);

        std::vector<ui32> leftover;
        leftover.reserve(rhs->Partitions.size());
        for (auto id : rhs->Partitions) {
            if (FindFamily(id) != lhs) {
                leftover.push_back(id);
            }
        }
        rhs->Partitions = std::move(leftover);
        if (rhs->Partitions.empty()) {
            rhs->Destroy();
        } else {
            rhs->RootPartitions = rhs->Partitions;
            rhs->ClassifyPartitions();
            rhs->AssertInvariants();
        }
        lhs->AttachReadyWantedPartitions(ctx);

        return {lhs, true};
    }

    if (lhs->IsActive() && rhs->IsActive()) { // lhs->Session != rhs->Session
        rhs->Release(ctx);
    }
    if (lhs->IsReleasing() && rhs->IsActive()) {
        std::swap(rhs, lhs);
    }
    if (lhs->IsActive() && rhs->IsReleasing() && rhs->TargetStatus == TPartitionFamily::ETargetStatus::Free) {
        rhs->TargetStatus = TPartitionFamily::ETargetStatus::Merge;
        rhs->MergeTo = lhs->Id;

        return {lhs, false};
    }

    // In this case, one of the families is either already being merged or is being destroyed. In any case, they cannot be merged.

    return {lhs, false};
}

void TConsumer::DestroyFamily(TPartitionFamily* family, const TActorContext& ctx) {
    TDoomedFamilyGuard doomed(*this);

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

void TConsumer::AttachReadableDescendants(TPartitionFamily* family, const TActorContext& ctx) {
    if (!family || AttachingDescendants) {
        return;
    }

    struct TGuard {
        bool& Flag;
        explicit TGuard(bool& flag)
            : Flag(flag)
        {
            Flag = true;
        }
        ~TGuard() {
            Flag = false;
        }
    } guard(AttachingDescendants);

    absl::flat_hash_set<ui32> roots;
    roots.insert(family->RootPartitions.begin(), family->RootPartitions.end());
    roots.insert(family->Partitions.begin(), family->Partitions.end());

    bool needReleaseChildren = false;
    for (auto partitionId : roots) {
        auto* partition = GetPartition(partitionId);
        if (partition && partition->NeedReleaseChildren()) {
            needReleaseChildren = true;
            break;
        }
    }

    absl::flat_hash_set<ui32> descendants;
    for (auto partitionId : roots) {
        GetPartitionGraph().Travers(partitionId, [&](ui32 id) {
            if (!IsReadable(id)) {
                return false;
            }
            if (!roots.contains(id)) {
                descendants.insert(id);
            }
            return true;
        });
    }

    // Hash-set order is not stable. Sort so a chain (root → parent → grandchild)
    // merges in a deterministic survivor family.
    std::vector<ui32> ordered(descendants.begin(), descendants.end());
    std::sort(ordered.begin(), ordered.end());

    for (auto id : ordered) {
        auto* node = GetPartitionGraph().GetPartition(id);
        if (!node) {
            continue;
        }

        bool allParentsMerged = true;
        if (node->DirectParents.size() > 1) {
            for (auto* parent : node->DirectParents) {
                if (!parent) {
                    YDB_LOG_WARN("Partition graph has a null DirectParents pointer",
                        {"logPrefix", LogPrefix()},
                        {"partitionId", id});
                    allParentsMerged = false;
                    continue;
                }
                auto* other = FindFamily(parent->Id);
                if (!other) {
                    allParentsMerged = false;
                    continue;
                }
                if (other != family) {
                    auto [f, ok] = MergeFamilies(family, other, ctx);
                    family = f;
                    allParentsMerged = allParentsMerged && ok;
                }
            }
        }

        if (!allParentsMerged) {
            family->WantedPartitions.insert(id);
            continue;
        }

        if (needReleaseChildren) {
            std::array<ui32, 1> partitionIds{id};
            if (!family->CanAttach(partitionIds)) {
                continue;
            }
            auto* other = FindFamily(id);
            if (other && other != family) {
                auto [f, ok] = MergeFamilies(family, other, ctx);
                family = f;
                if (!ok) {
                    family->WantedPartitions.insert(id);
                }
            } else {
                family->AttachePartitions({id}, ctx);
            }
        } else if (!FindFamily(id)) {
            CreateFamily({id}, ctx);
        }
    }

    family->AttachReadyWantedPartitions(ctx);
}

void TConsumer::RegisterReadingSession(TSession* session, const TActorContext& ctx) {
    YDB_LOG_INFO("Register reading session",
        {"logPrefix", LogPrefix()},
        {"debug", session->DebugStr()});

    Sessions[session->Pipe] = session;

    if (session->WithGroups()) {
        for (auto& [_, family] : Families) {
            if (session->AllPartitionsReadable(family->Partitions) && session->AllPartitionsReadable(family->WantedPartitions)) {
                family->SpecialSessions[session->Pipe] = session;
                FamiliesRequireBalancing[family->Id] = family.get();
            }
        }

        for (auto& partitionId : session->Partitions) {
            // A session that names a merge-child must not get it before all parents
            // are processed: CreateFamily here would make the partition immediately
            // balanceable.
            if (!FindFamily(partitionId) && IsReadable(partitionId)) {
                CreateFamily({partitionId}, ctx);
            }
        }
    } else {
        OrderedSessions.reset();
        WithCommonSessions = true;
    }
}


std::vector<TPartitionFamily*> Snapshot(const absl::flat_hash_map<size_t, std::unique_ptr<TPartitionFamily>>& families) {
    std::vector<TPartitionFamily*> result;
    result.reserve(families.size());

    for (auto& [_, family] : families) {
        result.push_back(family.get());
    }

    return result;
}

void TConsumer::UnregisterReadingSession(TSession* session, const TActorContext& ctx) {
    TDoomedFamilyGuard doomed(*this);

    auto pipe = session->Pipe;
    Sessions.erase(session->Pipe);
    if (!session->WithGroups()) {
        OrderedSessions.reset();
        WithCommonSessions = AnyOf(Sessions, [](const auto s) {
            return !s.second->WithGroups();
        });
    }

    auto resetHeldFamily = [&](TPartitionFamily* family, bool special) {
        auto live = Families.find(family->Id);
        if (live == Families.end() || live->second.get() != family) {
            return;
        }

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

        if (!family->CanAttach(family->WantedPartitions)) {
            targetStatus = TPartitionFamily::ETargetStatus::Destroy;
        }

        const size_t familyId = family->Id;
        if (family->Reset(targetStatus, ctx)) {
            auto it = Families.find(familyId);
            if (it == Families.end()) {
                // Reset(Merge) may destroy this family by attaching it to the target.
                return;
            }
            UnreadableFamilies[familyId] = it->second.get();
            FamiliesRequireBalancing.erase(familyId);
        } else {
            for (auto& r : roots) {
                if (IsReadable(r)) {
                    CreateFamily({r}, ctx);
                }
            }
        }
    };

    std::vector<TPartitionFamily*> held;
    held.reserve(session->Families.size());
    for (auto& [_, family] : session->Families) {
        held.push_back(family);
    }

    if (session->WithGroups()) {
        // A preferred session may sit in SpecialSessions of families it is not
        // currently reading. Scan those families only in this case.
        for (auto* family : Snapshot(Families)) {
            auto live = Families.find(family->Id);
            if (live == Families.end() || live->second.get() != family) {
                continue;
            }

            auto special = family->SpecialSessions.erase(pipe);
            for (auto it = family->SpecialSessions.begin(); it != family->SpecialSessions.end();) {
                if (it->second == session) {
                    family->SpecialSessions.erase(it++);
                } else {
                    ++it;
                }
            }

            if (session == family->Session) {
                resetHeldFamily(family, special);
            }
        }
    } else {
        for (auto* family : held) {
            resetHeldFamily(family, false);
        }
    }
}

bool TConsumer::Unlock(const TActorId& sender, ui32 partitionId, const TActorContext& ctx) {
    TDoomedFamilyGuard doomed(*this);

    auto* family = FindFamily(partitionId);
    if (!family) {
        YDB_LOG_CRIT("Unlocking the partition from unknown family",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId});
        return false;
    }

    if (!family->Unlock(sender, partitionId)) {
        return false;
    }

    // Reset may park this family in DoomedFamilies (Merge/Destroy). Drop it only
    // after Unlock returns to TConsumer, so TPartitionFamily methods never
    // continue on a destroyed this.
    family->Reset(ctx);
    return true;
}

bool TConsumer::IsReadable(ui32 partitionId) {
    if (!ScalingSupport()) {
        return true;
    }

    auto* node = GetPartitionGraph().GetPartition(partitionId);
    if (!node) {
        return false;
    }

    auto parentsProcessed = [&](const auto& parents) {
        for (auto* parent : parents) {
            if (!parent) {
                YDB_LOG_WARN("Partition graph has a null parent pointer",
                    {"logPrefix", LogPrefix()},
                    {"partitionId", partitionId});
                return false;
            }
            if (!IsInactive(parent->Id)) {
                return false;
            }
        }
        return true;
    };

    // DirectParents is the source of truth for a merge child. AllParents also
    // covers grandparents (chained split/merge). An empty AllParents must not
    // be treated as "no parents" — that used to make merge children readable
    // immediately.
    if (!parentsProcessed(node->DirectParents)) {
        return false;
    }
    if (!parentsProcessed(node->AllParents)) {
        return false;
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

TString TConsumer::LogPrefix() const {
    return TStringBuilder() << Balancer.LogPrefix() << "consumer " << ConsumerName << " ";
}

bool TConsumer::SetCommittedState(ui32 partitionId, ui32 generation, ui64 cookie) {
    if (!HasChildren(partitionId)) {
        PendingCommits[partitionId] = TPendingCommit{generation, cookie};
        return false;
    }
    PendingCommits.erase(partitionId);
    return Partitions[partitionId].SetCommittedState(generation, cookie);
}

bool TConsumer::ProccessReadingFinished(ui32 partitionId, bool wasInactive, const TActorContext& ctx) {
    TDoomedFamilyGuard doomed(*this);

    if (!ScalingSupport()) {
        return false;
    }

    auto& partition = Partitions[partitionId];

    auto* family = FindFamily(partitionId);
    if (!family) {
        return false;
    }
    if (!wasInactive) {
        family->InactivatePartition(partitionId);
    }

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
        YDB_LOG_DEBUG("Attache partitions",
            {"logPrefix", LogPrefix()},
            {"newPartitions", JoinRange(", ", newPartitions.begin(), newPartitions.end())},
            {"family", family->DebugStr()});
        for (auto id : newPartitions) {
            std::array<ui32, 1> partitionIds{id};
            if (family->CanAttach(partitionIds)) {
                auto* node = GetPartitionGraph().GetPartition(id);
                if (!node) {
                    continue;
                }
                bool allParentsMerged = true;
                if (node->DirectParents.size() > 1) {
                    // The partition was obtained as a result of the merge.
                    for (auto* c : node->DirectParents) {
                        if (!c) {
                            allParentsMerged = false;
                            continue;
                        }
                        auto* other = FindFamily(c->Id);
                        if (!other) {
                            allParentsMerged = false;
                            continue;
                        }

                        if (other != family) {
                            auto [f, v] = MergeFamilies(family, other, ctx);
                            allParentsMerged = allParentsMerged && v;
                            family = f;
                        }
                    }
                }

                if (allParentsMerged) {
                    auto* other = FindFamily(id);
                    if (other && other != family) {
                        auto [f, v] = MergeFamilies(family, other, ctx);
                        family = f;
                        if (!v) {
                            family->WantedPartitions.insert(id);
                        }
                    } else {
                        family->AttachePartitions({id}, ctx);
                    }
                } else {
                    family->WantedPartitions.insert(id);
                }
            } else {
                YDB_LOG_DEBUG("Can't attache partition",
                    {"logPrefix", LogPrefix()},
                    {"id", id},
                    {"family", family->DebugStr()});
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
    TDoomedFamilyGuard doomed(*this);

    if (!GetPartitionInfo(partitionId)) {
        YDB_LOG_NOTICE("Reading of the partition was started by but partition has been deleted",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId},
            {"consumerName", ConsumerName});
        return;
    }

    auto* partition = GetPartition(partitionId);
    if (!partition) {
        YDB_LOG_NOTICE("Reading of the partition was started by but partition does not exist",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId},
            {"consumerName", ConsumerName});
        return;
    }

    auto wasInactive = partition->IsInactive();
    if (!partition->IgnoreNextStartReading) {
        PendingFinishes.erase(partitionId);
        PendingCommits.erase(partitionId);
    }
    if (partition->StartReading()) {
        if (!ScalingSupport()) {
            return;
        }

        YDB_LOG_DEBUG("Reading of the partition was started by We stop reading from child partitions",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId},
            {"consumerName", ConsumerName});

        auto* family = FindFamily(partitionId);
        if (!family) {
            return;
        }

        if (!family->IsLonely()) {
            BreakUpFamily(family, partitionId, false, ctx);
        } else if (wasInactive) {
            family->ActivatePartition(partitionId);
        }

        // Stop reading children: the parent is being read again, so they are no longer readable.
        // Reset every descendant and destroy each child family once. ActivatePartition per child
        // underflows InactivePartitionCount when several finished descendants share a family:
        // TPartition::Reset() returns NeedReleaseChildren(), which is true for still-active
        // children, so the old loop decremented inactive counters that were already 0.
        absl::flat_hash_set<ui32> descendants;
        absl::flat_hash_set<TPartitionFamily*> childFamilies;
        bool childrenLeftInFamily = false;
        GetPartitionGraph().Travers(partitionId, [&](ui32 childId) {
            descendants.insert(childId);
            if (auto* child = GetPartition(childId)) {
                child->Reset();
            }
            if (auto* f = FindFamily(childId)) {
                if (f == family) {
                    childrenLeftInFamily = true;
                } else {
                    childFamilies.insert(f);
                }
            }
            return true;
        });
        if (childrenLeftInFamily) {
            // BreakUpFamily only splits when partitionId is a family root. A
            // reread of a nested parent otherwise leaves unreadable descendants
            // in Partitions; the next Balance StartReading would lock them.
            for (auto* f : ExtractDescendantsFromFamily(family, partitionId, ctx)) {
                childFamilies.insert(f);
            }
            family->ClassifyPartitions();
        }
        // After BreakUp of a merge family {0,1,2}, the leftover is {1,2}: 2 is a
        // descendant of 0, but 1 is a sibling parent. Destroying that family
        // orphans both. Pull descendants out and destroy only those families.
        absl::flat_hash_set<TPartitionFamily*> toDestroy;
        for (auto* f : childFamilies) {
            if (!Families.contains(f->Id)) {
                continue;
            }
            const bool leftover = std::any_of(f->Partitions.begin(), f->Partitions.end(),
                [&](ui32 id) { return !descendants.contains(id); });
            if (leftover) {
                for (auto* extracted : ExtractDescendantsFromFamily(f, partitionId, ctx)) {
                    toDestroy.insert(extracted);
                }
            } else {
                toDestroy.insert(f);
            }
        }
        for (auto* f : toDestroy) {
            if (Families.contains(f->Id)) {
                DestroyFamily(f, ctx);
            }
        }
        if (Families.contains(family->Id)) {
            family->AssertInvariants();
        }
    }
}

TString GetSdkDebugString0(bool scaleAwareSDK) {
    return scaleAwareSDK ? "ScaleAwareSDK" : "old SDK";
}

void TConsumer::FinishReading(TEvPersQueue::TEvReadingPartitionFinishedRequest::TPtr& ev, const TActorContext& ctx) {
    TDoomedFamilyGuard doomed(*this);

    auto& r = ev->Get()->Record;
    auto partitionId = r.GetPartitionId();

    EnsurePartition(partitionId);

    const auto* node = GetPartitionGraph().GetPartition(partitionId);
    if (!HasChildren(partitionId)) {
        PendingFinishes[partitionId] = TPendingFinish{
            r.GetScaleAwareSDK(),
            r.GetStartedReadingFromEndOffset(),
        };
        YDB_LOG_DEBUG("Reading of the partition was finished before children are known; pending",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId},
            {"consumer", r.GetConsumer()},
            {"hasNode", static_cast<bool>(node)});
        return;
    }

    PendingFinishes.erase(partitionId);

    if (ApplyFinishedState(partitionId, r.GetScaleAwareSDK(), r.GetStartedReadingFromEndOffset(), ctx)) {
        ScheduleBalance(ctx);
    }
}

bool TConsumer::HasChildren(ui32 partitionId) const {
    const auto* node = GetPartitionGraph().GetPartition(partitionId);
    return node && !node->DirectChildren.empty();
}

bool TConsumer::ApplyFinishedState(ui32 partitionId, bool scaleAwareSDK, bool startedReadingFromEndOffset, const TActorContext& ctx) {
    auto* partitionPtr = GetPartition(partitionId);
    if (!partitionPtr) {
        return false;
    }
    auto& partition = *partitionPtr;

    const bool wasInactive = partition.IsInactive();
    if (partition.SetFinishedState(scaleAwareSDK, startedReadingFromEndOffset) || wasInactive) {
        YDB_LOG_DEBUG("Reading of the partition was finished by",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId},
            {"consumer", ConsumerName},
            {"firstMessage", startedReadingFromEndOffset},
            {"scaleAwareSdk", GetSdkDebugString0(scaleAwareSDK)});

        return ProccessReadingFinished(partitionId, wasInactive, ctx);
    } else if (!partition.IsInactive()) {
        auto delay = std::min<size_t>(1ul << partition.Iteration, Balancer.GetLifetimeSeconds());

        YDB_LOG_DEBUG("Reading of the partition was finished by Scheduled release of the partition for re-reading. seconds",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId},
            {"consumer", ConsumerName},
            {"delay", delay},
            {"firstMessage", startedReadingFromEndOffset},
            {"scaleAwareSdk", GetSdkDebugString0(scaleAwareSDK)});

        ctx.Schedule(TDuration::Seconds(delay), new TEvPQ::TEvWakeupReleasePartition(ConsumerName, partitionId, partition.Cookie));
    }
    return false;
}

bool TConsumer::TryApplyPendingInactive(ui32 partitionId, const TActorContext& ctx) {
    if (!HasChildren(partitionId)) {
        return false;
    }

    EnsurePartition(partitionId);

    bool changed = false;

    if (auto it = PendingCommits.find(partitionId); it != PendingCommits.end()) {
        const auto pending = it->second;
        PendingCommits.erase(it);
        const bool wasInactive = IsInactive(partitionId);
        if (Partitions[partitionId].SetCommittedState(pending.Generation, pending.Cookie)) {
            YDB_LOG_DEBUG("Applying pending commit after children appeared",
                {"logPrefix", LogPrefix()},
                {"partitionId", partitionId});
            changed = ProccessReadingFinished(partitionId, wasInactive, ctx) || changed;
        }
    }

    if (auto it = PendingFinishes.find(partitionId); it != PendingFinishes.end()) {
        const auto pending = it->second;
        PendingFinishes.erase(it);
        YDB_LOG_DEBUG("Applying pending finish after children appeared",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId});
        changed = ApplyFinishedState(partitionId, pending.ScaleAwareSDK, pending.StartedReadingFromEndOffset, ctx) || changed;
    }

    return changed;
}

void TConsumer::ScheduleBalance(const TActorContext& ctx) {
    if (BalanceScheduled) {
        YDB_LOG_TRACE("Rebalancing already was scheduled",
            {"logPrefix", LogPrefix()});
        return;
    }

    BalanceScheduled = true;

    YDB_LOG_DEBUG("Rebalancing was scheduled",
        {"logPrefix", LogPrefix()});

    ctx.Send(Balancer.TopicActor.SelfId(), new TEvPQ::TEvBalanceConsumer(ConsumerName));
}

template<typename TSessions, typename TPredicate>
TLowLoadOrderedSessions OrderSessions(const TSessions& values, TPredicate predicate) {
    TLowLoadOrderedSessions result;
    for (auto& [_, v] : values) {
        if (predicate(v)) {
            result.insert(v);
        }
    }

    return result;
}

template<typename TSessions>
TLowLoadOrderedSessions OrderSessions(const TSessions& values) {
    return OrderSessions(values, [](const TSession*) { return true; });
}

TString DebugStr(const absl::flat_hash_map<size_t, TPartitionFamily*>& values) {
    TStringBuilder sb;
    for (auto& [id, family] : values) {
        sb << id << " (" << JoinRange(", ", family->Partitions.begin(), family->Partitions.end()) << "), ";
    }
    return sb;
}

std::vector<TPartitionFamily*> OrderFamilies(
    const absl::flat_hash_map<size_t, TPartitionFamily*>& values
) {
    std::vector<TPartitionFamily*> result;
    result.reserve(values.size());
    for (auto& [_, v] : values) {
        result.push_back(v);
    }
    std::sort(result.begin(), result.end(), TPartitionFamilyComparator{});
    return result;
}

template<typename TFamilies, typename TPredicate>
size_t GetStatistics(const TFamilies& values, TPredicate predicate) {
    size_t count = 0;

    for (auto& [_, family] : values) {
        if (predicate(family.get())) {
            ++count;
        }
    }

    return count;
}

void TConsumer::Balance(const TActorContext& ctx) {
    TDoomedFamilyGuard doomed(*this);

    YDB_LOG_DEBUG("Balancing",
        {"logPrefix", LogPrefix()},
        {"sessions", Sessions.size()},
        {"families", Families.size()},
        {"unreadableFamilies", UnreadableFamilies.size()},
        {"unreadableFamiliesDebug", DebugStr(UnreadableFamilies)},
        {"requireBalancing", FamiliesRequireBalancing.size()},
        {"familiesRequireBalancingDebug", DebugStr(FamiliesRequireBalancing)});

    if (Sessions.empty()) {
        return;
    }

    auto startTime = TAppData::TimeProvider->Now();

    // We try to balance the partitions by sessions that clearly want to read them, even if the distribution is not uniform.
    for (auto& [_, family] : Families) {
        if (family->Status != TPartitionFamily::EStatus::Active || family->IsCommon()) {
            continue;
        }
        if (!family->Session || !family->SpecialSessions.contains(family->Session->Pipe)) {
            YDB_LOG_DEBUG("Rebalance because exists the special session for it",
                {"logPrefix", LogPrefix()},
                {"family", family->DebugStr()});
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
                YDB_LOG_DEBUG("Balancing of the failed because there are no suitable reading sessions",
                    {"logPrefix", LogPrefix()},
                    {"family", family->DebugStr()});

                continue;
            }

            auto* session = *sit;

            // Reorder sessions
            sessions.erase(sit);

            YDB_LOG_DEBUG("Balancing",
                {"logPrefix", LogPrefix()},
                {"family", family->DebugStr()},
                {"debug", session->DebugStr()});
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

        YDB_LOG_DEBUG("Start rebalancing",
            {"logPrefix", LogPrefix()},
            {"familyCount", familyCount},
            {"sessionCount", commonSessions.size()},
            {"desiredFamilyCount", desiredFamilyCount},
            {"allowPlusOne", allowPlusOne});

        if (!OrderedSessions) {
            OrderedSessions.emplace();
            OrderedSessions->insert(commonSessions.begin(), commonSessions.end());
        }
        for (auto it = OrderedSessions->begin(); it != OrderedSessions->end(); ++it) {
            auto* session = *it;
            auto targerFamilyCount = desiredFamilyCount + (allowPlusOne ? 1 : 0);
            auto families = OrderFamilies(session->Families);
            for (auto fit = families.rbegin(); fit != families.rend() && session->ActiveFamilyCount > targerFamilyCount; ++fit) {
                auto* f = *fit;
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

            if (!family->IsActive() || !family->Session) {
                YDB_LOG_DEBUG("Skip balancing because it is not active",
                    {"logPrefix", LogPrefix()},
                    {"family", family->DebugStr()});

                FamiliesRequireBalancing.erase(it++);
                continue;
            }

            if (!family->SpecialSessions.contains(family->Session->Pipe)) {
                family->Release(ctx);
                FamiliesRequireBalancing.erase(it++);
                continue;
            }

            if (family->Session->ActiveFamilyCount == 1) {
                YDB_LOG_DEBUG("Skip balancing because it is considered a session that does not read anything else",
                    {"logPrefix", LogPrefix()},
                    {"family", family->DebugStr()});

                FamiliesRequireBalancing.erase(it++);
                continue;
            }

            if (family->SpecialSessions.size() <= 1) {
                YDB_LOG_DEBUG("Skip balancing because there are no other suitable reading sessions",
                    {"logPrefix", LogPrefix()},
                    {"family", family->DebugStr()});

                FamiliesRequireBalancing.erase(it++);
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
                FamiliesRequireBalancing.erase(it++);
            } else {
                YDB_LOG_DEBUG("Skip balancing because it is already being read by the best session",
                    {"logPrefix", LogPrefix()},
                    {"family", family->DebugStr()});
                ++it;
            }
        }
    }

    auto duration = TAppData::TimeProvider->Now() - startTime;
    YDB_LOG_DEBUG("Balancing",
        {"logPrefix", LogPrefix()},
        {"duration", duration});
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
template bool TSession::AllPartitionsReadable(const absl::flat_hash_set<ui32>& partitions) const;

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

const absl::flat_hash_map<ui32, TPartitionInfo>& TBalancer::GetPartitionsInfo() const {
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

const absl::flat_hash_map<TString, std::unique_ptr<TConsumer>>& TBalancer::GetConsumers() const {
    return Consumers;
}

const absl::flat_hash_map<TActorId, std::unique_ptr<TSession>, THash<TActorId>>& TBalancer::GetSessions() const {
    return Sessions;
}


void TBalancer::UpdateConfig(const std::vector<ui32>& addedPartitions, const std::vector<ui32>& deletedPartitions, const TActorContext& ctx) {
    YDB_LOG_DEBUG("Updating configuration. Deleted partitions Added partitions",
        {"logPrefix", LogPrefix()},
        {"deletedPartitions", JoinRange(", ", deletedPartitions.begin(), deletedPartitions.end())},
        {"addedPartitions", JoinRange(", ", addedPartitions.begin(), addedPartitions.end())});

    for (auto partitionId : deletedPartitions) {
        for (auto& [_, consumer] : Consumers) {
            consumer->UnregisterPartition(partitionId, ctx);
        }
    }

    for (auto& partitionId : addedPartitions) {
        for (auto& [_, balancingConsumer] : Consumers) {
            balancingConsumer->EnsurePartition(partitionId);
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
        YDB_LOG_DEBUG("The offset of the partition was commited by but the partition isn't readable",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId},
            {"consumerName", consumerName});
        return false;
    }

    auto wasInactive = consumer->IsInactive(partitionId);
    if (consumer->SetCommittedState(partitionId, generation, cookie)) {
        YDB_LOG_DEBUG("The offset of the partition was commited by",
            {"logPrefix", LogPrefix()},
            {"partitionId", partitionId},
            {"consumerName", consumerName});

        if (consumer->ProccessReadingFinished(partitionId, wasInactive, ctx)) {
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
    auto pipeClient = ActorIdFromProto(r.GetPipeClient());

    if (pipeClient && !Sessions.contains(pipeClient)) {
        YDB_LOG_DEBUG("Received TEvReadingPartitionStartedRequest from unknown pipe",
            {"logPrefix", LogPrefix()},
            {"pipeClient", pipeClient});
        return;
    }

    auto consumer = GetConsumer(r.GetConsumer());
    if (!consumer) {
        YDB_LOG_DEBUG("Received TEvReadingPartitionStartedRequest from unknown consumer",
            {"logPrefix", LogPrefix()},
            {"consumer", r.GetConsumer()});
        return;
    }

    consumer->StartReading(partitionId, ctx);
}

void TBalancer::Handle(TEvPersQueue::TEvReadingPartitionFinishedRequest::TPtr& ev, const TActorContext& ctx) {
    auto& r = ev->Get()->Record;
    auto pipeClient = ActorIdFromProto(r.GetPipeClient());

    auto consumer = GetConsumer(r.GetConsumer());
    if (!consumer) {
        YDB_LOG_DEBUG("Received TEvReadingPartitionFinishedRequest from unknown consumer",
            {"logPrefix", LogPrefix()},
            {"consumer", r.GetConsumer()});
        return;
    }

    if (pipeClient && !Sessions.contains(pipeClient)) {
        // The session died, but the consumer still has other sessions. Finish
        // is in-flight from the dying pipe; dropping it used to leave children
        // unreadable until a new session re-read the parent from offset 0.
        YDB_LOG_DEBUG("Received TEvReadingPartitionFinishedRequest from a disconnected pipe; applying while the consumer lives",
            {"logPrefix", LogPrefix()},
            {"pipeClient", pipeClient},
            {"partitionId", r.GetPartitionId()});
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
        YDB_LOG_CRIT("Client pipe got deleted partition",
            {"logPrefix", LogPrefix()},
            {"clientId", r.GetClientId()},
            {"sender", sender},
            {"r", r});
        return;
    }

    YDB_LOG_INFO("Client released partition from pipe session partition",
        {"logPrefix", LogPrefix()},
        {"clientId", r.GetClientId()},
        {"sender", sender},
        {"session", r.GetSession()},
        {"partitionId", partitionId});

    auto* consumer = GetConsumer(consumerName);
    if (!consumer) {
        YDB_LOG_CRIT("Client pipe is not connected and got release partitions request for session",
            {"logPrefix", LogPrefix()},
            {"clientId", r.GetClientId()},
            {"sender", sender},
            {"session", r.GetSession()});
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
        YDB_LOG_DEBUG("Skip releasing partition of consumer by reading finished timeout because offset is commited",
            {"logPrefix", LogPrefix()},
            {"partitionId", msg->PartitionId},
            {"consumer", msg->Consumer});
        return;
    }

    YDB_LOG_INFO("Releasing partition of consumer by reading finished timeout",
        {"logPrefix", LogPrefix()},
        {"partitionId", msg->PartitionId},
        {"consumer", msg->Consumer});

    consumer->Release(msg->PartitionId, ctx);
}

void TBalancer::Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext&) {
    const TActorId& sender = ev->Get()->ClientId;

    auto it = Sessions.find(sender);
    if (it == Sessions.end()) {
        auto [i, _] = Sessions.emplace(sender, std::make_unique<TSession>(sender));
        it = i;
    }
    auto& session = it->second;
    ++session->ServerActors;

    YDB_LOG_INFO("Pipe connected; active server",
        {"logPrefix", LogPrefix()},
        {"sender", sender},
        {"actors", session->ServerActors});
}

void TBalancer::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_DEBUG("Pipe disconnected",
        {"logPrefix", LogPrefix()},
        {"clientId", ev->Get()->ClientId});
    Subscriptions.erase(ev->Get()->ClientId);

    auto it = Sessions.find(ev->Get()->ClientId);

    if (it == Sessions.end()) {
        YDB_LOG_DEBUG("Pipe disconnected but there aren't sessions exists",
            {"logPrefix", LogPrefix()},
            {"clientId", ev->Get()->ClientId});
        return;
    }

    YDB_LOG_INFO("Pipe disconnected; active server",
        {"logPrefix", LogPrefix()},
        {"clientId", ev->Get()->ClientId},
        {"actors", (it != Sessions.end() ? it->second->ServerActors : -1)});

    auto& session = it->second;
    if (--(session->ServerActors) > 0) {
        return;
    }

    if (!session->SessionName.empty()) {
        YDB_LOG_NOTICE("Pipe client disconnected session",
            {"logPrefix", LogPrefix()},
            {"eventClientId", ev->Get()->ClientId},
            {"sessionClientId", session->ClientId},
            {"sessionName", session->SessionName});

        auto* consumer = GetConsumer(session->ClientId);
        if (consumer) {
            consumer->UnregisterReadingSession(session.get(), ctx);

            if (consumer->Sessions.empty()) {
                Notify(consumer->ConsumerName, NKikimrPQ::TEvBalancingSubscribeNotify::FREE, ctx);
                Consumers.erase(consumer->ConsumerName);
            } else {
                consumer->ScheduleBalance(ctx);
            }
        }

        Sessions.erase(it);
    } else {
        YDB_LOG_INFO("Pipe disconnected no session",
            {"logPrefix", LogPrefix()},
            {"clientId", ev->Get()->ClientId});

        Sessions.erase(it);
    }
}

void TBalancer::Handle(TEvPersQueue::TEvRegisterReadSession::TPtr& ev, const TActorContext& ctx) {
    const auto& r = ev->Get()->Record;
    auto& consumerName = r.GetClientId();

    TActorId pipe = ActorIdFromProto(r.GetPipeClient());
    YDB_LOG_NOTICE("Consumer register session for pipe session",
        {"logPrefix", LogPrefix()},
        {"consumerName", consumerName},
        {"pipe", pipe},
        {"session", r.GetSession()});

    if (consumerName.empty()) {
        YDB_LOG_CRIT("Ignored the session registration with empty consumer name",
            {"logPrefix", LogPrefix()});
        return;
    }

    if (r.GetSession().empty()) {
        YDB_LOG_CRIT("Ignored the session registration with empty session name",
            {"logPrefix", LogPrefix()});
        return;
    }

    if (!pipe) {
        YDB_LOG_CRIT("Ignored the session registration with empty Pipe",
            {"logPrefix", LogPrefix()});
        return;
    }

    auto jt = Sessions.find(pipe);
    if (jt == Sessions.end()) {
        YDB_LOG_CRIT("Client pipe is not connected and got register session request for session",
            {"logPrefix", LogPrefix()},
            {"consumerName", consumerName},
            {"pipe", pipe},
            {"session", r.GetSession()});
        return;
    }

    auto* consumerConfig = ::NKikimr::NPQ::GetConsumer(TopicActor.TabletConfig, consumerName);
    if (consumerConfig && consumerConfig->GetType() != ::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING) {
        auto response = std::make_unique<TEvPersQueue::TEvError>();
        response->Record.SetCode(NPersQueue::NErrorCode::BAD_REQUEST);
        response->Record.SetDescription(TStringBuilder() << "consumer \"" << consumerName << "\" is not streaming");
        ctx.Send(ev->Sender, std::move(response));
        return;
    }

    std::vector<ui32> partitions;
    partitions.reserve(r.GroupsSize());
    for (auto& group : r.GetGroups()) {
        auto partitionId = group - 1;
        if (group == 0 || !GetPartitionInfo(partitionId)) {
            auto response = std::make_unique<TEvPersQueue::TEvError>();
            response->Record.SetCode(NPersQueue::NErrorCode::BAD_REQUEST);
            response->Record.SetDescription(TStringBuilder() << "no group " << group << " in topic " << Topic());
            ctx.Send(ev->Sender, std::move(response));
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

        Notify(consumerName, NKikimrPQ::TEvBalancingSubscribeNotify::BALANCING, ctx);
    }

    auto* consumer = it->second.get();
    consumer->RegisterReadingSession(session, ctx);
    consumer->ScheduleBalance(ctx);
}

void TBalancer::Handle(TEvPersQueue::TEvGetReadSessionsInfo::TPtr& ev, const TActorContext& ctx) {
    const auto& r = ev->Get()->Record;

    absl::flat_hash_set<ui32> partitionsRequested;
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
            if (family && family->Session && consumer->Sessions.contains(family->Session->Pipe)
                    && family->LockedPartitions.contains(partitionId)) {
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

void TBalancer::Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& /*ctx*/) {
    const auto& record = ev->Get()->Record;
    for (const auto& partResult : record.GetPartResult()) {
        for (const auto& consumerResult : partResult.GetConsumerResult()) {
            PendingUpdates[partResult.GetPartition()].push_back(TData{partResult.GetGeneration(), partResult.GetCookie(), consumerResult.GetConsumer(), consumerResult.GetReadingFinished()});
        }
    }
}

void TBalancer::ProcessPendingStats(const TActorContext& ctx) {
    YDB_LOG_DEBUG("ProcessPendingStats. PendingUpdates size",
        {"logPrefix", LogPrefix()},
        {"pendingUpdatesSize", PendingUpdates.size()});

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

void TBalancer::Handle(TEvPersQueue::TEvBalancingSubscribe::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    YDB_LOG_DEBUG("Handle TEvPersQueue::TEvBalancingSubscribe",
        {"logPrefix", LogPrefix()},
        {"ev", record.ShortDebugString()});

    auto sender = ActorIdFromProto(record.GetSourceActor());
    auto status = Consumers.contains(record.GetConsumer()) ?
        NKikimrPQ::TEvBalancingSubscribeNotify::BALANCING : NKikimrPQ::TEvBalancingSubscribeNotify::FREE;
    Notify(sender, record.GetConsumer(), status, ctx);

    Subscriptions[ev->Sender].emplace_back(std::move(sender), std::move(*record.MutableConsumer()));
}

void TBalancer::Handle(TEvPersQueue::TEvBalancingUnsubscribe::TPtr& ev, const TActorContext&) {
    auto& record = ev->Get()->Record;
    YDB_LOG_DEBUG("Handle TEvPersQueue::TEvBalancingUnsubscribe",
        {"logPrefix", LogPrefix()},
        {"ev", record.ShortDebugString()});

    auto sender = ActorIdFromProto(record.GetSourceActor());
    auto& consumer = record.GetConsumer();

    auto it = Subscriptions.find(ev->Sender);
    if (it == Subscriptions.end()) {
        return;
    }

    std::vector<TSubscription>& subscriptions = it->second;
    std::vector<TSubscription> actualSubscriptions;
    actualSubscriptions.reserve(subscriptions.size());

    for (auto& [existsSender, existsConsumer] : subscriptions) {
        if (sender == existsSender && consumer == existsConsumer) {
            continue;
        }

        actualSubscriptions.emplace_back(std::move(existsSender), std::move(existsConsumer));
    }

    subscriptions = std::move(actualSubscriptions);
}

void TBalancer::Notify(const TString& consumer, NKikimrPQ::TEvBalancingSubscribeNotify::EStatus status, const TActorContext& ctx) {
    for (auto& [_, subscriptions] : Subscriptions) {
        for (auto& subscription : subscriptions) {
            if (subscription.Consumer == consumer) {
                Notify(subscription.Sender, consumer, status, ctx);
            }
        }
    }
}

void TBalancer::Notify(const TActorId subscriber, const TString& consumer, NKikimrPQ::TEvBalancingSubscribeNotify::EStatus status, const TActorContext& ctx) {
    ctx.Send(subscriber, new TEvPersQueue::TEvBalancingSubscribeNotify(TabletGeneration(), ++NotifyCookie, TopicPath(), consumer, status));
}

TString TBalancer::LogPrefix() const {
    return TStringBuilder() << "[" << TopicActor.TabletID() << "][" << Topic() << "] ";
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
    if (lhs->Partitions.size() != rhs->Partitions.size()) {
        return lhs->Partitions.size() < rhs->Partitions.size();
    }
    if (lhs->Order != rhs->Order) {
        return lhs->Order < rhs->Order;
    }
    return lhs->SessionName < rhs->SessionName;
}

}
