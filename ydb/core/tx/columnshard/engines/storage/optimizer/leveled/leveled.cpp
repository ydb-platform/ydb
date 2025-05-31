#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/changes/general_compaction.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <ydb/library/accessor/accessor.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/system/types.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLeveled {

struct TPortionComparator {
    using is_transparent = void;

    struct TSearchRange {
        NArrow::TSimpleRow Start;
        NArrow::TSimpleRow End;
    };

    bool operator()(const TPortionInfo::TPtr& a, const TPortionInfo::TPtr& b) const {
        auto order = a->IndexKeyEnd().CompareNotNull(b->IndexKeyStart());
        return order == std::partial_ordering::less;
    }

    bool operator()(const TPortionInfo::TPtr& a, const TSearchRange& b) const {
        auto order = a->IndexKeyEnd().CompareNotNull(b.Start);
        return order == std::partial_ordering::less;
    }

    bool operator()(const TSearchRange& a, const TPortionInfo::TPtr& b) const {
        auto order = a.End.CompareNotNull(b->IndexKeyStart());
        return order == std::partial_ordering::less;
    }
};

using TPortionSet = std::set<TPortionInfo::TPtr, TPortionComparator>;

struct TAccumulator {
    const ui32 Level;
    THashMap<ui64, TPortionInfo::TPtr> Portions;
    ui64 TotalBlobBytes = 0;

    explicit TAccumulator(ui32 level)
        : Level(level)
    {}

    bool Empty() const {
        return Portions.empty();
    }

    void Add(const TPortionInfo::TPtr& p) {
        auto it = Portions.find(p->GetPortionId());
        if (Y_UNLIKELY(it != Portions.end())) {
            TotalBlobBytes -= it->second->GetTotalBlobBytes();
        }
        Portions[p->GetPortionId()] = p;
        TotalBlobBytes += p->GetTotalBlobBytes();
    }

    void Remove(const TPortionInfo::TPtr& p) {
        if (Portions.erase(p->GetPortionId())) {
            TotalBlobBytes -= p->GetTotalBlobBytes();
        }
    }
};

struct TLevel : public TPortionSet {
    const ui32 Level;
    mutable bool CheckCompactions = true;

    explicit TLevel(ui32 level)
        : Level(level)
    {}
};

struct TSettings {
    ui32 LevelsBase = 1000;
    unsigned Factor = 10;
    unsigned ExpectedPortionCount = 2;
    ui64 ExpectedPortionSize = 4 * 1024 * 1024;
    // Simulations show enforced factor of 3 gives the best worst case write amplification up to ~100k portions
    unsigned MaxCompactionIntersections = 3;
    ui64 MaxCompactionBytes = 256 * 1024 * 1024;
    unsigned MaxAccumulateCount = 10;
    TDuration MaxAccumulateTime = TDuration::Seconds(60);

    NJson::TJsonValue SettingsJson;

    using TJsonValueHandler = std::function<TConclusionStatus(TSettings&, const NJson::TJsonValue&)>;

    template<class TCallback>
    static std::pair<TStringBuf, TJsonValueHandler> MakeNumberHandler(TStringBuf name, TCallback&& callback) {
        return {
            name,
            [name, callback](TSettings& self, const NJson::TJsonValue& value) {
                if (!value.IsUInteger()) {
                    return TConclusionStatus::Fail(TStringBuilder() << name << " must be a number");
                }
                callback(self, value.GetUInteger());
                return TConclusionStatus::Success();
            }
        };
    }

    template<class TCallback>
    static std::pair<TStringBuf, TJsonValueHandler> MakeDurationHandler(TStringBuf name, TCallback&& callback) {
        return {
            name,
            [name, callback](TSettings& self, const NJson::TJsonValue& value) {
                if (!value.IsString()) {
                    return TConclusionStatus::Fail(TStringBuilder() << name << " must be a duration string");
                }
                TDuration d;
                if (!TDuration::TryParse(value.GetString(), d)) {
                    return TConclusionStatus::Fail(TStringBuilder() << name << " value cannot be parsed as duration: " << value.GetString());
                }
                callback(self, d);
                return TConclusionStatus::Success();
            }
        };
    }

    static inline THashMap<TStringBuf, TJsonValueHandler> JsonValueHandlers = {
        MakeNumberHandler("factor", [](auto& self, auto value) { self.Factor = value; }),
        MakeNumberHandler("expected_portion_count", [](auto& self, auto value) { self.ExpectedPortionCount = value; }),
        MakeNumberHandler("expected_portion_size", [](auto& self, auto value) { self.ExpectedPortionSize = value; }),
        MakeNumberHandler("max_compaction_intersections", [](auto& self, auto value) { self.MaxCompactionIntersections = value; }),
        MakeNumberHandler("max_compaction_bytes", [](auto& self, auto value) { self.MaxCompactionBytes = value; }),
        MakeNumberHandler("max_accumulate_count", [](auto& self, auto value) { self.MaxAccumulateCount = value; }),
        MakeDurationHandler("max_accumulate_time", [](auto& self, auto value) { self.MaxAccumulateTime = value; }),
    };

    void SerializeToProto(NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TLeveledOptimizer& proto) const {
        if (SettingsJson.IsDefined()) {
            proto.SetJson(NJson::WriteJson(SettingsJson));
        }
    }

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TLeveledOptimizer& proto) {
        if (proto.HasJson()) {
            NJson::TJsonValue jsonInfo;
            if (!NJson::ReadJsonFastTree(proto.GetJson(), &jsonInfo)) {
                return TConclusionStatus::Fail("cannot parse previously serialized json");
            }
            return DeserializeFromJson(jsonInfo);
        }
        return TConclusionStatus::Success();
    }

    TConclusionStatus DeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
        if (!jsonInfo.IsMap()) {
            return TConclusionStatus::Fail("settings must be an object");
        }

        for (const auto& [name, value] : jsonInfo.GetMapSafe()) {
            auto it = JsonValueHandlers.find(name);
            if (it == JsonValueHandlers.end()) {
                return TConclusionStatus::Fail(TStringBuilder() << "unknown leveled compaction setting " << name);
            }
            TConclusionStatus status = it->second(*this, value);
            if (!status.IsSuccess()) {
                return status;
            }
        }

        SettingsJson = jsonInfo;

        return TConclusionStatus::Success();
    }
};

class TOptimizerPlanner : public IOptimizerPlanner, private TSettings {
    using TBase = IOptimizerPlanner;

public:
    TOptimizerPlanner(const TInternalPathId pathId, const std::shared_ptr<IStoragesManager>& storagesManager,
            const std::shared_ptr<arrow::Schema>& primaryKeysSchema, const TSettings& settings = {})
        : TBase(pathId)
        , TSettings(settings)
        , StoragesManager(storagesManager)
        , PrimaryKeysSchema(primaryKeysSchema)
    {
    }

    void UpdateSettings(const TSettings& settings) {
        // Update settings
        static_cast<TSettings&>(*this) = settings;
        // Make sure we recheck all levels
        for (auto& level : Levels) {
            level.CheckCompactions = true;
        }
        // Accumulator time might have changed
        AccumulatorTimeExceeded = false;
    }

private:
    NArrow::NMerger::TIntervalPositions GetBucketPositions() const override {
        // We allow and encourage intersections
        return {};
    }

    std::vector<TTaskDescription> DoGetTasksDescription() const override {
        // TODO
        return {};
    }

    bool DoIsLocked(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) const override {
        // This method is never used
        Y_UNUSED(dataLocksManager);
        return false;
    }

    bool IsAccumulatorEmpty() {
        for (const auto& acc : Accumulator) {
            if (!acc.Empty()) {
                return false;
            }
        }
        return true;
    }

    void AddAccumulator(const TPortionInfo::TPtr& p) {
        if (IsAccumulatorEmpty()) {
            AccumulatorTimeExceeded = false;
            AccumulatorLastCompaction = TInstant::Now();
        }
        ui32 compactionLevel = p->GetCompactionLevel();
        while (compactionLevel >= Accumulator.size()) {
            Accumulator.emplace_back(Accumulator.size());
        }
        Accumulator[compactionLevel].Add(p);
    }

    void RemoveAccumulator(const TPortionInfo::TPtr& p) {
        ui32 compactionLevel = p->GetCompactionLevel();
        if (compactionLevel < Accumulator.size()) {
            Accumulator[compactionLevel].Remove(p);
            if (compactionLevel == Accumulator.size()-1) {
                while (Accumulator.size() > 0 && Accumulator.back().Empty()) {
                    Accumulator.pop_back();
                }
            }
        }
    }

    void DoModifyPortions(const THashMap<ui64, TPortionInfo::TPtr>& add, const THashMap<ui64, TPortionInfo::TPtr>& remove) override {
        // Special handling during initialization
        if (Accumulator.empty() && Levels.empty()) {
            // Accumulator[0] should always exist
            Accumulator.emplace_back(0);

            std::vector<TPortionInfo::TPtr> leveled;
            for (const auto& [_, p] : add) {
                ui32 compactionLevel = p->GetCompactionLevel();
                if (compactionLevel < LevelsBase) {
                    // Accumulator portion
                    AddAccumulator(p);
                } else {
                    leveled.push_back(p);
                }
            }

            // Sort by level, then from older to newer portions
            std::sort(leveled.begin(), leveled.end(), [](const auto& a, const auto& b) {
                ui32 alevel = a->GetCompactionLevel();
                ui32 blevel = b->GetCompactionLevel();
                if (alevel != blevel) {
                    return alevel < blevel;
                }
                return a->GetPortionId() < b->GetPortionId();
            });

            // Start adding portions to levels, resetting compaction level when needed
            for (const auto& p : leveled) {
                ui32 target = Levels.size();
                auto range = TPortionComparator::TSearchRange{ p->IndexKeyStart(), p->IndexKeyEnd() };
                while (target > 0) {
                    auto res = Levels[target - 1].equal_range(range);
                    if (res.first != res.second) {
                        // There are some intersections
                        break;
                    }
                    // No intersections: promote to a lower level
                    --target;
                }
                if (Levels.size() == target) {
                    Levels.emplace_back(Levels.size());
                }
                if (p->GetCompactionLevel() != LevelsBase + target) {
                    // FIXME: we need to also remember these portions and request them to move
                    p->MutableMeta().ResetCompactionLevel(LevelsBase + target);
                }
                auto res = Levels[target].insert(p);
                AFL_VERIFY(res.second);
                Levels[target].CheckCompactions = true;
                if (target+1 < Levels.size()) {
                    Levels[target+1].CheckCompactions = true;
                }
            }

            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "leveled compaction: initialized")("accumulator", Accumulator.size())("levels", Levels.size());
            return;
        }

        // Compaction result: remove old portions first
        for (const auto& [_, p] : remove) {
            ui32 compactionLevel = p->GetCompactionLevel();
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "leveled compaction: removing portion")("id", p->GetPortionId())("size", p->GetTotalBlobBytes())("level", compactionLevel);
            if (compactionLevel < LevelsBase) {
                RemoveAccumulator(p);
            } else {
                ui32 target = compactionLevel - LevelsBase;
                if (target < Levels.size()) {
                    auto it = Levels[target].find(p);
                    if (it != Levels[target].end() && (*it)->GetPortionId() == p->GetPortionId()) {
                        Levels[target].erase(it);
                        Levels[target].CheckCompactions = true;
                        if (target+1 < Levels.size()) {
                            Levels[target+1].CheckCompactions = true;
                        }
                    }
                }
            }
        }

        while (Levels.size() > 0 && Levels.back().empty()) {
            Levels.pop_back();
        }

        // Compaction result: add new portions
        for (const auto& [_, p] : add) {
            ui32 compactionLevel = p->GetCompactionLevel();
            if (compactionLevel < LevelsBase && remove.size() > 0 && add.size() >= 2 && add.size() >= ExpectedPortionCount) {
                // We could have miscalculated the number of resulting parts
                compactionLevel = LevelsBase + Levels.size();
            }
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "leveled compaction: adding portion")("id", p->GetPortionId())("size", p->GetTotalBlobBytes())("level", compactionLevel);
            if (compactionLevel < LevelsBase) {
                AddAccumulator(p);
            } else {
                ui32 target = compactionLevel - LevelsBase;
                if (target > Levels.size()) {
                    target = Levels.size();
                }
                auto range = TPortionComparator::TSearchRange{ p->IndexKeyStart(), p->IndexKeyEnd() };
                bool movedUp = false;
                while (target < Levels.size()) {
                    auto res = Levels[target].equal_range(range);
                    if (res.first == res.second) {
                        // No intersections at this level
                        break;
                    }
                    // There are some intersections: move up
                    movedUp = true;
                    ++target;
                }
                while (target > 0 && !movedUp) {
                    auto res = Levels[target - 1].equal_range(range);
                    if (res.first != res.second) {
                        // There are some intersections
                        break;
                    }
                    // No intersections: promote to a lower level
                    --target;
                }
                if (Levels.size() == target) {
                    Levels.emplace_back(Levels.size());
                }
                if (p->GetCompactionLevel() != LevelsBase + target) {
                    // FIXME: we need to also remember these portions and request them to move
                    p->MutableMeta().ResetCompactionLevel(LevelsBase + target);
                }
                auto res = Levels[target].insert(p);
                AFL_VERIFY(res.second);
                Levels[target].CheckCompactions = true;
                if (target+1 < Levels.size()) {
                    Levels[target+1].CheckCompactions = true;
                }
            }
        }
    }

    struct TCompactionCandidate {
        TPortionInfo::TConstPtr Portion;
        TPortionSet::iterator MatchBegin;
        TPortionSet::iterator MatchEnd;
        size_t MatchSize;
        size_t Position;
    };

    bool NeedLevelCompaction(ui32 level) const {
        return Levels[level].CheckCompactions && Levels[level].size() * Factor > Levels[level - 1].size();
    }

    std::shared_ptr<TColumnEngineChanges> GetCompactLevelTask(
            const std::shared_ptr<TGranuleMeta>& granule,
            const std::shared_ptr<NDataLocks::TManager>& locksManager,
            ui32 level, std::vector<TCompactionCandidate>& candidates) const {
        // Next level must be at least Factor times bigger than the previous
        if (!NeedLevelCompaction(level)) {
            return nullptr;
        }

        // Reuse allocated buffer between levels
        candidates.clear();

        auto& current = Levels[level];
        auto& below = Levels[level - 1];

        auto it = current.begin();
        while (it != current.end()) {
            auto itCurrent = it++;
            auto p = *itCurrent;
            if (locksManager->IsLocked(*p, NDataLocks::ELockCategory::Compaction)) {
                current.CheckCompactions = false;
                return nullptr;
            }
            auto range = TPortionComparator::TSearchRange{ p->IndexKeyStart(), p->IndexKeyEnd() };
            auto matched = below.equal_range(range);
            if (matched.first == matched.second) {
                // No intersections with a level below, move this portion there
                // FIXME: we need to actually persist this move
                current.erase(itCurrent);
                current.CheckCompactions = true;
                if (level + 1 < Levels.size()) {
                    Levels[level + 1].CheckCompactions = true;
                }
                p->MutableMeta().ResetCompactionLevel(LevelsBase + level - 1);
                auto res = below.insert(p);
                AFL_VERIFY(res.second);
                below.CheckCompactions = true;
                continue;
            }
            bool skip = false;
            size_t matchSize = 0;
            for (auto it = matched.first; it != matched.second; ++it) {
                if (locksManager->IsLocked(**it, NDataLocks::ELockCategory::Compaction)) {
                    skip = true;
                    break;
                }
                ++matchSize;
            }
            if (skip) {
                continue;
            }
            candidates.push_back({ p, matched.first, matched.second, matchSize, candidates.size() });
        }

        if (!NeedLevelCompaction(level)) {
            return nullptr;
        }

        if (candidates.empty()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "leveled compaction: skipping level (no unlocked candidates to compact)")("level", level);
            Levels[level].CheckCompactions = false;
            return nullptr;
        }

        // We only have candidates that can be compacted, sort them by priority
        std::sort(candidates.begin(), candidates.end(), [](const auto& a, const auto& b) {
            if (a.MatchSize != b.MatchSize) {
                return a.MatchSize < b.MatchSize;
            }
            return a.Position < b.Position;
        });

        if (candidates.front().MatchSize > MaxCompactionIntersections) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "leveled compaction: skipping level (too many intersections)")("level", level);
            Levels[level].CheckCompactions = false;
            return nullptr;
        }

        std::vector<TPortionInfo::TConstPtr> portions;
        portions.reserve(1 + candidates.front().MatchSize);
        portions.push_back(candidates.front().Portion);
        portions.insert(portions.end(), candidates.front().MatchBegin, candidates.front().MatchEnd);
        ui64 compactionBytes = 0;
        for (const auto& p : portions) {
            compactionBytes += p->GetTotalBlobBytes();
        }
        size_t selected = 1;
        size_t index = 1;
        size_t last = 0;

        // Special case: select as many intersections with the same portion as possible
        while (portions.size() < 1000 && compactionBytes < MaxCompactionBytes && index < candidates.size() &&
                candidates[index].MatchSize == 1 &&
                candidates[index].MatchBegin == candidates[last].MatchBegin) {
            portions.push_back(candidates[index].Portion);
            compactionBytes += portions.back()->GetTotalBlobBytes();
            last = index++;
            ++selected;
        }

        // Special case: select as many zig-zag intersections below as necessary
        while (portions.size() < 1000 && compactionBytes < MaxCompactionBytes && index < candidates.size() &&
                // This level could still have the condition violated after compaction
                (Levels[level].size() - selected) * Factor > Levels[level - 1].size() &&
                // We have the same portion below between consecutive intersections
                std::next(candidates[index].MatchBegin) == candidates[last].MatchEnd)
        {
            portions.push_back(candidates[index].Portion);
            compactionBytes += portions.back()->GetTotalBlobBytes();
            for (auto it = std::next(candidates[index].MatchBegin); it != candidates[index].MatchEnd; ++it) {
                portions.push_back(*it);
                compactionBytes += (*it)->GetTotalBlobBytes();
            }
            last = index++;
            ++selected;
        }

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "leveled compaction: compacting level")("level", level)("count", portions.size());

        TSaverContext saverContext(StoragesManager);
        auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, portions, saverContext);
        result->SetTargetCompactionLevel(LevelsBase + level - 1);
        result->SetPortionExpectedSize(ExpectedPortionSize);
        return result;
    }

    bool NeedAccumulatorCompactionByTime() const {
        return AccumulatorTimeExceeded && Accumulator.size() > 0 && Accumulator.front().Portions.size() >= 2;
    }

    bool NeedAccumulatorCompaction(const TAccumulator& acc) const {
        return acc.Portions.size() >= MaxAccumulateCount || acc.TotalBlobBytes >= ExpectedPortionCount * ExpectedPortionSize;
    }

    bool NeedAccumulatorCompaction() const {
        if (NeedAccumulatorCompactionByTime()) {
            return true;
        }
        for (auto& acc : Accumulator) {
            return NeedAccumulatorCompaction(acc);
        }
        return false;
    }

    ui32 GetAccumulatorTargetLevel(ui32 level, ui64 totalBytes) const {
        if (totalBytes >= ExpectedPortionCount * ExpectedPortionSize) {
            // Enough for expected portion count
            return LevelsBase + Levels.size();
        } else {
            // Keep doing size-tiered compactions
            return level + 1;
        }
    }

    std::shared_ptr<TColumnEngineChanges> GetCompactAccumulatorTask(
            const std::shared_ptr<TGranuleMeta>& granule,
            const std::shared_ptr<NDataLocks::TManager>& locksManager,
            ui32 level) const {
        if (level >= Accumulator.size()) {
            return nullptr;
        }

        auto& acc = Accumulator[level];
        if (!NeedAccumulatorCompaction(acc)) {
            return nullptr;
        }

        std::vector<TPortionInfo::TConstPtr> portions;
        portions.reserve(acc.Portions.size());
        ui64 totalBytes = 0;
        for (const auto& [_, p] : acc.Portions) {
            if (locksManager->IsLocked(*p, NDataLocks::ELockCategory::Compaction)) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "leveled compaction: skipping accumulator (portions locked)")("level", level);
                return nullptr;
            }
            portions.push_back(p);
            totalBytes += p->GetTotalBlobBytes();
            if (totalBytes >= MaxCompactionBytes) {
                break;
            }
        }

        AccumulatorTimeExceeded = false;
        AccumulatorLastCompaction = TInstant::Now();

        ui32 compactionLevel = GetAccumulatorTargetLevel(level, totalBytes);

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "leveled compaction: compacting accumulator")("level", level)("count", portions.size())("compactionLevel", compactionLevel);

        TSaverContext saverContext(StoragesManager);
        auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, portions, saverContext);
        result->SetTargetCompactionLevel(compactionLevel);
        result->SetPortionExpectedSize(ExpectedPortionSize);
        return result;
    }

    std::shared_ptr<TColumnEngineChanges> GetCompactAccumulatorByTimeTask(
            const std::shared_ptr<TGranuleMeta>& granule,
            const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
        if (!NeedAccumulatorCompactionByTime()) {
            return nullptr;
        }

        std::vector<TPortionInfo::TConstPtr> portions;
        ui64 totalBytes = 0;
        bool done = false;
        for (auto& acc : Accumulator) {
            for (const auto& [_, p] : acc.Portions) {
                if (locksManager->IsLocked(*p, NDataLocks::ELockCategory::Compaction)) {
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "leveled compaction: skipping accumulator (portions locked)");
                    return nullptr;
                }
                portions.push_back(p);
                totalBytes += p->GetTotalBlobBytes();
                if (totalBytes >= MaxCompactionBytes) {
                    done = true;
                    break;
                }
            }
            if (done) {
                break;
            }
        }

        AccumulatorTimeExceeded = false;
        AccumulatorLastCompaction = TInstant::Now();

        ui32 compactionLevel = LevelsBase + Levels.size();

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "leveled compaction: compacting accumulator")("count", portions.size())("compactionLevel", compactionLevel);

        TSaverContext saverContext(StoragesManager);
        auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, portions, saverContext);
        result->SetTargetCompactionLevel(compactionLevel);
        result->SetPortionExpectedSize(ExpectedPortionSize);
        return result;
    }

    std::shared_ptr<TColumnEngineChanges> GetCompactAccumulatorTask(
            const std::shared_ptr<TGranuleMeta>& granule,
            const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
        if (auto result = GetCompactAccumulatorByTimeTask(granule, locksManager)) {
            return result;
        }

        for (ui32 level = 0; level < Accumulator.size(); ++level) {
            if (auto result = GetCompactAccumulatorTask(granule, locksManager, level)) {
                return result;
            }
        }

        return nullptr;
    }

    std::shared_ptr<TColumnEngineChanges> DoGetOptimizationTask(std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& locksManager) const override {
        // Check accumulator compactions
        if (auto result = GetCompactAccumulatorTask(granule, locksManager)) {
            return result;
        }

        // Check leveled compactions, top to bottom
        if (Levels.size() > 0) {
            std::vector<TCompactionCandidate> candidates;
            for (size_t level = Levels.size() - 1; level > 0; --level) {
                if (auto result = GetCompactLevelTask(granule, locksManager, level, candidates)) {
                    return result;
                }
            }
        }

        // Nothing to compact
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "leveled compaction: nothing to compact");
        return nullptr;
    }

    void DoActualize(const TInstant currentInstant) override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "leveled compaction: actualize called");
        if ((currentInstant - AccumulatorLastCompaction) >= MaxAccumulateTime) {
            AccumulatorTimeExceeded = true;
        }
    }

    TOptimizationPriority DoGetUsefulMetric() const override {
        if (NeedAccumulatorCompaction()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "leveled compaction: returning DoGetUsefulMetric for accumulator");
            return TOptimizationPriority::Critical(Accumulator.size());
        }
        if (Levels.size() > 0) {
            for (size_t level = Levels.size() - 1; level > 0; --level) {
                if (NeedLevelCompaction(level)) {
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "leveled compaction: returning DoGetUsefulMetric for level")("level", level);
                    return TOptimizationPriority::Critical(Levels[level].size() + Levels[level - 1].size());
                }
            }
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "leveled compaction: returning DoGetUsefulMetric zero");
        return TOptimizationPriority::Zero();
    }

    TString DoDebugString() const override {
        return "TODO: DebugString";
    }

    NJson::TJsonValue DoSerializeToJsonVisual() const override {
        return NJson::JSON_NULL;
    }

private:
    std::shared_ptr<IStoragesManager> StoragesManager;
    std::shared_ptr<arrow::Schema> PrimaryKeysSchema;

    // Accumulator accumulates small portions by size-tiered compaction
    mutable std::deque<TAccumulator> Accumulator;
    mutable bool AccumulatorTimeExceeded = false;
    mutable TInstant AccumulatorLastCompaction = {};

    // Leveled compaction, each level has portions without intersections
    mutable std::deque<TLevel> Levels;
};

class TOptimizerPlannerConstructor : public IOptimizerPlannerConstructor {
public:
    static TString GetClassNameStatic() {
        return "leveled";
    }

    TString GetClassName() const override {
        return GetClassNameStatic();
    }

private:
    static inline const TFactory::TRegistrator<TOptimizerPlannerConstructor> Registrator =
        TFactory::TRegistrator<TOptimizerPlannerConstructor>(GetClassNameStatic());

    void DoSerializeToProto(TProto& proto) const override {
        Settings.SerializeToProto(*proto.MutableLeveled());
    }

    bool DoDeserializeFromProto(const TProto& proto) override {
        if (!proto.HasLeveled()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse leveled compaction optimizer from proto")("proto", proto.DebugString());
            return false;
        }
        auto status = Settings.DeserializeFromProto(proto.GetLeveled());
        if (!status.IsSuccess()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse leveled compaction optimizer from proto")("description", status.GetErrorDescription());
            return false;
        }
        return true;
    }

    TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override {
        return Settings.DeserializeFromJson(jsonInfo);
    }

    bool DoApplyToCurrentObject(IOptimizerPlanner& current) const override {
        auto* itemClass = dynamic_cast<TOptimizerPlanner*>(&current);
        if (!itemClass) {
            return false;
        }
        itemClass->UpdateSettings(Settings);
        return true;
    }

    TConclusion<std::shared_ptr<IOptimizerPlanner>> DoBuildPlanner(const TBuildContext& context) const override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "creating leveled compaction optimizer");
        return std::make_shared<TOptimizerPlanner>(context.GetPathId(), context.GetStorages(), context.GetPKSchema(), Settings);
    }

private:
    TSettings Settings;
};

} // namespace NKikimr::NOlap::NStorageOptimizer::NLeveled
