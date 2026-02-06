#include "counters.h"

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
#include <ydb/library/intersection_tree/intersection_tree.h>

#include <ydb/library/accessor/accessor.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/system/types.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

struct TSettings {
    unsigned Factor = 10;
    unsigned MaxLevels = 10;
    unsigned ExpectedPortionCount = 1;
    ui64 ExpectedPortionSize = 4 * 1024 * 1024; // 4MiB
    unsigned MaxAccumulateCount = 10;
    ui64 MaxAccumulatePortionSize = 512 * 1024; // 512KiB
    TDuration MaxAccumulateTime = TDuration::Seconds(60);
    ui64 MaxCompactionBytes = 256 * 1024 * 1024;
    ui32 FullCompactionUntilLevel = 0;
    ui64 FullCompactionMaxBytes = -1;
    bool CompactNextLevelEdges = false;
    std::optional<ui64> NodePortionsCountLimit{};
    bool ShrinkLevel = true;

    NJson::TJsonValue SettingsJson;

    using TJsonValueHandler = std::function<TConclusionStatus(TSettings&, const NJson::TJsonValue&)>;

    template<class TCallback>
    static std::pair<TStringBuf, TJsonValueHandler> MakeBooleanHandler(TStringBuf name, TCallback&& callback) {
        return {
            name,
            [name, callback](TSettings& self, const NJson::TJsonValue& value) {
                if (!value.IsBoolean()) {
                    return TConclusionStatus::Fail(TStringBuilder() << name << " must be a boolean");
                }
                callback(self, value.GetBoolean());
                return TConclusionStatus::Success();
            }
        };
    }

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
        MakeNumberHandler("max_levels", [](auto& self, auto value) { self.MaxLevels = value; }),
        MakeNumberHandler("expected_portion_count", [](auto& self, auto value) { self.ExpectedPortionCount = value; }),
        MakeNumberHandler("expected_portion_size", [](auto& self, auto value) { self.ExpectedPortionSize = value; }),
        MakeNumberHandler("max_accumulate_count", [](auto& self, auto value) { self.MaxAccumulateCount = value; }),
        MakeNumberHandler("max_accumulate_portion_size", [](auto& self, auto value) { self.MaxAccumulatePortionSize = value; }),
        MakeDurationHandler("max_accumulate_time", [](auto& self, auto value) { self.MaxAccumulateTime = value; }),
        MakeNumberHandler("max_compaction_bytes", [](auto& self, auto value) { self.MaxCompactionBytes = value; }),
        MakeNumberHandler("full_compaction_until_level", [](auto& self, auto value) { self.FullCompactionUntilLevel = value; }),
        MakeNumberHandler("full_compaction_max_bytes", [](auto& self, auto value) { self.FullCompactionMaxBytes = value; }),
        MakeBooleanHandler("compact_next_level_edges", [](auto& self, auto value) { self.CompactNextLevelEdges = value; }),
    };

    void SerializeToProto(NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TTilingOptimizer& proto) const {
        if (SettingsJson.IsDefined()) {
            proto.SetJson(NJson::WriteJson(SettingsJson));
        }
    }

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TCompactionPlannerConstructorContainer::TTilingOptimizer& proto) {
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
                return TConclusionStatus::Fail(TStringBuilder() << "unknown tiling compaction setting " << name);
            }
            TConclusionStatus status = it->second(*this, value);
            if (!status.IsSuccess()) {
                return status;
            }
        }

        if (Factor < 2) {
            return TConclusionStatus::Fail("factor must be at least 2");
        }

        if (MaxLevels < 1 || MaxLevels > 10) {
            return TConclusionStatus::Fail("max_levels must be between 1 and 10");
        }

        if (MaxAccumulateCount < 2) {
            return TConclusionStatus::Fail("max_accumulate_count must be at least 2");
        }

        if (ExpectedPortionSize < MaxAccumulatePortionSize * 3) {
            return TConclusionStatus::Fail(TStringBuilder()
                << "expected_portion_size (" << ExpectedPortionSize << ") must be at least "
                << "3x larger than max_accumulate_portion_size (" << MaxAccumulatePortionSize << ")");
        }

        if (MaxAccumulateTime < TDuration::Seconds(15)) {
            return TConclusionStatus::Fail("max_accumulate_time must be at least 15 seconds");
        }

        if (MaxCompactionBytes < ExpectedPortionSize * 4) {
            return TConclusionStatus::Fail(TStringBuilder()
                << "max_compaction_bytes (" << MaxCompactionBytes << ") is too small "
                << "relative to expected_portion_size (" << ExpectedPortionSize << ")");
        }

        SettingsJson = jsonInfo;

        return TConclusionStatus::Success();
    }
};

using TPortionMap = THashMap<ui64, TPortionInfo::TPtr>;

struct TAccumulator {
    const ui32 Level;
    TPortionMap Portions;
    ui64 TotalBlobBytes = 0;

    TPortionMap Compacting;

    TInstant LastCompaction = {};
    bool TimeExceeded = false;

    const TLevelCounters& Counters;

    TAccumulator(ui32 level, const TLevelCounters& counters)
        : Level(level)
        , Counters(counters)
    {}

    bool Empty() const {
        return Portions.empty() && Compacting.empty();
    }

    void OnSettingsChanged(const TSettings&) {
        TimeExceeded = false;
    }

    void Actualize(const TSettings& settings, TInstant now) {
        if (Level == 0 && LastCompaction && (now - LastCompaction) >= settings.MaxAccumulateTime) {
            TimeExceeded = true;
        }
    }

    TOptimizationPriority NeedCompaction(const TSettings& settings) const {
        auto portions = Portions.size();
        if (portions < 2) {
            return TOptimizationPriority::Zero();
        }
        if (TimeExceeded ||
            Portions.size() >= settings.MaxAccumulateCount ||
            TotalBlobBytes >= settings.ExpectedPortionCount * settings.ExpectedPortionSize) {
            return TOptimizationPriority::Critical(Portions.size());
        }
        return TOptimizationPriority::LevelOptimization(Portions.size());
    }

    TPortionInfo::TConstPtr RemoveForCompaction(TPortionMap::iterator it) {
        auto p = it->second;
        Compacting[p->GetPortionId()] = p;
        TotalBlobBytes -= p->GetTotalBlobBytes();
        Portions.erase(it);
        return p;
    }

    std::vector<TPortionInfo::TConstPtr> StartCompaction(const TSettings& settings, const std::shared_ptr<NDataLocks::TManager>& locksManager) {
        std::vector<TPortionInfo::TConstPtr> portions;
        ui64 totalBytes = 0;
        auto it = Portions.begin();
        while (it != Portions.end()) {
            if (locksManager->IsLocked(*it->second, NDataLocks::ELockCategory::Compaction)) {
                // Skip locked portions
                ++it;
                continue;
            }
            auto p = RemoveForCompaction(it++);
            totalBytes += p->GetTotalBlobBytes();
            portions.push_back(std::move(p));
            if (portions.size() >= 2 && totalBytes >= settings.MaxCompactionBytes) {
                break;
            }
        }
        if (!portions.empty()) {
            LastCompaction = TInstant::Now();
            TimeExceeded = false;
        }
        return portions;
    }

    void Add(const TPortionInfo::TPtr& p) {
        if (Portions.contains(p->GetPortionId()) || Compacting.contains(p->GetPortionId())) {
            Remove(p->GetPortionId());
        }
        Counters.Portions->AddPortion(p);
        if (Portions.empty()) {
            LastCompaction = TInstant::Now();
        }
        Portions[p->GetPortionId()] = p;
        TotalBlobBytes += p->GetTotalBlobBytes();
        TimeExceeded = false;
        LastCompaction = TInstant::Now();
    }

    void Remove(ui64 id) {
        if (auto it = Compacting.find(id); it != Compacting.end()) {
            Counters.Portions->RemovePortion(it->second);
            Compacting.erase(it);
            return;
        }
        auto it = Portions.find(id);
        if (it != Portions.end()) {
            Counters.Portions->RemovePortion(it->second);
            TotalBlobBytes -= it->second->GetTotalBlobBytes();
            Portions.erase(it);
        }
    }

    NJson::TJsonValue DoSerializeToJsonVisual() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("Portions", Portions.size());
        result.InsertValue("PortionsCompacting", Compacting.size());
        result.InsertValue("TotalBlobBytes", TotalBlobBytes);
        result.InsertValue("LastCompaction", LastCompaction.ToString());
        result.InsertValue("TimeExceeded", TimeExceeded);
        return result;
    }
};

struct TSimpleKeyCompare {
    std::partial_ordering operator()(const NArrow::TSimpleRow& a, const NArrow::TSimpleRow& b) const {
        return a.CompareNotNull(b);
    }
};

struct TLevel {
    const ui32 Level;
    TPortionMap Portions;
    TIntersectionTree<NArrow::TSimpleRow, ui64, TSimpleKeyCompare> Intersections;
    ui64 TotalBlobBytes = 0;

    TPortionMap Compacting;
    bool CheckCompactions = true;

    const TLevelCounters& Counters;

    TLevel* Next = nullptr;

    TLevel(ui32 level, const TLevelCounters& counters)
        : Level(level)
        , Counters(counters)
    {}

    bool Empty() const {
        return Portions.empty() && Compacting.empty();
    }

    void OnSettingsChanged(const TSettings&) {
        CheckCompactions = true;
    }

    TOptimizationPriority NeedCompaction(const TSettings& settings) const {
        if (!CheckCompactions) {
            return TOptimizationPriority::Zero();
        }
        auto height = Intersections.GetMaxCount();
        Counters.Portions->SetHeight(height);
        if (height < 2) {
            return TOptimizationPriority::Zero();
        }
        if (height >= i32(settings.Factor)) {
            return TOptimizationPriority::Critical(height);
        }
        return TOptimizationPriority::LevelOptimization(height);
    }

    void Add(const TPortionInfo::TPtr& p) {
        if (Portions.contains(p->GetPortionId()) || Compacting.contains(p->GetPortionId())) {
            Remove(p->GetPortionId());
        }
        Counters.Portions->AddPortion(p);
        Portions[p->GetPortionId()] = p;
        TotalBlobBytes += p->GetTotalBlobBytes();
        Intersections.Add(p->GetPortionId(), p->IndexKeyStart(), p->IndexKeyEnd());
        CheckCompactions = true;
    }

    void Remove(ui64 id) {
        if (auto it = Compacting.find(id); it != Compacting.end()) {
            Counters.Portions->RemovePortion(it->second);
            Compacting.erase(it);
            CheckCompactions = true;
            return;
        }

        auto it = Portions.find(id);
        if (it != Portions.end()) {
            const auto& p = it->second;
            Counters.Portions->RemovePortion(it->second);
            Intersections.Remove(p->GetPortionId());
            TotalBlobBytes -= p->GetTotalBlobBytes();
            Portions.erase(it);
            CheckCompactions = true;
        }
    }

    TPortionInfo::TPtr FindPortion(ui64 id) {
        auto it = Portions.find(id);
        if (it != Portions.end()) {
            return it->second;
        }
        return nullptr;
    }

    TPortionInfo::TConstPtr RemoveForCompaction(TPortionMap::iterator it) {
        auto p = it->second;
        Intersections.Remove(p->GetPortionId());
        TotalBlobBytes -= p->GetTotalBlobBytes();
        Compacting[p->GetPortionId()] = p;
        Portions.erase(it);
        return p;
    }

    TPortionInfo::TConstPtr RemoveForCompaction(ui64 id) {
        auto it = Portions.find(id);
        if (it != Portions.end()) {
            return RemoveForCompaction(it);
        }
        return nullptr;
    }

    std::vector<TPortionInfo::TConstPtr> StartCompaction(const TSettings& settings, const std::shared_ptr<NDataLocks::TManager>& locksManager) {
        std::vector<TPortionInfo::TConstPtr> portions;
        if (auto range = Intersections.GetMaxRange()) {
            bool fullCompaction = Level < settings.FullCompactionUntilLevel && TotalBlobBytes <= Min(settings.MaxCompactionBytes, settings.FullCompactionMaxBytes);
            bool compactNextLevelEdges = Next && settings.CompactNextLevelEdges && !fullCompaction;

            std::vector<ui64> candidates;
            if (fullCompaction) {
                for (const auto& [id, p] : Portions) {
                    candidates.push_back(id);
                }
            } else {
                range.ForEachValue([&](ui64 id) {
                    candidates.push_back(id);
                    return true;
                });
            }

            std::vector<ui64> selected;
            selected.reserve(candidates.size());
            ui64 selectedBytes = 0;
            for (ui64 id : candidates) {
                auto it = Portions.find(id);
                if (it == Portions.end()) {
                    continue; // shouldn't happen
                }
                const auto& p = it->second;
                if (locksManager->IsLocked(*p, NDataLocks::ELockCategory::Compaction)) {
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "tiling compaction: skipping level (portions locked)")("level", Level);
                    CheckCompactions = false;
                    return {};
                }
                selected.push_back(id);
                selectedBytes += p->GetTotalBlobBytes();
                if (selected.size() >= 2 && selectedBytes >= settings.MaxCompactionBytes) {
                    break;
                }
            }

            std::optional<NArrow::TSimpleRow> minKey;
            std::optional<NArrow::TSimpleRow> maxKey;
            portions.reserve(selected.size());
            for (ui64 id : selected) {
                auto it = Portions.find(id);
                AFL_VERIFY(it != Portions.end());
                auto p = RemoveForCompaction(it);
                portions.push_back(p);
                if (compactNextLevelEdges) {
                    if (auto key = p->IndexKeyStart(); !minKey || key < *minKey) {
                        minKey = key;
                    }
                    if (auto key = p->IndexKeyEnd(); !maxKey || key > *maxKey) {
                        maxKey = key;
                    }
                }
            }

            if (compactNextLevelEdges && minKey && maxKey) {
                TPortionInfo::TPtr bestLeft, bestRight;
                if (auto minFound = Next->Intersections.FindRange(*minKey); minFound && minFound.GetCount() > 0) {
                    minFound.ForEachValue([&](ui64 id) {
                        if (auto p = Next->FindPortion(id); p && !locksManager->IsLocked(*p, NDataLocks::ELockCategory::Compaction)) {
                            // The best portion doesn't extend our compaction range by much
                            if (p->IndexKeyStart() < *minKey && p->IndexKeyEnd() < *maxKey &&
                                (!bestLeft || bestLeft->IndexKeyStart() < p->IndexKeyStart())) {
                                bestLeft = p;
                            }
                        }
                        return true;
                    });
                }
                if (auto maxFound = Next->Intersections.FindRange(*maxKey); maxFound && maxFound.GetCount() > 0) {
                    maxFound.ForEachValue([&](ui64 id) {
                        if (auto p = Next->FindPortion(id); p && !locksManager->IsLocked(*p, NDataLocks::ELockCategory::Compaction)) {
                            // The best portion doesn't extend our compaction range by much
                            if (*minKey < p->IndexKeyStart() && *maxKey < p->IndexKeyEnd() &&
                                (!bestRight || bestRight->IndexKeyEnd() > p->IndexKeyEnd())) {
                                bestRight = p;
                            }
                        }
                        return true;
                    });
                }
                if (bestLeft) {
                    if (auto p = Next->RemoveForCompaction(bestLeft->GetPortionId())) {
                        portions.push_back(p);
                    }
                }
                if (bestRight && bestRight != bestLeft) {
                    if (auto p = Next->RemoveForCompaction(bestRight->GetPortionId())) {
                        portions.push_back(p);
                    }
                }
            }
        }
        return portions;
    }

    NJson::TJsonValue DoSerializeToJsonVisual() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("Portions", Portions.size());
        result.InsertValue("PortionsCompacting", Compacting.size());
        result.InsertValue("TotalBlobBytes", TotalBlobBytes);
        return result;
    }
};

class TOptimizerPlanner : public IOptimizerPlanner, private TSettings {
    using TBase = IOptimizerPlanner;
    std::shared_ptr<TCounters> Counters;
    std::shared_ptr<TSimplePortionsGroupInfo> PortionsInfo;
    mutable bool Busy = false;
    size_t MaxPortionPromotion = 100;
    TDuration PromoteTime = TDuration::Seconds(180);

    struct TTimedPortion {
        TPortionInfo::TPtr Portion;
        TInstant LastUpdate;
        ui64 HeapIndex = -1;

        explicit TTimedPortion(TPortionInfo::TPtr portion)
            : Portion(portion), LastUpdate(TInstant::Now())
        { }

        friend bool operator<(const TTimedPortion& a, const TTimedPortion& b) {
            return a.LastUpdate < b.LastUpdate;
        }

        struct THeapIndex {
            ui64& operator()(TTimedPortion& item) const {
                return item.HeapIndex;
            }
        };
    };

    // THashMap<ui64, std::unique_ptr<TTimedPortion>> ToTimedPotrion;
    // TIntrusiveHeap<TTimedPortion, TTimedPortion::THeapIndex> PortionsByTime;

    TSet<std::pair<TInstant, TPortionInfo::TPtr>> PortionsByTime;
    THashMap<ui64, TInstant> TimeByPortion;
    THashMap<ui64, ui32> InternalLevel;

public:
    TOptimizerPlanner(const TInternalPathId pathId, const std::shared_ptr<IStoragesManager>& storagesManager,
            const std::shared_ptr<arrow::Schema>& primaryKeysSchema, const TSettings& settings = {})
        : TBase(pathId, settings.NodePortionsCountLimit)
        , TSettings(settings)
        , Counters(std::make_shared<TCounters>())
        , PortionsInfo(std::make_shared<TSimplePortionsGroupInfo>())
        , StoragesManager(storagesManager)
        , PrimaryKeysSchema(primaryKeysSchema)
    {
    }

    bool UpdateSettings(const TSettings& settings) {
        if (settings.MaxLevels != MaxLevels || settings.MaxAccumulatePortionSize != MaxAccumulatePortionSize) {
            // These settings affect portion distribution, cannot apply
            return false;
        }
        // Update settings
        static_cast<TSettings&>(*this) = settings;
        for (auto& acc : Accumulator) {
            acc.OnSettingsChanged(settings);
        }
        for (auto& level : Levels) {
            level.OnSettingsChanged(settings);
        }
        return true;
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

    bool IsAccumulatorPortion(const TPortionInfo::TPtr& p) {
        if (p->GetTotalBlobBytes() <= MaxAccumulatePortionSize && InternalLevel[p->GetPortionId()] < 3) {
            return true; // portion is too small
        }
        return false;
    }

    void PlacePortion(const TPortionInfo::TPtr& p, std::optional<ui32> overrideLevel = std::nullopt) {
        PortionsInfo->AddPortion(p);

        ui32 level;

        if (overrideLevel.has_value()) {
            level = *overrideLevel;
        } else {
            level = p->GetCompactionLevel();
            if (level >= MaxLevels) {
                level = MaxLevels - 1;
            }
        }

        InternalLevel[p->GetPortionId()] = level;

        if (level < MaxLevels - 1) {
            auto time = TInstant::Now();
            TimeByPortion[p->GetPortionId()] = time;
            PortionsByTime.insert({time, p});
        }

        if (IsAccumulatorPortion(p)) {
            EnsureAccumulator(level).Add(p);
        } else {
            EnsureLevel(level).Add(p);
        }
    }

    void RemovePortion(const TPortionInfo::TPtr& p) {
        PortionsInfo->RemovePortion(p);

        auto timeIt = TimeByPortion.find(p->GetPortionId());
        if (timeIt != TimeByPortion.end()) {
            PortionsByTime.erase({timeIt->second, p});
            TimeByPortion.erase(timeIt);
        }

        auto levelIt = InternalLevel.find(p->GetPortionId());
        if (levelIt != InternalLevel.end()) {
            ui32 level = levelIt->second;
            if (level < Accumulator.size()) {
                Accumulator[level].Remove(p->GetPortionId());
            }
            if (level < Levels.size()) {
                Levels[level].Remove(p->GetPortionId());
            }
            InternalLevel.erase(levelIt);
        }
    }

    void DoModifyPortions(const std::vector<TPortionInfo::TPtr>& add, const std::vector<TPortionInfo::TPtr>& remove) override {
        for (const auto& p : remove) {
            if (p->GetProduced() == NPortion::EVICTED) {
                continue;
            }
            RemovePortion(p);
        }

        for (const auto& p : add) {
            if (p->GetProduced() == NPortion::EVICTED) {
                continue;
            }
            PlacePortion(p);
        }
    }

    TLevel& EnsureLevel(ui32 level) {
        while (level >= Levels.size()) {
            ui32 next = Levels.size();
            Levels.emplace_back(next, Counters->GetLevelCounters(next));
            if (next > 0) {
                Levels[next - 1].Next = &Levels.back();
            }
        }
        return Levels[level];
    }

    TOptimizationPriority NeedLevelCompaction(ui32 level) const {
        return Levels[level].NeedCompaction(*this);
    }

    std::shared_ptr<TColumnEngineChanges> GetCompactLevelTask(
            const std::shared_ptr<TGranuleMeta>& granule,
            const std::shared_ptr<NDataLocks::TManager>& locksManager,
            ui32 level) const {
        if (NeedLevelCompaction(level).IsZero()) {
            return nullptr;
        }

        std::vector<TPortionInfo::TConstPtr> portions = Levels[level].StartCompaction(*this, locksManager);
        if (portions.empty()) {
            return nullptr;
        }

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "tiling compaction: compacting level")("level", level)("count", portions.size());

        ui32 targetLevel = level + 1;
        if (targetLevel >= MaxLevels) {
            targetLevel = MaxLevels - 1;
        }

        TSaverContext saverContext(StoragesManager);
        auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, portions, saverContext);
        result->SetTargetCompactionLevel(targetLevel);
        result->SetPortionExpectedSize(ExpectedPortionSize);
        return result;
    }

    TAccumulator& EnsureAccumulator(ui32 level) {
        while (level >= Accumulator.size()) {
            ui32 next = Accumulator.size();
            Accumulator.emplace_back(next, Counters->GetAccumulatorCounters(next));
        }
        return Accumulator[level];
    }

    TOptimizationPriority NeedAccumulatorCompaction(ui32 level) const {
        return Accumulator[level].NeedCompaction(*this);
    }

    std::shared_ptr<TColumnEngineChanges> GetCompactAccumulatorTask(
            const std::shared_ptr<TGranuleMeta>& granule,
            const std::shared_ptr<NDataLocks::TManager>& locksManager,
            ui32 level) const {
        if (NeedAccumulatorCompaction(level).IsZero()) {
            return nullptr;
        }

        std::vector<TPortionInfo::TConstPtr> portions = Accumulator[level].StartCompaction(*this, locksManager);
        if (portions.empty()) {
            return nullptr;
        }

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "tiling compaction: compacting accumulator")("level", level)("count", portions.size());

        ui32 targetLevel = level + 1;
        if (targetLevel >= MaxLevels) {
            targetLevel = MaxLevels - 1;
        }

        TSaverContext saverContext(StoragesManager);
        auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, portions, saverContext);
        result->SetTargetCompactionLevel(targetLevel);
        result->SetPortionExpectedSize(ExpectedPortionSize);
        return result;
    }

    void PromotePortions(const TInstant currentInstant) {
        if (Busy) {
            return;
        }

        std::vector<TPortionInfo::TPtr> portionsToPromote;

        size_t count = 0;
        for (auto it = PortionsByTime.begin();
             count < MaxPortionPromotion && it != PortionsByTime.end() && it->first + PromoteTime < currentInstant;
             ++it, ++count) {
            portionsToPromote.emplace_back(it->second);
        }

        if (portionsToPromote.empty()) {
            return;
        }

        for (const auto& portion : portionsToPromote) {
            ui32 currentLevel = InternalLevel[portion->GetPortionId()];
            ui32 newLevel = Min(currentLevel + 1, MaxLevels - 1);

            RemovePortion(portion);
            PlacePortion(portion, newLevel);
        }
    }

    std::vector<std::shared_ptr<TColumnEngineChanges>> DoGetOptimizationTasks(std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& locksManager) const override {
        // Check compactions, top to bottom
        Busy = true;

        for (size_t level = 0; level < Max(Accumulator.size(), Levels.size()); ++level) {
            if (level < Accumulator.size()) {
                if (NeedAccumulatorCompaction(level).IsCritical()) {
                    if (auto result = GetCompactAccumulatorTask(granule, locksManager, level)) {
                        return { result };
                    }
                }
            }
            if (level < Levels.size()) {
                if (NeedLevelCompaction(level).IsCritical()) {
                    if (auto result = GetCompactLevelTask(granule, locksManager, level)) {
                        return { result };
                    }
                }
            }
        }

        Busy = false;

        TOptimizationPriority maxPriority = TOptimizationPriority::Zero();
        bool isLevel = false;
        std::optional<size_t> maxLevel = std::nullopt;

        for (size_t level = 0; level < Max(Accumulator.size(), Levels.size()); ++level) {
            if (level < Accumulator.size()) {
                auto priority = NeedAccumulatorCompaction(level);
                if (maxPriority < priority) {
                    maxPriority = priority;
                    isLevel = false;
                    maxLevel = level;
                }
            }
            if (level < Levels.size()) {
                auto priority = NeedLevelCompaction(level);
                if (maxPriority < priority) {
                    maxPriority = priority;
                    isLevel = true;
                    maxLevel = level;
                }
            }
        }

        if (!maxLevel) {
            // Nothing to compact
            return {};
        }

        if (isLevel) {
            return { GetCompactLevelTask(granule, locksManager, *maxLevel) };
        }

        return { GetCompactAccumulatorTask(granule, locksManager, *maxLevel) };
    }

    void DoActualize(const TInstant currentInstant) override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "tiling compaction: actualize called");
        for (size_t level = 0; level < Accumulator.size(); ++level) {
            Accumulator[level].Actualize(*this, currentInstant);
        }
        PromotePortions(currentInstant);
    }

    TOptimizationPriority DoGetUsefulMetric() const override {
        TOptimizationPriority maxPriority = TOptimizationPriority::Zero();
        for (size_t level = 0; level < Max(Accumulator.size(), Levels.size()); ++level) {
            if (level < Accumulator.size()) {
                auto priority =  NeedAccumulatorCompaction(level);
                maxPriority = std::max(maxPriority, priority);
            }
            if (level < Levels.size()) {
                auto priority =  NeedLevelCompaction(level);
                maxPriority = std::max(maxPriority, priority);
            }
        }
        return maxPriority;
    }

    TString DoDebugString() const override {
        return DoSerializeToJsonVisual().GetString();
    }

    NJson::TJsonValue DoSerializeToJsonVisual() const override {
        NJson::TJsonValue compaction_info = NJson::JSON_MAP;
        compaction_info.InsertValue("1-Name", "TILING");

        auto& settings = compaction_info.InsertValue("2-Settings", NJson::JSON_MAP);
        settings.InsertValue("Factor", Factor);
        settings.InsertValue("MaxLevels", MaxLevels);
        settings.InsertValue("ExpectedPortionCount", ExpectedPortionCount);
        settings.InsertValue("ExpectedPortionSize", ExpectedPortionSize);
        settings.InsertValue("MaxAccumulateCount", MaxAccumulateCount);
        settings.InsertValue("MaxAccumulatePortionSize", MaxAccumulatePortionSize);
        settings.InsertValue("MaxAccumulateTime", MaxAccumulateTime.ToString());
        settings.InsertValue("MaxCompactionBytes", MaxCompactionBytes);
        settings.InsertValue("FullCompactionUntilLevel", FullCompactionUntilLevel);
        settings.InsertValue("FullCompactionMaxBytes", FullCompactionMaxBytes);
        settings.InsertValue("CompactNextLevelEdges", CompactNextLevelEdges);

        NJson::TJsonValue& levels = compaction_info.InsertValue("3-Levels",NJson::JSON_ARRAY);
        for (size_t level_i = 0; level_i < Max(Accumulator.size(), Levels.size()); ++level_i) {
            NJson::TJsonValue& level = levels.AppendValue(NJson::JSON_MAP);
            if (level_i < Accumulator.size()) {
                level.InsertValue("Accumulator", Accumulator[level_i].DoSerializeToJsonVisual());
            }
            if (level_i < Levels.size()) {
                level.InsertValue("Level", Levels[level_i].DoSerializeToJsonVisual());
            }
        }
        return compaction_info;
    }

private:
    std::shared_ptr<IStoragesManager> StoragesManager;
    std::shared_ptr<arrow::Schema> PrimaryKeysSchema;

    // Accumulator accumulates small portions by size-tiered compaction
    mutable std::deque<TAccumulator> Accumulator;

    // Levels compaction, each level has portions without intersections
    mutable std::deque<TLevel> Levels;
};

class TOptimizerPlannerConstructor : public IOptimizerPlannerConstructor {
public:
    static TString GetClassNameStatic() {
        return "tiling";
    }

    TString GetClassName() const override {
        return GetClassNameStatic();
    }

private:
    static inline const TFactory::TRegistrator<TOptimizerPlannerConstructor> Registrator =
        TFactory::TRegistrator<TOptimizerPlannerConstructor>(GetClassNameStatic());

    void DoSerializeToProto(TProto& proto) const override {
        Settings.SerializeToProto(*proto.MutableTiling());
    }

    bool DoDeserializeFromProto(const TProto& proto) override {
        if (!proto.HasTiling()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse tiling compaction optimizer from proto")("proto", proto.DebugString());
            return false;
        }
        auto status = Settings.DeserializeFromProto(proto.GetTiling());
        if (!status.IsSuccess()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse tiling compaction optimizer from proto")("description", status.GetErrorDescription());
            return false;
        }
        Settings.NodePortionsCountLimit = GetNodePortionsCountLimit();
        return true;
    }

    TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override {
        auto conclusion = Settings.DeserializeFromJson(jsonInfo);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        Settings.NodePortionsCountLimit = GetNodePortionsCountLimit();
        return TConclusionStatus::Success();
    }

    bool DoApplyToCurrentObject(IOptimizerPlanner& current) const override {
        auto* itemClass = dynamic_cast<TOptimizerPlanner*>(&current);
        if (!itemClass) {
            return false;
        }
        return itemClass->UpdateSettings(Settings);
    }

    TConclusion<std::shared_ptr<IOptimizerPlanner>> DoBuildPlanner(const TBuildContext& context) const override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "creating tiling compaction optimizer");
        return std::make_shared<TOptimizerPlanner>(context.GetPathId(), context.GetStorages(), context.GetPKSchema(), Settings);
    }

private:
    TSettings Settings;
};

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
