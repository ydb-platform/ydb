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
    unsigned ExpectedPortionCount = 10;
    ui64 ExpectedPortionSize = 4 * 1024 * 1024;
    unsigned MaxAccumulateCount = 10;
    TDuration MaxAccumulateTime = TDuration::Seconds(60);
    ui64 MaxCompactionBytes = 256 * 1024 * 1024;
    ui32 FullCompactionUntilLevel = 2;
    ui64 FullCompactionMaxBytes = MaxCompactionBytes;
    bool CompactNextLevelEdges = false;

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

        SettingsJson = jsonInfo;

        return TConclusionStatus::Success();
    }
};

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

    bool operator()(const TPortionInfo::TPtr& a, const NArrow::TSimpleRow& b) const {
        auto order = a->IndexKeyEnd().CompareNotNull(b);
        return order == std::partial_ordering::less;
    }

    bool operator()(const NArrow::TSimpleRow& a, const TPortionInfo::TPtr& b) const {
        auto order = a.CompareNotNull(b->IndexKeyStart());
        return order == std::partial_ordering::less;
    }
};

using TPortionSet = std::set<TPortionInfo::TPtr, TPortionComparator>;
using TPortionsMap = THashMap<ui64, TPortionInfo::TPtr>;

struct TAccumulator {
    TPortionsMap Portions;
    ui64 TotalBlobBytes = 0;

    TPortionsMap Compacting;

    TInstant LastCompaction = {};
    bool TimeExceeded = false;

    explicit TAccumulator()
    {}

    bool Empty() const {
        return Portions.empty() && Compacting.empty();
    }

    void OnSettingsChanged(const TSettings&) {
        TimeExceeded = false;
    }

    void Actualize(const TSettings& settings, TInstant now) {
        if (LastCompaction && (now - LastCompaction) >= settings.MaxAccumulateTime) {
            TimeExceeded = true;
        }
    }

    bool NeedCompaction(const TSettings& settings) const {
        return Portions.size() >= settings.MaxAccumulateCount ||
            Portions.size() > 1 && TimeExceeded ||
            Portions.size() > 1 && TotalBlobBytes >= settings.ExpectedPortionCount * settings.ExpectedPortionSize;
    }

    TPortionInfo::TConstPtr RemoveForCompaction(TPortionsMap::iterator it) {
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
        if (Portions.empty()) {
            LastCompaction = TInstant::Now();
        }
        Remove(p->GetPortionId());
        Portions[p->GetPortionId()] = p;
        TotalBlobBytes += p->GetTotalBlobBytes();
        TimeExceeded = false;
        LastCompaction = TInstant::Now();
    }

    void Remove(ui64 id) {
        Compacting.erase(id);
        auto it = Portions.find(id);
        if (it != Portions.end()) {
            TotalBlobBytes -= it->second->GetTotalBlobBytes();
            Portions.erase(it);
        }
    }
};

struct TSimpleKeyCompare {
    std::partial_ordering operator()(const NArrow::TSimpleRow& a, const NArrow::TSimpleRow& b) const {
        return a.CompareNotNull(b);
    }
};

struct TLevel {
    const ui32 Level;
    TPortionsMap Portions;
    TIntersectionTree<NArrow::TSimpleRow, ui64, TSimpleKeyCompare> Intersections;
    ui64 TotalBlobBytes = 0;

    TPortionsMap Compacting;
    bool CheckCompactions = true;

    TLevel* Next = nullptr;

    TLevel(ui32 level)
        : Level(level)
    {}

    bool Empty() const {
        return Portions.empty() && Compacting.empty();
    }

    void OnSettingsChanged(const TSettings&) {
        CheckCompactions = true;
    }

    bool NeedCompaction(const TSettings& settings) const {
        return Intersections.GetMaxCount() >= i32(settings.Factor);
    }

    void Add(const TPortionInfo::TPtr& p) {
        Remove(p->GetPortionId());
        Portions[p->GetPortionId()] = p;
        TotalBlobBytes += p->GetTotalBlobBytes();
        Intersections.Add(p->GetPortionId(), p->IndexKeyStart(), p->IndexKeyEnd());
        CheckCompactions = true;
    }

    void Remove(ui64 id) {
        Compacting.erase(id);

        auto it = Portions.find(id);
        if (it != Portions.end()) {
            const auto& p = it->second;
            Intersections.Remove(p->GetPortionId());
            TotalBlobBytes -= p->GetTotalBlobBytes();
            Portions.erase(it);
        }

        CheckCompactions = true;
    }

    TPortionInfo::TPtr FindPortion(ui64 id) {
        auto it = Portions.find(id);
        if (it != Portions.end()) {
            return it->second;
        }
        return nullptr;
    }

    TPortionInfo::TConstPtr RemoveForCompaction(TPortionsMap::iterator it) {
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
            bool fullCompaction = Level < settings.FullCompactionUntilLevel && TotalBlobBytes <= settings.FullCompactionMaxBytes;
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
        Accumulator.OnSettingsChanged(settings);
        for (auto& level : Levels) {
            level.OnSettingsChanged(settings);
        }
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

    void DoModifyPortions(const THashMap<ui64, TPortionInfo::TPtr>& add, const THashMap<ui64, TPortionInfo::TPtr>& remove) override {
        std::vector<TPortionInfo::TPtr> sortedRemove;
        for (const auto& [_, p] : remove) {
            sortedRemove.push_back(p);
        }
        std::sort(sortedRemove.begin(), sortedRemove.end(), [](const auto& a, const auto& b) {
            return b->GetPortionId() < a->GetPortionId();
        });

        for (const auto& p : sortedRemove) {
            switch (p->GetProduced()) {
                case NPortion::INSERTED: {
                    Accumulator.Remove(p->GetPortionId());
                    break;
                }
                default: {
                    ui32 level = p->GetCompactionLevel();
                    if (level < Levels.size()) {
                        Levels[level].Remove(p->GetPortionId());
                    }
                    break;
                }
            }
        }

        std::vector<TPortionInfo::TPtr> sortedAdd;
        for (const auto& [_, p] : add) {
            sortedAdd.push_back(p);
        }
        std::sort(sortedAdd.begin(), sortedAdd.end(), [](const auto& a, const auto& b) {
            return a->GetPortionId() < b->GetPortionId();
        });

        for (const auto& p : sortedAdd) {
            switch (p->GetProduced()) {
                case NPortion::INACTIVE:
                case NPortion::EVICTED:
                    break;
                case NPortion::INSERTED: {
                    Accumulator.Add(p);
                    break;
                }
                default: {
                    ui32 level = p->GetCompactionLevel();
                    if (level >= MaxLevels) {
                        level = MaxLevels - 1;
                        p->MutableMeta().ResetCompactionLevel(level);
                    }
                    while (level >= Levels.size()) {
                        AddLevel();
                    }
                    Levels[level].Add(p);
                    break;
                }
            }
        }
    }

    void AddLevel() {
        ui32 level = Levels.size();
        Levels.emplace_back(level);
        if (level > 0) {
            Levels[level - 1].Next = &Levels.back();
        }
    }

    bool NeedLevelCompaction(ui32 level) const {
        return Levels[level].NeedCompaction(*this);
    }

    std::shared_ptr<TColumnEngineChanges> GetCompactLevelTask(
            const std::shared_ptr<TGranuleMeta>& granule,
            const std::shared_ptr<NDataLocks::TManager>& locksManager,
            ui32 level) const {
        if (!NeedLevelCompaction(level)) {
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

    bool NeedAccumulatorCompaction() const {
        return Accumulator.NeedCompaction(*this);
    }

    std::shared_ptr<TColumnEngineChanges> GetCompactAccumulatorTask(
            const std::shared_ptr<TGranuleMeta>& granule,
            const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
        if (!NeedAccumulatorCompaction()) {
            return nullptr;
        }

        std::vector<TPortionInfo::TConstPtr> portions = Accumulator.StartCompaction(*this, locksManager);
        if (portions.empty()) {
            return nullptr;
        }

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "tiling compaction: compacting accumulator")("count", portions.size());

        TSaverContext saverContext(StoragesManager);
        auto result = std::make_shared<NCompaction::TGeneralCompactColumnEngineChanges>(granule, portions, saverContext);
        result->SetTargetCompactionLevel(0);
        result->SetPortionExpectedSize(ExpectedPortionSize);
        return result;
    }

    std::shared_ptr<TColumnEngineChanges> DoGetOptimizationTask(std::shared_ptr<TGranuleMeta> granule, const std::shared_ptr<NDataLocks::TManager>& locksManager) const override {
        if (auto result = GetCompactAccumulatorTask(granule, locksManager)) {
            return result;
        }

        // Check tiling compactions, top to bottom
        for (size_t level = 0; level < Levels.size(); ++level) {
            if (auto result = GetCompactLevelTask(granule, locksManager, level)) {
                return result;
            }
        }

        // Nothing to compact
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "tiling compaction: nothing to compact");
        return nullptr;
    }

    void DoActualize(const TInstant currentInstant) override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "tiling compaction: actualize called");
        Accumulator.Actualize(*this, currentInstant);
    }

    TOptimizationPriority DoGetUsefulMetric() const override {
        if (NeedAccumulatorCompaction()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "tiling compaction: returning DoGetUsefulMetric for accumulator");
            return TOptimizationPriority::Critical(Accumulator.Portions.size());
        }
        for (size_t level = 0; level < Levels.size(); ++level) {
            if (NeedLevelCompaction(level)) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "tiling compaction: returning DoGetUsefulMetric for level")("level", level);
                return TOptimizationPriority::Critical(Levels[level].Portions.size());
            }
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "tiling compaction: returning DoGetUsefulMetric zero");
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
    mutable TAccumulator Accumulator;

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
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("message", "creating tiling compaction optimizer");
        return std::make_shared<TOptimizerPlanner>(context.GetPathId(), context.GetStorages(), context.GetPKSchema(), Settings);
    }

private:
    TSettings Settings;
};

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
