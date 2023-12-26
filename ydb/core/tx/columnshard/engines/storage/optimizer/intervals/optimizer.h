#pragma once
#include "optimizer.h"
#include "counters.h"
#include "blob_size.h"
#include <ydb/core/formats/arrow/replace_key.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/splitter/settings.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/write.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>

namespace NKikimr::NOlap::NStorageOptimizer {

class TCounters;

class TSegmentPosition {
private:
    std::shared_ptr<TPortionInfo> Portion;
    const NArrow::TReplaceKey& Position;
    const bool IsStartFlag;
    TSegmentPosition(const std::shared_ptr<TPortionInfo>& data, const bool start)
        : Portion(data)
        , Position(start ? Portion->IndexKeyStart() : Portion->IndexKeyEnd())
        , IsStartFlag(start)
    {

    }
public:

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("is_start", IsStartFlag);
        return result;
    }

    TString DebugString() const {
        return TStringBuilder() <<
            (IsStartFlag ? "ADD" : "REMOVE") << ":" <<
            Position.DebugString() << ";" <<
            Portion->DebugString() << ";"
            ;
    }

    std::shared_ptr<TPortionInfo> GetPortionPtr() const {
        return Portion;
    }

    const TPortionInfo& GetPortion() const {
        return *Portion;
    }

    const NArrow::TReplaceKey& GetPosition() const {
        return Position;
    }

    static TSegmentPosition Start(const std::shared_ptr<TPortionInfo>& data) {
        return TSegmentPosition(data, true);
    }

    static TSegmentPosition Finish(const std::shared_ptr<TPortionInfo>& data) {
        return TSegmentPosition(data, false);
    }

    bool operator<(const TSegmentPosition& item) const {
        return Portion->GetPortion() < item.Portion->GetPortion();
    }
};

class TIntervalFeatures {
private:
    YDB_READONLY(i32, PortionsCount, 0);
    YDB_READONLY(i32, RecordsCount, 0);
    YDB_READONLY(i64, PortionsWeight, 0);
    YDB_READONLY(i64, PortionsRawWeight, 0);
    YDB_READONLY(i64, SmallPortionsWeight, 0);
    YDB_READONLY(i64, SmallPortionsCount, 0);
public:

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("p_count", PortionsCount);
        result.InsertValue("p_weight", PortionsWeight);
        result.InsertValue("p_raw_weight", PortionsRawWeight);
        result.InsertValue("sp_count", SmallPortionsCount);
        result.InsertValue("sp_weight", SmallPortionsWeight);
        result.InsertValue("r_count", RecordsCount);
        return result;
    }

    i64 GetUsefulMetric() const {
        if (PortionsCount == 1 || PortionsWeight == 0) {
            return 0;
        }
        return (i64)10000 * PortionsCount / (PortionsWeight * 1e-6);
    }

    double GetUsefulKff() const {
        if (PortionsCount == 0 || PortionsWeight == 0) {
            return Max<double>();
        }
        Y_ABORT_UNLESS(PortionsWeight);
        return 1.0 * GetUsefulMetric() / PortionsWeight;
    }

    void Add(const std::shared_ptr<TPortionInfo>& info) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "add_portion_in_summary")("portion_id", info->GetPortion())("count", GetPortionsCount())("this", (ui64)this);
        ++PortionsCount;
        const i64 portionBytes = info->BlobsBytes();
        PortionsWeight += portionBytes;
        PortionsRawWeight += info->RawBytesSum();
        RecordsCount += info->NumRows();
        if ((i64)portionBytes < TSplitSettings().GetMinBlobSize()) {
            ++SmallPortionsCount;
            SmallPortionsWeight += portionBytes;
        }
    }

    void Remove(const std::shared_ptr<TPortionInfo>& info) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "remove_portion_from_summary")("portion_id", info->GetPortion())("count", GetPortionsCount())("this", (ui64)this);
        Y_ABORT_UNLESS(--PortionsCount >= 0);
        const i64 portionBytes = info->BlobsBytes();
        PortionsWeight -= portionBytes;
        Y_ABORT_UNLESS(PortionsWeight >= 0);
        PortionsRawWeight -= info->RawBytesSum();
        Y_ABORT_UNLESS(PortionsRawWeight >= 0);
        RecordsCount -= info->NumRows();
        Y_ABORT_UNLESS(RecordsCount >= 0);
        if ((i64)portionBytes < TSplitSettings().GetMinBlobSize()) {
            Y_ABORT_UNLESS(--SmallPortionsCount >= 0);
            SmallPortionsWeight -= portionBytes;
            Y_ABORT_UNLESS(SmallPortionsWeight >= 0);
        }
    }

    bool operator!() const {
        return !PortionsCount;
    }

    bool operator<(const TIntervalFeatures& item) const {
        return GetUsefulMetric() < item.GetUsefulMetric();
    }
    bool IsEnoughWeight() const {
        return GetPortionsRawWeight() > TSplitSettings().GetMinBlobSize() * 10;
    }

};

class TIntervalsOptimizerPlanner: public IOptimizerPlanner {
private:
    static ui64 LimitSmallBlobsMerge;
    static ui64 LimitSmallBlobDetect;
    std::shared_ptr<IStoragesManager> StoragesManager;

    std::shared_ptr<TCounters> Counters;

    using TBase = IOptimizerPlanner;

    class TPortionIntervalPoint {
    private:
        YDB_READONLY(ui64, PortionId, 0);
        YDB_READONLY(bool, IsStart, false);
    public:
        TPortionIntervalPoint(const ui64 portionId, const bool isStart)
            : PortionId(portionId)
            , IsStart(isStart)
        {

        }

        bool operator<(const TPortionIntervalPoint& item) const {
            return std::tie(PortionId, IsStart) < std::tie(item.PortionId, item.IsStart);
        }
    };

    class TBorderPositions {
    private:
        const NArrow::TReplaceKey Position;
        std::map<TPortionIntervalPoint, TSegmentPosition> Positions;
        YDB_READONLY_DEF(TIntervalFeatures, Features);
    public:
        TBorderPositions(const NArrow::TReplaceKey& position)
            : Position(position)
        {

        }

        const std::map<TPortionIntervalPoint, TSegmentPosition>& GetPositions() const {
            return Positions;
        }

        NJson::TJsonValue DebugJson() const {
            NJson::TJsonValue result = NJson::JSON_MAP;
            result.InsertValue("p", Position.DebugString());
            auto& segments = result.InsertValue("segments", NJson::JSON_ARRAY);
            for (auto&& i : Positions) {
                segments.AppendValue(i.second.DebugJson());
            }
            result.InsertValue("features", Features.DebugJson());
            return result;
        }

        void CopyFrom(const TBorderPositions& source) {
            Features = source.Features;
        }

        const NArrow::TReplaceKey& GetPosition() const {
            return Position;
        }

        void AddStart(const std::shared_ptr<TPortionInfo>& info) {
            Y_ABORT_UNLESS(Positions.emplace(TPortionIntervalPoint(info->GetPortion(), true), TSegmentPosition::Start(info)).second);
        }
        void AddFinish(const std::shared_ptr<TPortionInfo>& info) {
            Y_ABORT_UNLESS(Positions.emplace(TPortionIntervalPoint(info->GetPortion(), false), TSegmentPosition::Finish(info)).second);
        }
        bool RemoveStart(const std::shared_ptr<TPortionInfo>& info) {
            Y_ABORT_UNLESS(Positions.erase(TPortionIntervalPoint(info->GetPortion(), true)));
            return Positions.empty();
        }
        bool RemoveFinish(const std::shared_ptr<TPortionInfo>& info) {
            Y_ABORT_UNLESS(Positions.erase(TPortionIntervalPoint(info->GetPortion(), false)));
            return Positions.empty();
        }
        void AddSummary(const std::shared_ptr<TPortionInfo>& info);
        void RemoveSummary(const std::shared_ptr<TPortionInfo>& info);
    };

    std::map<TIntervalFeatures, std::set<const TBorderPositions*>> RangedSegments;

    using TPositions = std::map<NArrow::TReplaceKey, TBorderPositions>;
    TPositions Positions;
    TBlobsBySize SizeProblemBlobs;

    void RemovePortion(const std::shared_ptr<TPortionInfo>& info);
    void AddPortion(const std::shared_ptr<TPortionInfo>& info);

    void RemoveRanged(const TBorderPositions& data);
    void AddRanged(const TBorderPositions& data);

    bool RemoveSmallPortion(const std::shared_ptr<TPortionInfo>& info);

    bool AddSmallPortion(const std::shared_ptr<TPortionInfo>& info);

    std::vector<std::shared_ptr<TPortionInfo>> GetPortionsForIntervalStartedIn(const NArrow::TReplaceKey& keyStart, const ui32 countExpectation) const;

protected:
    virtual void DoModifyPortions(const THashMap<ui64, std::shared_ptr<TPortionInfo>>& add, const THashMap<ui64, std::shared_ptr<TPortionInfo>>& remove) override;
    virtual std::shared_ptr<TColumnEngineChanges> DoGetOptimizationTask(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule, const THashSet<TPortionAddress>& busyPortions) const override;

    virtual TOptimizationPriority DoGetUsefulMetric() const override;
    virtual TString DoDebugString() const override;

public:
    TIntervalsOptimizerPlanner(const ui64 pathId, const std::shared_ptr<IStoragesManager>& storagesManager);
};

} // namespace NKikimr::NOlap
