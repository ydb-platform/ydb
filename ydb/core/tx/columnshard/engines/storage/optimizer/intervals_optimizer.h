#pragma once
#include "optimizer.h"
#include <ydb/core/formats/arrow/replace_key.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NStorageOptimizer {

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
    YDB_READONLY(i64, PortionsWeight, 0);
    YDB_READONLY(i64, PortionsRawWeight, 0);
    YDB_READONLY(i32, SmallPortionsWeight, 0);
    YDB_READONLY(i64, SmallPortionsCount, 0);
    std::map<ui64, std::shared_ptr<TPortionInfo>> SummaryPortions;
public:

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("p_count", PortionsCount);
        result.InsertValue("p_weight", PortionsWeight);
        result.InsertValue("p_raw_weight", PortionsRawWeight);
        result.InsertValue("sp_count", SmallPortionsCount);
        result.InsertValue("sp_weight", SmallPortionsWeight);
        auto& pIds = result.InsertValue("portion_ids", NJson::JSON_ARRAY);
        for (auto&& i : SummaryPortions) {
            pIds.AppendValue(i.first);
        }
        return result;
    }

    bool Merge(const TIntervalFeatures& features, const i64 sumWeightLimit) {
        if (PortionsRawWeight + features.PortionsRawWeight > sumWeightLimit) {
            return false;
        }
        for (auto&& i : features.SummaryPortions) {
            if (SummaryPortions.contains(i.first)) {
                continue;
            }
            Add(i.second);
        }
        return true;
    }

    i64 GetUsefulMetric() const {
        if (PortionsCount == 1) {
            return 0;
        }
        return PortionsCount;
    }

    double GetUsefulKff() const {
        if (PortionsCount == 0) {
            return Max<double>();
        }
        Y_VERIFY(PortionsWeight);
        return 1.0 * GetUsefulMetric() / PortionsWeight;
    }

    void Add(const std::shared_ptr<TPortionInfo>& info) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "add_portion_in_summary")("portion_id", info->GetPortion())("count", SummaryPortions.size())("this", (ui64)this);
        AFL_VERIFY(SummaryPortions.emplace(info->GetPortion(), info).second)("portion_id", info->GetPortion())("this", (ui64)this);
        ++PortionsCount;
        const i64 portionBytes = info->BlobsBytes();
        PortionsWeight += portionBytes;
        PortionsRawWeight += info->RawBytesSum();
        if ((i64)portionBytes < TSplitSettings().GetMinBlobSize()) {
            ++SmallPortionsCount;
            SmallPortionsWeight += portionBytes;
        }
    }

    void Remove(const std::shared_ptr<TPortionInfo>& info) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "remove_portion_from_summary")("portion_id", info->GetPortion())("count", SummaryPortions.size())("this", (ui64)this);
        AFL_VERIFY(SummaryPortions.erase(info->GetPortion()))("portion_id", info->GetPortion())("this", (ui64)this);
        Y_VERIFY(--PortionsCount >= 0);
        const i64 portionBytes = info->BlobsBytes();
        PortionsWeight -= portionBytes;
        Y_VERIFY(PortionsWeight >= 0);
        PortionsRawWeight -= info->RawBytesSum();
        Y_VERIFY(PortionsRawWeight >= 0);
        if ((i64)portionBytes < TSplitSettings().GetMinBlobSize()) {
            Y_VERIFY(--SmallPortionsCount >= 0);
            SmallPortionsWeight -= portionBytes;
            Y_VERIFY(SmallPortionsWeight >= 0);
        }
    }

    bool operator!() const {
        return !PortionsCount;
    }

    bool operator<(const TIntervalFeatures& item) const {
        return GetUsefulMetric() > item.GetUsefulMetric();
    }
    const std::map<ui64, std::shared_ptr<TPortionInfo>>& GetSummaryPortions() const {
        return SummaryPortions;
    }
    bool IsEnoughWeight() const {
        return GetPortionsWeight() > TSplitSettings().GetMinBlobSize();
    }

    bool IsCritical() const {
        return PortionsCount > 1 || SmallPortionsCount;
    }

};

class TIntervalsOptimizerPlanner: public IOptimizerPlanner {
private:
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
            Y_VERIFY(Positions.emplace(TPortionIntervalPoint(info->GetPortion(), true), TSegmentPosition::Start(info)).second);
        }
        void AddFinish(const std::shared_ptr<TPortionInfo>& info) {
            Y_VERIFY(Positions.emplace(TPortionIntervalPoint(info->GetPortion(), false), TSegmentPosition::Finish(info)).second);
        }
        bool RemoveStart(const std::shared_ptr<TPortionInfo>& info) {
            Y_VERIFY(Positions.erase(TPortionIntervalPoint(info->GetPortion(), true)));
            return Positions.empty();
        }
        bool RemoveFinish(const std::shared_ptr<TPortionInfo>& info) {
            Y_VERIFY(Positions.erase(TPortionIntervalPoint(info->GetPortion(), false)));
            return Positions.empty();
        }
        void AddSummary(const std::shared_ptr<TPortionInfo>& info) {
            Features.Add(info);
        }
        void RemoveSummary(const std::shared_ptr<TPortionInfo>& info) {
            Features.Remove(info);
        }
    };
    std::map<TIntervalFeatures, std::set<const TBorderPositions*>> RangedSegments;
    using TPositions = std::map<NArrow::TReplaceKey, TBorderPositions>;
    TPositions Positions;
    i64 SumSmall = 0;
    std::map<NArrow::TReplaceKey, std::map<ui64, std::shared_ptr<TPortionInfo>>> SmallBlobs;

    void RemoveRanged(const TBorderPositions& data) {
        if (!!data.GetFeatures()) {
            auto itFeatures = RangedSegments.find(data.GetFeatures());
            Y_VERIFY(itFeatures->second.erase(&data));
            if (itFeatures->second.empty()) {
                RangedSegments.erase(itFeatures);
            }
        }
    }

    void AddRanged(const TBorderPositions& data) {
        if (!!data.GetFeatures()) {
            Y_VERIFY(RangedSegments[data.GetFeatures()].emplace(&data).second);
        }
    }

    bool RemoveSmallPortion(const std::shared_ptr<TPortionInfo>& info) {
        if (info->BlobsBytes() < 1024 * 1024) {
            auto it = SmallBlobs.find(info->IndexKeyStart());
            Y_VERIFY(it->second.erase(info->GetPortion()));
            if (it->second.empty()) {
                SmallBlobs.erase(it);
            }
            SumSmall -= info->BlobsBytes();
            Y_VERIFY(SumSmall >= 0);
            return true;
        }
        return false;
    }

    bool AddSmallPortion(const std::shared_ptr<TPortionInfo>& info) {
        if (info->BlobsBytes() < 1024 * 1024) {
            Y_VERIFY(SmallBlobs[info->IndexKeyStart()].emplace(info->GetPortion(), info).second);
            SumSmall += info->BlobsBytes();
            return true;
        }
        return false;
    }

    std::shared_ptr<TColumnEngineChanges> GetSmallPortionsMergeTask(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule) const;

protected:
    virtual void DoAddPortion(const std::shared_ptr<TPortionInfo>& info) override {
        AddSmallPortion(info);
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "add_portion")("portion_id", info->GetPortion());
        auto itStart = Positions.find(info->IndexKeyStart());
        if (itStart == Positions.end()) {
            itStart = Positions.emplace(info->IndexKeyStart(), TBorderPositions(info->IndexKeyStart())).first;
            if (itStart != Positions.begin()) {
                auto itStartCopy = itStart;
                --itStartCopy;
                itStart->second.CopyFrom(itStartCopy->second);
                AddRanged(itStart->second);
            }
        }
        auto itEnd = Positions.find(info->IndexKeyEnd());
        if (itEnd == Positions.end()) {
            itEnd = Positions.emplace(info->IndexKeyEnd(), TBorderPositions(info->IndexKeyEnd())).first;
            Y_VERIFY(itEnd != Positions.begin());
            auto itEndCopy = itEnd;
            --itEndCopy;
            itEnd->second.CopyFrom(itEndCopy->second);
            AddRanged(itEnd->second);
            itStart = Positions.find(info->IndexKeyStart());
        }
        Y_VERIFY(itStart != Positions.end());
        Y_VERIFY(itEnd != Positions.end());
        itStart->second.AddStart(info);
        itEnd->second.AddFinish(info);
        for (auto it = itStart; it != itEnd; ++it) {
            RemoveRanged(it->second);
            it->second.AddSummary(info);
            AddRanged(it->second);
        }
    }
    virtual void DoRemovePortion(const std::shared_ptr<TPortionInfo>& info) override {
        RemoveSmallPortion(info);
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "remove_portion")("portion_id", info->GetPortion());
        auto itStart = Positions.find(info->IndexKeyStart());
        auto itFinish = Positions.find(info->IndexKeyEnd());
        Y_VERIFY(itStart != Positions.end());
        Y_VERIFY(itFinish != Positions.end());
        for (auto it = itStart; it != itFinish; ++it) {
            RemoveRanged(it->second);
            it->second.RemoveSummary(info);
            AddRanged(it->second);
        }
        if (itStart->second.RemoveStart(info)) {
            RemoveRanged(itStart->second);
            Positions.erase(itStart);
        }
        if (itFinish->second.RemoveFinish(info)) {
            RemoveRanged(itFinish->second);
            Positions.erase(itFinish);
        }
    }
    virtual std::shared_ptr<TColumnEngineChanges> DoGetOptimizationTask(const TCompactionLimits& limits, std::shared_ptr<TGranuleMeta> granule, const THashSet<TPortionAddress>& busyPortions) const override;
    virtual i64 DoGetUsefulMetric() const override {
        if (RangedSegments.empty()) {
            return 0;
        }
        auto& topSegment = **RangedSegments.begin()->second.begin();
        auto& topFeaturesTask = topSegment.GetFeatures();
        return topFeaturesTask.GetUsefulMetric();
    }
public:
    virtual TString GetDescription() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        auto& positions = result.InsertValue("positions", NJson::JSON_ARRAY);
        for (auto&& i : Positions) {
            positions.AppendValue(i.second.DebugJson());
        }
        return result.GetStringRobust();
    }

    TIntervalsOptimizerPlanner(const ui64 granuleId)
        : TBase(granuleId)
    {

    }
};

} // namespace NKikimr::NOlap
