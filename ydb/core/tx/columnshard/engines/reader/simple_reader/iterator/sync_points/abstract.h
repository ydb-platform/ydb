#pragma once

namespace NKikimr::NOlap::NReader::NSimple {

class TPlainReadData;

class ISyncPoint {
private:
    YDB_READONLY(ui32, PointIndex, 0);
    YDB_READONLY_DEF(TString, PointName);
    std::optional<ui32> LastSourceIdx;
    virtual void OnSourceAdded(const std::shared_ptr<IDataSource>& source) = 0;
    virtual bool IsSourcePrepared(const std::shared_ptr<IDataSource>& source) const = 0;
    virtual bool OnSourceReady(const std::shared_ptr<IDataSource>& source) const = 0;
    virtual std::shared_ptr<TFetchingScript> BuildFetchingPlan(const std::shared_ptr<IDataSource>& source) const = 0;

protected:
    std::shared_ptr<ISyncPoint> Next;
    std::deque<std::shared_ptr<IDataSource>> SourcesSequentially;

public:
    TString GetShortPointName() const {
        if (PointName.size() < 2) {
            return PointName;
        } else {
            return PointName.substr(0, 2);
        }
    }

    ISyncPoint(const ui32 pointIndex, const TString& pointName)
        : PointIndex(pointIndex)
        , PointName(pointName) {
    }

    void AddSource(const std::shared_ptr<IDataSource>& source) {
        source->SetPurposeSyncPointIndex(GetPointIndex());
        AFL_VERIFY(!!source);
        if (!LastSourceIdx) {
            LastSourceIdx = source->GetSourceIdx();
        } else {
            AFL_VERIFY(*LastSourceIdx < source->GetSourceIdx());
        }
        LastSourceIdx = source->GetSourceIdx();
        SourcesSequentially.emplace_back(source);
        source->InitFetchingPlan(BuildFetchingPlan());
        source->StartProcessing(source);
    }

    void OnSourcePrepared(const std::shared_ptr<IDataSource>& sourceInput, TPlainReadData& reader) {
        AFL_VERIFY(sourceInput->IsSyncSection());
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, sourceInput->AddEvent("f" + GetShortPointName()));
        AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event_log", sourceInput->GetEventsReport())("count", SourcesSequentially.size())(
            "source_id", sourceInput->GetSourceId());
        AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event", "OnSourcePrepared")("syn_point", GetPointName())(
            "source_id", sourceInput->GetSourceId());
        if (SourcesSequentially.size()) {
            AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event_log", sourceInput->GetEventsReport())("count", SourcesSequentially.size())(
                "source_id", sourceInput->GetSourceId())("first_source_id", SourcesSequentially.front()->GetSourceId());
        }
        while (SourcesSequentially.size() && IsSourcePrepared(SourcesSequentially.front())) {
            auto source = SourcesSequentially.front();
            if (!OnSourceReady(source)) {
                AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event", "ready_source_false")("source_id", source->GetSourceId());
                break;
            } else {
                AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event", "ready_source_true")("source_id", source->GetSourceId());
            }
            if (Next) {
                Next->AddSource(source);
            } else {
                reader.GetScanner().MutableSourcesCollection().OnSourceFinished(source);
            }
            SourcesSequentially.pop_front();
        }
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
