#pragma once
#include <ydb/core/tx/columnshard/engines/reader/common/result.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TPlainReadData;
class ISourcesCollection;

class ISyncPoint {
public:
    enum class ESourceAction {
        Finish,
        ProvideNext,
        Wait
    };

private:
    YDB_READONLY(ui32, PointIndex, 0);
    YDB_READONLY_DEF(TString, PointName);
    std::optional<ui32> LastSourceIdx;
    virtual void OnAddSource(const std::shared_ptr<IDataSource>& /*source*/) {
    }
    virtual bool IsSourcePrepared(const std::shared_ptr<IDataSource>& source) const {
        return source->IsSyncSection() && source->HasStageResult();
    }
    virtual ESourceAction OnSourceReady(const std::shared_ptr<IDataSource>& source, TPlainReadData& reader) = 0;
    virtual void DoAbort() = 0;
    bool AbortFlag = false;

protected:
    const std::shared_ptr<TSpecialReadContext> Context;
    const std::shared_ptr<ISourcesCollection> Collection;
    std::shared_ptr<ISyncPoint> Next;
    std::deque<std::shared_ptr<IDataSource>> SourcesSequentially;

public:
    virtual ~ISyncPoint() = default;

    void Continue(const TPartialSourceAddress& continueAddress, TPlainReadData& reader) {
        AFL_VERIFY(PointIndex == continueAddress.GetSyncPointIndex());
        AFL_VERIFY(SourcesSequentially.size() && SourcesSequentially.front()->GetSourceId() == continueAddress.GetSourceId());
        const NActors::TLogContextGuard gLogging =
            NActors::TLogContextBuilder::Build()("sync_point", GetPointName())("event", "continue_source");
        AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("source_id", SourcesSequentially.front()->GetSourceId());
        OnSourcePrepared(SourcesSequentially.front(), reader);
    }

    TString DebugString() const {
        return "";
    }

    void Abort() {
        SourcesSequentially.clear();
        if (!AbortFlag) {
            AbortFlag = true;
            DoAbort();
        }
    }

    bool IsFinished() const {
        return SourcesSequentially.empty();
    }

    void SetNext(const std::shared_ptr<ISyncPoint>& next) {
        AFL_VERIFY(!Next);
        Next = next;
    }

    TString GetShortPointName() const {
        if (PointName.size() < 2) {
            return PointName;
        } else {
            return PointName.substr(0, 2);
        }
    }

    ISyncPoint(const ui32 pointIndex, const TString& pointName, const std::shared_ptr<TSpecialReadContext>& context,
        const std::shared_ptr<ISourcesCollection>& collection)
        : PointIndex(pointIndex)
        , PointName(pointName)
        , Context(context)
        , Collection(collection) {
    }

    void AddSource(const std::shared_ptr<IDataSource>& source) {
        const NActors::TLogContextGuard gLogging =
            NActors::TLogContextBuilder::Build()("sync_point", GetPointName())("event", "add_source");
        AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("source_id", source->GetSourceId());
        AFL_VERIFY(!AbortFlag);
        source->SetPurposeSyncPointIndex(GetPointIndex());
        if (Next) {
            source->SetNeedFullAnswer(false);
        }
        AFL_VERIFY(!!source);
        if (!LastSourceIdx) {
            LastSourceIdx = source->GetSourceIdx();
        } else {
            AFL_VERIFY(*LastSourceIdx < source->GetSourceIdx());
        }
        LastSourceIdx = source->GetSourceIdx();
        SourcesSequentially.emplace_back(source);
        if (!source->HasFetchingPlan()) {
            source->InitFetchingPlan(Context->GetColumnsFetchingPlan(source));
        }
        OnAddSource(source);
        source->StartProcessing(source);
    }

    void OnSourcePrepared(const std::shared_ptr<IDataSource>& sourceInput, TPlainReadData& reader);
};

}   // namespace NKikimr::NOlap::NReader::NSimple
