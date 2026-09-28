#pragma once
#include <ydb/core/tx/columnshard/engines/reader/common/result.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/context.h>
#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/source.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NReader::NTrivial {

class TPlainReadData;
class ISourcesCollection;

class ISyncPoint {
public:
    enum class ESourceAction {
        Finish,
        ProvideNext,
        Continue,
        Wait
    };

protected:
    class TSourceEntry {
    public:
        const ui32 SourceIdx;
        const ui64 MemoryGroupIdx;
        std::unique_ptr<NCommon::TDataSourceLease> Lease;

        explicit TSourceEntry(const NCommon::IDataSource& source)
            : SourceIdx(source.GetSourceIdx())
            , MemoryGroupIdx(source.GetSequentialMemoryGroupIdx())
        {
        }
    };

private:
    YDB_READONLY(ui32, PointIndex, 0);
    YDB_READONLY_DEF(TString, PointName);
    std::optional<ui32> LastSourceIdx;
    virtual bool IsSourcePrepared(const NCommon::IDataSource& source) const = 0;
    virtual ESourceAction OnSourceReady(const NCommon::TDataSourceLease& lease, TPlainReadData& reader) = 0;
    virtual void DoAbort() = 0;
    bool AbortFlag = false;

    bool IsPrepared(const TSourceEntry& entry) const {
        return entry.Lease && IsSourcePrepared(entry.Lease->GetSource());
    }

    void InitSourceTracingMetrics(NCommon::IDataSource& source) const;

protected:
    const std::shared_ptr<TSpecialReadContext> Context;
    const std::shared_ptr<ISourcesCollection> Collection;
    std::shared_ptr<ISyncPoint> Next;
    std::deque<TSourceEntry> SourcesSequentially;

    virtual std::unique_ptr<NCommon::TDataSourceLease> DoOnSourceFinishedOnPreviouse() {
        return nullptr;
    }

    void OnSourceFinished();
    void ReleaseInFlightForPreparedEmptySources();

    virtual TString DoDebugString() const {
        return "";
    }

public:
    virtual ~ISyncPoint() = default;

    virtual std::unique_ptr<NCommon::TDataSourceLease> OnAddSource(std::unique_ptr<NCommon::TDataSourceLease> lease) {
        auto& source = lease->GetSource();
        SourcesSequentially.emplace_back(source);
        if (!source.GetAs<IDataSource>()->HasFetchingPlan()) {
            source.MutableAs<IDataSource>()->InitFetchingPlan(Context->GetColumnsFetchingPlan(source, !Next));
        }
        return lease;
    }

    void Continue(const TPartialSourceAddress& continueAddress, TPlainReadData& reader);

    TString DebugString() const;

    void Abort() {
        SourcesSequentially.clear();
        if (!AbortFlag) {
            AbortFlag = true;
            DoAbort();
        }
    }

    virtual bool IsFinished() const {
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
        , Collection(collection)
    {
    }

    void AddSource(std::unique_ptr<NCommon::TDataSourceLease> lease);

    void OnSourcePrepared(std::unique_ptr<NCommon::TDataSourceLease> lease, TPlainReadData& reader);
};

}   // namespace NKikimr::NOlap::NReader::NTrivial
