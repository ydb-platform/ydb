#pragma once
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

#include <ydb/library/accessor/positive_integer.h>

namespace NKikimr::NOlap::NReader::NCommon {
class TSpecialReadContext;
}

namespace NKikimr::NOlap::NReader::NSimple {

class ISourcesCollection {
private:
    virtual bool DoIsFinished() const = 0;
    virtual std::unique_ptr<NCommon::TDataSourceLease> DoTryExtractNext() = 0;
    virtual bool DoCheckInFlightLimits() const = 0;
    virtual void DoOnSourceFinished(const NCommon::IDataSource& source) = 0;
    virtual void DoClear() = 0;
    virtual void DoAbort() = 0;

    TPositiveControlInteger SourcesInFlightCount;
    YDB_READONLY(ui64, MaxInFlight, 1024);

    virtual TString DoDebugString() const {
        return "";
    }

    // The key the source starts at in scan direction. It reaches KQP as the result's LastKey, which decides
    // which shards a re-resolution may skip, so it must never be past a row still to come. Only a collection
    // whose results leave in key order has one at all.
    virtual std::shared_ptr<NArrow::TSimpleRow> DoGetSourceStartPK(const NCommon::IDataSource& /*source*/) const {
        return nullptr;
    }

    virtual bool DoHasData() const = 0;

protected:
    const std::shared_ptr<TSpecialReadContext> Context;
    std::unique_ptr<NCommon::ISourcesConstructor> SourcesConstructor;

public:
    ui64 GetTabletId() const {
        return Context->GetCommonContext()->GetReadMetadata()->GetTabletId();
    }

    virtual TString GetClassName() const = 0;

    template <class T>
    T& MutableConstructorsAs() {
        auto result = static_cast<T*>(SourcesConstructor.get());
        AFL_VERIFY(result);
        return *result;
    }

    ui64 GetSourcesInFlightCount() const {
        return SourcesInFlightCount.Val();
    }

    bool HasData() const {
        return DoHasData();
    }

    std::shared_ptr<IScanCursor> BuildCursor(const NCommon::IDataSource& source, const ui32 readyRecords, const ui64 tabletId) const;

    TString DebugString() const;

    virtual ~ISourcesCollection() = default;

    std::unique_ptr<NCommon::TDataSourceLease> TryExtractNext() {
        auto result = DoTryExtractNext();
        if (result) {
            SourcesInFlightCount.Inc();
        }
        return result;
    }

    bool IsFinished() const {
        return DoIsFinished();
    }

    void OnSourceFinished(const NCommon::IDataSource& source) {
        DoOnSourceFinished(source);
        if (!source.IsInFlightReleased()) {
            SourcesInFlightCount.Dec();
        }
    }

    void ReleaseInFlight(NCommon::IDataSource& source) {
        source.SetInFlightReleased();
        SourcesInFlightCount.Dec();
    }

    bool CheckInFlightLimits() const {
        return DoCheckInFlightLimits();
    }

    void Clear() {
        DoClear();
    }

    void Abort() {
        DoAbort();
    }

    ISourcesCollection(const std::shared_ptr<TSpecialReadContext>& context, std::unique_ptr<NCommon::ISourcesConstructor>&& sourcesConstructor);
};

}   // namespace NKikimr::NOlap::NReader::NSimple
