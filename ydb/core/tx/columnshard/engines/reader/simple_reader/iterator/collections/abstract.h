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
    virtual std::shared_ptr<NCommon::IDataSource> DoTryExtractNext() = 0;
    virtual bool DoCheckInFlightLimits() const = 0;
    virtual void DoOnSourceFinished(const std::shared_ptr<NCommon::IDataSource>& source) = 0;
    virtual void DoClear() = 0;
    virtual void DoAbort() = 0;

    TPositiveControlInteger SourcesInFlightCount;
    YDB_READONLY(ui64, MaxInFlight, 1024);

    virtual TString DoDebugString() const {
        return "";
    }
    virtual std::shared_ptr<IScanCursor> DoBuildCursor(const std::shared_ptr<NCommon::IDataSource>& source, const ui32 readyRecords) const = 0;
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

    std::shared_ptr<IScanCursor> BuildCursor(
        const std::shared_ptr<NCommon::IDataSource>& source, const ui32 readyRecords, const ui64 tabletId) const;

    TString DebugString() const;

    virtual ~ISourcesCollection() = default;

    std::shared_ptr<NCommon::IDataSource> TryExtractNext() {
        if (auto result = DoTryExtractNext()) {
            SourcesInFlightCount.Inc();
            return result;
        } else {
            return nullptr;
        }
    }

    bool IsFinished() const {
        return DoIsFinished();
    }

    void OnSourceFinished(const std::shared_ptr<NCommon::IDataSource>& source) {
        AFL_VERIFY(source);
        DoOnSourceFinished(source);
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
