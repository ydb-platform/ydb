#pragma once
#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TCategorySignals: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

    NMonitoring::THistogramPtr HistogramRawDataSizeBytes;
    NMonitoring::THistogramPtr HistogramRawDataSizeCount;
    NMonitoring::THistogramPtr HistogramRawDataSizeDuration;

    NMonitoring::THistogramPtr HistogramBlobDataSizeBytes;
    NMonitoring::THistogramPtr HistogramBlobDataSizeCount;
    NMonitoring::THistogramPtr HistogramBlobDataSizeDuration;

public:
    TCategorySignals(NColumnShard::TCommonCountersOwner& owner, const TString& categoryName)
        : TBase(owner, "category", categoryName)
        , HistogramRawDataSizeBytes(TBase::GetHistogram("RawData/BySize/Bytes", NMonitoring::ExponentialHistogram(15, 2, 100)))
        , HistogramRawDataSizeCount(TBase::GetHistogram("RawData/BySize/Count", NMonitoring::ExponentialHistogram(15, 2, 100)))
        , HistogramRawDataSizeDuration(TBase::GetHistogram("RawData/BySize/Duration/Us", NMonitoring::ExponentialHistogram(15, 2, 100)))
        , HistogramBlobDataSizeBytes(TBase::GetHistogram("BlobData/BySize/Bytes", NMonitoring::ExponentialHistogram(15, 2, 100)))
        , HistogramBlobDataSizeCount(TBase::GetHistogram("BlobData/BySize/Count", NMonitoring::ExponentialHistogram(15, 2, 100)))
        , HistogramBlobDataSizeDuration(TBase::GetHistogram("BlobData/BySize/Duration/Us", NMonitoring::ExponentialHistogram(15, 2, 100))) {
    }

    void OnBlobSize(const i64 rawDataSize, const i64 blobDataSize, const TDuration d) const {
        HistogramBlobDataSizeBytes->Collect(blobDataSize, blobDataSize);
        HistogramBlobDataSizeCount->Collect(blobDataSize);
        HistogramBlobDataSizeDuration->Collect(blobDataSize, d.MicroSeconds());

        HistogramRawDataSizeBytes->Collect(rawDataSize, rawDataSize);
        HistogramRawDataSizeCount->Collect(rawDataSize);
        HistogramRawDataSizeDuration->Collect(rawDataSize, d.MicroSeconds());
    }
};

class TSignalsImpl: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

    TCategorySignals ColumnSignals;
    TCategorySignals OtherSignals;

public:
    TSignalsImpl()
        : TBase("sub_columns")
        , ColumnSignals(*this, "columns")
        , OtherSignals(*this, "other") {
    }

    const TCategorySignals& GetColumnSignals() const {
        return ColumnSignals;
    }
    const TCategorySignals& GetOtherSignals() const {
        return OtherSignals;
    }
};

class TSignals {
public:
    static const TCategorySignals& GetColumnSignals() {
        return Singleton<TSignalsImpl>()->GetColumnSignals();
    }
    static const TCategorySignals& GetOtherSignals() {
        return Singleton<TSignalsImpl>()->GetOtherSignals();
    }
};
}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
