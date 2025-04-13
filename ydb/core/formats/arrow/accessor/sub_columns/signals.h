#pragma once
#include <ydb/library/signals/owner.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TCategorySignals: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

    NMonitoring::THistogramPtr HistogramRawDataSizeBytes;
    NMonitoring::THistogramPtr HistogramBlobDataSizeBytes;
    NMonitoring::THistogramPtr HistogramRawDataSizeCount;
    NMonitoring::THistogramPtr HistogramBlobDataSizeCount;

public:
    TCategorySignals(NColumnShard::TCommonCountersOwner& owner, const TString& categoryName)
        : TBase(owner, "category", categoryName)
        , HistogramRawDataSizeBytes(TBase::GetHistogram("RawData/BySize/Bytes", NMonitoring::ExponentialHistogram(15, 2, 100)))
        , HistogramBlobDataSizeBytes(TBase::GetHistogram("BlobData/BySize/Bytes", NMonitoring::ExponentialHistogram(15, 2, 100)))
        , HistogramRawDataSizeCount(TBase::GetHistogram("RawData/BySize/Count", NMonitoring::ExponentialHistogram(15, 2, 100)))
        , HistogramBlobDataSizeCount(TBase::GetHistogram("BlobData/BySize/Count", NMonitoring::ExponentialHistogram(15, 2, 100))) {
    }

    void OnBlobSize(const ui32 rawDataSize, const ui32 blobDataSize) const {
        HistogramBlobDataSizeBytes->Collect(blobDataSize, blobDataSize);
        HistogramRawDataSizeBytes->Collect(rawDataSize, rawDataSize);

        HistogramBlobDataSizeCount->Collect(blobDataSize);
        HistogramRawDataSizeCount->Collect(rawDataSize);
    }
};

class TSignalsImpl: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

    TCategorySignals ColumnSignals;
    TCategorySignals OtherSignals;

public:
    TCategorySignals()
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
