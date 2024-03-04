#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include "common/owner.h"

namespace NKikimr::NColumnShard {

class TSplitterCaseCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr TrashDataSerializationBytes;
    NMonitoring::TDynamicCounters::TCounterPtr TrashDataSerialization;
    NMonitoring::THistogramPtr TrashDataSerializationHistogramBytes;

    NMonitoring::TDynamicCounters::TCounterPtr CorrectDataSerializationBytes;
    NMonitoring::TDynamicCounters::TCounterPtr CorrectDataSerialization;
    NMonitoring::THistogramPtr CorrectDataSerializationHistogramBytes;
public:
    TSplitterCaseCounters(const TCommonCountersOwner& owner, const TString& splitterType)
        : TBase(owner)
    {
        DeepSubGroup("splitter_type", splitterType);

        TrashDataSerializationBytes = TBase::GetDeriviative("TrashDataSerialization/Bytes");
        TrashDataSerialization = TBase::GetDeriviative("TrashDataSerialization/Count");
        TrashDataSerializationHistogramBytes = TBase::GetHistogram("TrashDataSerialization/Bytes", NMonitoring::ExponentialHistogram(15, 2, 1024));
        CorrectDataSerializationBytes = TBase::GetDeriviative("CorrectDataSerialization/Bytes");
        CorrectDataSerialization = TBase::GetDeriviative("CorrectDataSerialization/Count");
        CorrectDataSerializationHistogramBytes = TBase::GetHistogram("CorrectDataSerialization/Bytes", NMonitoring::ExponentialHistogram(15, 2, 1024));
    }

    void OnTrashSerialized(const ui64 bytes) const {
        TrashDataSerializationHistogramBytes->Collect(bytes);
        TrashDataSerialization->Add(1);
        TrashDataSerializationBytes->Add(bytes);
    }

    void OnCorrectSerialized(const ui64 bytes) const {
        CorrectDataSerializationHistogramBytes->Collect(bytes);
        CorrectDataSerialization->Add(1);
        CorrectDataSerializationBytes->Add(bytes);
    }
};

class TBlobResultCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr BlobsCount;
    NMonitoring::TDynamicCounters::TCounterPtr BlobsBytes;
    NMonitoring::THistogramPtr BlobsBytesHistogram;
public:
    TBlobResultCounters(const TCommonCountersOwner& owner, const TString& blobsType)
        : TBase(owner) {
        DeepSubGroup("blobs_type", blobsType);

        BlobsCount = TBase::GetDeriviative("DataSerialization/Bytes");
        BlobsBytes = TBase::GetDeriviative("DataSerialization/Count");
        BlobsBytesHistogram = TBase::GetHistogram("DataSerialization/Bytes", NMonitoring::ExponentialHistogram(15, 2, 1024));
    }

    void OnBlobData(const ui64 size) const {
        BlobsCount->Add(1);
        BlobsBytes->Add(size);
        BlobsBytesHistogram->Collect(size);
    }

};

class TSplitterCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
public:
    TSplitterCounters(const TCommonCountersOwner& owner);
    const TSplitterCaseCounters SimpleSplitter;
    const TSplitterCaseCounters BySizeSplitter;
    const TBlobResultCounters SplittedBlobs;
    const TBlobResultCounters MonoBlobs;
};

}
