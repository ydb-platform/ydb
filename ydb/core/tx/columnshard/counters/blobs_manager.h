#pragma once
#include <ydb/core/base/logoblob.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include "common/owner.h"

namespace NKikimr::NColumnShard {

class TBlobsManagerCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr CollectDropExplicitBytes;
    NMonitoring::TDynamicCounters::TCounterPtr CollectDropExplicitCount;
    NMonitoring::TDynamicCounters::TCounterPtr CollectDropImplicitBytes;
    NMonitoring::TDynamicCounters::TCounterPtr CollectDropImplicitCount;
    NMonitoring::TDynamicCounters::TCounterPtr CollectKeepBytes;
    NMonitoring::TDynamicCounters::TCounterPtr CollectKeepCount;
    NMonitoring::TDynamicCounters::TCounterPtr PutBlobBytes;
    NMonitoring::TDynamicCounters::TCounterPtr PutBlobCount;
    NMonitoring::TDynamicCounters::TCounterPtr CollectGen;
    NMonitoring::TDynamicCounters::TCounterPtr CollectStep;
    NMonitoring::TDynamicCounters::TCounterPtr DeleteBlobMarkerBytes;
    NMonitoring::TDynamicCounters::TCounterPtr DeleteBlobMarkerCount;
    NMonitoring::TDynamicCounters::TCounterPtr DeleteBlobDelayedMarkerBytes;
    NMonitoring::TDynamicCounters::TCounterPtr DeleteBlobDelayedMarkerCount;
    NMonitoring::TDynamicCounters::TCounterPtr AddSmallBlobBytes;
    NMonitoring::TDynamicCounters::TCounterPtr AddSmallBlobCount;
    NMonitoring::TDynamicCounters::TCounterPtr DeleteSmallBlobBytes;
    NMonitoring::TDynamicCounters::TCounterPtr DeleteSmallBlobCount;
    NMonitoring::TDynamicCounters::TCounterPtr BrokenKeepCount;
    NMonitoring::TDynamicCounters::TCounterPtr BrokenKeepBytes;
    NMonitoring::TDynamicCounters::TCounterPtr BlobsKeepCount;
    NMonitoring::TDynamicCounters::TCounterPtr BlobsKeepBytes;
    NMonitoring::TDynamicCounters::TCounterPtr BlobsDeleteCount;
    NMonitoring::TDynamicCounters::TCounterPtr BlobsDeleteBytes;
    NMonitoring::TDynamicCounters::TCounterPtr KeepMarkerCount;
    NMonitoring::TDynamicCounters::TCounterPtr KeepMarkerBytes;

public:
    NMonitoring::TDynamicCounters::TCounterPtr SkipCollection;
    NMonitoring::TDynamicCounters::TCounterPtr StartCollection;

    TBlobsManagerCounters(const TString& module);

    void OnKeepMarker(const ui64 size) const {
        KeepMarkerCount->Add(1);
        KeepMarkerBytes->Add(size);
    }

    void OnBlobsKeep(const TSet<TLogoBlobID>& blobs) const;

    void OnBlobsDelete(const TSet<TLogoBlobID>& /*blobs*/) const;

    void OnAddSmallBlob(const ui32 bSize) const {
        AddSmallBlobBytes->Add(bSize);
        AddSmallBlobCount->Add(1);
    }

    void OnDeleteBlobDelayedMarker(const ui32 bSize) const {
        DeleteBlobDelayedMarkerBytes->Add(bSize);
        DeleteBlobDelayedMarkerCount->Add(1);
    }

    void OnDeleteBlobMarker(const ui32 bSize) const {
        DeleteBlobMarkerBytes->Add(bSize);
        DeleteBlobMarkerCount->Add(1);
    }

    void OnNewCollectStep(const ui32 gen, const ui32 step) const {
        CollectGen->Set(gen);
        CollectStep->Set(step);
    }

    void OnDeleteSmallBlob(const ui32 bSize) const {
        DeleteSmallBlobBytes->Add(bSize);
        DeleteSmallBlobCount->Add(1);
    }

    void OnPutResult(const ui32 bSize) const {
        PutBlobBytes->Add(bSize);
        PutBlobCount->Add(1);
    }

    void OnCollectKeep(const ui32 bSize) const {
        CollectKeepBytes->Add(bSize);
        CollectKeepCount->Add(1);
    }

    void OnBrokenKeep(const ui32 bSize) const {
        BrokenKeepBytes->Add(bSize);
        BrokenKeepCount->Add(1);
    }

    void OnCollectDropExplicit(const ui32 bSize) const {
        CollectDropExplicitBytes->Add(bSize);
        CollectDropExplicitCount->Add(1);
    }

    void OnCollectDropImplicit(const ui32 bSize) const {
        CollectDropImplicitBytes->Add(bSize);
        CollectDropImplicitCount->Add(1);
    }
};

}
