#pragma once

#include <yql/essentials/core/histogram/eq_height_histogram.h>

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

namespace NKikimr {

// Test-only access to TEqHeightHistogramBuilder internals.
// Methods that read the summary call Flush() first.
class TEqHeightHistogramBuilderTestApi {
public:
    explicit TEqHeightHistogramBuilderTestApi(TEqHeightHistogramBuilder& builder)
        : Builder_(builder)
    {
    }

    const TVector<TEqHeightHistogramBuilder::TEntry>& GetEntries() {
        Builder_.Flush();
        return Builder_.Summary_.Entries();
    }

    ui64 GetMaxRankError() {
        Builder_.Flush();
        return Builder_.Summary_.MaxRankUncertainty();
    }

    bool IsExact() {
        return GetMaxRankError() == 0;
    }

    bool GetBudgetForced() {
        Builder_.Flush();
        return Builder_.Summary_.BudgetForced();
    }

    ui64 GetCountCap() const {
        return Builder_.Capacity();
    }

    ui64 GetFlushCount() const {
        return Builder_.FlushCount_;
    }

    TStringBuf GetMinKey() const {
        return Builder_.MinKey_;
    }

    void SetSummaryBytes(ui64 bytes) {
        Builder_.Flush();
        Builder_.Summary_.Bytes_ = bytes;
    }

    ui64 GetSummaryBytes() {
        Builder_.Flush();
        return Builder_.Summary_.Bytes();
    }

    ui64 GetStateBytes() {
        Builder_.Flush();
        return Builder_.StateBytes();
    }

private:
    TEqHeightHistogramBuilder& Builder_;
};

} // namespace NKikimr
