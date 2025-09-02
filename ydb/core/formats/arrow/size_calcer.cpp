#include "size_calcer.h"
#include "switch/switch_type.h"
#include "arrow_helpers.h"
#include "dictionary/conversion.h"
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/system/yassert.h>
#include <util/string/builder.h>

namespace NKikimr::NArrow {

TConclusion<std::vector<TSerializedBatch>> SplitByBlobSize(const std::shared_ptr<arrow::RecordBatch>& batch, const TBatchSplitttingContext& context) {
    if (GetBatchDataSize(batch) <= context.GetSizeLimit()) {
        return TSerializedBatch::BuildWithLimit(batch, context);
    }
    TRowSizeCalculator rowCalculator(8);
    if (!rowCalculator.InitBatch(batch)) {
        return TConclusionStatus::Fail("unexpected column type on batch initialization for row size calculator");
    }
    ui32 currentSize = 0;
    ui32 startIdx = 0;
    std::vector<TSerializedBatch> result;
    for (ui32 i = 0; i < batch->num_rows(); ++i) {
        const ui32 rowSize = rowCalculator.GetRowBytesSize(i);
        if (rowSize > context.GetSizeLimit()) {
            return TConclusionStatus::Fail("there is row with size more then limit (" + ::ToString(context.GetSizeLimit()) + ")");
        }
        if (rowCalculator.GetApproxSerializeSize(currentSize + rowSize) > context.GetSizeLimit()) {
            if (!currentSize) {
                return TConclusionStatus::Fail("there is row with size + metadata more then limit (" + ::ToString(context.GetSizeLimit()) + ")");
            }
            auto localResult = TSerializedBatch::BuildWithLimit(batch->Slice(startIdx, i - startIdx), context);
            if (localResult.IsFail()) {
                return TConclusionStatus::Fail("cannot build blobs for batch slice (" + ::ToString(i - startIdx) + " rows): " + localResult.GetErrorMessage());
            } else {
                result.insert(result.end(), localResult.GetResult().begin(), localResult.GetResult().end());
            }
            currentSize = 0;
            startIdx = i;
        }
        currentSize += rowSize;
    }
    if (currentSize) {
        auto localResult = TSerializedBatch::BuildWithLimit(batch->Slice(startIdx, batch->num_rows() - startIdx), context);
        if (localResult.IsFail()) {
            return TConclusionStatus::Fail("cannot build blobs for last batch slice (" + ::ToString(batch->num_rows() - startIdx) + " rows): " + localResult.GetErrorMessage());
        } else {
            result.insert(result.end(), localResult.GetResult().begin(), localResult.GetResult().end());
        }
    }
    return result;
}

NKikimr::NArrow::TSerializedBatch TSerializedBatch::Build(std::shared_ptr<arrow::RecordBatch> batch, const TBatchSplitttingContext& context) {
    std::optional<TString> specialKeysPayload;
    std::optional<TString> specialKeysFull;
    if (context.GetFieldsForSpecialKeys().size()) {
        TFirstLastSpecialKeys specialKeys(batch, context.GetFieldsForSpecialKeys());
        specialKeysPayload = specialKeys.SerializePayloadToString();
        specialKeysFull = specialKeys.SerializeFullToString();
    }
    return TSerializedBatch(NArrow::SerializeBatchNoCompression(batch), batch->num_rows(),
        NArrow::GetBatchDataSize(batch), specialKeysPayload, specialKeysFull);
}

TConclusionStatus TSerializedBatch::BuildWithLimit(std::shared_ptr<arrow::RecordBatch> batch, const TBatchSplitttingContext& context, std::optional<TSerializedBatch>& sbL, std::optional<TSerializedBatch>& sbR) {
    TSerializedBatch sb = TSerializedBatch::Build(batch, context);
    const ui32 length = batch->num_rows();
    if (sb.GetSize() <= context.GetSizeLimit()) {
        sbL = std::move(sb);
        return TConclusionStatus::Success();
    } else if (length == 1) {
        return TConclusionStatus::Fail(TStringBuilder() << "original batch too big: " << sb.GetSize() << " and contains 1 row (cannot be splitted)");
    } else {
        const ui32 delta = length / 2;
        TSerializedBatch localSbL = TSerializedBatch::Build(batch->Slice(0, delta), context);
        TSerializedBatch localSbR = TSerializedBatch::Build(batch->Slice(delta, length - delta), context);
        if (localSbL.GetSize() > context.GetSizeLimit() || localSbR.GetSize() > context.GetSizeLimit()) {
            return TConclusionStatus::Fail(TStringBuilder() << "original batch too big: " << sb.GetSize() << " and after 2 parts split we have: "
                << localSbL.GetSize() << "(" << localSbL.GetRowsCount() << ")" << " / "
                << localSbR.GetSize() << "(" << localSbR.GetRowsCount() << ")" << " part sizes. Its unexpected for limit " << context.GetSizeLimit());
        }
        sbL = std::move(localSbL);
        sbR = std::move(localSbR);
        return TConclusionStatus::Success();
    }
}

TConclusion<std::vector<TSerializedBatch>> TSerializedBatch::BuildWithLimit(std::shared_ptr<arrow::RecordBatch> batch, const TBatchSplitttingContext& context) {
    std::vector<TSerializedBatch> result;
    std::optional<TSerializedBatch> sbL;
    std::optional<TSerializedBatch> sbR;
    auto simpleResult = TSerializedBatch::BuildWithLimit(batch, context, sbL, sbR);
    if (simpleResult.IsFail()) {
        return simpleResult;
    }
    if (sbL) {
        result.emplace_back(std::move(*sbL));
    }
    if (sbR) {
        result.emplace_back(std::move(*sbR));
    }
    return result;
}

TString TSerializedBatch::DebugString() const {
    return TStringBuilder() << "(data_size=" << Data.size() << ";rows_count=" << RowsCount << ";raw_bytes=" << RawBytes << ";)";
}

}
