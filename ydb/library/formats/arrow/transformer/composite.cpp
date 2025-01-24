#include "composite.h"
#include <util/string/builder.h>

namespace NKikimr::NArrow::NTransformation {

std::shared_ptr<arrow::RecordBatch> TCompositeTransformer::DoTransform(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    std::shared_ptr<arrow::RecordBatch> current = batch;
    for (auto&& i : Transformers) {
        current = i->Transform(current);
    }
    return current;
}

TString TCompositeTransformer::DoDebugString() const {
    TStringBuilder sb;
    for (auto&& i : Transformers) {
        sb << "(" << i->DebugString() << ");";
    }
    return sb;
}

}
