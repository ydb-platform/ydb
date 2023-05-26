#include "composite.h"

namespace NKikimr::NArrow::NTransformation {

std::shared_ptr<arrow::RecordBatch> TCompositeTransformer::DoTransform(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    std::shared_ptr<arrow::RecordBatch> current = batch;
    for (auto&& i : Transformers) {
        current = i->Transform(current);
    }
    return current;
}

}
