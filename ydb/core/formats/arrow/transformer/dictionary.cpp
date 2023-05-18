#include "dictionary.h"
#include <ydb/core/formats/arrow/dictionary/conversion.h>
namespace NKikimr::NArrow::NTransformation {

std::shared_ptr<arrow::RecordBatch> TDictionaryPackTransformer::DoTransform(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    return ArrayToDictionary(batch);
}

std::shared_ptr<arrow::RecordBatch> TDictionaryUnpackTransformer::DoTransform(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    return DictionaryToArray(batch);
}

}
