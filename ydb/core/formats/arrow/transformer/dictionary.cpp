#include "dictionary.h"
#include <ydb/core/formats/arrow/dictionary/conversion.h>
namespace NKikimr::NArrow::NTransformation {

std::shared_ptr<arrow20::RecordBatch> TDictionaryPackTransformer::DoTransform(const std::shared_ptr<arrow20::RecordBatch>& batch) const {
    return ArrayToDictionary(batch);
}

std::shared_ptr<arrow20::RecordBatch> TDictionaryUnpackTransformer::DoTransform(const std::shared_ptr<arrow20::RecordBatch>& batch) const {
    return DictionaryToArray(batch);
}

}
