#include "data_extractor.h"

#include <util/string/split.h>
#include <util/string/vector.h>
#include <yql/essentials/types/binary_json/format.h>
#include <yql/essentials/types/binary_json/read.h>

namespace NKikimr::NArrow::NAccessor {

TConclusionStatus TFirstLevelSchemaData::DoAddDataToBuilders(const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const {
    if (sourceArray->type()->id() != arrow::binary()->id()) {
        return TConclusionStatus::Fail("incorrect base type for subcolumns schema usage");
    }

    auto arr = std::static_pointer_cast<arrow::BinaryArray>(sourceArray);
    for (ui32 i = 0; i < arr->length(); ++i) {
        const std::string_view view = arr->GetView(i);
        auto reader = NBinaryJson::TBinaryJsonReader::Make(view);
        auto cursor = reader->GetRootCursor();
        if (cursor.GetType() != NBinaryJson::EContainerType::Object) {
            return;
        }
        auto it = cursor.GetObjectIterator();
        while (it.HasNext()) {
            auto [key, value] = it.Next();
            if (key.GetType() != NBinaryJson::EEntryType::String) {
                continue;
            }
            if (value.GetType() == NBinaryJson::EEntryType::String) {
                dataBuilder.AddKV(key.GetString(), value.GetString());
            } else if (value.GetType() == NBinaryJson::EEntryType::Number) {
                dataBuilder.AddKVOwn(key.GetString(), ::ToString(value.GetNumber()));
            } else if (value.GetType() == NBinaryJson::EEntryType::BoolFalse) {
                dataBuilder.AddKVOwn(key.GetString(), "0");
            } else if (value.GetType() == NBinaryJson::EEntryType::BoolTrue) {
                dataBuilder.AddKVOwn(key.GetString(), "1");
            } else {
                continue;
            }
        }
        dataBuilder.StartNextRecord();
    }
    return TConclusionStatus::Success();
}

TConclusionStatus IDataAdapter::AddDataToBuilders(const std::shared_ptr<arrow::Array>& sourceArray, const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders) const {
    AFL_VERIFY((ui32)schema->num_fields() == builders.size())("schema", schema->ToString())("builders_count", builders.size());
    return DoAddDataToBuilders(sourceArray, schema, builders);
}

}   // namespace NKikimr::NArrow::NAccessor
