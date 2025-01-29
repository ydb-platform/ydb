#include "data_extractor.h"

#include <util/string/split.h>
#include <util/string/vector.h>
#include <yql/essentials/types/binary_json/format.h>
#include <yql/essentials/types/binary_json/read.h>

namespace NKikimr::NArrow::NAccessor {

TConclusion<std::shared_ptr<arrow::Schema>> TFirstLevelSchemaData::DoBuildSchemaForData(
    const std::shared_ptr<IChunkedArray>& sourceArray) const {
    IChunkedArray::TReader reader(sourceArray);
    if (sourceArray->GetDataType()->id() != arrow::binary()->id()) {
        return TConclusionStatus::Fail("incorrect base type for subcolumns schema usage");
    }

    THashMap<TString, ui32> frq;
    for (ui32 i = 0; i < reader.GetRecordsCount();) {
        auto address = reader.GetReadChunk(i);

        auto arr = std::static_pointer_cast<arrow::BinaryArray>(address.GetArray());
        for (ui32 i = 0; i < arr->length(); ++i) {
            const auto view = arr->GetView(i);
            NBinaryJson::TBinaryJson bJson(view.data(), view.size());
            auto reader = NBinaryJson::TBinaryJsonReader::Make(bJson);
            auto cursor = reader->GetRootCursor();
            if (cursor.GetType() != NBinaryJson::EContainerType::Object) {
                continue;
            }
            auto it = cursor.GetObjectIterator();
            while (it.HasNext()) {
                auto [key, value] = it.Next();
                if (key.GetType() != NBinaryJson::EEntryType::String) {
                    continue;
                }
                ++frq[key.GetString()];
            }
        }

        i += address.GetArray()->length();
        AFL_VERIFY(i <= reader.GetRecordsCount());
    }

    std::vector<TString> fieldNames;
    for (auto&& i : frq) {
        if (i.second > sourceArray->GetRecordsCount() * 0.9) {
            fieldNames.emplace_back(i.first);
        }
    }
    std::sort(fieldNames.begin(), fieldNames.end());
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto&& f : fieldNames) {
        fields.emplace_back(std::make_shared<arrow::Field>(f, arrow::utf8()));
    }
    return std::make_shared<arrow::Schema>(fields);
}

TConclusionStatus TFirstLevelSchemaData::DoAddDataToBuilders(const std::shared_ptr<arrow::Array>& sourceArray,
    const std::shared_ptr<arrow::Schema>& schema, const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders) const {
    if (sourceArray->type()->id() != arrow::binary()->id()) {
        return TConclusionStatus::Fail("incorrect base type for subcolumns schema usage");
    }

    auto arr = std::static_pointer_cast<arrow::BinaryArray>(sourceArray);
    for (ui32 i = 0; i < arr->length(); ++i) {
        const auto view = arr->GetView(i);
        NBinaryJson::TBinaryJson bJson(view.data(), view.size());
        auto reader = NBinaryJson::TBinaryJsonReader::Make(bJson);
        for (ui32 fIndex = 0; fIndex < (ui32)schema->num_fields(); ++fIndex) {
            auto cursor = reader->GetRootCursor();
            if (cursor.GetType() != NBinaryJson::EContainerType::Object) {
                TStatusValidator::Validate(builders[fIndex]->AppendNull());
                continue;
            }
            auto* strBuilder = static_cast<arrow::StringBuilder*>(builders[fIndex].get());
            const auto path = StringSplitter(schema->field(fIndex)->name()).SplitByString("$$").ToList<TString>();
            for (ui32 pathIdx = 0; pathIdx < path.size(); ++pathIdx) {
                auto next = cursor.Lookup(path[pathIdx]);
                if (!next) {
                    TStatusValidator::Validate(strBuilder->AppendNull());
                    break;
                } else if (pathIdx + 1 < path.size()) {
                    if (next->GetType() == NBinaryJson::EEntryType::Container &&
                        next->GetContainer().GetType() == NBinaryJson::EContainerType::Object) {
                        cursor = next->GetContainer();
                    } else {
                        TStatusValidator::Validate(strBuilder->AppendNull());
                        break;
                    }
                } else {
                    switch (next->GetType()) {
                        case NBinaryJson::EEntryType::Container:
                        case NBinaryJson::EEntryType::Null:
                            TStatusValidator::Validate(strBuilder->AppendNull());
                            break;
                        case NBinaryJson::EEntryType::Number: {
                            auto str = ::ToString(next->GetNumber());
                            TStatusValidator::Validate(strBuilder->Append(str.data(), str.size()));
                            break;
                        }
                        case NBinaryJson::EEntryType::String:
                            TStatusValidator::Validate(strBuilder->Append(next->GetString().data(), next->GetString().size()));
                            break;
                        case NBinaryJson::EEntryType::BoolFalse:
                            TStatusValidator::Validate(strBuilder->Append("0", 1));
                            break;
                        case NBinaryJson::EEntryType::BoolTrue:
                            TStatusValidator::Validate(strBuilder->Append("1", 1));
                            break;
                    }
                }
            }
        }
    }
    return TConclusionStatus::Success();
}

TConclusionStatus IDataAdapter::AddDataToBuilders(const std::shared_ptr<arrow::Array>& sourceArray, const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders) const {
    AFL_VERIFY((ui32)schema->num_fields() == builders.size())("schema", schema->ToString())("builders_count", builders.size());
    return DoAddDataToBuilders(sourceArray, schema, builders);
}

}   // namespace NKikimr::NArrow::NAccessor
