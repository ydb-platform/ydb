#include "hash_by_columns.h"
#include <ydb/library/services/services.pb.h>
#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/minikql/jsonpath/jsonpath.h>
#include <ydb/library/yql/minikql/jsonpath/value.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <library/cpp/json/fast_sax/parser.h>
#include <ydb/library/actors/core/log.h>
#include <util/string/split.h>
#include <util/string/join.h>

namespace NKikimr::NMetadata::NCSIndex {

THashByColumns::TFactory::TRegistrator<THashByColumns> THashByColumns::Registrator(THashByColumns::ClassName);
THashByColumns::TFactory::TRegistrator<THashByColumns> THashByColumns::RegistratorDeprecated("city64");

template <class TArrayBuilder>
class TArrayInserter {
protected:
    TArrayBuilder ArrayBuilder;
public:
    TArrayInserter(const ui32 rowsCount) {
        YQL_ENSURE(ArrayBuilder.Reserve(rowsCount).ok());
    }

    std::shared_ptr<arrow::Array> BuildColumn() {
        std::shared_ptr<arrow::StringArray> result;
        if (!ArrayBuilder.Finish(&result).ok()) {
            ALS_ERROR(NKikimrServices::EXT_INDEX) << "cannot build array for hash calculation";
            return nullptr;
        }
        return result;
    }
};

class TStringArrayInserter: public TArrayInserter<arrow::StringBuilder> {
private:
    using TBase = TArrayInserter<arrow::StringBuilder>;
public:
    using TBase::TBase;
    void OnFound(const TStringBuf& value) {
        YQL_ENSURE(ArrayBuilder.Append(value.data(), value.size()).ok());
    }
};



std::vector<ui64> THashByColumns::DoExtractIndex(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    auto schema = batch->schema();
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> columns;
    std::vector<TString> fieldIds;
    for (ui32 i = 0; i < Fields.size(); ++i) {
        auto c = batch->GetColumnByName(Fields[i].GetFieldId());
        auto f = schema->GetFieldByName(Fields[i].GetFieldId());
        if (!c || !f) {
            ALS_ERROR(NKikimrServices::EXT_INDEX) << "incorrect field name in batch: " << Fields[i].GetFieldId();
            return {};
        }
        if (!Fields[i].GetJsonPath()) {
            fields.emplace_back(f);
            columns.emplace_back(c);
            fieldIds.emplace_back(f->name());
        } else if (c->type()->id() == arrow::Type::STRING) {
            ALS_ERROR(NKikimrServices::EXT_INDEX) << "json have not been simple string. it must be JsonDocument for " << Fields[i].GetFieldId();
            return {};
        } else if (c->type()->id() == arrow::Type::BINARY) {
            auto typedColumn = std::static_pointer_cast<arrow::BinaryArray>(c);

            std::shared_ptr<TStringArrayInserter> fetcher = std::make_shared<TStringArrayInserter>(batch->num_rows());
            TVector<TString> values;
            for (ui32 r = 0; r < batch->num_rows(); ++r) {
                auto sv = typedColumn->Value(r);
                TStringBuf sb(sv.data(), sv.size());
                auto reader = NBinaryJson::TBinaryJsonReader::Make(sb);
                auto binaryJsonRoot = NYql::NJsonPath::TValue(reader->GetRootCursor());

                NYql::TIssues issues;
                const NYql::NJsonPath::TJsonPathPtr jsonPath = NYql::NJsonPath::ParseJsonPath(Fields[i].GetJsonPath(), issues, 100);
                if (!issues.Empty()) {
                    ALS_ERROR(NKikimrServices::EXT_INDEX) << "cannot parse path for json extraction: " << issues.ToString();
                    return {};
                }

                const auto result = NYql::NJsonPath::ExecuteJsonPath(jsonPath, binaryJsonRoot, NYql::NJsonPath::TVariablesMap{}, nullptr);
                if (result.IsError()) {
                    ALS_ERROR(NKikimrServices::EXT_INDEX) << "Runtime errors found on json path usage: " << result.GetError().ToString();
                    return {};
                }

                const auto& nodes = result.GetNodes();
                for (size_t i = 0; i < nodes.size(); i++) {
                    switch (nodes[i].GetType()) {
                        case NYql::NJsonPath::EValueType::Bool:
                            values.emplace_back(::ToString(nodes[i].GetBool()));
                            break;
                        case NYql::NJsonPath::EValueType::Number:
                            values.emplace_back(::ToString(nodes[i].GetNumber()));
                            break;
                        case NYql::NJsonPath::EValueType::String:
                            values.emplace_back(nodes[i].GetString());
                            break;
                        case NYql::NJsonPath::EValueType::Null:
                            values.emplace_back("");
                            break;
                        case NYql::NJsonPath::EValueType::Object:
                        case NYql::NJsonPath::EValueType::Array:
                            ALS_ERROR(NKikimrServices::EXT_INDEX) << "Cannot use object and array as hash param for index construction";
                            return {};
                    }
                }
                fetcher->OnFound(JoinSeq(",", values));
                values.clear();
            }
            fieldIds.emplace_back(Fields[i].GetFullId());
            fields.emplace_back(std::make_shared<arrow::Field>(Fields[i].GetFullId(), std::make_shared<arrow::StringType>()));
            columns.emplace_back(fetcher->BuildColumn());
        } else {
            ALS_ERROR(NKikimrServices::EXT_INDEX) << "incorrect column type for json extraction: " << Fields[i].GetFieldId();
            return {};
        }
    }
    auto sBuilder = std::make_shared<arrow::SchemaBuilder>(fields);
    auto newSchema = sBuilder->Finish();
    if (!newSchema.ok()) {
        ALS_ERROR(NKikimrServices::EXT_INDEX) << "cannot build new schema";
        return {};
    }
    auto newBatch = arrow::RecordBatch::Make(*newSchema, batch->num_rows(), columns);
    if (HashType == EHashType::XX64) {
        NArrow::NHash::TXX64 hash(fieldIds, NArrow::NHash::TXX64::ENoColumnPolicy::ReturnEmpty);
        auto result = hash.Execute(newBatch);
        AFL_VERIFY(result);
        return *result;
    } else {
        ALS_ERROR(NKikimrServices::EXT_INDEX) << "undefined hash type: " << HashType;
        return {};
    }
}

bool THashByColumns::DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
    const NJson::TJsonValue::TArray* jsonFields;
    if (!jsonInfo["fields"].GetArrayPointer(&jsonFields)) {
        return false;
    }
    for (auto&& i : *jsonFields) {
        TExtractorField field;
        if (!field.DeserializeFromJson(i)) {
            return false;
        }
        Fields.emplace_back(field);
    }
    if (Fields.size() == 0) {
        return false;
    }

    if (jsonInfo.Has("hash_type")) {
        if (!jsonInfo["hash_type"].IsString()) {
            return false;
        }
        if (!TryFromString(jsonInfo["hash_type"].GetString(), HashType)) {
            return false;
        }
    }

    return true;
}

NJson::TJsonValue THashByColumns::DoSerializeToJson() const {
    NJson::TJsonValue result;
    result.InsertValue("hash_type", ::ToString(HashType));
    auto& jsonFields = result.InsertValue("fields", NJson::JSON_ARRAY);
    for (auto&& i : Fields) {
        jsonFields.AppendValue(i.SerializeToJson());
    }
    return result;
}

}
