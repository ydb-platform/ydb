#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/conclusion/status.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/generic/hash.h>

namespace NKikimr::NArrow {

class TSchemaLite {
private:
    YDB_READONLY_DEF(std::vector<std::shared_ptr<arrow::Field>>, Fields);

public:
    TSchemaLite() = default;
    TSchemaLite(const std::shared_ptr<arrow::Schema>& schema) {
        AFL_VERIFY(schema);
        Fields = schema->fields();
    }

    const std::shared_ptr<arrow::Field>& field(const ui32 index) const {
        return GetFieldByIndexVerified(index);
    }

    bool Equals(const TSchemaLite& schema, const bool withMetadata = false) const {
        if (Fields.size() != schema.Fields.size()) {
            return false;
        }
        for (ui32 i = 0; i < Fields.size(); ++i) {
            if (!Fields[i]->Equals(schema.Fields[i], withMetadata)) {
                return false;
            }
        }
        return true;
    }

    const std::vector<std::shared_ptr<arrow::Field>>& fields() const {
        return Fields;
    }

    int num_fields() const {
        return Fields.size();
    }

    std::vector<std::string> field_names() const {
        std::vector<std::string> result;
        result.reserve(Fields.size());
        for (auto&& f : Fields) {
            result.emplace_back(f->name());
        }
        return result;
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "[";
        for (auto&& f : Fields) {
            sb << f->ToString() << ";";
        }
        sb << "]";

        return sb;
    }

    TString ToString() const {
        return DebugString();
    }

    const std::shared_ptr<arrow::Field>& GetFieldByIndexVerified(const ui32 index) const {
        AFL_VERIFY(index < Fields.size());
        return Fields[index];
    }

    const std::shared_ptr<arrow::Field>& GetFieldByIndexOptional(const ui32 index) const {
        if (index < Fields.size()) {
            return Fields[index];
        }
        return Default<std::shared_ptr<arrow::Field>>();
    }

    TSchemaLite(std::vector<std::shared_ptr<arrow::Field>>&& fields)
        : Fields(std::move(fields)) {
    }

    TSchemaLite(const std::vector<std::shared_ptr<arrow::Field>>& fields)
        : Fields(fields) {
    }
};

}   // namespace NKikimr::NArrow

namespace NKikimr::NArrow::NModifier {
class TSchema {
private:
    bool Initialized = false;
    THashMap<std::string, ui32> IndexByName;
    std::vector<std::shared_ptr<arrow::Field>> Fields;
    bool Finished = false;

    void Initialize(const std::vector<std::shared_ptr<arrow::Field>>& fields);

public:
    TSchema() = default;
    TSchema(const std::shared_ptr<TSchema>& schema);

    TSchema(const std::shared_ptr<arrow::Schema>& schema);

    TSchema(const std::vector<std::shared_ptr<arrow::Field>>& fields) {
        Initialize(fields);
    }

    i32 GetFieldIndex(const std::string& fName) const {
        auto it = IndexByName.find(fName);
        if (it == IndexByName.end()) {
            return -1;
        }
        return it->second;
    }

    const std::vector<std::shared_ptr<arrow::Field>>& GetFields() const {
        return Fields;
    }

    TString ToString() const;

    std::shared_ptr<arrow::Schema> Finish();
    [[nodiscard]] TConclusionStatus AddField(const std::shared_ptr<arrow::Field>& f);
    const std::shared_ptr<arrow::Field>& GetFieldByName(const std::string& name) const;
    void DeleteFieldsByIndex(const std::vector<ui32>& idxs);

    bool HasField(const std::string& name) const {
        return IndexByName.contains(name);
    }

    i32 num_fields() const {
        return Fields.size();
    }

    const std::shared_ptr<arrow::Field>& GetFieldVerified(const ui32 index) const;

    const std::shared_ptr<arrow::Field>& field(const ui32 index) const;

private:
    class TFieldsErasePolicy {
    private:
        TSchema* const Owner;

    public:
        TFieldsErasePolicy(TSchema* const owner)
            : Owner(owner) {
        }

        void OnEraseItem(const std::shared_ptr<arrow::Field>& item) const {
            Owner->IndexByName.erase(item->name());
        }

        void OnMoveItem(const std::shared_ptr<arrow::Field>& item, const ui64 new_index) const {
            auto* findField = Owner->IndexByName.FindPtr(item->name());
            AFL_VERIFY(findField);
            *findField = new_index;
        }
    };
};
}   // namespace NKikimr::NArrow::NModifier
