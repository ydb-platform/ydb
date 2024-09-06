#pragma once
#include <ydb/library/actors/core/log.h>
#include <ydb/library/conclusion/status.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/generic/hash.h>

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
}