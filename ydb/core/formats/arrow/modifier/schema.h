#pragma once
#include <ydb/library/conclusion/status.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/generic/hash.h>

namespace NKikimr::NArrow::NModifier {
class TSchema {
private:
    THashMap<std::string, ui32> IndexByName;
    std::vector<std::shared_ptr<arrow::Field>> Fields;
    bool Finished = false;
public:
    TSchema() = default;
    TSchema(const std::shared_ptr<arrow::Schema>& schema)
        : TSchema(schema->fields())
    {
    }

    TSchema(const std::vector<std::shared_ptr<arrow::Field>>& fields) {
        for (auto&& i : fields) {
            IndexByName.emplace(i->name(), Fields.size());
            Fields.emplace_back(i);
        }
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

    bool HasField(const std::string& name) const {
        return IndexByName.contains(name);
    }

    i32 num_fields() const {
        return Fields.size();
    }

    const std::shared_ptr<arrow::Field>& GetFieldVerified(const ui32 index) const;

    const std::shared_ptr<arrow::Field>& field(const ui32 index) const;
};
}