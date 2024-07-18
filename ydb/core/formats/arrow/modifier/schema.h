#pragma once

namespace NKikimr::NArrow::NModifier {
class TSchema {
private:
    THashMap<std::string, std::shared_ptr<arrow::Field>> FieldsByName;
    std::vector<std::shared_ptr<arrow::Field>> Fields;
    bool Finished = false;
public:
    TSchema() = default;
    TSchema(const std::shared_ptr<arrow::Schema>& schema) {
        for (auto&& i : schema->num_fields()) {
            FieldsByName.emplace(i->name(), i);
            Fields.emplace_back(i);
        }
    }

    std::shared_ptr<arrow::Schema> Finish();
    [[nodiscard]] TConclusionStatus AddField(const std::shared_ptr<arrow::Field>& f);
    const std::shared_ptr<arrow::Field>& GetFieldByName(const std::string& name) const;

    bool HasField(const std::string& name) const {
        return FieldsByName.contains(name);
    }

    ui32 num_fields() const {
        return Fields.size();
    }

    const std::shared_ptr<arrow::Field>& field(const ui32 index) const {
        AFL_VERIFY(index < Fields.size());
        return Fields[index];
    }
};
}