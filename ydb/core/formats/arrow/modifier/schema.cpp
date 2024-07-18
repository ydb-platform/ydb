#include "schema.h"

namespace NKikimr::NArrow::NModifier {

std::shared_ptr<arrow::Schema> TSchema::Finish() {
    AFL_VERIFY(!Finished);
    Finished = true;
    return std::make_shared<arrow::Schema>(Fields);
}

const std::shared_ptr<arrow::Field>& TSchema::GetFieldByName(const std::string& name) const {
    AFL_VERIFY(!Finished);
    auto it = FieldsByName.find(name);
    if (it == FieldsByName.end()) {
        return Default<std::shared_ptr<arrow::Field>>();
    } else {
        return it->second;
    }
}

TConclusionStatus TSchema::AddField(const std::shared_ptr<arrow::Field>& f) {
    AFL_VERIFY(!Finished);
    if (!FieldsByName.emplace(f->name(), f).second) {
        return TConclusionStatus::Fail("field name duplication: " + f->name());
    }
    Fields.emplace_back(f);
    return TConclusionStatus::Success();
}

}