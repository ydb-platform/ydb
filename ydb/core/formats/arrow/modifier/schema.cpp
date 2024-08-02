#include "schema.h"
#include <util/string/builder.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NArrow::NModifier {

std::shared_ptr<arrow::Schema> TSchema::Finish() {
    AFL_VERIFY(!Finished);
    Finished = true;
    return std::make_shared<arrow::Schema>(Fields);
}

const std::shared_ptr<arrow::Field>& TSchema::GetFieldByName(const std::string& name) const {
    AFL_VERIFY(!Finished);
    auto it = IndexByName.find(name);
    if (it == IndexByName.end()) {
        return Default<std::shared_ptr<arrow::Field>>();
    } else {
        return Fields[it->second];
    }
}

TConclusionStatus TSchema::AddField(const std::shared_ptr<arrow::Field>& f) {
    AFL_VERIFY(!Finished);
    if (!IndexByName.emplace(f->name(), Fields.size()).second) {
        return TConclusionStatus::Fail("field name duplication: " + f->name());
    }
    Fields.emplace_back(f);
    return TConclusionStatus::Success();
}

TString TSchema::ToString() const {
    TStringBuilder result;
    for (auto&& i : Fields) {
        result << i->ToString() << ";";
    }
    return result;
}

const std::shared_ptr<arrow::Field>& TSchema::field(const ui32 index) const {
    AFL_VERIFY(index < Fields.size());
    return Fields[index];
}

const std::shared_ptr<arrow::Field>& TSchema::GetFieldVerified(const ui32 index) const {
    AFL_VERIFY(index < Fields.size());
    return Fields[index];
}

void TSchema::Initialize(const std::vector<std::shared_ptr<arrow::Field>>& fields) {
    AFL_VERIFY(!Initialized);
    Initialized = true;
    for (auto&& i : fields) {
        IndexByName.emplace(i->name(), Fields.size());
        Fields.emplace_back(i);
    }
}

TSchema::TSchema(const std::shared_ptr<TSchema>& schema) {
    AFL_VERIFY(schema);
    Initialize(schema->Fields);
}

TSchema::TSchema(const std::shared_ptr<arrow::Schema>& schema) {
    AFL_VERIFY(schema);
    Initialize(schema->fields());
}

}