#include "object.h"

namespace NKikimr::NMetadata::NModifications {

namespace {

class TDefaultColumnValuesMerger : public IColumnValuesMerger {
public:
    virtual TConclusionStatus Merge(Ydb::Value& value, const Ydb::Value& patch) const override {
        value = patch;
        return TConclusionStatus::Success();
    }
};

}

IColumnValuesMerger::TPtr TBaseObject::BuildMerger(const TString& columnName) const {
    Y_UNUSED(columnName);
    return std::make_shared<TDefaultColumnValuesMerger>();
}

TConclusionStatus TBaseObject::MergeRecords(NInternal::TTableRecord& value, const NInternal::TTableRecord& patch) const {
    for (const auto& [columnId, patchValue] : patch.GetValues()) {
        const auto& merger = BuildMerger(columnId);
        const auto& status = merger->Merge(*value.GetMutableValuePtr(columnId), patchValue);
        if (status.IsFail()) {
            return status;
        }
    }
    return TConclusionStatus::Success();
}

Ydb::Table::CreateTableRequest TBaseObject::AddHistoryTableScheme(const Ydb::Table::CreateTableRequest& baseScheme, const TString& tableName) {
    Ydb::Table::CreateTableRequest result = baseScheme;
    result.add_primary_key("historyInstant");
    result.set_path(tableName);
    {
        auto& column = *result.add_columns();
        column.set_name("historyAction");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
    }
    {
        auto& column = *result.add_columns();
        column.set_name("historyUserId");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
    }
    {
        auto& column = *result.add_columns();
        column.set_name("historyInstant");
        column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
    }
    return result;
}

}
