#include "initialization.h"
#include <ydb/services/bg_tasks/abstract/task.h>

namespace NKikimr::NBackgroundTasks {

void TBGTasksInitializer::DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const {
    const TString tableName = Config.GetTablePath();
    TVector<NMetadata::NInitializer::ITableModifier::TPtr> result;
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(tableName);
        request.add_primary_key("id");
        {
            auto& column = *request.add_columns();
            column.set_name("id");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("enabled");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::BOOL);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("class");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("executorId");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("lastPing");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT32);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("startInstant");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT32);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("constructInstant");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT32);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("activity");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("scheduler");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("state");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        result.emplace_back(new NMetadata::NInitializer::TGenericTableModifier<NMetadata::NRequest::TDialogCreateTable>(request, "create"));
        result.emplace_back(NMetadata::NInitializer::TACLModifierConstructor::GetReadOnlyModifier(tableName, "acl"));
    }
    controller->OnPreparationFinished(result);
}

}
