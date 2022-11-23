#pragma once
#include "kqp_common.h"
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actorid.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/actor_virtual.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/object_factory/object_factory.h>

#include <ydb/core/base/events.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/services/metadata/initializer/common.h>
#include <ydb/services/metadata/manager/common.h>
#include <ydb/services/metadata/manager/table_record.h>

namespace NKikimr::NMetadata {

class TOperationParsingResult {
private:
    YDB_READONLY_FLAG(Success, false);
    YDB_READONLY_DEF(TString, ErrorMessage);
    YDB_READONLY_DEF(NMetadataManager::TTableRecord, Record);
public:
    TOperationParsingResult(const char* errorMessage)
        : SuccessFlag(false)
        , ErrorMessage(errorMessage) {

    }

    TOperationParsingResult(const TString& errorMessage)
        : SuccessFlag(false)
        , ErrorMessage(errorMessage) {

    }

    TOperationParsingResult(NMetadataManager::TTableRecord&& record)
        : SuccessFlag(true)
        , Record(record) {

    }
};

class IInitializationBehaviour {
protected:
    virtual void DoPrepare(NMetadataInitializer::IInitializerInput::TPtr controller) const = 0;
public:
    using TPtr = std::shared_ptr<IInitializationBehaviour>;
    virtual ~IInitializationBehaviour() = default;
    void Prepare(NMetadataInitializer::IInitializerInput::TPtr controller) const {
        return DoPrepare(controller);
    }
};

class IAlterCommand {
private:
    YDB_READONLY_DEF(std::vector<NMetadataManager::TTableRecord>, Records);
    YDB_READONLY_DEF(IOperationsManager::TPtr, Manager);
    YDB_READONLY_DEF(NMetadataManager::IAlterController::TPtr, Controller);
    YDB_READONLY_DEF(IOperationsManager::TModificationContext, Context);
protected:
    virtual void DoExecute() const = 0;
public:
    using TPtr = std::shared_ptr<IAlterCommand>;
    virtual ~IAlterCommand() = default;

    IAlterCommand(const std::vector<NMetadataManager::TTableRecord>& records,
        IOperationsManager::TPtr manager,
        NMetadataManager::IAlterController::TPtr controller,
        const IOperationsManager::TModificationContext& context)
        : Records(records)
        , Manager(manager)
        , Controller(controller)
        , Context(context) {

    }

    IAlterCommand(const NMetadataManager::TTableRecord& record,
        IOperationsManager::TPtr manager,
        NMetadataManager::IAlterController::TPtr controller,
        const IOperationsManager::TModificationContext& context)
        : Manager(manager)
        , Controller(controller)
        , Context(context) {
        Records.emplace_back(record);

    }

    void Execute() const {
        DoExecute();
    }
};

}
