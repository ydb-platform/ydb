#pragma once
#include "common.h"
#include "table_record.h"

#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/tx/locks/sys_tables.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/library/conclusion/result.h>

#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/abstract/parsing.h>

#include <library/cpp/threading/future/core/future.h>
#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr::NMetadata::NModifications {

using TOperationParsingResult = TConclusion<NInternal::TTableRecord>;

class TAlterOperationContext {
private:
    YDB_READONLY_DEF(TString, SessionId);
    YDB_READONLY_DEF(TString, TransactionId);
    YDB_READONLY_DEF(NInternal::TTableRecords, RestoreObjectIds);
public:
    TAlterOperationContext(const TString& sessionId, const TString& transactionId, const NInternal::TTableRecords& RestoreObjectIds)
        : SessionId(sessionId)
        , TransactionId(transactionId)
        , RestoreObjectIds(RestoreObjectIds) {
        }
};

class TColumnInfo {
private:
    YDB_READONLY_FLAG(Primary, false);
    YDB_READONLY_DEF(Ydb::Column, YDBColumn);
public:
    TColumnInfo(const bool primary, const Ydb::Column& info)
        : PrimaryFlag(primary)
        , YDBColumn(info) {

    }
};

class TTableSchema {
private:
    YDB_READONLY_DEF(std::vector<TColumnInfo>, Columns);
    YDB_READONLY_DEF(std::vector<Ydb::Column>, YDBColumns);
    YDB_READONLY_DEF(std::vector<Ydb::Column>, PKColumns);
    YDB_READONLY_DEF(std::vector<TString>, PKColumnIds);

public:
    TTableSchema() = default;
    TTableSchema(const THashMap<ui32, TSysTables::TTableColumnInfo>& description);

    TTableSchema& AddColumn(const bool primary, const Ydb::Column& info) noexcept;
};

class IOperationsManager {
public:
    using TPtr = std::shared_ptr<IOperationsManager>;
    using TYqlConclusionStatus = TConclusionSpecialStatus<NYql::TIssuesIds::EIssueCode, NYql::TIssuesIds::SUCCESS, NYql::TIssuesIds::DEFAULT_ERROR>;

    enum class EActivityType {
        Undefined,
        Upsert,
        Create,
        Alter,
        Drop
    };

    class TExternalModificationContext {
    private:
        YDB_ACCESSOR_DEF(std::optional<NACLib::TUserToken>, UserToken);
        YDB_ACCESSOR_DEF(TString, Database);
        using TActorSystemPtr = TActorSystem*;
        YDB_ACCESSOR_DEF(TActorSystemPtr, ActorSystem);
    };

    class TInternalModificationContext {
    private:
        YDB_READONLY_DEF(TExternalModificationContext, ExternalData);
        YDB_ACCESSOR(EActivityType, ActivityType, EActivityType::Undefined);
    public:
        TInternalModificationContext(const TExternalModificationContext& externalData)
            : ExternalData(externalData)
        {

        }
    };
private:
    YDB_ACCESSOR_DEF(std::optional<TTableSchema>, ActualSchema);
protected:
    virtual NThreading::TFuture<TYqlConclusionStatus> DoModify(const NYql::TObjectSettingsImpl& settings, const ui32 nodeId,
        const IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const = 0;

    virtual TYqlConclusionStatus DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings,
        const IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const = 0;
public:
    virtual ~IOperationsManager() = default;

    NThreading::TFuture<TYqlConclusionStatus> UpsertObject(const NYql::TUpsertObjectSettings& settings, const ui32 nodeId,
        const IClassBehaviour::TPtr& manager, const TExternalModificationContext& context) const;

    NThreading::TFuture<TYqlConclusionStatus> CreateObject(const NYql::TCreateObjectSettings& settings, const ui32 nodeId,
        const IClassBehaviour::TPtr& manager, const TExternalModificationContext& context) const;

    NThreading::TFuture<TYqlConclusionStatus> AlterObject(const NYql::TAlterObjectSettings& settings, const ui32 nodeId,
        const IClassBehaviour::TPtr& manager, const TExternalModificationContext& context) const;

    NThreading::TFuture<TYqlConclusionStatus> DropObject(const NYql::TDropObjectSettings& settings, const ui32 nodeId,
        const IClassBehaviour::TPtr& manager, const TExternalModificationContext& context) const;

    TYqlConclusionStatus PrepareUpsertObjectSchemeOperation(NKqpProto::TKqpSchemeOperation& schemeOperation,
        const NYql::TUpsertObjectSettings& settings, const IClassBehaviour::TPtr& manager,
        const TExternalModificationContext& context) const;

    TYqlConclusionStatus PrepareCreateObjectSchemeOperation(NKqpProto::TKqpSchemeOperation& schemeOperation,
        const NYql::TCreateObjectSettings& settings, const IClassBehaviour::TPtr& manager,
        const TExternalModificationContext& context) const;

    TYqlConclusionStatus PrepareAlterObjectSchemeOperation(NKqpProto::TKqpSchemeOperation& schemeOperation,
        const NYql::TAlterObjectSettings& settings, const IClassBehaviour::TPtr& manager,
        const TExternalModificationContext& context) const;

    TYqlConclusionStatus PrepareDropObjectSchemeOperation(NKqpProto::TKqpSchemeOperation& schemeOperation,
        const NYql::TDropObjectSettings& settings, const IClassBehaviour::TPtr& manager,
        const TExternalModificationContext& context) const;

    virtual NThreading::TFuture<TYqlConclusionStatus> ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation,
        const ui32 nodeId, const IClassBehaviour::TPtr& manager, const TExternalModificationContext& context) const = 0;

    const TTableSchema& GetSchema() const {
        Y_ABORT_UNLESS(!!ActualSchema);
        return *ActualSchema;
    }
};

template <class TObject>
class IObjectOperationsManager: public IOperationsManager {
protected:
    virtual TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
        TInternalModificationContext& context) const = 0;
    virtual void DoPrepareObjectsBeforeModification(std::vector<TObject>&& patchedObjects,
        typename IAlterPreparationController<TObject>::TPtr controller,
        const TInternalModificationContext& context, const TAlterOperationContext& alterContext) const = 0;
public:
    using TPtr = std::shared_ptr<IObjectOperationsManager<TObject>>;

    TOperationParsingResult BuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
        IOperationsManager::TInternalModificationContext& context) const {
        TOperationParsingResult result = DoBuildPatchFromSettings(settings, context);
        if (result.IsSuccess()) {
            if (!settings.GetFeaturesExtractor().IsFinished()) {
                return TConclusionStatus::Fail("undefined params: " + settings.GetFeaturesExtractor().GetRemainedParamsString());
            }
        }
        return result;
    }

    void PrepareObjectsBeforeModification(std::vector<TObject>&& patchedObjects,
        typename NModifications::IAlterPreparationController<TObject>::TPtr controller,
        const TInternalModificationContext& context, const TAlterOperationContext& alterContext) const {
        return DoPrepareObjectsBeforeModification(std::move(patchedObjects), controller, context, alterContext);
    }
};

class IObjectModificationCommand {
private:
    YDB_READONLY_DEF(std::vector<NInternal::TTableRecord>, Records);
    YDB_ACCESSOR_DEF(IClassBehaviour::TPtr, Behaviour);
    YDB_READONLY_DEF(IAlterController::TPtr, Controller);
protected:
    IOperationsManager::TInternalModificationContext Context;
    virtual void DoExecute() const = 0;
public:
    using TPtr = std::shared_ptr<IObjectModificationCommand>;
    virtual ~IObjectModificationCommand() = default;

    template <class TObject>
    std::shared_ptr<IObjectOperationsManager<TObject>> GetOperationsManagerFor() const {
        auto result = std::dynamic_pointer_cast<IObjectOperationsManager<TObject>>(Behaviour->GetOperationsManager());
        Y_ABORT_UNLESS(result);
        return result;
    }

    const IOperationsManager::TInternalModificationContext& GetContext() const {
        return Context;
    }

    IObjectModificationCommand(const std::vector<NInternal::TTableRecord>& records,
        IClassBehaviour::TPtr behaviour,
        NModifications::IAlterController::TPtr controller,
        const IOperationsManager::TInternalModificationContext& context)
        : Records(records)
        , Behaviour(behaviour)
        , Controller(controller)
        , Context(context) {
        Y_ABORT_UNLESS(Behaviour->GetOperationsManager());
    }

    IObjectModificationCommand(const NInternal::TTableRecord& record,
        IClassBehaviour::TPtr behaviour,
        NModifications::IAlterController::TPtr controller,
        const IOperationsManager::TInternalModificationContext& context)
        : Behaviour(behaviour)
        , Controller(controller)
        , Context(context) {
        Y_ABORT_UNLESS(Behaviour->GetOperationsManager());
        Records.emplace_back(record);

    }

    void Execute() const {
        if (!Behaviour) {
            Controller->OnAlteringProblem("behaviour not ready");
            return;
        }
        if (!Behaviour->GetOperationsManager()) {
            Controller->OnAlteringProblem("behaviour's manager not initialized");
            return;
        }
        DoExecute();
    }
};

}
