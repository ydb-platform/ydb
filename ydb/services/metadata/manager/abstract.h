#pragma once
#include "common.h"
#include "table_record.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/tx/locks/sys_tables.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/abstract/parsing.h>

#include <library/cpp/threading/future/core/future.h>

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

    class TLocalModificationContext {
    public:
        TLocalModificationContext() = default;
        TLocalModificationContext(TActorSystem* actorSystem)
            : ActorSystem(actorSystem) {
        }

    private:
        using TActorSystemPtr = TActorSystem*;

        YDB_ACCESSOR_DEF(TActorSystemPtr, ActorSystem)
    };

    class TExternalModificationContext {
    private:
        YDB_ACCESSOR_DEF(std::optional<NACLib::TUserToken>, UserToken);
        YDB_ACCESSOR_DEF(TString, Database);
        // TODO: Remove LocalModifictionContext entirely; replace nodeId with ActorContext or ActorSystem on all object modification path
        YDB_ACCESSOR_DEF(TLocalModificationContext, LocalData);

    public:
        TExternalModificationContext() = default;

        TExternalModificationContext(TLocalModificationContext localData)
            : LocalData(std::move(localData)) {
        }
    };

    class TInternalModificationContext {
    private:
        using TProto = NKikimrSchemeOp::TObjectModificationContext;

        YDB_ACCESSOR_DEF(TExternalModificationContext, ExternalData);
        YDB_ACCESSOR(EActivityType, ActivityType, EActivityType::Undefined);

    private:
        static NKikimrSchemeOp::TObjectModificationContext::EActivityType SerializeToProto(EActivityType activityType) {
            switch (activityType) {
                case EActivityType::Undefined:
                    return NKikimrSchemeOp::TObjectModificationContext_EActivityType_Undefined;
                case EActivityType::Upsert:
                    return NKikimrSchemeOp::TObjectModificationContext_EActivityType_Upsert;
                case EActivityType::Create:
                    return NKikimrSchemeOp::TObjectModificationContext_EActivityType_Create;
                case EActivityType::Alter:
                    return NKikimrSchemeOp::TObjectModificationContext_EActivityType_Alter;
                case EActivityType::Drop:
                    return NKikimrSchemeOp::TObjectModificationContext_EActivityType_Drop;
            }
        }

        static EActivityType DeserializeFromProto(NKikimrSchemeOp::TObjectModificationContext::EActivityType serialized) {
            switch (serialized) {
                case NKikimrSchemeOp::TObjectModificationContext_EActivityType_Undefined:
                    return EActivityType::Undefined;
                case NKikimrSchemeOp::TObjectModificationContext_EActivityType_Upsert:
                    return EActivityType::Upsert;
                case NKikimrSchemeOp::TObjectModificationContext_EActivityType_Create:
                    return EActivityType::Create;
                case NKikimrSchemeOp::TObjectModificationContext_EActivityType_Alter:
                    return EActivityType::Alter;
                case NKikimrSchemeOp::TObjectModificationContext_EActivityType_Drop:
                    return EActivityType::Drop;
            }
        }

    public:
        TInternalModificationContext() = default;

        TInternalModificationContext(TExternalModificationContext externalData)
            : ExternalData(std::move(externalData)) {
        }

        TProto SerializeToProto() const {
            TProto result;
            if (ExternalData.GetUserToken()) {
                result.SetUserToken(ExternalData.GetUserToken()->SerializeAsString());
            }
            result.SetDatabase(ExternalData.GetDatabase());
            result.SetActivityType(SerializeToProto(ActivityType));
            return result;
        }

        bool DeserializeFromProto(const TProto& serialized) {
            if (serialized.HasUserToken()) {
                ExternalData.SetUserToken(NACLib::TUserToken(serialized.GetUserToken()));
            } else {
                ExternalData.SetUserToken(std::nullopt);
            }
            ExternalData.SetDatabase(serialized.GetDatabase());
            ActivityType = DeserializeFromProto(serialized.GetActivityType());
            return true;
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

    // TODO: Prepare can be asynchronous
    // TODO: Consider reverting to use separate settings for each type of operations (how it was)
    TYqlConclusionStatus PrepareSchemeOperation(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TDropObjectSettings& settings,
        const IClassBehaviour::TPtr& manager, const TExternalModificationContext& context, EActivityType operationType) const;

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
    virtual void DoPrepareObjectsBeforeModification(std::vector<TObject>&& patchedObjects,
        typename IAlterPreparationController<TObject>::TPtr controller, const TInternalModificationContext& context,
        const TAlterOperationContext& alterContext) const = 0;

public:
    using TPtr = std::shared_ptr<IObjectOperationsManager<TObject>>;

    // S: called in Metadata service, allows waiting for response from actors
    void PrepareObjectsBeforeModification(std::vector<TObject>&& patchedObjects,
        typename NModifications::IAlterPreparationController<TObject>::TPtr controller, const TInternalModificationContext& context,
        const TAlterOperationContext& alterContext) const {
        return DoPrepareObjectsBeforeModification(std::move(patchedObjects), controller, context, alterContext);
        }
};

class TPatchBuilderBase {
protected:
    virtual TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings) const = 0;

public:
    TOperationParsingResult BuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings) const {
        TOperationParsingResult result = DoBuildPatchFromSettings(settings);
        if (result.IsSuccess()) {
            if (!settings.GetFeaturesExtractor().IsFinished()) {
                return TConclusionStatus::Fail("undefined params: " + settings.GetFeaturesExtractor().GetRemainedParamsString());
            }
        }
        return result;
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
