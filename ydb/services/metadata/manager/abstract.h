#pragma once
#include "common.h"
#include "table_record.h"

#include <ydb/core/tx/datashard/sys_tables.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/yql/ast/yql_expr_builder.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/abstract/parsing.h>

#include <library/cpp/threading/future/core/future.h>

namespace NKikimr::NMetadata::NModifications {

using TOperationParsingResult = TConclusion<NInternal::TTableRecord>;

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
        Create,
        Alter,
        Drop
    };

    class TExternalModificationContext {
    private:
        YDB_ACCESSOR_DEF(std::optional<NACLib::TUserToken>, UserToken);
        YDB_ACCESSOR_DEF(TString, Database);
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
        IClassBehaviour::TPtr manager, TInternalModificationContext& context) const = 0;
public:
    virtual ~IOperationsManager() = default;

    NThreading::TFuture<TYqlConclusionStatus> CreateObject(const NYql::TCreateObjectSettings& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, const TExternalModificationContext& context) const;

    NThreading::TFuture<TYqlConclusionStatus> AlterObject(const NYql::TAlterObjectSettings& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, const TExternalModificationContext& context) const;

    NThreading::TFuture<TYqlConclusionStatus> DropObject(const NYql::TDropObjectSettings& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, const TExternalModificationContext& context) const;

    const TTableSchema& GetSchema() const {
        Y_VERIFY(!!ActualSchema);
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
        const TInternalModificationContext& context) const = 0;
public:
    using TPtr = std::shared_ptr<IObjectOperationsManager<TObject>>;

    TOperationParsingResult BuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
        IOperationsManager::TInternalModificationContext& context) const {
        TOperationParsingResult result = DoBuildPatchFromSettings(settings, context);
        if (result) {
            if (!settings.GetFeaturesExtractor().IsFinished()) {
                return TConclusionStatus::Fail("undefined params: " + settings.GetFeaturesExtractor().GetRemainedParamsString());
            }
        }
        return result;
    }

    void PrepareObjectsBeforeModification(std::vector<TObject>&& patchedObjects,
        typename NModifications::IAlterPreparationController<TObject>::TPtr controller,
        const TInternalModificationContext& context) const {
        return DoPrepareObjectsBeforeModification(std::move(patchedObjects), controller, context);
    }
};

class IAlterCommand {
private:
    YDB_READONLY_DEF(std::vector<NInternal::TTableRecord>, Records);
    YDB_ACCESSOR_DEF(IClassBehaviour::TPtr, Behaviour);
    YDB_READONLY_DEF(IAlterController::TPtr, Controller);
protected:
    IOperationsManager::TInternalModificationContext Context;
    virtual void DoExecute() const = 0;
public:
    using TPtr = std::shared_ptr<IAlterCommand>;
    virtual ~IAlterCommand() = default;

    template <class TObject>
    std::shared_ptr<IObjectOperationsManager<TObject>> GetOperationsManagerFor() const {
        auto result = std::dynamic_pointer_cast<IObjectOperationsManager<TObject>>(Behaviour->GetOperationsManager());
        Y_VERIFY(result);
        return result;
    }

    const IOperationsManager::TInternalModificationContext& GetContext() const {
        return Context;
    }

    IAlterCommand(const std::vector<NInternal::TTableRecord>& records,
        IClassBehaviour::TPtr behaviour,
        NModifications::IAlterController::TPtr controller,
        const IOperationsManager::TInternalModificationContext& context)
        : Records(records)
        , Behaviour(behaviour)
        , Controller(controller)
        , Context(context) {
        Y_VERIFY(Behaviour->GetOperationsManager());
    }

    IAlterCommand(const NInternal::TTableRecord& record,
        IClassBehaviour::TPtr behaviour,
        NModifications::IAlterController::TPtr controller,
        const IOperationsManager::TInternalModificationContext& context)
        : Behaviour(behaviour)
        , Controller(controller)
        , Context(context) {
        Y_VERIFY(Behaviour->GetOperationsManager());
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
