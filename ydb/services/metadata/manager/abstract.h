#pragma once
#include "common.h"
#include "table_record.h"

#include <ydb/core/tx/datashard/sys_tables.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/yql/ast/yql_expr_builder.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <ydb/services/metadata/abstract/kqp_common.h>

#include <library/cpp/threading/future/core/future.h>

namespace NYql {
class TObjectSettingsImpl {
public:
    using TFeatures = std::map<TString, TString>;
private:
    YDB_READONLY_DEF(TString, TypeId);
    YDB_READONLY_DEF(TString, ObjectId);
    YDB_READONLY_DEF(TFeatures, Features);
public:
    template <class T>
    std::optional<T> GetFeature(const TString& featureId, const std::optional<T> noExistsValue = {}) const {
        T result;
        auto it = Features.find(featureId);
        if (it == Features.end()) {
            return noExistsValue;
        } else if (TryFromString(it->second, result)) {
            return result;
        } else {
            return {};
        }
    }

    template <class TKiObject>
    bool DeserializeFromKi(const TKiObject& data) {
        ObjectId = data.ObjectId();
        TypeId = data.TypeId();
        for (auto&& i : data.Features()) {
            if (auto maybeAtom = i.template Maybe<NYql::NNodes::TCoAtom>()) {
                Features.emplace(maybeAtom.Cast().StringValue(), "");
            } else if (auto maybeTuple = i.template Maybe<NNodes::TCoNameValueTuple>()) {
                auto tuple = maybeTuple.Cast();
                if (auto tupleValue = tuple.Value().template Maybe<NNodes::TCoAtom>()) {
                    Features.emplace(tuple.Name().Value(), tupleValue.Cast().Value());
                }
            }
        }
        return true;
    }
};

using TCreateObjectSettings = TObjectSettingsImpl;
using TAlterObjectSettings = TObjectSettingsImpl;
using TDropObjectSettings = TObjectSettingsImpl;

}

namespace NKikimr::NMetadata::NModifications {

class TOperationParsingResult {
private:
    YDB_READONLY_FLAG(Success, false);
    YDB_READONLY_DEF(TString, ErrorMessage);
    YDB_READONLY_DEF(NInternal::TTableRecord, Record);
public:
    TOperationParsingResult(const char* errorMessage)
        : SuccessFlag(false)
        , ErrorMessage(errorMessage) {

    }

    TOperationParsingResult(const TString& errorMessage)
        : SuccessFlag(false)
        , ErrorMessage(errorMessage) {

    }

    TOperationParsingResult(NInternal::TTableRecord&& record)
        : SuccessFlag(true)
        , Record(record) {

    }
};

class TObjectOperatorResult {
private:
    YDB_READONLY_FLAG(Success, false);
    YDB_ACCESSOR_DEF(TString, ErrorMessage);
public:
    explicit TObjectOperatorResult(const bool success)
        : SuccessFlag(success) {

    }

    TObjectOperatorResult(const TString& errorMessage)
        : SuccessFlag(false)
        , ErrorMessage(errorMessage) {

    }

    TObjectOperatorResult(const char* errorMessage)
        : SuccessFlag(false)
        , ErrorMessage(errorMessage) {

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
    virtual NThreading::TFuture<TObjectOperatorResult> DoModify(const NYql::TObjectSettingsImpl& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, TInternalModificationContext& context) const = 0;
public:
    virtual ~IOperationsManager() = default;

    NThreading::TFuture<TObjectOperatorResult> CreateObject(const NYql::TCreateObjectSettings& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, const TExternalModificationContext& context) const;

    NThreading::TFuture<TObjectOperatorResult> AlterObject(const NYql::TAlterObjectSettings& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, const TExternalModificationContext& context) const;

    NThreading::TFuture<TObjectOperatorResult> DropObject(const NYql::TDropObjectSettings& settings, const ui32 nodeId,
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
        return DoBuildPatchFromSettings(settings, context);
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
