#pragma once
#include "common.h"
#include "table_record.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/yql/ast/yql_expr_builder.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <ydb/services/metadata/abstract/kqp_common.h>

#include <library/cpp/threading/future/core/future.h>

namespace NYql {
class TObjectSettingsImpl {
private:
    YDB_READONLY_DEF(TString, TypeId);
    YDB_READONLY_DEF(TString, ObjectId);

    using TFeatures = std::map<TString, TString>;
    YDB_READONLY_DEF(TFeatures, Features);
public:
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

struct TCreateObjectSettings: public TObjectSettingsImpl {
public:
};

struct TAlterObjectSettings: public TObjectSettingsImpl {
public:
};

struct TDropObjectSettings: public TObjectSettingsImpl {
public:
};

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
    TObjectOperatorResult(const bool success)
        : SuccessFlag(success) {

    }

    TObjectOperatorResult(const TString& errorMessage)
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

    TTableSchema& AddColumn(const bool primary, const Ydb::Column& info) noexcept;
public:
    TTableSchema(const Ydb::Table::DescribeTableResult& description);
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

    class TModificationContext {
    private:
        YDB_ACCESSOR_DEF(std::optional<NACLib::TUserToken>, UserToken);
        YDB_ACCESSOR(EActivityType, ActivityType, EActivityType::Undefined);
    public:
        TModificationContext() = default;
    };
private:
    YDB_ACCESSOR_DEF(std::optional<TTableSchema>, ActualSchema);
protected:
    virtual NThreading::TFuture<TObjectOperatorResult> DoCreateObject(const NYql::TCreateObjectSettings& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, const TModificationContext& context) const = 0;
    virtual NThreading::TFuture<TObjectOperatorResult> DoAlterObject(const NYql::TAlterObjectSettings& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, const TModificationContext& context) const = 0;
    virtual NThreading::TFuture<TObjectOperatorResult> DoDropObject(const NYql::TDropObjectSettings& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, const TModificationContext& context) const = 0;
public:
    virtual ~IOperationsManager() = default;

    NThreading::TFuture<TObjectOperatorResult> CreateObject(const NYql::TCreateObjectSettings& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, const TModificationContext& context) const {
        return DoCreateObject(settings, nodeId, manager, context);
    }

    NThreading::TFuture<TObjectOperatorResult> AlterObject(const NYql::TAlterObjectSettings& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, const TModificationContext& context) const {
        return DoAlterObject(settings, nodeId, manager, context);
    }

    NThreading::TFuture<TObjectOperatorResult> DropObject(const NYql::TDropObjectSettings& settings, const ui32 nodeId,
        IClassBehaviour::TPtr manager, const TModificationContext& context) const {
        return DoDropObject(settings, nodeId, manager, context);
    }

    const TTableSchema& GetSchema() const {
        Y_VERIFY(!!ActualSchema);
        return *ActualSchema;
    }
};

template <class TObject>
class IObjectOperationsManager: public IOperationsManager {
protected:
    virtual TOperationParsingResult DoBuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
        const TModificationContext& context) const = 0;
    virtual void DoPrepareObjectsBeforeModification(std::vector<TObject>&& patchedObjects,
        typename IAlterPreparationController<TObject>::TPtr controller,
        const IOperationsManager::TModificationContext& context) const = 0;
public:
    using TPtr = std::shared_ptr<IObjectOperationsManager<TObject>>;

    TOperationParsingResult BuildPatchFromSettings(const NYql::TObjectSettingsImpl& settings,
        const IOperationsManager::TModificationContext& context) const {
        return DoBuildPatchFromSettings(settings, context);
    }

    void PrepareObjectsBeforeModification(std::vector<TObject>&& patchedObjects,
        typename NModifications::IAlterPreparationController<TObject>::TPtr controller,
        const IOperationsManager::TModificationContext& context) const {
        return DoPrepareObjectsBeforeModification(std::move(patchedObjects), controller, context);
    }
};

class IAlterCommand {
private:
    YDB_READONLY_DEF(std::vector<NInternal::TTableRecord>, Records);
    YDB_ACCESSOR_DEF(IClassBehaviour::TPtr, Manager);
    YDB_READONLY_DEF(IAlterController::TPtr, Controller);
protected:
    mutable IOperationsManager::TModificationContext Context;
    virtual void DoExecute() const = 0;
public:
    using TPtr = std::shared_ptr<IAlterCommand>;
    virtual ~IAlterCommand() = default;

    template <class TObject>
    std::shared_ptr<IObjectOperationsManager<TObject>> GetOperationsManagerFor() const {
        auto result = std::dynamic_pointer_cast<IObjectOperationsManager<TObject>>(Manager->GetOperationsManager());
        Y_VERIFY(result);
        return result;
    }

    const IOperationsManager::TModificationContext& GetContext() const {
        return Context;
    }

    IAlterCommand(const std::vector<NInternal::TTableRecord>& records,
        IClassBehaviour::TPtr manager,
        NModifications::IAlterController::TPtr controller,
        const IOperationsManager::TModificationContext& context)
        : Records(records)
        , Manager(manager)
        , Controller(controller)
        , Context(context) {
        Y_VERIFY(Manager->GetOperationsManager());
    }

    IAlterCommand(const NInternal::TTableRecord& record,
        IClassBehaviour::TPtr manager,
        NModifications::IAlterController::TPtr controller,
        const IOperationsManager::TModificationContext& context)
        : Manager(manager)
        , Controller(controller)
        , Context(context) {
        Y_VERIFY(Manager->GetOperationsManager());
        Records.emplace_back(record);

    }

    void Execute() const {
        DoExecute();
    }
};

}
