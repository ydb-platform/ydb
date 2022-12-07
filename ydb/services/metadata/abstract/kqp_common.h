#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/object_factory/object_factory.h>
#include <util/generic/maybe.h>

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

namespace NKikimr::NMetadata {

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

class IInitializationBehaviour;

class IOperationsManager {
public:
    using TFactory = NObjectFactory::TObjectFactory<IOperationsManager, TString>;
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
    mutable std::shared_ptr<IInitializationBehaviour> InitializationBehaviour;
protected:
    virtual NThreading::TFuture<TObjectOperatorResult> DoCreateObject(const NYql::TCreateObjectSettings& settings, const ui32 nodeId,
        IOperationsManager::TPtr manager, const TModificationContext& context) const = 0;
    virtual NThreading::TFuture<TObjectOperatorResult> DoAlterObject(const NYql::TAlterObjectSettings& settings, const ui32 nodeId,
        IOperationsManager::TPtr manager, const TModificationContext& context) const = 0;
    virtual NThreading::TFuture<TObjectOperatorResult> DoDropObject(const NYql::TDropObjectSettings& settings, const ui32 nodeId,
        IOperationsManager::TPtr manager, const TModificationContext& context) const = 0;
    virtual std::shared_ptr<IInitializationBehaviour> DoGetInitializationBehaviour() const = 0;
public:
    virtual ~IOperationsManager() = default;
    NThreading::TFuture<TObjectOperatorResult> CreateObject(const NYql::TCreateObjectSettings& settings, const ui32 nodeId,
        IOperationsManager::TPtr manager, const TModificationContext& context) const {
        return DoCreateObject(settings, nodeId, manager, context);
    }

    NThreading::TFuture<TObjectOperatorResult> AlterObject(const NYql::TAlterObjectSettings& settings, const ui32 nodeId,
        IOperationsManager::TPtr manager, const TModificationContext& context) const {
        return DoAlterObject(settings, nodeId, manager, context);
    }

    NThreading::TFuture<TObjectOperatorResult> DropObject(const NYql::TDropObjectSettings& settings, const ui32 nodeId,
        IOperationsManager::TPtr manager, const TModificationContext& context) const {
        return DoDropObject(settings, nodeId, manager, context);
    }

    std::shared_ptr<IInitializationBehaviour> GetInitializationBehaviour() const {
        if (!InitializationBehaviour) {
            InitializationBehaviour = DoGetInitializationBehaviour();
        }
        return InitializationBehaviour;
    }
    virtual TString GetTypeId() const = 0;
    virtual TString GetTablePath() const = 0;
};

}
