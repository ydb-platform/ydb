#include "behaviour.h"
#include "manager.h"

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/kqp/gateway/actors/scheme.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/base/path.h>

#include <util/string/type.h>

namespace NKikimr::NKqp {

class ITableStoreOperation {
public:
    using TFactory = NObjectFactory::TObjectFactory<ITableStoreOperation, TString>;
    using TPtr = std::shared_ptr<ITableStoreOperation>;
private:
    TString PresetName = "default";
    TString WorkingDir;
    TString StoreName;
public:
    virtual ~ITableStoreOperation() {};

    NMetadata::NModifications::TObjectOperatorResult Deserialize(const NYql::TObjectSettingsImpl& settings) {
        std::pair<TString, TString> pathPair;
        {
            TString error;
            if (!NYql::IKikimrGateway::TrySplitTablePath(settings.GetObjectId(), pathPair, error)) {
                return NMetadata::NModifications::TObjectOperatorResult(error);
            }
            WorkingDir = pathPair.first;
            StoreName = pathPair.second;
        }
        {
            auto it = settings.GetFeatures().find("PRESET_NAME");
            if (it != settings.GetFeatures().end()) {
                PresetName = it->second;
            }
        }
        return DoDeserialize(settings.GetFeatures());
    }

    void SerializeScheme(NKikimrSchemeOp::TModifyScheme& scheme) const {
        scheme.SetWorkingDir(WorkingDir);
        scheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterColumnStore);
        
        NKikimrSchemeOp::TAlterColumnStore* alter = scheme.MutableAlterColumnStore();
        alter->SetName(StoreName);
        auto schemaPresetObject = alter->AddAlterSchemaPresets();
        schemaPresetObject->SetName(PresetName);
        return DoSerializeScheme(*schemaPresetObject);
    }
private:
    virtual NMetadata::NModifications::TObjectOperatorResult DoDeserialize(const NYql::TObjectSettingsImpl::TFeatures& features) = 0;
    virtual void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchemaPreset& scheme) const = 0;
};

class TAddColumnOperation : public ITableStoreOperation {
    static TString GetTypeName() {
        return "NEW_COLUMN";
    }

    static inline auto Registrator = TFactory::TRegistrator<TAddColumnOperation>(GetTypeName());
private:
    TString ColumnName;
    TString ColumnType;
    bool NotNull = false;
public:
    NMetadata::NModifications::TObjectOperatorResult DoDeserialize(const NYql::TObjectSettingsImpl::TFeatures& features) override {
        {
            auto it = features.find("NAME");
            if (it == features.end()) {
                return NMetadata::NModifications::TObjectOperatorResult("can't find  alter parameter NAME");
            }
            ColumnName = it->second;
        }
        {
            auto it = features.find("TYPE");
            if (it == features.end()) {
                return NMetadata::NModifications::TObjectOperatorResult("can't find alter parameter TYPE");
            }
            ColumnType = it->second;
        }
        {
            auto it = features.find("NOT_NULL");
            if (it != features.end()) {
                NotNull = IsTrue(it->second);
            }
        }
        return NMetadata::NModifications::TObjectOperatorResult(true);
    }

    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchemaPreset& presetProto) const override {
        auto schemaData = presetProto.MutableAlterSchema();
        auto column = schemaData->AddColumns();
        column->SetName(ColumnName);
        column->SetType(ColumnType);
        column->SetNotNull(NotNull);
    }
};

NThreading::TFuture<NMetadata::NModifications::TObjectOperatorResult> TTableStoreManager::DoModify(const NYql::TObjectSettingsImpl& settings, const ui32 nodeId,
        NMetadata::IClassBehaviour::TPtr manager, TInternalModificationContext& context) const {
            Y_UNUSED(nodeId);
            Y_UNUSED(manager);
        auto promise = NThreading::NewPromise<NMetadata::NModifications::TObjectOperatorResult>();
        auto result = promise.GetFuture();

        switch (context.GetActivityType()) {
            case EActivityType::Create:
            case EActivityType::Drop:
            case EActivityType::Undefined:
                return NThreading::MakeFuture<NMetadata::NModifications::TObjectOperatorResult>(NMetadata::NModifications::TObjectOperatorResult("not impelemented"));
            case EActivityType::Alter:
            try {
                auto actionIt = settings.GetFeatures().find("ACTION");
                if (actionIt == settings.GetFeatures().end()) {
                    return NThreading::MakeFuture<NMetadata::NModifications::TObjectOperatorResult>(NMetadata::NModifications::TObjectOperatorResult("can't find ACTION"));
                }
                ITableStoreOperation::TPtr operation(ITableStoreOperation::TFactory::Construct(actionIt->second));
                if (!operation) {
                    return NThreading::MakeFuture<NMetadata::NModifications::TObjectOperatorResult>(NMetadata::NModifications::TObjectOperatorResult("invalid ACTION: " + actionIt->second));
                }
                {
                    auto parsingResult = operation->Deserialize(settings);
                    if (!parsingResult.IsSuccess()) {
                        return NThreading::MakeFuture<NMetadata::NModifications::TObjectOperatorResult>(parsingResult);
                    }
                }
                auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
                ev->Record.SetDatabaseName(context.GetExternalData().GetDatabase());
                if (context.GetExternalData().GetUserToken()) {
                    ev->Record.SetUserToken(context.GetExternalData().GetUserToken()->GetSerializedToken());
                }
                auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
                operation->SerializeScheme(schemeTx);

                auto promiseScheme = NThreading::NewPromise<NKqp::TSchemeOpRequestHandler::TResult>(); 
                TActivationContext::AsActorContext().Register(new NKqp::TSchemeOpRequestHandler(ev.Release(), promiseScheme, false));
                return promiseScheme.GetFuture().Apply([](const NThreading::TFuture<NKqp::TSchemeOpRequestHandler::TResult>& f) {
                    if (f.HasValue() && !f.HasException() && f.GetValue().Success()) {
                        NMetadata::NModifications::TObjectOperatorResult localResult(true);
                        return localResult;
                    } else if (f.HasValue()) {
                        NMetadata::NModifications::TObjectOperatorResult localResult(f.GetValue().Issues().ToString());
                        return localResult;
                    }
                    NMetadata::NModifications::TObjectOperatorResult localResult(false);
                    return localResult;
                    });
            } catch (yexception& e) {
                return NThreading::MakeFuture<NMetadata::NModifications::TObjectOperatorResult>(NMetadata::NModifications::TObjectOperatorResult(e.what()));
            }
        }
        return result;
}

}

