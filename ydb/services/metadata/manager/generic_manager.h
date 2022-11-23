#pragma once
#include <ydb/services/metadata/abstract/manager.h>
#include <ydb/services/metadata/manager/common.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadata {

class TOperationsController: public NMetadataManager::IAlterController {
private:
    YDB_READONLY_DEF(NThreading::TPromise<NMetadata::TObjectOperatorResult>, Promise);
public:
    TOperationsController(NThreading::TPromise<NMetadata::TObjectOperatorResult>&& p)
        : Promise(std::move(p))
    {

    }

    virtual void AlterProblem(const TString& errorMessage) override {
        Promise.SetValue(NMetadata::TObjectOperatorResult(false).SetErrorMessage(errorMessage));
    }
    virtual void AlterFinished() override {
        Promise.SetValue(NMetadata::TObjectOperatorResult(true));
    }

};

template <class T>
class TGenericOperationsManager: public NMetadata::IOperationsManager {
protected:
    virtual NThreading::TFuture<NMetadata::TObjectOperatorResult> DoCreateObject(
        const NYql::TCreateObjectSettings& settings, const ui32 nodeId,
        NMetadata::IOperationsManager::TPtr manager, const TModificationContext& context) const override
    {
        NMetadata::TOperationParsingResult patch(T::BuildPatchFromSettings(settings, context));
        if (!patch.IsSuccess()) {
            NMetadata::TObjectOperatorResult result(patch.GetErrorMessage());
            return NThreading::MakeFuture<NMetadata::TObjectOperatorResult>(result);
        }
        auto promise = NThreading::NewPromise<NMetadata::TObjectOperatorResult>();
        auto result = promise.GetFuture();
        auto c = std::make_shared<TOperationsController>(std::move(promise));
        auto command = std::make_shared<NMetadataManager::TCreateCommand<T>>(patch.GetRecord(), manager, c, context);
        TActivationContext::Send(new IEventHandle(NMetadataProvider::MakeServiceId(nodeId), {},
            new NMetadataProvider::TEvAlterObjects(command)));
        return result;
    }
    virtual NThreading::TFuture<NMetadata::TObjectOperatorResult> DoAlterObject(
        const NYql::TAlterObjectSettings& settings, const ui32 nodeId,
        NMetadata::IOperationsManager::TPtr manager, const TModificationContext& context) const override
    {
        NMetadata::TOperationParsingResult patch(T::BuildPatchFromSettings(settings, context));
        if (!patch.IsSuccess()) {
            return NThreading::MakeFuture<NMetadata::TObjectOperatorResult>(NMetadata::TObjectOperatorResult(patch.GetErrorMessage()));
        }
        auto promise = NThreading::NewPromise<NMetadata::TObjectOperatorResult>();
        auto result = promise.GetFuture();
        auto c = std::make_shared<TOperationsController>(std::move(promise));
        auto command = std::make_shared<NMetadataManager::TAlterCommand<T>>(patch.GetRecord(), manager, c, context);
        TActivationContext::Send(new IEventHandle(NMetadataProvider::MakeServiceId(nodeId), {},
            new NMetadataProvider::TEvAlterObjects(command)));
        return result;
    }
    virtual NThreading::TFuture<NMetadata::TObjectOperatorResult> DoDropObject(
        const NYql::TDropObjectSettings& settings, const ui32 nodeId,
        NMetadata::IOperationsManager::TPtr manager, const TModificationContext& context) const override
    {
        NMetadata::TOperationParsingResult patch(T::BuildPatchFromSettings(settings, context));
        if (!patch.IsSuccess()) {
            return NThreading::MakeFuture<NMetadata::TObjectOperatorResult>(NMetadata::TObjectOperatorResult(patch.GetErrorMessage()));
        }
        auto promise = NThreading::NewPromise<NMetadata::TObjectOperatorResult>();
        auto result = promise.GetFuture();
        auto c = std::make_shared<TOperationsController>(std::move(promise));
        auto command = std::make_shared<NMetadataManager::TDropCommand<T>>(patch.GetRecord(), manager, c, context);
        TActivationContext::Send(new IEventHandle(NMetadataProvider::MakeServiceId(nodeId), {},
            new NMetadataProvider::TEvAlterObjects(command)));
        return result;
    }
public:
    virtual TString GetTablePath() const override {
        return T::GetStorageTablePath();
    }

    virtual TString GetTypeId() const override {
        return GetTypeIdStatic();
    }

    static TString GetTypeIdStatic() {
        return T::GetTypeId();
    }
};

}
