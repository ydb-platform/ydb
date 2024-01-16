#pragma once
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/services/metadata/request/request_actor.h>
#include <ydb/services/ext_index/metadata/object.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <ydb/services/ext_index/common/config.h>

namespace NKikimr::NCSIndex {

class IDeletingExternalController {
public:
    using TPtr = std::shared_ptr<IDeletingExternalController>;
    virtual ~IDeletingExternalController() = default;
    virtual void OnDeletingFailed(Ydb::StatusIds::StatusCode status, const TString& errorMessage, const TString& requestId) = 0;
    virtual void OnDeletingSuccess(const TString& requestId) = 0;
};

class TDeleting:
    public NMetadata::NRequest::IExternalController<NMetadata::NRequest::TDialogYQLRequest>,
    public NMetadata::NInitializer::IModifierExternalController
{
private:
    std::shared_ptr<TDeleting> SelfContainer;
    NMetadata::NCSIndex::TObject Object;
    IDeletingExternalController::TPtr ExternalController;
    const TString RequestId;
    const TConfig Config;

    NKikimr::NMetadata::NRequest::TDialogYQLRequest::TRequest BuildDeleteRequest() const;

protected:
    virtual void OnModificationFinished(const TString& modificationId) override;

    virtual void OnModificationFailed(Ydb::StatusIds::StatusCode status, const TString& errorMessage, const TString& modificationId) override;

    virtual void OnRequestFailed(Ydb::StatusIds::StatusCode status, const TString& errorMessage) override {
        ExternalController->OnDeletingFailed(status, errorMessage, RequestId);
        SelfContainer = nullptr;
    }

    virtual void OnRequestResult(NMetadata::NRequest::TDialogYQLRequest::TResponse&& /*result*/) override {
        ExternalController->OnDeletingSuccess(RequestId);
        SelfContainer = nullptr;
    }
public:
    void Start(std::shared_ptr<TDeleting> selfContainer);

    TDeleting(const NMetadata::NCSIndex::TObject& object,
        IDeletingExternalController::TPtr externalController, const TString& requestId, const TConfig& config)
        : Object(object)
        , ExternalController(externalController)
        , RequestId(requestId)
        , Config(config)
    {

    }

};

}
