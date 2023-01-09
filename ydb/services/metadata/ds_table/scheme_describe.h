#pragma once

#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/services/metadata/common/ss_dialog.h>

namespace NKikimr::NMetadata::NProvider {

class ISchemeDescribeController {
public:
    using TPtr = std::shared_ptr<ISchemeDescribeController>;
    virtual ~ISchemeDescribeController() = default;
    virtual void OnDescriptionFailed(const TString& errorMessage, const TString& requestId) const = 0;
    virtual void OnDescriptionSuccess(THashMap<ui32, TSysTables::TTableColumnInfo>&& result, const TString& requestId) const = 0;
};

class TEvTableDescriptionFailed: public TEventLocal<TEvTableDescriptionFailed, EEvents::EvTableDescriptionFailed> {
private:
    YDB_READONLY_DEF(TString, ErrorMessage);
    YDB_READONLY_DEF(TString, RequestId);
public:
    explicit TEvTableDescriptionFailed(const TString& errorMessage, const TString& reqId)
        : ErrorMessage(errorMessage)
        , RequestId(reqId) {

    }
};

class TEvTableDescriptionSuccess: public TEventLocal<TEvTableDescriptionSuccess, EEvents::EvTableDescriptionSuccess> {
private:
    using TDescription = THashMap<ui32, TSysTables::TTableColumnInfo>;
    YDB_READONLY_DEF(TString, RequestId);
    YDB_READONLY_DEF(TDescription, Description);
public:
    TEvTableDescriptionSuccess(TDescription&& description, const TString& reqId)
        : RequestId(reqId)
        , Description(std::move(description)) {
    }

    NModifications::TTableSchema GetSchema() const {
        return NModifications::TTableSchema(Description);
    }
};

class TSchemeDescriptionActor: public NActors::TActorBootstrapped<TSchemeDescriptionActor> {
private:
    using TBase = NActors::TActorBootstrapped<TSchemeDescriptionActor>;
    ISchemeDescribeController::TPtr Controller;
    TString Path;
    TString RequestId;
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
public:
    static NKikimrServices::TActivity::EType ActorActivityType();
    void Bootstrap();

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default:
                return;
        }
    }
    TSchemeDescriptionActor(ISchemeDescribeController::TPtr controller, const TString& reqId, const TString& path)
        : Controller(controller)
        , Path(path)
        , RequestId(reqId)
    {

    }
};

}
