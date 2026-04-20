#include "schema.h"

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/services/persqueue_v1/actors/events.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

namespace NKikimr::NPQ::NSchema {

namespace {

class TAlterTopicInternalActor: public NPQ::TBaseActor<TAlterTopicInternalActor>
                              , public NPQ::TConstantLogPrefix {

public:
    TAlterTopicInternalActor(
        NThreading::TPromise<TAlterTopicResponse>&& promise,
        TAlterTopicSettings&& settings
    )
        : NPQ::TBaseActor<TAlterTopicInternalActor>(NKikimrServices::PQ_ALTER_TOPIC)
        , Promise(std::move(promise))
        , Settings(std::move(settings))
    {
    }

    void Bootstrap() {
        Become(&TAlterTopicInternalActor::StateWork);

        Register(NPQ::NSchema::CreateAlterTopicActor(SelfId(), {
            .Database = Settings.Database,
            .Request = Settings.Request,
            .UserToken = std::move(Settings.UserToken),
            .IfExists = Settings.IfExists
        }));
    }

    void OnException(const std::exception& exc) override {
        LOG_E("OnException: " << exc.what());

        TEvAlterTopicResponse response;
        response.Status = Ydb::StatusIds::INTERNAL_ERROR;
        response.ErrorMessage = exc.what();
        
        Promise.SetValue(std::move(response));
    }

    TString BuildLogPrefix() const override {
        return TStringBuilder() << SelfId() << "[" << Settings.Database << "][" << Settings.Request.path() << "] ";
    }

private:
    void Handle(NPQ::NSchema::TEvAlterTopicResponse::TPtr& ev) {
        LOG_E("Handle TEvAlterTopicResponse. Status: " << ev->Get()->Status << ", ErrorMessage: " << ev->Get()->ErrorMessage);

        Promise.SetValue({
            .Status = ev->Get()->Status,
            .ErrorMessage = std::move(ev->Get()->ErrorMessage),
            .ModifyScheme = std::move(ev->Get()->ModifyScheme)
        });

        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NSchema::TEvAlterTopicResponse, Handle);
        }
    }

private:
    NThreading::TPromise<TAlterTopicResponse> Promise;
    TAlterTopicSettings Settings;
};

} // namespace
    
NActors::IActor* CreateAlterTopicActor(
    NThreading::TPromise<TAlterTopicResponse>&& promise,
    TAlterTopicSettings&& settings
) {
    return new TAlterTopicInternalActor(std::move(promise), std::move(settings));
}

} // namespace NKikimr::NPQ::NSchema
