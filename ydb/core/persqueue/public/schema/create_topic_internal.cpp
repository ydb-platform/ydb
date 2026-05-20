#include "schema.h"

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/services/persqueue_v1/actors/events.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

namespace NKikimr::NPQ::NSchema {

namespace {

class TCreateTopicInternalActor: public NPQ::TBaseActor<TCreateTopicInternalActor>
                               , public NPQ::TConstantLogPrefix {

public:
    TCreateTopicInternalActor(
        NThreading::TPromise<TCreateTopicResponse>&& promise,
        TCreateTopicSettings&& settings
    )
        : NPQ::TBaseActor<TCreateTopicInternalActor>(NKikimrServices::PQ_SCHEMA)
        , Promise(std::move(promise))
        , Settings(std::move(settings))
        , Path(Settings.Request.path())
    {
    }

    void Bootstrap() {
        Become(&TCreateTopicInternalActor::StateWork);

        Register(NPQ::NSchema::CreateCreateTopicActor(SelfId(), std::move(Settings)));
    }

    void OnException(const std::exception& exc) override {
        LOG_E("OnException: " << exc.what());

        TEvCreateTopicResponse response;
        response.Status = Ydb::StatusIds::INTERNAL_ERROR;
        response.ErrorMessage = exc.what();

        Promise.SetValue(std::move(response));
    }

    TString BuildLogPrefix() const override {
        return TStringBuilder() << "[" << Path << "] ";
    }

private:
    void Handle(NPQ::NSchema::TEvCreateTopicResponse::TPtr& ev) {
        LOG_D("Handle TEvCreateTopicResponse. Status: " << ev->Get()->Status << ", ErrorMessage: " << ev->Get()->ErrorMessage);

        Promise.SetValue({
            .Status = ev->Get()->Status,
            .ErrorMessage = std::move(ev->Get()->ErrorMessage),
            .ModifyScheme = std::move(ev->Get()->ModifyScheme)
        });

        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NSchema::TEvCreateTopicResponse, Handle);
        }
    }

private:
    NThreading::TPromise<TCreateTopicResponse> Promise;
    TCreateTopicSettings Settings;
    const TString Path;
};

} // namespace

NActors::IActor* CreateCreateTopicActor(
    NThreading::TPromise<TCreateTopicResponse>&& promise,
    TCreateTopicSettings&& settings
) {
    return new TCreateTopicInternalActor(std::move(promise), std::move(settings));
}

} // namespace NKikimr::NPQ::NSchema
