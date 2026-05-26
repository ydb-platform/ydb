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
        : NPQ::TBaseActor<TAlterTopicInternalActor>(NKikimrServices::PQ_SCHEMA)
        , Promise(std::move(promise))
        , Settings(std::move(settings))
        , Path(Settings.Request.path())
    {
    }

    void Bootstrap() {
        Become(&TAlterTopicInternalActor::StateWork);

        Register(NPQ::NSchema::CreateAlterTopicActor(SelfId(), std::move(Settings)));
    }

    void OnException(const std::exception& exc) override {
        LOG_E("OnException: " << exc.what());

        TEvAlterTopicResponse response;
        response.Status = Ydb::StatusIds::INTERNAL_ERROR;
        response.ErrorMessage = exc.what();

        Promise.SetValue(std::move(response));
    }

    TString BuildLogPrefix() const override {
        return TStringBuilder() << "[" << Path << "] ";
    }

private:
    void Handle(NPQ::NSchema::TEvAlterTopicResponse::TPtr& ev) {
        LOG_D("Handle TEvAlterTopicResponse. Status: " << ev->Get()->Status << ", ErrorMessage: " << ev->Get()->ErrorMessage);

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
    const TString Path;
};

} // namespace

NActors::IActor* CreateAlterTopicActor(
    NThreading::TPromise<TAlterTopicResponse>&& promise,
    TAlterTopicSettings&& settings
) {
    return new TAlterTopicInternalActor(std::move(promise), std::move(settings));
}

} // namespace NKikimr::NPQ::NSchema
