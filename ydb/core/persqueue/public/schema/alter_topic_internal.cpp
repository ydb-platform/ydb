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
        NThreading::TPromise<TSchemaResponse>&& promise,
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

        TEvSchemaResponse response(Path, Ydb::StatusIds::INTERNAL_ERROR, exc.what());

        Promise.SetValue(std::move(response));
    }

    TString BuildLogPrefix() const override {
        return TStringBuilder() << "[" << Path << "] ";
    }

private:
    void Handle(NPQ::NSchema::TEvSchemaResponse::TPtr& ev) {
        LOG_D("Handle TEvSchemaResponse. Status: " << ev->Get()->Status << ", ErrorMessage: " << ev->Get()->ErrorMessage);

        Promise.SetValue({
            .Path = Path,
            .Status = ev->Get()->Status,
            .ErrorMessage = std::move(ev->Get()->ErrorMessage),
            .ModifyScheme = std::move(ev->Get()->ModifyScheme)
        });

        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NSchema::TEvSchemaResponse, Handle);
        }
    }

private:
    NThreading::TPromise<TSchemaResponse> Promise;
    TAlterTopicSettings Settings;
    const TString Path;
};

} // namespace

NActors::IActor* CreateAlterTopicActor(
    NThreading::TPromise<TSchemaResponse>&& promise,
    TAlterTopicSettings&& settings
) {
    return new TAlterTopicInternalActor(std::move(promise), std::move(settings));
}

} // namespace NKikimr::NPQ::NSchema
