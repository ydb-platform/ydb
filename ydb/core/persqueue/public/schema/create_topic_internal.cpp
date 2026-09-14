#include "schema.h"

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/services/persqueue_v1/actors/events.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NPQ::NSchema {

namespace {

class TCreateTopicInternalActor: public NPQ::TBaseActor<TCreateTopicInternalActor>
                               , public NPQ::TConstantLogPrefix {

public:
    TCreateTopicInternalActor(
        NThreading::TPromise<TSchemaResponse>&& promise,
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
        YDB_LOG_ERROR("Catch exception",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"onException", exc.what()});

        TEvSchemaResponse response(Path, Ydb::StatusIds::INTERNAL_ERROR, exc.what());

        Promise.SetValue(std::move(response));
    }

    TString BuildLogPrefix() const override {
        return TStringBuilder() << "[" << Path << "] ";
    }

private:
    void Handle(NPQ::NSchema::TEvSchemaResponse::TPtr& ev) {
        YDB_LOG_DEBUG("Handle TEvSchemaResponse",
            {"logPrefix", NPQ_LOG_PREFIX},
            {"status", ev->Get()->Status},
            {"errorMessage", ev->Get()->ErrorMessage});

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
    TCreateTopicSettings Settings;
    const TString Path;
};

} // namespace

NActors::IActor* CreateCreateTopicActor(
    NThreading::TPromise<TSchemaResponse>&& promise,
    TCreateTopicSettings&& settings
) {
    return new TCreateTopicInternalActor(std::move(promise), std::move(settings));
}

} // namespace NKikimr::NPQ::NSchema
