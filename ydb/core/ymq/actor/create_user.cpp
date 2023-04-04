#include "action.h"
#include "error.h"
#include "schema.h"

#include <ydb/core/ymq/base/helpers.h>

namespace NKikimr::NSQS {

class TCreateUserActor
    : public TActionActor<TCreateUserActor>
{
public:
    static constexpr bool NeedExistingQueue() {
        return false;
    }

    TCreateUserActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::CreateUser, std::move(cb))
    {
    }

private:
    bool DoValidate() override {
        if (!Request().GetUserName()) {
            MakeError(Response_.MutableCreateUser(), NErrors::MISSING_PARAMETER, "No user name parameter.");
            return false;
        }

        if (!ValidateQueueNameOrUserName(Request().GetUserName())) {
            MakeError(Response_.MutableCreateUser(), NErrors::INVALID_PARAMETER_VALUE, "Invalid user name.");
            return false;
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableCreateUser()->MutableError();
    }

    void DoAction() override {
        Become(&TThis::StateFunc);

        SchemaActor = Register(
            new TCreateUserSchemaActor(Cfg().GetRoot(), Request().GetUserName(), SelfId(), RequestId_, UserCounters_)
        );
    }

    TString DoGetQueueName() const override {
        return TString();
    }

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup,         HandleWakeup);
            hFunc(TSqsEvents::TEvUserCreated, HandleUserCreated);
        }
    }

    void HandleUserCreated(TSqsEvents::TEvUserCreated::TPtr& ev) {
        SchemaActor = TActorId();
        if (ev->Get()->Success) {
        } else {
            MakeError(Response_.MutableCreateUser(), NErrors::INTERNAL_FAILURE);
        }

        SendReplyAndDie();
    }

    void PassAway() override {
        if (SchemaActor) {
            Send(SchemaActor, new TEvPoisonPill());
            SchemaActor = TActorId();
        }
        TActionActor<TCreateUserActor>::PassAway();
    }

    const TCreateUserRequest& Request() const {
        return SourceSqsRequest_.GetCreateUser();
    }

private:
    TActorId SchemaActor;
};

IActor* CreateCreateUserActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TCreateUserActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
