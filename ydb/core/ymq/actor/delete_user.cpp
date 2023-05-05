#include "action.h"
#include "error.h"
#include "executor.h"
#include "log.h"
#include "queue_schema.h"

#include <ydb/public/lib/value/value.h>

#include <util/generic/set.h>

using NKikimr::NClient::TValue;

namespace NKikimr::NSQS {

class TDeleteUserActor
    : public TActionActor<TDeleteUserActor>
{
public:
    static constexpr bool NeedExistingQueue() {
        return false;
    }

    TDeleteUserActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::DeleteUser, std::move(cb))
    {
    }

private:
    bool DoValidate() override {
        if (!Request().GetUserName()) {
            MakeError(Response_.MutableDeleteUser(), NErrors::MISSING_PARAMETER, "No user name parameter.");
            return false;
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableDeleteUser()->MutableError();
    }

    void DoAction() override {
        Become(&TThis::StateFunc);

        TExecutorBuilder(SelfId(), RequestId_)
            .User(Request().GetUserName())
            .QueryId(LIST_QUEUES_ID)
            .Counters(QueueCounters_)
            .RetryOnTimeout()
            .Params()
                .Utf8("FOLDERID", "")
                .Utf8("USER_NAME", UserName_)
            .ParentBuilder().Start();
    }

    TString DoGetQueueName() const override {
        return TString();
    }

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup,          HandleWakeup);
            hFunc(TSqsEvents::TEvExecuted,     HandleExecuted);
            hFunc(TSqsEvents::TEvQueueDeleted, HandleQueueDeleted);
            hFunc(TSqsEvents::TEvUserDeleted,  HandleUserDeleted);
        }
    }

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        ui32 status = record.GetStatus();
        auto* result = Response_.MutableDeleteUser();

        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
            const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
            const TValue queues(val["queues"]);

            if (queues.Size() == 0) {
                Register(
                    new TDeleteUserSchemaActor(
                        Cfg().GetRoot(), Request().GetUserName(), SelfId(), RequestId_, UserCounters_)
                );
                return;
            }

            for (size_t i = 0; i < queues.Size(); ++i) {
                const TString name((TString(queues[i]["QueueName"])));
                const ui64 version(queues[i]["Version"]);
                const bool isFifo(queues[i]["FifoQueue"]);
                const ui32 tablesFormat(queues[i]["TablesFormat"]);

                Queues_.insert(name);

                Register(
                    new TDeleteQueueSchemaActorV2(
                        TQueuePath(Cfg().GetRoot(), Request().GetUserName(), name, version),
                        isFifo,
                        tablesFormat,
                        SelfId(),
                        RequestId_,
                        UserCounters_
                    )
                );
            }

            return;
        } else {
            RLOG_SQS_WARN("Request failed: " << record);
            MakeError(result, NErrors::INTERNAL_FAILURE);
        }

        SendReplyAndDie();
    }

    void HandleQueueDeleted(TSqsEvents::TEvQueueDeleted::TPtr& ev) {
        if (ev->Get()->Success) {
            Queues_.erase(ev->Get()->QueuePath.QueueName);

            if (Queues_.empty()) {
                Register(
                    new TDeleteUserSchemaActor(
                        Cfg().GetRoot(), Request().GetUserName(), SelfId(), RequestId_, UserCounters_)
                );
            }

            return;
        } else {
            MakeError(Response_.MutableDeleteUser(), NErrors::INTERNAL_FAILURE, ev->Get()->Message);
        }

        SendReplyAndDie();
    }

    void HandleUserDeleted(TSqsEvents::TEvUserDeleted::TPtr& ev) {
        if (ev->Get()->Success) {
        } else {
            MakeError(Response_.MutableDeleteUser(), NErrors::INTERNAL_FAILURE, "Can't delete user: " + ev->Get()->Error);
        }

        SendReplyAndDie();
    }

    const TDeleteUserRequest& Request() const {
        return SourceSqsRequest_.GetDeleteUser();
    }

private:
    TSet<TString> Queues_;
};

IActor* CreateDeleteUserActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TDeleteUserActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
