#include "queues_list_reader.h"
#include "cfg.h"
#include "executor.h"
#include "events.h"

namespace NKikimr::NSQS {

TQueuesListReader::TQueuesListReader(const TIntrusivePtr<TTransactionCounters>& transactionCounters)
    : TActor(&TQueuesListReader::StateFunc)
    , TransactionCounters(transactionCounters)
{
}

TQueuesListReader::~TQueuesListReader() {
}

STATEFN(TQueuesListReader::StateFunc) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TSqsEvents::TEvReadQueuesList, HandleReadQueuesList);
        hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
    default:
        LOG_SQS_ERROR("Unknown type of event came to SQS user settings reader actor: " << ev->Type << " (" << ev->GetTypeName() << "), sender: " << ev->Sender);
    }
}

void TQueuesListReader::HandleReadQueuesList(TSqsEvents::TEvReadQueuesList::TPtr& ev) {
    Recipients.insert(ev->Sender);

    if (!ListingQueues) {
        ListingQueues = true;
        Result = MakeHolder<TSqsEvents::TEvQueuesList>();
        CurrentUser = TString();
        CurrentQueue = TString();
        if (CompiledQuery) {
            NextRequest();
        } else {
            CompileRequest();
        }
    }
}

void TQueuesListReader::HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
    ev->Get()->Call();
}

void TQueuesListReader::CompileRequest() {
    TExecutorBuilder(SelfId(), "")
        .Mode(NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE)
        .QueryId(GET_QUEUES_LIST_ID)
        .RetryOnTimeout()
        .OnExecuted([this](const TSqsEvents::TEvExecuted::TRecord& ev) { OnRequestCompiled(ev); })
        .Counters(TransactionCounters)
        .Start();
}

void TQueuesListReader::OnRequestCompiled(const TSqsEvents::TEvExecuted::TRecord& record) {
    LOG_SQS_TRACE("Handle compiled get queues list query: " << record);
    if (record.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        CompiledQuery = record.GetMiniKQLCompileResults().GetCompiledProgram();
        NextRequest();
    } else {
        LOG_SQS_WARN("Get queues list request compilation failed: " << record);
        Fail();
    }
}

void TQueuesListReader::NextRequest() {
    TExecutorBuilder(SelfId(), "")
        .QueryId(GET_QUEUES_LIST_ID)
        .Bin(CompiledQuery)
        .RetryOnTimeout()
        .OnExecuted([this](const TSqsEvents::TEvExecuted::TRecord& ev) { OnQueuesList(ev); })
        .Counters(TransactionCounters)
        .Params()
            .Utf8("FROM_USER", CurrentUser)
            .Utf8("FROM_QUEUE", CurrentQueue)
            .Uint64("BATCH_SIZE", Cfg().GetQueuesListReadBatchSize())
        .ParentBuilder().Start();
}

void TQueuesListReader::OnQueuesList(const TSqsEvents::TEvExecuted::TRecord& record) {
    LOG_SQS_TRACE("Handle queues list: " << record);
    if (record.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        using NKikimr::NClient::TValue;
        const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
        const TValue queuesVal(val["queues"]);
        const bool cloudMode = Cfg().GetYandexCloudMode();
        std::tuple<TString, TString> maxUserQueue;
        for (size_t i = 0; i < queuesVal.Size(); ++i) {
            const TValue row = queuesVal[i];
            TString user = row["Account"];
            TString queue = row["QueueName"];
            if (user == CurrentUser && queue == CurrentQueue) {
                continue;
            }

            if (std::tie(user, queue) > maxUserQueue) {
                std::get<0>(maxUserQueue) = user;
                std::get<1>(maxUserQueue) = queue;
            }

            const ui64 state = row["QueueState"];
            if (state != 1 && state != 3) { // not finished queue creation
                continue;
            }

            const TValue leaderTabletId = row["MasterTabletId"];
            if (!leaderTabletId.HaveValue()) {
                LOG_SQS_ERROR("Queue [" << user << "/" << queue << "] without leader tablet id detected");
                continue;
            }

            Result->SortedQueues.emplace_back();
            auto& rec = Result->SortedQueues.back();
            rec.UserName = std::move(user);
            rec.QueueName = std::move(queue);
            rec.LeaderTabletId = leaderTabletId;
            if (cloudMode) {
                rec.CustomName = row["CustomQueueName"];
            } else {
                rec.CustomName = rec.QueueName;
            }
            const TValue version = row["Version"];
            if (version.HaveValue()) {
                rec.Version = version;
            }
            const TValue tablesFormat = row["TablesFormat"];
            if (tablesFormat.HaveValue()) {
                rec.TablesFormat = tablesFormat;
            }
            rec.FolderId = row["FolderId"];
            rec.ShardsCount = row["Shards"];
            rec.DlqName = row["DlqName"];
            rec.CreatedTimestamp = TInstant::MilliSeconds(ui64(row["CreatedTimestamp"]));
            rec.IsFifo = row["FifoQueue"];
        }

        const bool truncated = val["truncated"];
        if (truncated) {
            CurrentUser = std::get<0>(maxUserQueue);
            CurrentQueue = std::get<1>(maxUserQueue);
            NextRequest();
        } else {
            std::sort(Result->SortedQueues.begin(), Result->SortedQueues.end()); // If .Queues table consists of many shards, result is possibly not sorted.
            Success();
        }
    } else {
        LOG_SQS_WARN("Get queues list request failed: " << record);
        Fail();
    }
}

void TQueuesListReader::Success() {
    if (Recipients.size() == 1) {
        Send(*Recipients.begin(), std::move(Result));
    } else {
        for (const auto& recipientId : Recipients) {
            auto result = MakeHolder<TSqsEvents::TEvQueuesList>();
            result->SortedQueues = Result->SortedQueues;
            result->Success = Result->Success;
            Send(recipientId, result.Release());
        }
    }

    Recipients.clear();

    ListingQueues = false;
}

void TQueuesListReader::Fail() {
    for (const auto& recipientId : Recipients) {
        Send(recipientId, new TSqsEvents::TEvQueuesList(false));
    }

    Recipients.clear();

    ListingQueues = false;
}

} // namespace NKikimr::NSQS
