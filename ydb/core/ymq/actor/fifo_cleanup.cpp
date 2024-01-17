#include "fifo_cleanup.h"
#include "cfg.h"
#include "log.h"
#include "executor.h"

#include <ydb/public/lib/value/value.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/ymq/queues/common/key_hashes.h>

#include <ydb/library/actors/core/hfunc.h>

#include <util/random/random.h>
#include <util/generic/guid.h>

namespace NKikimr::NSQS {

TCleanupActor::TCleanupActor(
    const TQueuePath& queuePath,
    ui32 tablesFormat,
    const TActorId& queueLeader,
    ECleanupType cleanupType
)
    : QueuePath_(queuePath)
    , TablesFormat_(tablesFormat)
    , RequestId_(CreateGuidAsString())
    , QueueLeader_(queueLeader)
    , CleanupType(cleanupType)
{}

void TCleanupActor::Bootstrap() {
    RLOG_SQS_INFO("Bootstrap cleanup actor for queue " << TLogQueueName(QueuePath_));
    Become(&TThis::StateFunc);
    Schedule(RandomCleanupPeriod(), new TEvWakeup());
}

TDuration TCleanupActor::RandomCleanupPeriod() {
    const ui64 cleanupPeriodMs = Cfg().GetCleanupPeriodMs();
    Y_ABORT_UNLESS(cleanupPeriodMs > 0);
    return TDuration::MilliSeconds(cleanupPeriodMs) +
        TDuration::MilliSeconds(RandomNumber<ui64>(cleanupPeriodMs / 4));
}

void TCleanupActor::HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const ui32 status = record.GetStatus();
    if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        using NKikimr::NClient::TValue;
        const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
        const bool shouldContinue = val["moreData"];
        const TValue lastProcessedKey = val["lastProcessedKey"];
        if (lastProcessedKey.HaveValue()) {
            KeyRangeStart = lastProcessedKey;
        }

        if (shouldContinue) {
            RunCleanupQuery();
        } else {
            Schedule(RandomCleanupPeriod(), new TEvWakeup());
        }
    } else {
        RLOG_SQS_ERROR("Cleanup query failed. Queue: " << TLogQueueName(QueuePath_));
        Schedule(RandomCleanupPeriod(), new TEvWakeup());
    }
}

void TCleanupActor::HandlePoisonPill(TEvPoisonPill::TPtr&) {
    PassAway();
}

void TCleanupActor::HandleWakeup() {
    KeyRangeStart = "";
    RunCleanupQuery();
}

void TCleanupActor::RunCleanupQuery() {
    TExecutorBuilder builder(SelfId(), RequestId_);
    builder
        .User(QueuePath_.UserName)
        .Queue(QueuePath_.QueueName)
        .QueueLeader(QueueLeader_)
        .TablesFormat(TablesFormat_)
        .QueryId(GetCleanupQueryId())
        .RetryOnTimeout()
        .Params()
            .Uint64("QUEUE_ID_NUMBER", QueuePath_.Version)
            .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(QueuePath_.Version))
            .Uint64("NOW", Now().MilliSeconds())
            .Uint64("BATCH_SIZE", Cfg().GetCleanupBatchSize());

    switch (CleanupType) {
    case ECleanupType::Deduplication:
        builder.Params().String("KEY_RANGE_START", KeyRangeStart);
        break;
    case ECleanupType::Reads:
        builder.Params().Utf8("KEY_RANGE_START", KeyRangeStart);
        break;
    }

    builder.Start();

    RLOG_SQS_DEBUG("Executing cleanup request for queue " << TLogQueueName(QueuePath_));
}

STATEFN(TCleanupActor::StateFunc) {
    switch (ev->GetTypeRewrite()) {
        cFunc(TEvWakeup::EventType, HandleWakeup);
        hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
        hFunc(TEvPoisonPill, HandlePoisonPill);
    }
}

EQueryId TCleanupActor::GetCleanupQueryId() const {
    switch (CleanupType) {
    case ECleanupType::Deduplication:
        return CLEANUP_DEDUPLICATION_ID;
    case ECleanupType::Reads:
        return CLEANUP_READS_ID;
    }
}

} // namespace NKikimr::NSQS
