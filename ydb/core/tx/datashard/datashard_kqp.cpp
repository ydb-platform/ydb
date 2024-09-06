#include "datashard_kqp.h"
#include "datashard_impl.h"
#include "datashard_user_db.h"

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/runtime/kqp_tasks_runner.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/locks/locks.h>
#include <ydb/core/tx/datashard/datashard_user_table.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/protos/kqp_stats.pb.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>

#include <util/generic/size_literals.h>

namespace NKikimr {
namespace NDataShard {

namespace {

const ui32 MaxDatashardReplySize = 48 * 1024 * 1024;  // 48 MB

using namespace NYql;

bool KqpValidateTask(const NYql::NDqProto::TDqTask& task, bool isImmediate, ui64 txId, const TActorContext& ctx, bool& hasPersistentChannels)
{
    for (auto& input : task.GetInputs()) {
        for (auto& channel : input.GetChannels()) {
            if (channel.GetIsPersistent()) {
                hasPersistentChannels = true;
                if (isImmediate) {
                    LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "KQP validate, txId: " << txId << ", immediate KQP transaction cannot have persistent input channels"
                                                                                           << ", task: " << task.GetId() << ", channelId: " << channel.GetId());
                    return false;
                }

                if (!channel.GetSrcEndpoint().HasTabletId()) {
                    LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "KQP validate, txId: " << txId << ", persistent input channel without src tablet id"
                                                                                           << ", task: " << task.GetId() << ", channelId: " << channel.GetId());
                    return false;
                }
            }
        }
    }

    for (auto& output : task.GetOutputs()) {
        for (auto& channel : output.GetChannels()) {
            if (channel.GetIsPersistent()) {
                hasPersistentChannels = true;
                if (isImmediate) {
                    LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "KQP validate, txId: " << txId << ", immediate KQP transaction cannot have persistent output channels"
                                                                                           << ", task: " << task.GetId() << ", channelId: " << channel.GetId());
                    return false;
                }

                if (!channel.GetDstEndpoint().HasTabletId()) {
                    LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "KQP validate, txId: " << txId << ", persistent output channel without dst tablet id"
                                                                                           << ", task: " << task.GetId() << ", channelId: " << channel.GetId());
                    return false;
                }
            }
        }
    }

    return true;
}

NUdf::EFetchStatus FetchAllOutput(NDq::IDqOutputChannel* channel, NDq::TDqSerializedBatch& buffer) {
    auto result = channel->PopAll(buffer);
    Y_UNUSED(result);

    if (channel->IsFinished()) {
        return NUdf::EFetchStatus::Finish;
    }

    return NUdf::EFetchStatus::Yield;
}

NUdf::EFetchStatus FetchOutput(NDq::IDqOutputChannel* channel, NDq::TDqSerializedBatch& buffer) {
    auto result = channel->Pop(buffer);
    Y_UNUSED(result);

    if (channel->IsFinished()) {
        return NUdf::EFetchStatus::Finish;
    }

    return NUdf::EFetchStatus::Yield;
}

NDq::ERunStatus RunKqpTransactionInternal(const TActorContext& ctx, ui64 txId, const TInputOpData::TInReadSets* inReadSets, bool useGenericReadSets, NKqp::TKqpTasksRunner& tasksRunner, bool applyEffects)
{
    THashMap<ui64, std::pair<ui64, ui32>> inputChannelsMap;  // channelId -> (taskId, input index)
    for (auto& [taskId, task] : tasksRunner.GetTasks()) {
        for (ui32 i = 0; i < task.InputsSize(); ++i) {
            auto& input = task.GetInputs(i);
            for (auto& channel : input.GetChannels()) {
                auto channelInfo = std::make_pair(task.GetId(), i);
                auto result = inputChannelsMap.emplace(channel.GetId(), channelInfo);
                MKQL_ENSURE_S(result.second);
            }
        }
    }

    if (inReadSets) {
        YQL_ENSURE(applyEffects);

        for (auto& readSet : *inReadSets) {
            auto& key = readSet.first;
            auto& dataList = readSet.second;

            ui64 source = key.first;
            ui64 target = key.second;

            for (auto& data : dataList) {
                NKikimrTxDataShard::TKqpReadset kqpReadset;
                if (useGenericReadSets) {
                    NKikimrTx::TReadSetData genericData;
                    bool ok = genericData.ParseFromString(data.Body);
                    Y_ABORT_UNLESS(ok, "Failed to parse generic readset data from %" PRIu64 " to %" PRIu64 " origin %" PRIu64, source, target, data.Origin);

                    if (genericData.HasData()) {
                        ok = genericData.GetData().UnpackTo(&kqpReadset);
                        Y_ABORT_UNLESS(ok, "Failed to parse kqp readset data from %" PRIu64 " to %" PRIu64 " origin %" PRIu64, source, target, data.Origin);
                    }
                } else {
                    Y_PROTOBUF_SUPPRESS_NODISCARD kqpReadset.ParseFromString(data.Body);
                }

                for (int outputId = 0; outputId < kqpReadset.GetOutputs().size(); ++outputId) {
                    auto* channelData = kqpReadset.MutableOutputs()->Mutable(outputId);
                    auto channelId = channelData->GetChannelId();
                    auto inputInfo = inputChannelsMap.FindPtr(channelId);
                    MKQL_ENSURE_S(inputInfo);

                    auto taskId = inputInfo->first;

                    LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Added KQP readset"
                                                                        << ", source: " << source << ", target: " << target << ", origin: " << data.Origin << ", TxId: " << txId << ", task: " << taskId << ", channelId: " << channelId);

                    auto channel = tasksRunner.GetInputChannel(taskId, channelId);
                    NDq::TDqSerializedBatch batch;
                    batch.Proto = std::move(*(channelData->MutableData()));
                    MKQL_ENSURE_S(!batch.IsOOB());
                    channel->Push(std::move(batch));

                    MKQL_ENSURE_S(channelData->GetFinished());
                    channel->Finish();
                }
            }
        }
    }

    auto runStatus = NDq::ERunStatus::PendingInput;
    bool hasInputChanges = true;

    while (runStatus == NDq::ERunStatus::PendingInput && hasInputChanges) {
        runStatus = tasksRunner.Run(applyEffects);
        if (runStatus == NDq::ERunStatus::Finished) {
            break;
        }

        // we must set output buffers big enough to avoid PendingOutput state here
        MKQL_ENSURE_S(runStatus == NDq::ERunStatus::PendingInput);

        hasInputChanges = false;
        for (auto& taskIt : tasksRunner.GetTasks()) {
            const auto& task = taskIt.second;
            for (ui32 i = 0; i < task.OutputsSize(); ++i) {
                for (auto& channel : task.GetOutputs(i).GetChannels()) {
                    if (auto* inputInfo = inputChannelsMap.FindPtr(channel.GetId())) {
                        auto transferState = tasksRunner.TransferData(task.GetId(), channel.GetId(), inputInfo->first, channel.GetId());

                        if (transferState.first) {
                            hasInputChanges = true;
                            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Forwarded KQP channel data"
                                                                                << ", TxId: " << txId << ", srcTask: " << task.GetId() << ", dstTask: " << inputInfo->first << ", channelId: " << channel.GetId());
                        }

                        if (transferState.second) {
                            hasInputChanges = true;
                            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Finished input channel"
                                                                                << ", TxId: " << txId << ", srcTask: " << task.GetId() << ", dstTask: " << inputInfo->first << ", channelId: " << channel.GetId());
                        }
                    }
                }
            }
        }
    }

    return runStatus;
}

bool NeedValidateLocks(NKikimrDataEvents::TKqpLocks::ELocksOp op) {
    switch (op) {
        case NKikimrDataEvents::TKqpLocks::Commit:
            return true;

        case NKikimrDataEvents::TKqpLocks::Rollback:
        case NKikimrDataEvents::TKqpLocks::Unspecified:
            return false;
    }
}

bool NeedEraseLocks(NKikimrDataEvents::TKqpLocks::ELocksOp op) {
    switch (op) {
        case NKikimrDataEvents::TKqpLocks::Commit:
        case NKikimrDataEvents::TKqpLocks::Rollback:
            return true;

        case NKikimrDataEvents::TKqpLocks::Unspecified:
            return false;
    }
}

bool NeedCommitLocks(NKikimrDataEvents::TKqpLocks::ELocksOp op) {
    switch (op) {
        case NKikimrDataEvents::TKqpLocks::Commit:
            return true;

        case NKikimrDataEvents::TKqpLocks::Rollback:
        case NKikimrDataEvents::TKqpLocks::Unspecified:
            return false;
    }
}

TVector<TCell> MakeLockKey(const NKikimrDataEvents::TLock& lockProto) {
    auto lockId = lockProto.GetLockId();
    auto lockDatashard = lockProto.GetDataShard();
    auto lockSchemeShard = lockProto.GetSchemeShard();
    auto lockPathId = lockProto.GetPathId();

    Y_ASSERT(TCell::CanInline(sizeof(lockId)));
    Y_ASSERT(TCell::CanInline(sizeof(lockDatashard)));
    Y_ASSERT(TCell::CanInline(sizeof(lockSchemeShard)));
    Y_ASSERT(TCell::CanInline(sizeof(lockPathId)));

    TVector<TCell> lockKey{
        TCell(reinterpret_cast<const char*>(&lockId), sizeof(lockId)),
        TCell(reinterpret_cast<const char*>(&lockDatashard), sizeof(lockDatashard)),
        TCell(reinterpret_cast<const char*>(&lockSchemeShard), sizeof(lockSchemeShard)),
        TCell(reinterpret_cast<const char*>(&lockPathId), sizeof(lockPathId))};

    return lockKey;
}

// returns list of broken locks
TVector<NKikimrDataEvents::TLock> ValidateLocks(const NKikimrDataEvents::TKqpLocks& txLocks, TSysLocks& sysLocks, ui64 tabletId)
{
    TVector<NKikimrDataEvents::TLock> brokenLocks;

    if (!NeedValidateLocks(txLocks.GetOp())) {
        return {};
    }

    for (auto& lockProto : txLocks.GetLocks()) {
        if (lockProto.GetDataShard() != tabletId) {
            continue;
        }

        auto lockKey = MakeLockKey(lockProto);

        auto lock = sysLocks.GetLock(lockKey);
        if (lock.Generation != lockProto.GetGeneration() || lock.Counter != lockProto.GetCounter()) {
            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "ValidateLocks: broken lock " << lockProto.GetLockId() << " expected " << lockProto.GetGeneration() << ":" << lockProto.GetCounter() << " found " << lock.Generation << ":" << lock.Counter);
            brokenLocks.push_back(lockProto);
        }
    }

    return brokenLocks;
}

bool SendLocks(const NKikimrDataEvents::TKqpLocks& locks, ui64 shardId) {
    auto& sendingShards = locks.GetSendingShards();
    auto it = std::find(sendingShards.begin(), sendingShards.end(), shardId);
    return it != sendingShards.end();
}

bool ReceiveLocks(const NKikimrDataEvents::TKqpLocks& locks, ui64 shardId) {
    auto& receivingShards = locks.GetReceivingShards();
    auto it = std::find(receivingShards.begin(), receivingShards.end(), shardId);
    return it != receivingShards.end();
}

}  // namespace

bool KqpValidateTransaction(const ::google::protobuf::RepeatedPtrField<::NYql::NDqProto::TDqTask>& tasks, bool isImmediate, ui64 txId, const TActorContext& ctx, bool& hasPersistentChannels)
{
    for (const auto& task : tasks) {
        if (!KqpValidateTask(task, isImmediate, txId, ctx, hasPersistentChannels)) {
            return false;
        }
    }

    return true;
}

namespace {

using TReadOpMeta = NKikimrTxDataShard::TKqpTransaction::TDataTaskMeta::TReadOpMeta;
using TWriteOpMeta = NKikimrTxDataShard::TKqpTransaction::TDataTaskMeta::TWriteOpMeta;
using TColumnMeta = NKikimrTxDataShard::TKqpTransaction::TColumnMeta;

NTable::TColumn GetColumn(const TColumnMeta& columnMeta) {
    auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(columnMeta.GetType(), columnMeta.HasTypeInfo() ? &columnMeta.GetTypeInfo() : nullptr);
    return NTable::TColumn(columnMeta.GetName(), columnMeta.GetId(), typeInfoMod.TypeInfo, typeInfoMod.TypeMod);
}

TVector<NTable::TColumn> GetColumns(const TReadOpMeta& readMeta) {
    TVector<NTable::TColumn> columns;
    columns.reserve(readMeta.GetColumns().size());

    for (auto& column : readMeta.GetColumns()) {
        columns.push_back(GetColumn(column));
    }

    return columns;
}

TVector<TKeyValidator::TColumnWriteMeta> GetColumnWrites(const TWriteOpMeta& writeMeta) {
    TVector<TKeyValidator::TColumnWriteMeta> writeColumns;
    writeColumns.reserve(writeMeta.ColumnsSize());
    for (const auto& columnMeta : writeMeta.GetColumns()) {
        TKeyValidator::TColumnWriteMeta writeColumn;
        writeColumn.Column = GetColumn(columnMeta.GetColumn());
        writeColumn.MaxValueSizeBytes = columnMeta.GetMaxValueSizeBytes();

        writeColumns.push_back(std::move(writeColumn));
    }

    return writeColumns;
}

template <bool Read>
void KqpSetTxKeysImpl(ui64 tabletId, ui64 taskId, const TTableId& tableId, const TUserTable* tableInfo, const NKikimrTxDataShard::TKqpTransaction_TDataTaskMeta_TKeyRange& rangeKind, const TReadOpMeta* readMeta, const TWriteOpMeta* writeMeta, const NScheme::TTypeRegistry& typeRegistry, const TActorContext& ctx, TKeyValidator& keyValidator)
{
    if (Read) {
        Y_ABORT_UNLESS(readMeta);
    } else {
        Y_ABORT_UNLESS(writeMeta);
    }

    switch (rangeKind.Kind_case()) {
        case NKikimrTxDataShard::TKqpTransaction_TDataTaskMeta_TKeyRange::kRanges: {
            auto& ranges = rangeKind.GetRanges();
            Y_DEBUG_ABORT_UNLESS(ranges.GetKeyRanges().size() + ranges.GetKeyPoints().size() > 0);

            for (auto& range : ranges.GetKeyRanges()) {
                TSerializedTableRange tableRange;
                tableRange.Load(range);

                LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Table " << tableInfo->Path << ", shard: " << tabletId << ", task: " << taskId << ", " << (Read ? "read range " : "write range ") << DebugPrintRange(tableInfo->KeyColumnTypes, tableRange.ToTableRange(), typeRegistry));

                Y_DEBUG_ABORT_UNLESS(!(tableRange.To.GetCells().empty() && tableRange.ToInclusive));

                if constexpr (Read) {
                    keyValidator.AddReadRange(tableId, GetColumns(*readMeta), tableRange.ToTableRange(), tableInfo->KeyColumnTypes, readMeta->GetItemsLimit(), readMeta->GetReverse());
                } else {
                    keyValidator.AddWriteRange(tableId, tableRange.ToTableRange(), tableInfo->KeyColumnTypes, GetColumnWrites(*writeMeta), writeMeta->GetIsPureEraseOp());
                }
            }

            for (auto& point : ranges.GetKeyPoints()) {
                TSerializedTableRange tablePoint(point, point, true, true);
                tablePoint.Point = true;

                LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Table " << tableInfo->Path << ", shard: " << tabletId << ", task: " << taskId << ", " << (Read ? "read point " : "write point ") << DebugPrintPoint(tableInfo->KeyColumnTypes, tablePoint.From.GetCells(), typeRegistry));

                if constexpr (Read) {
                    keyValidator.AddReadRange(tableId, GetColumns(*readMeta), tablePoint.ToTableRange(), tableInfo->KeyColumnTypes, readMeta->GetItemsLimit(), readMeta->GetReverse());
                } else {
                    keyValidator.AddWriteRange(tableId, tablePoint.ToTableRange(), tableInfo->KeyColumnTypes, GetColumnWrites(*writeMeta), writeMeta->GetIsPureEraseOp());
                }
            }

            break;
        }

        case NKikimrTxDataShard::TKqpTransaction_TDataTaskMeta_TKeyRange::kFullRange: {
            TSerializedTableRange tableRange;
            tableRange.Load(rangeKind.GetFullRange());

            LOG_TRACE_S(ctx, NKikimrServices::TX_DATASHARD, "Table " << tableInfo->Path << ", shard: " << tabletId << ", task: " << taskId << ", " << (Read ? "read range: FULL " : "write range: FULL ") << DebugPrintRange(tableInfo->KeyColumnTypes, tableRange.ToTableRange(), typeRegistry));

            if constexpr (Read) {
                keyValidator.AddReadRange(tableId, GetColumns(*readMeta), tableRange.ToTableRange(), tableInfo->KeyColumnTypes, readMeta->GetItemsLimit(), readMeta->GetReverse());
            } else {
                keyValidator.AddWriteRange(tableId, tableRange.ToTableRange(), tableInfo->KeyColumnTypes, GetColumnWrites(*writeMeta), writeMeta->GetIsPureEraseOp());
            }

            break;
        }

        case NKikimrTxDataShard::TKqpTransaction_TDataTaskMeta_TKeyRange::KIND_NOT_SET: {
            LOG_ERROR_S(ctx, NKikimrServices::TX_DATASHARD, "Table " << tableInfo->Path << ", shard: " << tabletId << ", task: " << taskId << ", " << (Read ? "read range: UNSPECIFIED" : "write range: UNSPECIFIED"));

            if constexpr (Read) {
                keyValidator.AddReadRange(tableId, GetColumns(*readMeta), tableInfo->Range.ToTableRange(), tableInfo->KeyColumnTypes, readMeta->GetItemsLimit(), readMeta->GetReverse());
            } else {
                keyValidator.AddWriteRange(tableId, tableInfo->Range.ToTableRange(), tableInfo->KeyColumnTypes, GetColumnWrites(*writeMeta), writeMeta->GetIsPureEraseOp());
            }

            break;
        }
    }
}

}  // anonymous namespace

void KqpSetTxKeys(ui64 tabletId, ui64 taskId, const TUserTable* tableInfo, const NKikimrTxDataShard::TKqpTransaction_TDataTaskMeta& meta, const NScheme::TTypeRegistry& typeRegistry, const TActorContext& ctx, TKeyValidator& keyValidator)
{
    auto& tableMeta = meta.GetTable();
    auto tableId = TTableId(tableMeta.GetTableId().GetOwnerId(), tableMeta.GetTableId().GetTableId(), tableMeta.GetSchemaVersion());

    for (auto& read : meta.GetReads()) {
        KqpSetTxKeysImpl<true>(tabletId, taskId, tableId, tableInfo, read.GetRange(), &read, nullptr, typeRegistry, ctx, keyValidator);
    }

    if (meta.HasWrites()) {
        KqpSetTxKeysImpl<false>(tabletId, taskId, tableId, tableInfo, meta.GetWrites().GetRange(), nullptr, &meta.GetWrites(), typeRegistry, ctx, keyValidator);
    }
}

void KqpSetTxLocksKeys(const NKikimrDataEvents::TKqpLocks& locks, const TSysLocks& sysLocks, TKeyValidator& keyValidator) {
    if (locks.LocksSize() == 0) {
        return;
    }

    static TTableId sysLocksTableId = TTableId(TSysTables::SysSchemeShard, TSysTables::SysTableLocks2);
    static TVector<NScheme::TTypeInfo> lockRowType = {
        NScheme::TTypeInfo(NScheme::TUint64::TypeId),
        NScheme::TTypeInfo(NScheme::TUint64::TypeId),
        NScheme::TTypeInfo(NScheme::TUint64::TypeId),
        NScheme::TTypeInfo(NScheme::TUint64::TypeId),
    };

    for (auto& lock : locks.GetLocks()) {
        auto lockKey = MakeLockKey(lock);
        if (sysLocks.IsMyKey(lockKey)) {
            auto point = TTableRange(lockKey, true, {}, true, /* point */ true);
            if (NeedValidateLocks(locks.GetOp())) {
                keyValidator.AddReadRange(sysLocksTableId, {}, point, lockRowType);
            }
            if (NeedEraseLocks(locks.GetOp())) {
                keyValidator.AddWriteRange(sysLocksTableId, point, lockRowType, {}, /* isPureEraseOp */ true);
            }
        }
    }
}

NYql::NDq::ERunStatus KqpRunTransaction(const TActorContext& ctx, ui64 txId, bool useGenericReadSets, NKqp::TKqpTasksRunner& tasksRunner)
{
    return RunKqpTransactionInternal(ctx, txId, /* inReadSets */ nullptr, useGenericReadSets, tasksRunner, /* applyEffects */ false);
}

THolder<TEvDataShard::TEvProposeTransactionResult> KqpCompleteTransaction(const TActorContext& ctx, ui64 origin, ui64 txId, const TInputOpData::TInReadSets* inReadSets, bool useGenericReadSets, NKqp::TKqpTasksRunner& tasksRunner, const NMiniKQL::TKqpDatashardComputeContext& computeCtx)
{
    auto runStatus = RunKqpTransactionInternal(ctx, txId, inReadSets, useGenericReadSets, tasksRunner, /* applyEffects */ true);

    if (computeCtx.HadInconsistentReads()) {
        return nullptr;
    }

    if (runStatus == NYql::NDq::ERunStatus::PendingInput && computeCtx.IsTabletNotReady()) {
        return nullptr;
    }

    MKQL_ENSURE_S(runStatus == NYql::NDq::ERunStatus::Finished);

    if (computeCtx.HasVolatileReadDependencies()) {
        return nullptr;
    }

    auto result = MakeHolder<TEvDataShard::TEvProposeTransactionResult>(NKikimrTxDataShard::TX_KIND_DATA, origin, txId, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);

    for (auto& [taskId, task] : tasksRunner.GetTasks()) {
        auto& taskRunner = tasksRunner.GetTaskRunner(task.GetId());

        for (ui32 i = 0; i < task.OutputsSize(); ++i) {
            for (auto& channel : task.GetOutputs(i).GetChannels()) {
                auto computeActor = computeCtx.GetTaskOutputChannel(task.GetId(), channel.GetId());
                if (computeActor) {
                    size_t seqNo = 1;
                    auto fetchStatus = NUdf::EFetchStatus::Yield;
                    while (fetchStatus != NUdf::EFetchStatus::Finish) {
                        auto dataEv = MakeHolder<NYql::NDq::TEvDqCompute::TEvChannelData>();
                        dataEv->Record.SetSeqNo(seqNo++);
                        dataEv->Record.MutableChannelData()->SetChannelId(channel.GetId());
                        dataEv->Record.SetNoAck(true);
                        auto outputData = dataEv->Record.MutableChannelData()->MutableData();
                        NDq::TDqSerializedBatch serialized;
                        try {
                            fetchStatus = FetchOutput(taskRunner.GetOutputChannel(channel.GetId()).Get(), serialized);
                        } catch (const NDq::TDqOutputChannelChunkSizeLimitExceeded& ex) {
                            auto message = TStringBuilder() << "Datashard " << origin << ": " << ex.what();
                            LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, message);
                            result->SetExecutionError(NKikimrTxDataShard::TError::REPLY_SIZE_EXCEEDED, message);
                            break;
                        }
                        const size_t outputDataSize = serialized.Size();
                        *outputData = std::move(serialized.Proto);
                        outputData->ClearPayloadId();
                        if (!serialized.Payload.IsEmpty()) {
                            outputData->SetPayloadId(dataEv->AddPayload(std::move(serialized.Payload)));
                        }
                        dataEv->Record.MutableChannelData()->SetFinished(fetchStatus == NUdf::EFetchStatus::Finish);
                        if (outputDataSize > MaxDatashardReplySize) {
                            auto message = TStringBuilder() << "Datashard " << origin
                                                            << ": reply size limit exceeded (" << outputDataSize << " > "
                                                            << MaxDatashardReplySize << ")";
                            LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, message);
                            result->SetExecutionError(NKikimrTxDataShard::TError::REPLY_SIZE_EXCEEDED, message);
                            break;
                        } else {
                            ctx.Send(computeActor, dataEv.Release());
                        }
                    }
                } else {
                    NDq::TDqSerializedBatch outputData;
                    auto fetchStatus = FetchOutput(taskRunner.GetOutputChannel(channel.GetId()).Get(), outputData);
                    MKQL_ENSURE_S(fetchStatus == NUdf::EFetchStatus::Finish);
                    MKQL_ENSURE_S(outputData.Proto.GetRows() == 0);
                }
            }
        }
    }

    TString replyStr;
    NKikimrTxDataShard::TKqpReply reply;
    Y_PROTOBUF_SUPPRESS_NODISCARD reply.SerializeToString(&replyStr);

    result->SetTxResult(replyStr);
    return result;
}

void KqpFillOutReadSets(TOutputOpData::TOutReadSets& outReadSets, const NKikimrDataEvents::TKqpLocks& kqpLocks, bool useGenericReadSets, NKqp::TKqpTasksRunner* tasksRunner, TSysLocks& sysLocks, ui64 tabletId)
{
    TMap<std::pair<ui64, ui64>, NKikimrTxDataShard::TKqpReadset> readsetData;

    if (tasksRunner) {
        for (auto& [taskId, task] : tasksRunner->GetTasks()) {
            auto& taskRunner = tasksRunner->GetTaskRunner(task.GetId());

            for (ui32 i = 0; i < task.OutputsSize(); ++i) {
                for (auto& channel : task.GetOutputs(i).GetChannels()) {
                    if (channel.GetIsPersistent()) {
                        MKQL_ENSURE_S(channel.GetSrcEndpoint().HasTabletId());
                        MKQL_ENSURE_S(channel.GetDstEndpoint().HasTabletId());

                        NDq::TDqSerializedBatch outputData;
                        auto fetchStatus = FetchAllOutput(taskRunner.GetOutputChannel(channel.GetId()).Get(), outputData);
                        MKQL_ENSURE_S(fetchStatus == NUdf::EFetchStatus::Finish);
                        MKQL_ENSURE(!outputData.IsOOB(), "Out-of-band data transport is not yet supported");

                        auto key = std::make_pair(channel.GetSrcEndpoint().GetTabletId(), channel.GetDstEndpoint().GetTabletId());
                        auto& channelData = *readsetData[key].AddOutputs();

                        channelData.SetChannelId(channel.GetId());
                        channelData.SetFinished(true);
                        channelData.MutableData()->Swap(&outputData.Proto);
                    }
                }
            }
        }
    }

    NKikimrTx::TReadSetData::EDecision decision = NKikimrTx::TReadSetData::DECISION_COMMIT;
    TMap<std::pair<ui64, ui64>, NKikimrTx::TReadSetData> genericData;

    if (kqpLocks.HasOp() && NeedValidateLocks(kqpLocks.GetOp())) {
        bool sendLocks = SendLocks(kqpLocks, tabletId);
        YQL_ENSURE(sendLocks == !kqpLocks.GetLocks().empty());

        if (sendLocks && !kqpLocks.GetReceivingShards().empty()) {
            auto brokenLocks = ValidateLocks(kqpLocks, sysLocks, tabletId);

            NKikimrTxDataShard::TKqpValidateLocksResult validateLocksResult;
            validateLocksResult.SetSuccess(brokenLocks.empty());

            for (auto& lock : brokenLocks) {
                LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Found broken lock: " << lock.ShortDebugString());
                if (useGenericReadSets) {
                    decision = NKikimrTx::TReadSetData::DECISION_ABORT;
                } else {
                    validateLocksResult.AddBrokenLocks()->Swap(&lock);
                }
            }

            for (auto& dstTabletId : kqpLocks.GetReceivingShards()) {
                if (tabletId == dstTabletId) {
                    continue;
                }

                LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Send locks from " << tabletId << " to " << dstTabletId << ", locks: " << validateLocksResult.ShortDebugString());

                auto key = std::make_pair(tabletId, dstTabletId);
                if (useGenericReadSets) {
                    genericData[key].SetDecision(decision);
                } else {
                    readsetData[key].MutableValidateLocksResult()->CopyFrom(validateLocksResult);
                }
            }
        }
    }

    if (useGenericReadSets) {
        for (const auto& [key, data] : readsetData) {
            bool ok = genericData[key].MutableData()->PackFrom(data);
            Y_ABORT_UNLESS(ok, "Failed to pack readset data from %" PRIu64 " to %" PRIu64, key.first, key.second);
        }

        for (auto& [key, data] : genericData) {
            if (!data.HasDecision()) {
                data.SetDecision(decision);
            }

            TString bodyStr;
            bool ok = data.SerializeToString(&bodyStr);
            Y_ABORT_UNLESS(ok, "Failed to serialize readset from %" PRIu64 " to %" PRIu64, key.first, key.second);

            outReadSets[key] = std::move(bodyStr);
        }
    } else {
        for (auto& [key, data] : readsetData) {
            TString bodyStr;
            Y_PROTOBUF_SUPPRESS_NODISCARD data.SerializeToString(&bodyStr);

            outReadSets[key] = bodyStr;
        }
    }
}

std::tuple<bool, TVector<NKikimrDataEvents::TLock>> KqpValidateLocks(ui64 origin, TSysLocks& sysLocks, const NKikimrDataEvents::TKqpLocks* kqpLocks, bool useGenericReadSets, const TInputOpData::TInReadSets& inReadSets) {
    if (kqpLocks == nullptr || !NeedValidateLocks(kqpLocks->GetOp())) {
        return {true, {}};
    }

    bool sendLocks = SendLocks(*kqpLocks, origin);
    YQL_ENSURE(sendLocks == !kqpLocks->GetLocks().empty());

    if (sendLocks) {
        auto brokenLocks = ValidateLocks(*kqpLocks, sysLocks, origin);

        if (!brokenLocks.empty()) {
            return {false, std::move(brokenLocks)};
        }
    }

    for (const auto& readSet : inReadSets) {
        for (const auto& data : readSet.second) {
            if (useGenericReadSets) {
                NKikimrTx::TReadSetData genericData;
                bool ok = genericData.ParseFromString(data.Body);
                Y_ABORT_UNLESS(ok, "Failed to parse generic readset from %" PRIu64 " to %" PRIu64 " tabletId %" PRIu64, readSet.first.first, readSet.first.second, data.Origin);

                if (genericData.GetDecision() != NKikimrTx::TReadSetData::DECISION_COMMIT) {
                    // Note: we don't know details on what failed at that shard
                    return {false, {}};
                }
            } else {
                NKikimrTxDataShard::TKqpReadset kqpReadset;
                Y_PROTOBUF_SUPPRESS_NODISCARD kqpReadset.ParseFromString(data.Body);

                if (kqpReadset.HasValidateLocksResult()) {
                    auto& validateResult = kqpReadset.GetValidateLocksResult();
                    if (!validateResult.GetSuccess()) {
                        TVector<NKikimrDataEvents::TLock> brokenLocks;
                        brokenLocks.reserve(validateResult.GetBrokenLocks().size());
                        std::copy(validateResult.GetBrokenLocks().begin(), validateResult.GetBrokenLocks().end(), std::back_inserter(brokenLocks));
                        return {false, std::move(brokenLocks)};
                    }
                }
            }
        }
    }

    return {true, {}};
}

bool KqpLocksHasArbiter(const NKikimrDataEvents::TKqpLocks* kqpLocks) {
    return kqpLocks && kqpLocks->GetArbiterShard() != 0;
}

bool KqpLocksIsArbiter(ui64 tabletId, const NKikimrDataEvents::TKqpLocks* kqpLocks) {
    return KqpLocksHasArbiter(kqpLocks) && kqpLocks->GetArbiterShard() == tabletId;
}

std::tuple<bool, TVector<NKikimrDataEvents::TLock>> KqpValidateVolatileTx(ui64 origin, TSysLocks& sysLocks, const NKikimrDataEvents::TKqpLocks* kqpLocks, bool useGenericReadSets, ui64 txId, const TVector<NKikimrTx::TEvReadSet>& delayedInReadSets, TInputOpData::TAwaitingDecisions& awaitingDecisions, TOutputOpData::TOutReadSets& outReadSets) {
    if (kqpLocks == nullptr || !NeedValidateLocks(kqpLocks->GetOp())) {
        return {true, {}};
    }

    // Volatile transactions cannot work with non-generic readsets
    YQL_ENSURE(useGenericReadSets);

    // We may have some stale data since before the restart
    // We expect all stale data to be cleared on restarts
    Y_ABORT_UNLESS(outReadSets.empty());
    Y_ABORT_UNLESS(awaitingDecisions.empty());

    const bool hasArbiter = KqpLocksHasArbiter(kqpLocks);
    const bool isArbiter = KqpLocksIsArbiter(origin, kqpLocks);

    // Note: usually all shards send locks, since they either have side effects or need to validate locks
    // However it is technically possible to have pure-read shards, that don't contribute to the final decision
    bool sendLocks = SendLocks(*kqpLocks, origin);
    if (sendLocks) {
        // Note: it is possible to have no locks
        auto brokenLocks = ValidateLocks(*kqpLocks, sysLocks, origin);

        if (!brokenLocks.empty()) {
            return {false, std::move(brokenLocks)};
        }

        // We need to form decision readsets for all other participants
        for (ui64 dstTabletId : kqpLocks->GetReceivingShards()) {
            if (dstTabletId == origin) {
                // Don't send readsets to ourselves
                continue;
            }

            if (hasArbiter && !isArbiter && dstTabletId != kqpLocks->GetArbiterShard()) {
                // Non-arbiter shards only send locks to the arbiter
                continue;
            }

            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Send commit decision from " << origin << " to " << dstTabletId);

            auto key = std::make_pair(origin, dstTabletId);
            NKikimrTx::TReadSetData data;
            data.SetDecision(NKikimrTx::TReadSetData::DECISION_COMMIT);

            TString bodyStr;
            bool ok = data.SerializeToString(&bodyStr);
            Y_ABORT_UNLESS(ok, "Failed to serialize readset from %" PRIu64 " to %" PRIu64, key.first, key.second);

            outReadSets[key] = std::move(bodyStr);
        }
    } else {
        Y_ABORT_UNLESS(!isArbiter, "Arbiter is not in the sending shards set");
    }

    bool receiveLocks = ReceiveLocks(*kqpLocks, origin);
    if (receiveLocks) {
        // Note: usually only shards with side-effects receive locks, since they
        //       need the final outcome to decide whether to commit or abort.
        for (ui64 srcTabletId : kqpLocks->GetSendingShards()) {
            if (srcTabletId == origin) {
                // Don't await decision from ourselves
                continue;
            }

            if (hasArbiter && !isArbiter && srcTabletId != kqpLocks->GetArbiterShard()) {
                // Non-arbiter shards only await decision from the arbiter
                continue;
            }

            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Will wait for volatile decision from " << srcTabletId << " to " << origin);

            awaitingDecisions.insert(srcTabletId);
        }

        bool aborted = false;

        for (const auto& record : delayedInReadSets) {
            ui64 srcTabletId = record.GetTabletSource();
            ui64 dstTabletId = record.GetTabletDest();
            if (dstTabletId != origin) {
                LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Ignoring unexpected readset from " << srcTabletId << " to " << dstTabletId << " for txId# " << txId << " at tablet " << origin);
                continue;
            }
            if (!awaitingDecisions.contains(srcTabletId)) {
                continue;
            }

            if (record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_NO_DATA) {
                Y_ABORT_UNLESS(!(record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET), "Unexpected FLAG_EXPECT_READSET + FLAG_NO_DATA in delayed readsets");

                // No readset data: participant aborted the transaction
                LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Processed readset without data from" << srcTabletId << " to " << dstTabletId << " will abort txId# " << txId);
                aborted = true;
                break;
            }

            NKikimrTx::TReadSetData data;
            bool ok = data.ParseFromString(record.GetReadSet());
            Y_ABORT_UNLESS(ok, "Failed to parse readset from %" PRIu64 " to %" PRIu64, srcTabletId, dstTabletId);

            if (data.GetDecision() != NKikimrTx::TReadSetData::DECISION_COMMIT) {
                // Explicit decision that is not a commit, need to abort
                LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Processed decision " << ui32(data.GetDecision()) << " from " << srcTabletId << " to " << dstTabletId << " for txId# " << txId);
                aborted = true;
                break;
            }

            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Processed commit decision from " << srcTabletId << " to " << dstTabletId << " for txId# " << txId);
            awaitingDecisions.erase(srcTabletId);
        }

        if (aborted) {
            return {false, {}};
        }
    } else {
        Y_ABORT_UNLESS(!isArbiter, "Arbiter is not in the receiving shards set");
    }

    return {true, {}};
}

void KqpEraseLocks(ui64 origin, const NKikimrDataEvents::TKqpLocks* kqpLocks, TSysLocks& sysLocks) {
    if (kqpLocks == nullptr || !NeedEraseLocks(kqpLocks->GetOp())) {
        return;
    }

    for (const auto& lockProto : kqpLocks->GetLocks()) {
        if (lockProto.GetDataShard() != origin) {
            continue;
        }

        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "KqpEraseLock " << lockProto.ShortDebugString());

        auto lockKey = MakeLockKey(lockProto);
        sysLocks.EraseLock(lockKey);
    }
}

void KqpCommitLocks(ui64 origin, const NKikimrDataEvents::TKqpLocks* kqpLocks, TSysLocks& sysLocks, const TRowVersion& writeVersion, IDataShardUserDb& userDb) {
    if (kqpLocks == nullptr) {
        return;
    }

    if (NeedCommitLocks(kqpLocks->GetOp())) {
        // We assume locks have been validated earlier
        for (const auto& lockProto : kqpLocks->GetLocks()) {
            if (lockProto.GetDataShard() != origin) {
                continue;
            }

            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "KqpCommitLock " << lockProto.ShortDebugString());

            auto lockKey = MakeLockKey(lockProto);
            sysLocks.CommitLock(lockKey);

            TTableId tableId(lockProto.GetSchemeShard(), lockProto.GetPathId());
            auto txId = lockProto.GetLockId();

            userDb.CommitChanges(tableId, txId, writeVersion);
        }
    } else {
        KqpEraseLocks(origin, kqpLocks, sysLocks);
    }
}

void KqpPrepareInReadsets(TInputOpData::TInReadSets& inReadSets, const NKikimrDataEvents::TKqpLocks& kqpLocks, const NKqp::TKqpTasksRunner* tasksRunner, ui64 tabletId)
{
    if (tasksRunner) {
        for (auto& [taskId, task] : tasksRunner->GetTasks()) {
            for (ui32 i = 0; i < task.InputsSize(); ++i) {
                for (auto& channel : task.GetInputs(i).GetChannels()) {
                    if (channel.GetIsPersistent()) {
                        MKQL_ENSURE_S(channel.GetSrcEndpoint().HasTabletId());
                        MKQL_ENSURE_S(channel.GetDstEndpoint().HasTabletId());

                        auto key = std::make_pair(channel.GetSrcEndpoint().GetTabletId(), channel.GetDstEndpoint().GetTabletId());

                        inReadSets.emplace(key, TVector<TRSData>());
                    }
                }
            }
        }
    }

    if (ReceiveLocks(kqpLocks, tabletId)) {
        for (ui64 shardId : kqpLocks.GetSendingShards()) {
            if (shardId == tabletId) {
                continue;
            }

            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, "Prepare InReadsets from " << shardId << " to " << tabletId);

            auto key = std::make_pair(shardId, tabletId);
            inReadSets.emplace(key, TVector<TRSData>());
        }
    }
}

void KqpUpdateDataShardStatCounters(TDataShard& dataShard, const NMiniKQL::TEngineHostCounters& counters) {
    dataShard.IncCounter(COUNTER_ENGINE_HOST_SELECT_ROW, counters.NSelectRow);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_SELECT_RANGE, counters.NSelectRange);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_UPDATE_ROW, counters.NUpdateRow);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_ERASE_ROW, counters.NEraseRow);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_SELECT_ROW_BYTES, counters.SelectRowBytes);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_SELECT_RANGE_ROWS, counters.SelectRangeRows);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_SELECT_RANGE_BYTES, counters.SelectRangeBytes);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_SELECT_RANGE_ROW_SKIPS, counters.SelectRangeDeletedRowSkips);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_UPDATE_ROW_BYTES, counters.UpdateRowBytes);
    dataShard.IncCounter(COUNTER_ENGINE_HOST_ERASE_ROW_BYTES, counters.EraseRowBytes);
    if (counters.NSelectRow > 0) {
        dataShard.IncCounter(COUNTER_SELECT_ROWS_PER_REQUEST, counters.NSelectRow);
    }
    if (counters.NSelectRange > 0) {
        dataShard.IncCounter(COUNTER_RANGE_READ_ROWS_PER_REQUEST, counters.SelectRangeRows);
    }
}

void KqpFillTxStats(TDataShard& dataShard, const NMiniKQL::TEngineHostCounters& counters, NKikimrQueryStats::TTxStats& stats)
{
    auto& perTable = *stats.AddTableAccessStats();
    perTable.MutableTableInfo()->SetSchemeshardId(dataShard.GetPathOwnerId());
    Y_ABORT_UNLESS(dataShard.GetUserTables().size() == 1, "TODO: Fix handling of collocated tables");
    auto tableInfo = dataShard.GetUserTables().begin();
    perTable.MutableTableInfo()->SetPathId(tableInfo->first);
    perTable.MutableTableInfo()->SetName(tableInfo->second->Path);
    if (counters.NSelectRow) {
        perTable.MutableSelectRow()->SetCount(counters.NSelectRow);
        perTable.MutableSelectRow()->SetRows(counters.SelectRowRows);
        perTable.MutableSelectRow()->SetBytes(counters.SelectRowBytes);
    }
    if (counters.NSelectRange) {
        perTable.MutableSelectRange()->SetCount(counters.NSelectRange);
        perTable.MutableSelectRange()->SetRows(counters.SelectRangeRows);
        perTable.MutableSelectRange()->SetBytes(counters.SelectRangeBytes);
    }
    if (counters.NUpdateRow) {
        perTable.MutableUpdateRow()->SetCount(counters.NUpdateRow);
        perTable.MutableUpdateRow()->SetRows(counters.NUpdateRow);
        perTable.MutableUpdateRow()->SetBytes(counters.UpdateRowBytes);
    }
    if (counters.NEraseRow) {
        perTable.MutableEraseRow()->SetCount(counters.NEraseRow);
        perTable.MutableEraseRow()->SetRows(counters.NEraseRow);
        perTable.MutableEraseRow()->SetBytes(counters.EraseRowBytes);
    }
}

void KqpFillStats(TDataShard& dataShard, const NKqp::TKqpTasksRunner& tasksRunner, NMiniKQL::TKqpDatashardComputeContext& computeCtx, const NYql::NDqProto::EDqStatsMode& statsMode, TEvDataShard::TEvProposeTransactionResult& result)
{
    Y_ABORT_UNLESS(dataShard.GetUserTables().size() == 1, "TODO: Fix handling of collocated tables");
    auto tableInfo = dataShard.GetUserTables().begin();

    auto& computeActorStats = *result.Record.MutableComputeActorStats();

    ui64 minFirstRowTimeMs = std::numeric_limits<ui64>::max();
    ui64 maxFinishTimeMs = 0;

    for (auto& [taskId, taskStats] : tasksRunner.GetTasksStats()) {
        // Always report statistics required for system views & request unit computation
        auto* protoTask = computeActorStats.AddTasks();

        auto taskTableStats = computeCtx.GetTaskCounters(taskId);

        auto* protoTable = protoTask->AddTables();
        protoTable->SetTablePath(tableInfo->second->Path);
        protoTable->SetReadRows(taskTableStats.SelectRowRows + taskTableStats.SelectRangeRows);
        protoTable->SetReadBytes(taskTableStats.SelectRowBytes + taskTableStats.SelectRangeBytes);
        protoTable->SetWriteRows(taskTableStats.NUpdateRow);
        protoTable->SetWriteBytes(taskTableStats.UpdateRowBytes);
        protoTable->SetEraseRows(taskTableStats.NEraseRow);

        if (statsMode <= NYql::NDqProto::DQ_STATS_MODE_NONE) {  // UNSPECIFIED === NONE
            continue;
        }

        auto stageId = tasksRunner.GetTask(taskId).GetStageId();
        NYql::NDq::FillTaskRunnerStats(taskId, stageId, *taskStats, protoTask, NYql::NDq::StatsModeToCollectStatsLevel(statsMode));

        // minFirstRowTimeMs = std::min(minFirstRowTimeMs, protoTask->GetFirstRowTimeMs());
        // maxFinishTimeMs = std::max(maxFinishTimeMs, protoTask->GetFinishTimeMs());

        computeActorStats.SetCpuTimeUs(computeActorStats.GetCpuTimeUs() + protoTask->GetCpuTimeUs());

        if (Y_UNLIKELY(statsMode >= NYql::NDqProto::DQ_STATS_MODE_FULL)) {
            NKqpProto::TKqpShardTableExtraStats tableExtraStats;
            tableExtraStats.SetShardId(dataShard.TabletID());
            protoTable->MutableExtra()->PackFrom(tableExtraStats);
        }
    }

    if (maxFinishTimeMs >= minFirstRowTimeMs) {
        computeActorStats.SetDurationUs((maxFinishTimeMs - minFirstRowTimeMs) * 1'000);
    }
}

NYql::NDq::TDqTaskRunnerMemoryLimits DefaultKqpDataReqMemoryLimits() {
    NYql::NDq::TDqTaskRunnerMemoryLimits memoryLimits;
    // Data queries require output channel to be drained only once, and it must contain complete result
    // (i.e. channel must be Finished).
    // So we have to set such a big buffer size.
    // @link https://a.yandex-team.ru/arc/trunk/arcadia/ydb/core/tx/datashard/datashard_kqp.cpp?rev=6199480#L196-197
    memoryLimits.ChannelBufferSize = std::numeric_limits<ui32>::max();
    memoryLimits.OutputChunkMaxSize = std::numeric_limits<ui32>::max();

    return memoryLimits;
}

namespace {

class TKqpTaskRunnerExecutionContext: public NDq::IDqTaskRunnerExecutionContext {
public:
    NDq::IDqOutputConsumer::TPtr CreateOutputConsumer(const NDqProto::TTaskOutput& outputDesc, const NMiniKQL::TType* type, NUdf::IApplyContext* applyCtx, const NMiniKQL::TTypeEnvironment& typeEnv, const NKikimr::NMiniKQL::THolderFactory& holderFactory, TVector<NDq::IDqOutput::TPtr>&& outputs) const override {
        return NKqp::KqpBuildOutputConsumer(outputDesc, type, applyCtx, typeEnv, holderFactory, std::move(outputs));
    }

    NDq::IDqChannelStorage::TPtr CreateChannelStorage(ui64 /* channelId */, bool /* withSpilling */) const override {
        return {};
    }

    NDq::IDqChannelStorage::TPtr CreateChannelStorage(ui64 /* channelId */, bool /* withSpilling */, TActorSystem* /* actorSystem */) const override {
        return {};
    }

    NDq::TWakeUpCallback GetWakeupCallback() const override {
        return {};
    }

    NDq::TErrorCallback GetErrorCallback() const override {
        return {};
    }

    TIntrusivePtr<NDq::TSpillingTaskCounters> GetSpillingTaskCounters() const override {
        return {};
    }

    NDq::TTxId GetTxId() const override {
        return {};
    }
};

}  // anonymous namespace

THolder<NYql::NDq::IDqTaskRunnerExecutionContext> DefaultKqpExecutionContext() {
    return THolder(new TKqpTaskRunnerExecutionContext);
}

}  // namespace NDataShard
}  // namespace NKikimr
