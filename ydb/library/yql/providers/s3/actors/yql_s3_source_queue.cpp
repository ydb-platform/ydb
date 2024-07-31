#include <util/system/platform.h>
#if defined(_linux_) || defined(_darwin_)
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeArray.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeDate.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeDateTime64.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypesDecimal.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeEnum.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeFactory.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeInterval.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeNothing.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeNullable.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeString.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeTuple.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypeUUID.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/DataTypes/DataTypesNumber.h>

#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBuffer.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBufferFromFile.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Core/Block.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Core/ColumnsWithTypeAndName.h>

#include <ydb/library/yql/udfs/common/clickhouse/client/src/Formats/FormatFactory.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Processors/Formats/InputStreamFromInputFormat.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Processors/Formats/Impl/ArrowBufferedStreams.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/compute/cast.h>
#include <arrow/status.h>
#include <arrow/util/future.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>

#include <library/cpp/protobuf/util/pb_io.h>
#include <google/protobuf/text_format.h>

#endif

#include "yql_arrow_column_converters.h"
#include "yql_s3_actors_util.h"
#include "yql_s3_read_actor.h"
#include "yql_s3_source_queue.h"

#include <ydb/core/base/events.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/dq/actors/compute/retry_queue.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_terminator.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/udf/arrow/block_builder.h>
#include <ydb/library/yql/public/udf/arrow/block_reader.h>
#include <ydb/library/yql/public/udf/arrow/util.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/arrow.h>

#include <ydb/library/yql/providers/s3/common/util.h>
#include <ydb/library/yql/providers/s3/common/source_context.h>
#include <ydb/library/yql/providers/s3/compressors/factory.h>
#include <ydb/library/yql/providers/s3/credentials/credentials.h>
#include <ydb/library/yql/providers/s3/events/events.h>
#include <ydb/library/yql/providers/s3/object_listers/yql_s3_list.h>
#include <ydb/library/yql/providers/s3/proto/range.pb.h>
#include <ydb/library/yql/providers/s3/proto/file_queue.pb.h>
#include <ydb/library/yql/providers/s3/range_helpers/path_list_reader.h>
#include <ydb/library/yql/providers/s3/serializations/serialization_interval.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor_coroutine.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/util/datetime.h>

#include <util/generic/size_literals.h>
#include <util/stream/format.h>
#include <util/system/fstat.h>

#include <algorithm>
#include <queue>

#ifdef THROW
#undef THROW
#endif
#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/xml/document/xml-document.h>

#define LOG_E(name, stream) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_W(name, stream) \
    LOG_WARN_S (*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_I(name, stream) \
    LOG_INFO_S (*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_D(name, stream) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_T(name, stream) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)

#define LOG_CORO_E(stream) \
    LOG_ERROR_S(GetActorContext(), NKikimrServices::KQP_COMPUTE, "TS3ReadCoroImpl: " << SelfActorId << ", CA: " << ComputeActorId << ", TxId: " << TxId \
    << " [" << Path << "]. RETRY{ Offset: " << RetryStuff->Offset << ", Delay: " << RetryStuff->NextRetryDelay << ", RequestId: " << RetryStuff->RequestId << "}. " << stream)
#define LOG_CORO_W(stream) \
    LOG_WARN_S (GetActorContext(), NKikimrServices::KQP_COMPUTE, "TS3ReadCoroImpl: " << SelfActorId << ", CA: " << ComputeActorId << ", TxId: " << TxId \
    << " [" << Path << "]. RETRY{ Offset: " << RetryStuff->Offset << ", Delay: " << RetryStuff->NextRetryDelay << ", RequestId: " << RetryStuff->RequestId << "}. " << stream)
#define LOG_CORO_I(stream) \
    LOG_INFO_S (GetActorContext(), NKikimrServices::KQP_COMPUTE, "TS3ReadCoroImpl: " << SelfActorId << ", CA: " << ComputeActorId << ", TxId: " << TxId \
    << " [" << Path << "]. RETRY{ Offset: " << RetryStuff->Offset << ", Delay: " << RetryStuff->NextRetryDelay << ", RequestId: " << RetryStuff->RequestId << "}. " << stream)
#define LOG_CORO_D(stream) \
    LOG_DEBUG_S(GetActorContext(), NKikimrServices::KQP_COMPUTE, "TS3ReadCoroImpl: " << SelfActorId << ", CA: " << ComputeActorId << ", TxId: " << TxId \
    << " [" << Path << "]. RETRY{ Offset: " << RetryStuff->Offset << ", Delay: " << RetryStuff->NextRetryDelay << ", RequestId: " << RetryStuff->RequestId << "}. " << stream)
#define LOG_CORO_T(stream) \
    LOG_TRACE_S(GetActorContext(), NKikimrServices::KQP_COMPUTE, "TS3ReadCoroImpl: " << SelfActorId << ", CA: " << ComputeActorId << ", TxId: " << TxId \
    << " [" << Path << "]. RETRY{ Offset: " << RetryStuff->Offset << ", Delay: " << RetryStuff->NextRetryDelay << ", RequestId: " << RetryStuff->RequestId << "}. " << stream)

namespace NYql::NDq {

class TS3FileQueueActor : public NActors::TActorBootstrapped<TS3FileQueueActor> {
public:
    static constexpr char ActorName[] = "YQ_S3_FILE_QUEUE_ACTOR";

    struct TEvPrivatePrivate {
        enum {
            EvBegin = TEvRetryQueuePrivate::EvEnd,  // Leave space for RetryQueue events

            EvNextListingChunkReceived = EvBegin,
            EvRoundRobinStageTimeout,
            EvTransitToErrorState,

            EvEnd
        };
        static_assert(
            EvEnd <= EventSpaceEnd(NActors::TEvents::ES_PRIVATE),
            "expected EvEnd <= EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvNextListingChunkReceived : public NActors::TEventLocal<TEvNextListingChunkReceived, EvNextListingChunkReceived> {
            NS3Lister::TListResult ListingResult;
            TEvNextListingChunkReceived(NS3Lister::TListResult listingResult)
                : ListingResult(std::move(listingResult)){};
        };

        struct TEvRoundRobinStageTimeout : public NActors::TEventLocal<TEvRoundRobinStageTimeout, EvRoundRobinStageTimeout> {
        };

        struct TEvTransitToErrorState : public NActors::TEventLocal<TEvTransitToErrorState, EvTransitToErrorState> {
            explicit TEvTransitToErrorState(TIssues&& issues)
                : Issues(issues) {
            }
            TIssues Issues;
        };
    };
    using TBase = TActorBootstrapped<TS3FileQueueActor>;

    TS3FileQueueActor(
        TTxId  txId,
        NS3Details::TPathList paths,
        size_t prefetchSize,
        ui64 fileSizeLimit,
        ui64 readLimit,
        bool useRuntimeListing,
        ui64 consumersCount,
        ui64 batchSizeLimit,
        ui64 batchObjectCountLimit,
        IHTTPGateway::TPtr gateway,
        IHTTPGateway::TRetryPolicy::TPtr retryPolicy,
        TString url,
        const TS3Credentials& credentials,
        TString pattern,
        NS3Lister::ES3PatternVariant patternVariant,
        NS3Lister::ES3PatternType patternType)
        : TxId(std::move(txId))
        , PrefetchSize(prefetchSize)
        , FileSizeLimit(fileSizeLimit)
        , ReadLimit(readLimit)
        , MaybeIssues(Nothing())
        , UseRuntimeListing(useRuntimeListing)
        , ConsumersCount(consumersCount)
        , BatchSizeLimit(batchSizeLimit)
        , BatchObjectCountLimit(batchObjectCountLimit)
        , Gateway(std::move(gateway))
        , RetryPolicy(std::move(retryPolicy))
        , Url(std::move(url))
        , Credentials(credentials)
        , Pattern(std::move(pattern))
        , PatternVariant(patternVariant)
        , PatternType(patternType) {
        for (size_t i = 0; i < paths.size(); ++i) {
            NS3::FileQueue::TObjectPath object;
            object.SetPath(paths[i].Path);
            object.SetPathIndex(paths[i].PathIndex);
            if (paths[i].IsDirectory) {
                object.SetSize(0);
                Directories.emplace_back(std::move(object));
            } else {
                object.SetSize(paths[i].Size);
                Objects.emplace_back(std::move(object));
                ObjectsTotalSize += paths[i].Size;
            }
        }
    }

    void Bootstrap() {
        if (UseRuntimeListing) {
            Schedule(PoisonTimeout, new NActors::TEvents::TEvPoison());
        }
        if (Directories.empty()) {
            LOG_I("TS3FileQueueActor", "Bootstrap there is no directories to list, consumersCount=" << ConsumersCount);
            Become(&TS3FileQueueActor::NoMoreDirectoriesState);
        } else {
            LOG_I("TS3FileQueueActor", "Bootstrap there are directories to list, consumersCount=" << ConsumersCount);
            TryPreFetch();
            Become(&TS3FileQueueActor::ThereAreDirectoriesToListState);
        }
    }

    STATEFN(ThereAreDirectoriesToListState) {
        try {
            switch (const auto etype = ev->GetTypeRewrite()) {
                hFunc(TEvS3Provider::TEvUpdateConsumersCount, HandleUpdateConsumersCount);
                hFunc(TEvS3Provider::TEvGetNextBatch, HandleGetNextBatch);
                hFunc(TEvPrivatePrivate::TEvNextListingChunkReceived, HandleNextListingChunkReceived);
                cFunc(TEvPrivatePrivate::EvRoundRobinStageTimeout, HandleRoundRobinStageTimeout);
                hFunc(TEvPrivatePrivate::TEvTransitToErrorState, HandleTransitToErrorState);
                cFunc(NActors::TEvents::TSystem::Poison, HandlePoison);
                default:
                    MaybeIssues = TIssues{TIssue{TStringBuilder() << "An event with unknown type has been received: '" << etype << "'"}};
                    TransitToErrorState();
                    break;
            }
        } catch (const std::exception& e) {
            MaybeIssues = TIssues{TIssue{TStringBuilder() << "An unknown exception has occurred: '" << e.what() << "'"}};
            TransitToErrorState();
        }
    }

    void HandleGetNextBatch(TEvS3Provider::TEvGetNextBatch::TPtr& ev) {
        if (HasEnoughToSend()) {
            LOG_D("TS3FileQueueActor", "HandleGetNextBatch sending right away");
            TrySendObjects(ev->Sender, ev->Get()->Record.GetTransportMeta());
            TryPreFetch();
        } else {
            LOG_D("TS3FileQueueActor", "HandleGetNextBatch have not enough objects cached. Start fetching");
            ScheduleRequest(ev->Sender, ev->Get()->Record.GetTransportMeta());
            TryFetch();
        }
    }

    void HandleNextListingChunkReceived(TEvPrivatePrivate::TEvNextListingChunkReceived::TPtr& ev) {
        Y_ENSURE(FetchingInProgress());
        ListingFuture = Nothing();
        LOG_D("TS3FileQueueActor", "HandleNextListingChunkReceived");
        if (SaveRetrievedResults(ev->Get()->ListingResult)) {
            AnswerPendingRequests(true);
            if (!HasPendingRequests) {
                LOG_D("TS3FileQueueActor", "HandleNextListingChunkReceived no pending requests. Trying to prefetch");
                TryPreFetch();
            } else {
                LOG_D("TS3FileQueueActor", "HandleNextListingChunkReceived there are pending requests. Fetching more objects");
                TryFetch();
            }
        } else {
            TransitToErrorState();
        }
    }

    void HandleTransitToErrorState(TEvPrivatePrivate::TEvTransitToErrorState::TPtr& ev) {
        MaybeIssues = ev->Get()->Issues;
        TransitToErrorState();
    }

    bool SaveRetrievedResults(const NS3Lister::TListResult& listingResult) {
        LOG_T("TS3FileQueueActor", "SaveRetrievedResults");
        if (std::holds_alternative<NS3Lister::TListError>(listingResult)) {
            MaybeIssues = std::get<NS3Lister::TListError>(listingResult).Issues;
            return false;
        }

        auto listingChunk = std::get<NS3Lister::TListEntries>(listingResult);
        LOG_D("TS3FileQueueActor", "SaveRetrievedResults saving: " << listingChunk.Objects.size() << " entries");
        Y_ENSURE(listingChunk.Directories.empty());
        for (auto& object: listingChunk.Objects) {
            if (object.Path.EndsWith('/')) {
                // skip 'directories'
                continue;
            }
            const ui64 bytesUsed = std::min(object.Size, ReadLimit);
            if (bytesUsed > FileSizeLimit) {
                auto errorMessage = TStringBuilder()
                                    << "Size of object " << object.Path << " = "
                                    << object.Size
                                    << " and exceeds limit = " << FileSizeLimit;
                LOG_E("TS3FileQueueActor", errorMessage);
                MaybeIssues = TIssues{TIssue{errorMessage}};
                return false;
            }
            LOG_T("TS3FileQueueActor", "SaveRetrievedResults adding path: " << object.Path);
            NS3::FileQueue::TObjectPath objectPath;
            objectPath.SetPath(object.Path);
            objectPath.SetSize(bytesUsed);
            objectPath.SetPathIndex(CurrentDirectoryPathIndex);
            Objects.emplace_back(std::move(objectPath));
            ObjectsTotalSize += bytesUsed;
        }
        return true;
    }

    bool FetchingInProgress() const { return ListingFuture.Defined(); }

    void TransitToNoMoreDirectoriesToListState() {
        LOG_I("TS3FileQueueActor", "TransitToNoMoreDirectoriesToListState no more directories to list");
        AnswerPendingRequests();
        Become(&TS3FileQueueActor::NoMoreDirectoriesState);
    }

    void TransitToErrorState() {
        Y_ENSURE(MaybeIssues.Defined());
        LOG_I("TS3FileQueueActor", "TransitToErrorState an error occurred sending ");
        AnswerPendingRequests();
        Objects.clear();
        Directories.clear();
        Become(&TS3FileQueueActor::AnErrorOccurredState);
    }

    STATEFN(NoMoreDirectoriesState) {
        try {
            switch (const auto etype = ev->GetTypeRewrite()) {
                hFunc(TEvS3Provider::TEvUpdateConsumersCount, HandleUpdateConsumersCount);
                hFunc(TEvS3Provider::TEvGetNextBatch, HandleGetNextBatchForEmptyState);
                cFunc(TEvPrivatePrivate::EvRoundRobinStageTimeout, HandleRoundRobinStageTimeout);
                cFunc(NActors::TEvents::TSystem::Poison, HandlePoison);
                default:
                    MaybeIssues = TIssues{TIssue{TStringBuilder() << "An event with unknown type has been received: '" << etype << "'"}};
                    TransitToErrorState();
                    break;
            }
        } catch (const std::exception& e) {
            MaybeIssues = TIssues{TIssue{TStringBuilder() << "An unknown exception has occurred: '" << e.what() << "'"}};
            TransitToErrorState();
        }
    }

    void HandleGetNextBatchForEmptyState(TEvS3Provider::TEvGetNextBatch::TPtr& ev) {
        LOG_T(
            "TS3FileQueueActor",
            "HandleGetNextBatchForEmptyState Giving away rest of Objects");
        TrySendObjects(ev->Sender, ev->Get()->Record.GetTransportMeta());
    }

    STATEFN(AnErrorOccurredState) {
        try {
            switch (const auto etype = ev->GetTypeRewrite()) {
                hFunc(TEvS3Provider::TEvUpdateConsumersCount, HandleUpdateConsumersCount);
                hFunc(TEvS3Provider::TEvGetNextBatch, HandleGetNextBatchForErrorState);
                cFunc(TEvPrivatePrivate::EvRoundRobinStageTimeout, HandleRoundRobinStageTimeout);
                cFunc(NActors::TEvents::TSystem::Poison, HandlePoison);
                default:
                    MaybeIssues = TIssues{TIssue{TStringBuilder() << "An event with unknown type has been received: '" << etype << "'"}};
                    break;
            }
        } catch (const std::exception& e) {
            MaybeIssues = TIssues{TIssue{TStringBuilder() << "An unknown exception has occurred: '" << e.what() << "'"}};
        }
    }

    void HandleGetNextBatchForErrorState(TEvS3Provider::TEvGetNextBatch::TPtr& ev) {
        LOG_D(
            "TS3FileQueueActor",
            "HandleGetNextBatchForErrorState Giving away rest of Objects");
        Send(ev->Sender, new TEvS3Provider::TEvObjectPathReadError(*MaybeIssues, ev->Get()->Record.GetTransportMeta()));
        TryFinish(ev->Sender, ev->Get()->Record.GetTransportMeta().GetSeqNo());
    }

    void HandleUpdateConsumersCount(TEvS3Provider::TEvUpdateConsumersCount::TPtr& ev) {
        if (!UpdatedConsumers.contains(ev->Sender)) {
            LOG_D(
                "TS3FileQueueActor",
                "HandleUpdateConsumersCount Reducing ConsumersCount by " << ev->Get()->Record.GetConsumersCountDelta() << ", recieved from " << ev->Sender);
            UpdatedConsumers.insert(ev->Sender);
            ConsumersCount -= ev->Get()->Record.GetConsumersCountDelta();
        }
        Send(ev->Sender, new TEvS3Provider::TEvAck(ev->Get()->Record.GetTransportMeta()));
    }

    void HandleRoundRobinStageTimeout() {
        LOG_T("TS3FileQueueActor","Handle start stage timeout");
        if (!RoundRobinStageFinished) {
            RoundRobinStageFinished = true;
            AnswerPendingRequests();
        }
    }

    void HandlePoison() {
        AnswerPendingRequests();
        PassAway();
    }

    void PassAway() override {
        LOG_D("TS3FileQueueActor", "PassAway");
        TBase::PassAway();
    }

private:
    void TrySendObjects(const NActors::TActorId& consumer, const NDqProto::TMessageTransportMeta& transportMeta) {
        if (CanSendToConsumer(consumer)) {
            SendObjects(consumer, transportMeta);
        } else {
            ScheduleRequest(consumer, transportMeta);
        }
    }

    void SendObjects(const NActors::TActorId& consumer, const NDqProto::TMessageTransportMeta& transportMeta) {
        Y_ENSURE(!MaybeIssues.Defined());
        std::vector<NS3::FileQueue::TObjectPath> result;
        if (Objects.size() > 0) {
            size_t totalSize = 0;
            do {
                result.push_back(Objects.back());
                Objects.pop_back();
                totalSize += result.back().GetSize();
            } while (Objects.size() > 0 && result.size() < BatchObjectCountLimit && totalSize < BatchSizeLimit);
            ObjectsTotalSize -= totalSize;
        }

        LOG_T("TS3FileQueueActor", "SendObjects Sending " << result.size() << " objects to consumer with id " << consumer);
        Send(consumer, new TEvS3Provider::TEvObjectPathBatch(std::move(result), HasNoMoreItems(), transportMeta));

        if (HasNoMoreItems()) {
            TryFinish(consumer, transportMeta.GetSeqNo());
        }

        if (!RoundRobinStageFinished) {
            if (StartedConsumers.empty()) {
                Schedule(RoundRobinStageTimeout, new TEvPrivatePrivate::TEvRoundRobinStageTimeout());
            }
            StartedConsumers.insert(consumer);
            if ((StartedConsumers.size() == ConsumersCount || HasNoMoreItems()) && !IsRoundRobinFinishScheduled) {
                IsRoundRobinFinishScheduled = true;
                Send(SelfId(), new TEvPrivatePrivate::TEvRoundRobinStageTimeout());
            }
        }
    }

    bool HasEnoughToSend() {
        return Objects.size() >= BatchObjectCountLimit || ObjectsTotalSize >= BatchSizeLimit;
    }

    bool CanSendToConsumer(const NActors::TActorId& consumer) {
        return !UseRuntimeListing || RoundRobinStageFinished ||
               (StartedConsumers.size() < ConsumersCount && !StartedConsumers.contains(consumer));
    }

    bool HasNoMoreItems() const {
        return !(MaybeLister.Defined() && (*MaybeLister)->HasNext()) &&
               Directories.empty() && Objects.empty();
    }

    bool TryPreFetch() {
        if (Objects.size() < PrefetchSize) {
            return TryFetch();
        }
        return false;
    }

    bool TryFetch() {
        if (FetchingInProgress()) {
            LOG_D("TS3FileQueueActor", "TryFetch fetching already in progress");
            return true;
        }

        if (MaybeLister.Defined() && (*MaybeLister)->HasNext()) {
            LOG_D("TS3FileQueueActor", "TryFetch fetching from current lister");
            Fetch();
            return true;
        }

        if (!Directories.empty()) {
            LOG_D("TS3FileQueueActor", "TryFetch fetching from new lister");

            auto object = Directories.back();
            Directories.pop_back();
            CurrentDirectoryPathIndex = object.GetPathIndex();
            MaybeLister = NS3Lister::MakeS3Lister(
                Gateway,
                RetryPolicy,
                NS3Lister::TListingRequest{
                    Url,
                    Credentials,
                    PatternVariant == NS3Lister::ES3PatternVariant::PathPattern
                        ? Pattern
                        : TStringBuilder{} << object.GetPath() << Pattern,
                    PatternType,
                    object.GetPath()},
                Nothing(),
                false);
            Fetch();
            return true;
        }

        LOG_D("TS3FileQueueActor", "TryFetch couldn't start fetching");
        MaybeLister = Nothing();
        TransitToNoMoreDirectoriesToListState();
        return false;
    }

    void Fetch() {
        Y_ENSURE(!ListingFuture.Defined());
        Y_ENSURE(MaybeLister.Defined());
        NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
        ListingFuture =
            (*MaybeLister)
                ->Next()
                .Subscribe([actorSystem, selfId = SelfId()](
                               const NThreading::TFuture<NS3Lister::TListResult>& future) {
                    try {
                        actorSystem->Send(
                            selfId,
                            new TEvPrivatePrivate::TEvNextListingChunkReceived(
                                future.GetValue()));
                    } catch (const std::exception& e) {
                        actorSystem->Send(
                            selfId,
                            new TEvPrivatePrivate::TEvTransitToErrorState(
                                TIssues{TIssue{TStringBuilder() << "An unknown exception has occurred: '" << e.what() << "'"}}));
                    }
                });
    }

    void ScheduleRequest(const NActors::TActorId& consumer, const NDqProto::TMessageTransportMeta& transportMeta) {
        PendingRequests[consumer].push_back(transportMeta);
        HasPendingRequests = true;
    }

    void AnswerPendingRequests(bool earlyStop = false) {
        bool handledRequest = true;
        while (HasPendingRequests && handledRequest) {
            bool isEmpty = true;
            handledRequest = false;
            for (auto& [consumer, requests] : PendingRequests) {
                if (!CanSendToConsumer(consumer) || (earlyStop && !HasEnoughToSend())) {
                    if (!requests.empty()) {
                        isEmpty = false;
                    }
                    continue;
                }
                if (!requests.empty()) {
                    if (!MaybeIssues.Defined()) {
                        SendObjects(consumer, requests.front());
                    } else {
                        Send(consumer, new TEvS3Provider::TEvObjectPathReadError(*MaybeIssues, requests.front()));
                        TryFinish(consumer, requests.front().GetSeqNo());
                    }
                    requests.pop_front();
                    handledRequest = true;
                }
                if (!requests.empty()) {
                    isEmpty = false;
                }
            }
            if (isEmpty) {
                HasPendingRequests = false;
            }
        }
    }

    void TryFinish(const NActors::TActorId& consumer, ui64 seqNo) {
        LOG_T("TS3FileQueueActor", "TryFinish from consumer " << consumer << ", " << FinishedConsumers.size() << " consumers already finished, seqNo=" << seqNo);
        if (FinishingConsumerToLastSeqNo.contains(consumer)) {
            LOG_T("TS3FileQueueActor", "TryFinish FinishingConsumerToLastSeqNo=" << FinishingConsumerToLastSeqNo[consumer]);
            if (FinishingConsumerToLastSeqNo[consumer] < seqNo || SelfId().NodeId() == consumer.NodeId()) {
                FinishedConsumers.insert(consumer);
                if (FinishedConsumers.size() == ConsumersCount) {
                    PassAway();
                }
            }
        } else {
            FinishingConsumerToLastSeqNo[consumer] = seqNo;
        }
    }

private:
    const TTxId TxId;

    std::vector<NS3::FileQueue::TObjectPath> Objects;
    std::vector<NS3::FileQueue::TObjectPath> Directories;

    size_t PrefetchSize;
    ui64 FileSizeLimit;
    ui64 ReadLimit;
    TMaybe<NS3Lister::IS3Lister::TPtr> MaybeLister = Nothing();
    TMaybe<NThreading::TFuture<NS3Lister::TListResult>> ListingFuture;
    size_t CurrentDirectoryPathIndex = 0;
    THashMap<NActors::TActorId, TDeque<NDqProto::TMessageTransportMeta>> PendingRequests;
    TMaybe<TIssues> MaybeIssues;
    bool UseRuntimeListing;
    ui64 ConsumersCount;
    ui64 BatchSizeLimit;
    ui64 BatchObjectCountLimit;
    ui64 ObjectsTotalSize = 0;
    THashMap<NActors::TActorId, ui64> FinishingConsumerToLastSeqNo;
    THashSet<NActors::TActorId> FinishedConsumers;
    bool RoundRobinStageFinished = false;
    bool IsRoundRobinFinishScheduled = false;
    bool HasPendingRequests = false;
    THashSet<NActors::TActorId> StartedConsumers;
    THashSet<NActors::TActorId> UpdatedConsumers;

    const IHTTPGateway::TPtr Gateway;
    const IHTTPGateway::TRetryPolicy::TPtr RetryPolicy;
    const TString Url;
    const TS3Credentials Credentials;
    const TString Pattern;
    const NS3Lister::ES3PatternVariant PatternVariant;
    const NS3Lister::ES3PatternType PatternType;

    static constexpr TDuration PoisonTimeout = TDuration::Hours(3);
    static constexpr TDuration RoundRobinStageTimeout = TDuration::Seconds(3);
};

NActors::IActor* CreateS3FileQueueActor(
        TTxId  txId,
        NS3Details::TPathList paths,
        size_t prefetchSize,
        ui64 fileSizeLimit,
        ui64 readLimit,
        bool useRuntimeListing,
        ui64 consumersCount,
        ui64 batchSizeLimit,
        ui64 batchObjectCountLimit,
        IHTTPGateway::TPtr gateway,
        IHTTPGateway::TRetryPolicy::TPtr retryPolicy,
        TString url,
        const TS3Credentials& credentials,
        TString pattern,
        NS3Lister::ES3PatternVariant patternVariant,
        NS3Lister::ES3PatternType patternType) {
    return new TS3FileQueueActor(
        txId,
        paths,
        prefetchSize,
        fileSizeLimit,
        readLimit,
        useRuntimeListing,
        consumersCount,
        batchSizeLimit,
        batchObjectCountLimit,
        gateway,
        retryPolicy,
        url,
        credentials,
        pattern,
        patternVariant,
        patternType
    );
}

} // namespace NYql::NDq
