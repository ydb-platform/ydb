#pragma once

#include "global.h"

#include <ydb/core/base/row_version.h>
#include <ydb/core/persqueue/pqtablet/blob/blob.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/persqueue/common/blob_refcounter.h>
#include <ydb/core/persqueue/common/key.h>
#include <ydb/core/persqueue/common/metering.h>
#include <ydb/core/persqueue/common/sourceid_info.h>
#include <ydb/core/persqueue/public/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/public/counters/percentile_counter.h>
#include <ydb/core/persqueue/public/write_id.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>
#include <util/generic/maybe.h>
#include <expected>

namespace NYdb::inline Dev {
    class ICredentialsProviderFactory;
}

namespace NKikimr {

namespace NPQ {

    struct TCacheClientContext {
        TActorId ProxyId;
        ui64 NextReadId = 1;
    };

    struct TCacheServiceData {
        //ui32 TabletId;
        ui32 Generation = 0;
        TMap<ui64, std::shared_ptr<NKikimrClient::TResponse>> StagedReads;
        TMap<ui64, std::shared_ptr<NKikimrClient::TResponse>> Reads;
        TMaybe<TCacheClientContext> Client;
        TCacheServiceData() = delete;

        TCacheServiceData(ui32 generation)
            : Generation(generation)
        {}
    };

    struct TRequestedBlob {
        mutable TKey Key;
        mutable TString RawValue;
        ui64 Offset;
        ui16 PartNo;
        ui32 Count;
        ui16 InternalPartsCount;
        ui32 Size;
        bool Cached;
        ui64 CreationUnixTime;
        mutable std::shared_ptr<TVector<TBatch>> Batches;

        TRequestedBlob() = delete;
        TRequestedBlob(ui64 offset, ui16 partNo, ui32 count, ui16 internalPartsCount, ui32 size, TString value, const TKey& key, ui64 creationUnixTime);

        bool Empty() const;
        void Clear();
        std::shared_ptr<TVector<TBatch>> GetBatches() const;
    };

    struct TDataKey {
        TKey Key;
        ui32 Size = 0;
        TInstant Timestamp;
        ui64 CumulativeSize = 0;
        TBlobKeyTokenPtr BlobKeyToken = nullptr;
    };

    struct TErrorInfo {
        NPersQueue::NErrorCode::EErrorCode ErrorCode;
        TString ErrorStr;

        TErrorInfo()
        : ErrorCode(NPersQueue::NErrorCode::OK)
        {}

        TErrorInfo(NPersQueue::NErrorCode::EErrorCode err, const TString& str)
        : ErrorCode(err)
        , ErrorStr(str)
        {}

        bool HasError() const {
            return ErrorCode != NPersQueue::NErrorCode::OK;
        }
    };

    template <typename T>
    inline bool HasError(const T& event) {
        return event.Error.HasError();
    }
} // namespace NPQ;

struct TEvPQ {
    enum EEv {
        EvWrite = TEvPersQueue::EvInternalEvents,
        EvRead,
        EvDie,
        EvMonRequest,
        EvMonResponse,
        EvReadTimeout,
        EvGetMaxSeqNoRequest,
        EvGetClientOffset,
        EvSetClientInfo,
        EvPartitionOffsets,
        EvPartitionOffsetsResponse,
        EvPartitionStatus,
        EvPartitionStatusResponse,
        EvProxyResponse,
        EvError,
        EvBlobRequest,
        EvBlobResponse,
        EvInitComplete,
        EvChangeOwner,
        EvChangePartitionConfig,
        EvChangeCacheConfig,
        EvPartitionCounters,
        EvTabletCacheCounters,
        EvPartitionLabeledCounters,
        EvGetPartitionClientInfo,
        EvUpdateAvailableSize,
        EvPipeDisconnected,
        EvReserveBytes,
        EvPartitionLabeledCountersDrop,
        EvUpdateWriteTimestamp,
        EvHandleWriteResponse,
        EvQuotaDeadlineCheck,
        EvRegisterMessageGroup,
        EvDeregisterMessageGroup,
        EvSplitMessageGroup,
        EvUpdateCounters,
        EvMirrorerCounters,
        EvAccountQuotaRequest,
        EvAccountQuotaResponse,
        EvAccountQuotaConsumed,
        EvAccountQuotaCounters,
        EvRetryWrite,
        EvInitCredentials,
        EvCredentialsCreated,
        EvCreateConsumer,
        EvRequestPartitionStatus,
        EvReaderEventArrived,
        EvMetering,
        EvTxCalcPredicate,
        EvTxCalcPredicateResult,
        EvProposePartitionConfig,
        EvProposePartitionConfigResult,
        EvTxCommit,
        EvTxCommitDone,
        EvTxRollback,
        EvPartitionConfigChanged,
        EvSubDomainStatus,
        EvStatsWakeup,
        EvRequestQuota,
        EvApproveReadQuota,
        EvApproveWriteQuota,
        EvConsumed,
        EvQuotaUpdated,
        EvAccountQuotaCountersUpdated,
        EvQuotaCountersUpdated,
        EvConsumerRemoved,
        EvFetchResponse,
        EvPublishRead,
        EvForgetRead,
        EvRegisterDirectReadSession,
        EvRegisterDirectReadSessionResponse,
        EvDeregisterDirectReadSession,
        EvStageDirectReadData,
        EvCacheProxyPublishRead,
        EvCacheProxyForgetRead,
        EvGetFullDirectReadData,
        EvProvideDirectReadInfo,
        EvCheckPartitionStatusRequest,
        EvCheckPartitionStatusResponse,
        EvGetWriteInfoRequest,
        EvGetWriteInfoResponse,
        EvGetWriteInfoError,
        EvTxBatchComplete,
        EvReadingPartitionStatusRequest,
        EvProcessChangeOwnerRequests,
        EvWakeupReleasePartition,
        EvPartitionScaleStatusChanged,
        EvPartitionScaleRequestDone,
        EvBalanceConsumer,
        EvDeletePartition,
        EvDeletePartitionDone,
        EvTransactionCompleted,
        EvListAllTopicsResponse,
        EvAcquireExclusiveLock,
        EvExclusiveLockAcquired,
        EvReleaseExclusiveLock,
        EvRunCompaction,
        EvMirrorTopicDescription,
        EvBroadcastPartitionError,
        EvForceCompaction,
        EvMLPGetPartitionRequest,
        EvMLPGetPartitionResponse,
        EvMLPErrorResponse,
        EvMLPReadRequest,
        EvMLPReadResponse,
        EvMLPCommitRequest,
        EvMLPCommitResponse,
        EvMLPUnlockRequest,
        EvMLPUnlockResponse,
        EvMLPChangeMessageDeadlineRequest,
        EvMLPChangeMessageDeadlineResponse,
        EvGetMLPConsumerStateRequest,
        EvGetMLPConsumerStateResponse,
        EvMLPConsumerUpdateConfig,
        EvMLPDLQMoverResponse,
        EvEndOffsetChanged,
        EvMLPConsumerState,
        EvTxDone,
        EvMLPConsumerMonRequest,
        EvMLPPurgeRequest,
        EvMLPPurgeResponse,
        EvMLPConsumerStatus,
        EvEnd
    };

    struct TEvHandleWriteResponse : TEventLocal<TEvHandleWriteResponse, EvHandleWriteResponse> {
        explicit TEvHandleWriteResponse(ui64 cookie) :
            Cookie(cookie)
        {
        }

        ui64 Cookie = 0;
    };

    struct TEvWrite : public TEventLocal<TEvWrite, EvWrite> {
        struct TMsg {
            TString SourceId;
            ui64 SeqNo;
            ui16 PartNo;
            ui16 TotalParts;
            ui32 TotalSize;
            ui64 CreateTimestamp;
            ui64 ReceiveTimestamp;
            bool DisableDeduplication;
            ui64 WriteTimestamp;
            TString Data;
            ui32 UncompressedSize;
            TString PartitionKey;
            TString ExplicitHashKey;
            bool External;
            bool IgnoreQuotaDeadline;
            // If specified, Data will contain heartbeat's data
            std::optional<TRowVersion> HeartbeatVersion;

            // For Kafka deduplication:
            bool EnableKafkaDeduplication = false;
            TMaybe<i16> ProducerEpoch;

            std::optional<TString> MessageDeduplicationId;
        };

        TEvWrite(const ui64 cookie, const ui64 messageNo, const TString& ownerCookie, const TMaybe<ui64> offset, TVector<TMsg> &&msgs, bool isDirectWrite, std::optional<ui64> initialSeqNo)
        : Cookie(cookie)
        , MessageNo(messageNo)
        , OwnerCookie(ownerCookie)
        , Offset(offset)
        , Msgs(std::move(msgs))
        , IsDirectWrite(isDirectWrite)
        , InitialSeqNo(initialSeqNo)
        {
        }

        ui64 Cookie;
        ui64 MessageNo;
        TString OwnerCookie;
        TMaybe<ui64> Offset;
        TVector<TMsg> Msgs;
        bool IsDirectWrite;
        std::optional<ui64> InitialSeqNo;

    };

    struct TEvReadTimeout : public TEventLocal<TEvReadTimeout, EvReadTimeout> {
        explicit TEvReadTimeout(const ui64 cookie)
        : Cookie(cookie)
        {}

        ui64 Cookie;
    };

    struct TEvRead : public TEventLocal<TEvRead, EvRead> {
        TEvRead(const ui64 cookie, const ui64 offset, ui64 lastOffset, const ui16 partNo, const ui32 count,
                const TString& sessionId, const TString& clientId, const ui32 timeout, const ui32 size,
                const ui32 maxTimeLagMs, const ui64 readTimestampMs, const TString& clientDC,
                bool externalOperation, const TActorId& pipeClient, const TActorId& replyTo = {})
            : Cookie(cookie)
            , Offset(offset)
            , PartNo(partNo)
            , Count(count)
            , SessionId(sessionId)
            , ClientId(clientId)
            , Timeout(timeout)
            , Size(size)
            , MaxTimeLagMs(maxTimeLagMs)
            , ReadTimestampMs(readTimestampMs)
            , ClientDC(clientDC)
            , ExternalOperation(externalOperation)
            , PipeClient(pipeClient)
            , LastOffset(lastOffset)
            , ReplyTo(replyTo)
        {}

        ui64 Cookie;
        ui64 Offset;
        ui16 PartNo;
        ui32 Count;
        TString SessionId;
        TString ClientId;
        ui32 Timeout;
        ui32 Size;
        ui32 MaxTimeLagMs;
        ui64 ReadTimestampMs;
        TString ClientDC;
        bool ExternalOperation;
        TActorId PipeClient;
        ui64 LastOffset;
        TActorId ReplyTo;

        bool IsInternal() {
            return !!ReplyTo;
        }
    };

    struct TMessageGroup {
        ui64 SeqNo;
        NPQ::TPartitionKeyRange KeyRange;
    };

    using TMessageGroups = std::unordered_map<TString, TMessageGroup>;
    using TMessageGroupsPtr = std::shared_ptr<TMessageGroups>;

    struct TEvDirectReadBase {
        TEvDirectReadBase(ui64 cookie, const NPQ::TDirectReadKey& readKey, const TActorId& pipeClient)
            : Cookie(cookie)
            , ReadKey(readKey)
            , PipeClient(pipeClient)
        {}
        ui64 Cookie;
        NPQ::TDirectReadKey ReadKey;
        TActorId PipeClient;
    };

    struct TEvMonRequest : public TEventLocal<TEvMonRequest, EvMonRequest> {
        TEvMonRequest(const TActorId& sender, const TString& query)
        : Sender(sender)
        , Query(query)
        {}

        TActorId Sender;
        TString Query;
    };

    struct TEvGetMaxSeqNoRequest : public TEventLocal<TEvGetMaxSeqNoRequest, EvGetMaxSeqNoRequest> {
        TEvGetMaxSeqNoRequest(const ui64 cookie, const TVector<TString>& sourceIds)
        : Cookie(cookie)
        , SourceIds(sourceIds)
        {}

        ui64 Cookie;
        TVector<TString> SourceIds;
    };

    struct TEvMonResponse : public TEventLocal<TEvMonResponse, EvMonResponse> {
        TEvMonResponse(const NPQ::TPartitionId& partition, const TString& str)
        : Partition(partition)
        , Str(str)
        {}

        TEvMonResponse(const TString& str)
        : Str(str)
        {}

        TMaybe<NPQ::TPartitionId> Partition;
        TString Str;
    };


    struct TEvSetClientInfo : public TEventLocal<TEvSetClientInfo, EvSetClientInfo> {
        enum ESetClientInfoType {
            ESCI_OFFSET = 0,
            ESCI_CREATE_SESSION,
            ESCI_DROP_SESSION,
            ESCI_INIT_READ_RULE,
            ESCI_DROP_READ_RULE
        };

        TEvSetClientInfo(const ui64 cookie, const TString& clientId, const ui64 offset, const TString& sessionId, const ui64 partitionSessionId,
                            const ui32 generation, const ui32 step, const TActorId& pipeClient,
                            ESetClientInfoType type = ESCI_OFFSET, ui64 readRuleGeneration = 0, bool strict = false,
                            const std::optional<TString>& сommittedMetadata = std::nullopt)
        : Cookie(cookie)
        , ClientId(clientId)
        , Offset(offset)
        , SessionId(sessionId)
        , PartitionSessionId(partitionSessionId)
        , Generation(generation)
        , Step(step)
        , Type(type)
        , ReadRuleGeneration(readRuleGeneration)
        , Strict(strict)
        , PipeClient(pipeClient)
        , CommittedMetadata(сommittedMetadata)
        {
        }

        ui64 Cookie;
        TString ClientId;
        ui64 Offset;
        TString SessionId;
        ui64 PartitionSessionId;
        ui32 Generation;
        ui32 Step;
        ESetClientInfoType Type;
        ui64 ReadRuleGeneration;
        bool Strict;
        TActorId PipeClient;
        std::optional<TString> CommittedMetadata;
        bool IsInternal = false;
    };


    struct TEvGetClientOffset : public TEventLocal<TEvGetClientOffset, EvGetClientOffset> {
        TEvGetClientOffset(const ui64 cookie, const TString& clientId)
        : Cookie(cookie)
        , ClientId(clientId)
        {}

        ui64 Cookie;
        TString ClientId;
    };


    struct TEvUpdateWriteTimestamp : public TEventLocal<TEvUpdateWriteTimestamp, EvUpdateWriteTimestamp> {
        TEvUpdateWriteTimestamp(const ui64 cookie, const ui64 writeTimestamp)
        : Cookie(cookie)
        , WriteTimestamp(writeTimestamp)
        {}

        ui64 Cookie;
        ui64 WriteTimestamp;
    };


    struct TEvPartitionOffsets : public TEventLocal<TEvPartitionOffsets, EvPartitionOffsets> {
        TEvPartitionOffsets(const TActorId& sender, const TString& clientId)
        : Sender(sender)
        , ClientId(clientId)
        {}

        TActorId Sender;
        TString ClientId;
    };

    struct TEvPartitionOffsetsResponse : public TEventLocal<TEvPartitionOffsetsResponse, EvPartitionOffsetsResponse> {
        TEvPartitionOffsetsResponse(NKikimrPQ::TOffsetsResponse::TPartResult& partResult, const NPQ::TPartitionId& partition)
        : PartResult(partResult)
        , Partition(partition)
        {}

        NKikimrPQ::TOffsetsResponse::TPartResult PartResult;
        NPQ::TPartitionId Partition;
    };

    struct TEvPartitionStatus : public TEventLocal<TEvPartitionStatus, EvPartitionStatus> {
        explicit TEvPartitionStatus(const TActorId& sender, const TString& clientId, bool getStatForAllConsumers)
        : Sender(sender)
        , ClientId(clientId)
        , GetStatForAllConsumers(getStatForAllConsumers)
        {}

        explicit TEvPartitionStatus(const TActorId& sender, const TVector<TString>& consumers)
        : Sender(sender)
        , Consumers(consumers)
        {}

        TActorId Sender;
        TString ClientId;
        bool GetStatForAllConsumers;
        TVector<TString> Consumers;
    };

    struct TEvPartitionStatusResponse : public TEventLocal<TEvPartitionStatusResponse, EvPartitionStatusResponse> {
        TEvPartitionStatusResponse(NKikimrPQ::TStatusResponse::TPartResult& partResult, const NPQ::TPartitionId& partition)
        : PartResult(partResult)
        , Partition(partition)
        {}

        NKikimrPQ::TStatusResponse::TPartResult PartResult;
        NPQ::TPartitionId Partition;
    };


    struct TEvProxyResponse : public TEventLocal<TEvProxyResponse, EvProxyResponse> {
        TEvProxyResponse(ui64 cookie, bool isInternal)
            : Cookie(cookie)
            , IsInternal(isInternal)
            , Response(std::make_shared<NKikimrClient::TResponse>())
        {}
        ui64 Cookie;
        bool IsInternal;
        std::shared_ptr<NKikimrClient::TResponse> Response;
    };

    struct TEvInitComplete : public TEventLocal<TEvInitComplete, EvInitComplete> {
        explicit TEvInitComplete(const NPQ::TPartitionId& partition)
        : Partition(partition)
        {}

        NPQ::TPartitionId Partition;
    };

    struct TEvError : public TEventLocal<TEvError, EvError> {
        TEvError(const NPersQueue::NErrorCode::EErrorCode errorCode, const TString& error, ui64 cookie, bool internal = false)
        : ErrorCode(errorCode)
        , Error(error)
        , Cookie(cookie)
        , IsInternal(internal)
        {}

        NPersQueue::NErrorCode::EErrorCode ErrorCode;
        TString Error;
        ui64 Cookie;
        bool IsInternal = false;
    };

    struct TEvBlobRequest : public TEventLocal<TEvBlobRequest, EvBlobRequest> {
        TEvBlobRequest(const ui64 cookie, const NPQ::TPartitionId& partition,
                       TVector<NPQ::TRequestedBlob>&& blobs)
        : Cookie(cookie)
        , Partition(partition)
        , Blobs(std::move(blobs))
        {}

        ui64 Cookie;
        NPQ::TPartitionId Partition;
        TVector<NPQ::TRequestedBlob> Blobs;
    };

    class TEvBlobResponse : public TEventLocal<TEvBlobResponse, EvBlobResponse> {
    public:
        NPQ::TErrorInfo Error;

        TEvBlobResponse(const ui64 cookie, TVector<NPQ::TRequestedBlob>&& blobs, NPQ::TErrorInfo error = NPQ::TErrorInfo())
        : Error(error)
        , Cookie(cookie)
        , Blobs(std::move(blobs))
        {}

        ui64 GetCookie() const
        {
            return Cookie;
        }

        const TVector<NPQ::TRequestedBlob>& GetBlobs() const
        {
            return Blobs;
        }

        void Check() const
        {
            //error or empty response(all from cache) or not empty response at all
            AFL_ENSURE(Error.HasError() || Blobs.empty() || (!Blobs[0].Empty()))
                ("Cookie", Cookie)("Error code", NPersQueue::NErrorCode::EErrorCode_Name(Error.ErrorCode))("blobs count", Blobs.size());
        }

    private:
        ui64 Cookie;
        TVector<NPQ::TRequestedBlob> Blobs;
    };

    struct TEvChangeOwner : public TEventLocal<TEvChangeOwner, EvChangeOwner> {
        explicit TEvChangeOwner(const ui64 cookie, const TString& owner, const TActorId& pipeClient, const TActorId& sender, const bool force, const bool registerIfNotExists = true)
        : Cookie(cookie)
        , Owner(owner)
        , PipeClient(pipeClient)
        , Sender(sender)
        , Force(force)
        , RegisterIfNotExists(registerIfNotExists)
        {}

        ui64 Cookie;
        TString Owner;
        TActorId PipeClient;
        TActorId Sender;
        bool Force;
        bool RegisterIfNotExists;
    };

    struct TEvPipeDisconnected : public TEventLocal<TEvPipeDisconnected, EvPipeDisconnected> {
        explicit TEvPipeDisconnected(const TString& owner, const TActorId& pipeClient)
            : Owner(owner)
            , PipeClient(pipeClient)
        {}

        TString Owner;
        TActorId PipeClient;
    };

    struct TEvReserveBytes : public TEventLocal<TEvReserveBytes, EvReserveBytes> {
        explicit TEvReserveBytes(const ui64 cookie, const ui32 size, const TString& ownerCookie, const ui64 messageNo, bool lastRequest)
        : Cookie(cookie)
        , Size(size)
        , OwnerCookie(ownerCookie)
        , MessageNo(messageNo)
        , LastRequest(lastRequest)
        {}

        ui64 Cookie;
        ui32 Size;
        TString OwnerCookie;
        ui64 MessageNo;
        bool LastRequest;
    };

    struct TEvChangePartitionConfig : public TEventLocal<TEvChangePartitionConfig, EvChangePartitionConfig> {
        TEvChangePartitionConfig(const NPersQueue::TTopicConverterPtr& topicConverter, const NKikimrPQ::TPQTabletConfig& config)
            : TopicConverter(topicConverter)
            , Config(config)
        {}

        NPersQueue::TTopicConverterPtr TopicConverter;
        NKikimrPQ::TPQTabletConfig Config;
    };

    struct TEvPartitionConfigChanged : public TEventLocal<TEvPartitionConfigChanged, EvPartitionConfigChanged> {
        explicit TEvPartitionConfigChanged(const NPQ::TPartitionId& partition) :
            Partition(partition)
        {
        }

        NPQ::TPartitionId Partition;
    };

    struct TEvChangeCacheConfig : public TEventLocal<TEvChangeCacheConfig, EvChangeCacheConfig> {
        explicit TEvChangeCacheConfig(ui32 maxSize)
        : MaxSize(maxSize)
        {}

        TEvChangeCacheConfig(const TString& topicName, ui32 maxSize)
        : TopicName(topicName)
        , MaxSize(maxSize)
        {}

        TString TopicName;
        ui32 MaxSize;
    };

    struct TEvPartitionCounters : public TEventLocal<TEvPartitionCounters, EvPartitionCounters> {
        TEvPartitionCounters(const NPQ::TPartitionId& partition, const TTabletCountersBase& counters)
            : Partition(partition)
        {
            Counters.Populate(counters);
        }

        const NPQ::TPartitionId Partition;
        TTabletCountersBase Counters;
    };

    struct TEvPartitionLabeledCounters : public TEventLocal<TEvPartitionLabeledCounters, EvPartitionLabeledCounters> {
        TEvPartitionLabeledCounters(const NPQ::TPartitionId& partition, const TTabletLabeledCountersBase& labeledCounters)
            : Partition(partition)
            , LabeledCounters(labeledCounters)
        {
        }

        const NPQ::TPartitionId Partition;
        TTabletLabeledCountersBase LabeledCounters;
    };

    struct TEvPartitionLabeledCountersDrop : public TEventLocal<TEvPartitionLabeledCountersDrop, EvPartitionLabeledCountersDrop> {
        TEvPartitionLabeledCountersDrop(const NPQ::TPartitionId& partition, const TString& group)
            : Partition(partition)
            , Group(group)
        {
        }

        const NPQ::TPartitionId Partition;
        TString Group;
    };


    struct TEvTabletCacheCounters : public TEventLocal<TEvTabletCacheCounters, EvTabletCacheCounters> {
        struct TCacheCounters {
            ui64 CacheSizeBytes = 0;
            ui64 CacheSizeBlobs = 0;
            ui64 CachedOnRead = 0;
            ui64 CachedOnWrite = 0;
        };

        TEvTabletCacheCounters()
        {}

        TCacheCounters Counters;
    };

    struct TEvGetPartitionClientInfo : TEventLocal<TEvGetPartitionClientInfo, EvGetPartitionClientInfo> {
        TEvGetPartitionClientInfo(const TActorId& sender)
            : Sender(sender)
        {}

        TActorId Sender;
    };

    struct TEvUpdateAvailableSize : TEventLocal<TEvUpdateAvailableSize, EvUpdateAvailableSize> {
        TEvUpdateAvailableSize()
        {}
    };

    struct TEvQuotaDeadlineCheck : TEventLocal<TEvQuotaDeadlineCheck, EvQuotaDeadlineCheck> {
        TEvQuotaDeadlineCheck()
        {}
    };

    struct TEvRegisterMessageGroup : TEventLocal<TEvRegisterMessageGroup, EvRegisterMessageGroup> {
        struct TBody {
            explicit TBody(const TString& sourceId, TMaybe<NKikimrPQ::TPartitionKeyRange>&& keyRange, ui64 seqNo, bool afterSplit)
                : SourceId(sourceId)
                , KeyRange(std::move(keyRange))
                , SeqNo(seqNo)
                , AfterSplit(afterSplit)
            {}

            TString SourceId;
            TMaybe<NKikimrPQ::TPartitionKeyRange> KeyRange;
            ui64 SeqNo;
            bool AfterSplit;
            TMaybe<ui64> AssignedOffset; // will be assigned upon registration
        };

        template <typename... Args>
        explicit TEvRegisterMessageGroup(ui64 cookie, Args&&... args)
            : Cookie(cookie)
            , Body(std::forward<Args>(args)...)
        {}

        const ui64 Cookie;
        TBody Body;
    };

    struct TEvDeregisterMessageGroup : TEventLocal<TEvDeregisterMessageGroup, EvDeregisterMessageGroup> {
        struct TBody {
            explicit TBody(const TString& sourceId)
                : SourceId(sourceId)
            {}

            TString SourceId;
        };

        template <typename... Args>
        explicit TEvDeregisterMessageGroup(ui64 cookie, Args&&... args)
            : Cookie(cookie)
            , Body(std::forward<Args>(args)...)
        {}

        const ui64 Cookie;
        TBody Body;
    };

    struct TEvSplitMessageGroup : TEventLocal<TEvSplitMessageGroup, EvSplitMessageGroup> {
        using TRegisterBody = TEvRegisterMessageGroup::TBody;
        using TDeregisterBody = TEvDeregisterMessageGroup::TBody;

        explicit TEvSplitMessageGroup(ui64 cookie, TVector<TDeregisterBody>&& deregistrations, TVector<TRegisterBody>&& registrations)
            : Cookie(cookie)
            , Deregistrations(std::move(deregistrations))
            , Registrations(std::move(registrations))
        {}

        const ui64 Cookie;
        TVector<TDeregisterBody> Deregistrations;
        TVector<TRegisterBody> Registrations;
    };

    struct TEvUpdateCounters : public TEventLocal<TEvUpdateCounters, EvUpdateCounters> {
        TEvUpdateCounters()
        {}
    };

    struct TEvMirrorerCounters : public TEventLocal<TEvMirrorerCounters, EvMirrorerCounters> {
        TEvMirrorerCounters(const NKikimr::TTabletCountersBase& counters) {
            Counters.Populate(counters);
        }

        NKikimr::TTabletCountersBase Counters;
    };

    struct TEvMirrorTopicDescription : public TEventLocal<TEvMirrorTopicDescription, EvMirrorTopicDescription> {
        TEvMirrorTopicDescription(NYdb::NTopic::TDescribeTopicResult description)
            : Description(std::move(description))
        {
        }

        TEvMirrorTopicDescription(TString error)
            : Description(std::unexpected(std::move(error)))
        {
        }

        std::expected<NYdb::NTopic::TDescribeTopicResult, TString> Description;
    };


    struct TEvRetryWrite : public TEventLocal<TEvRetryWrite, EvRetryWrite> {
        TEvRetryWrite()
        {}
    };

    struct TEvInitCredentials : public TEventLocal<TEvInitCredentials, EvInitCredentials> {
        TEvInitCredentials()
        {}
    };

    struct TEvCredentialsCreated : public TEventLocal<TEvCredentialsCreated, EvCredentialsCreated> {
        TEvCredentialsCreated(const TString& error)
            : Error(error)
        {}

        TEvCredentialsCreated(std::shared_ptr<NYdb::ICredentialsProviderFactory> credentials)
            : Credentials(credentials)
        {}

        std::shared_ptr<NYdb::ICredentialsProviderFactory> Credentials;
        std::optional<TString> Error;
    };

    struct TEvCreateConsumer : public TEventLocal<TEvCreateConsumer, EvCreateConsumer> {
        TEvCreateConsumer()
        {}
    };

    struct TEvRequestPartitionStatus : public TEventLocal<TEvRequestPartitionStatus, EvRequestPartitionStatus> {
        TEvRequestPartitionStatus()
        {}
    };

    struct TEvReaderEventArrived : public TEventLocal<TEvReaderEventArrived, EvReaderEventArrived> {
        TEvReaderEventArrived(ui64 id) : Id(id)
        {}

        ui64 Id;
    };

    struct TEvMetering : public TEventLocal<TEvMetering, EvMetering> {
        TEvMetering(NPQ::EMeteringJson type, ui64 quantity)
        : Type (type)
        , Quantity(quantity)
        {}

        NPQ::EMeteringJson Type;
        ui64 Quantity;
    };

    struct TEvTxCalcPredicate : public TEventLocal<TEvTxCalcPredicate, EvTxCalcPredicate> {
        TEvTxCalcPredicate(ui64 step, ui64 txId) :
            Step(step),
            TxId(txId)
        {
        }

        void AddOperation(TString consumer, ui64 begin, ui64 end, bool forceCommit = false, bool killReadSession = false, bool onlyCheckCommitedToFinish = false, TString readSessionId = {}) {
            NKikimrPQ::TPartitionOperation operation;
            operation.SetCommitOffsetsBegin(begin);
            operation.SetCommitOffsetsEnd(end);
            operation.SetConsumer(std::move(consumer));
            operation.SetForceCommit(forceCommit);
            operation.SetKillReadSession(killReadSession);
            operation.SetOnlyCheckCommitedToFinish(onlyCheckCommitedToFinish);
            operation.SetReadSessionId(readSessionId);

            Operations.push_back(std::move(operation));
        }

        void AddKafkaOffsetCommitOperation(TString consumer, ui64 offset) {
            NKikimrPQ::TPartitionOperation operation;
            operation.SetConsumer(std::move(consumer));
            operation.SetKafkaTransaction(true);
            operation.SetCommitOffsetsEnd(offset);
            operation.SetForceCommit(true);

            Operations.push_back(std::move(operation));
        }

        ui64 Step;
        ui64 TxId;
        TVector<NKikimrPQ::TPartitionOperation> Operations;
        TActorId SupportivePartitionActor;
        bool ForcePredicateFalse = false;

        NWilson::TSpan Span;
    };

    struct TEvTxCalcPredicateResult : public TEventLocal<TEvTxCalcPredicateResult, EvTxCalcPredicateResult> {
        TEvTxCalcPredicateResult(ui64 step, ui64 txId,
                                 const NPQ::TPartitionId& partition,
                                 TMaybe<bool> predicate,
                                 const TString& issueMsg) :
            Step(step),
            TxId(txId),
            Partition(partition),
            Predicate(predicate),
            IssueMsg(issueMsg)
        {
        }

        ui64 Step;
        ui64 TxId;
        NPQ::TPartitionId Partition;
        TMaybe<bool> Predicate;
        TString IssueMsg;
    };

    struct TEvProposePartitionConfig : public TEventLocal<TEvProposePartitionConfig, EvProposePartitionConfig> {
        TEvProposePartitionConfig(ui64 step, ui64 txId) :
            Step(step),
            TxId(txId)
        {
        }

        ui64 Step;
        ui64 TxId;
        NPersQueue::TTopicConverterPtr TopicConverter;
        NKikimrPQ::TPQTabletConfig Config;
    };

    struct TEvProposePartitionConfigResult : public TEventLocal<TEvProposePartitionConfigResult, EvProposePartitionConfigResult> {
        TEvProposePartitionConfigResult(ui64 step, ui64 txId, const NPQ::TPartitionId& partition) :
            Step(step),
            TxId(txId),
            Partition(partition)
        {
        }

        ui64 Step;
        ui64 TxId;
        NPQ::TPartitionId Partition;

        NKikimrPQ::TPartitions::TPartitionInfo Data;
    };

    struct TEvTxCommit : public TEventLocal<TEvTxCommit, EvTxCommit> {
        TEvTxCommit(ui64 step, ui64 txId, TMessageGroupsPtr explicitMessageGroups = nullptr)
            : Step(step)
            , TxId(txId)
            , ExplicitMessageGroups(std::move(explicitMessageGroups))
        {
        }

        ui64 Step;
        ui64 TxId;

        TMessageGroupsPtr ExplicitMessageGroups;
        TMaybe<NKikimrPQ::TTransaction> SerializedTx;
        TMaybe<NKikimrPQ::TPQTabletConfig> TabletConfig;
        TMaybe<NKikimrPQ::TBootstrapConfig> BootstrapConfig;
        TMaybe<NKikimrPQ::TPartitions> PartitionsData;

        NWilson::TSpan Span;
    };

    struct TEvTxRollback : public TEventLocal<TEvTxRollback, EvTxRollback> {
        TEvTxRollback(ui64 step, ui64 txId) :
            Step(step),
            TxId(txId)
        {
        }

        ui64 Step;
        ui64 TxId;
        TMaybe<NKikimrPQ::TTransaction> SerializedTx;

        NWilson::TSpan Span;
    };

    struct TEvTxDone : public TEventLocal<TEvTxDone, EvTxDone> {
        TEvTxDone(ui64 step, ui64 txId, const NPQ::TPartitionId& partition) :
            Step(step),
            TxId(txId),
            Partition(partition)
        {
        }

        ui64 Step;
        ui64 TxId;
        NPQ::TPartitionId Partition;

        NWilson::TSpan Span;
    };

    struct TEvSubDomainStatus : public TEventPB<TEvSubDomainStatus, NKikimrPQ::TEvSubDomainStatus, EvSubDomainStatus> {
        TEvSubDomainStatus() {
        }

        explicit TEvSubDomainStatus(bool subDomainOutOfSpace)
        {
            Record.SetSubDomainOutOfSpace(subDomainOutOfSpace);
        }

        bool SubDomainOutOfSpace() const { return Record.GetSubDomainOutOfSpace(); }
    };

    struct TEvStatsWakeup : public TEventLocal<TEvStatsWakeup, EvStatsWakeup> {
        TEvStatsWakeup(ui64 round)
            : Round(round)
        {}

        ui64 Round;
    };

    struct TEvRequestQuota : public TEventLocal<TEvRequestQuota, EvRequestQuota> {
        TEvRequestQuota(ui64 cookie, TAutoPtr<IEventHandle>&& request)
            : Cookie(cookie)
            , Request(std::move(request))
        {}

        ui64 Cookie;
        TAutoPtr<IEventHandle> Request;
    };

    struct TEvApproveReadQuota : public TEventLocal<TEvApproveReadQuota, EvApproveReadQuota> {
        TEvApproveReadQuota(TEvPQ::TEvRead::TPtr readRequest, TDuration& waitTime)
            : ReadRequest(readRequest)
            , WaitTime(std::move(waitTime))
        {}

        TEvPQ::TEvRead::TPtr ReadRequest;
        TDuration WaitTime;
    };

    struct TEvApproveWriteQuota : public TEventLocal<TEvApproveWriteQuota, EvApproveWriteQuota> {
        TEvApproveWriteQuota(ui64 requestCookie, const TDuration& accountWaitTime, const TDuration& partitionWaitTime)
            : Cookie(requestCookie)
            , AccountQuotaWaitTime(accountWaitTime)
            , PartitionQuotaWaitTime(partitionWaitTime)
        {}
        ui64 Cookie;
        TDuration AccountQuotaWaitTime;
        TDuration PartitionQuotaWaitTime;
    };

    struct TEvConsumed : public TEventLocal<TEvConsumed, EvConsumed> {
        TEvConsumed(ui64 consumedBytes, ui64 consumedDeduplicationIds, ui64 requestCookie, const TString& consumer)
            : ConsumedBytes(consumedBytes)
            , ConsumedDeduplicationIds(consumedDeduplicationIds)
            , RequestCookie(requestCookie)
            , Consumer(consumer)
        {}

        TEvConsumed(ui64 consumedBytes)
            : ConsumedBytes(consumedBytes)
            , IsOverhead(true)
        {}

        ui64 ConsumedBytes;
        ui64 ConsumedDeduplicationIds;
        ui64 RequestCookie;
        TString Consumer;
        bool IsOverhead = false;
    };

    struct TEvConsumerRemoved : public TEventLocal<TEvConsumerRemoved, EvConsumerRemoved> {
        TEvConsumerRemoved(const TString& consumer)
            : Consumer(consumer)
        {}

        TString Consumer;
    };

    struct TEvFetchResponse : public TEventLocal<TEvFetchResponse, EvFetchResponse> {
        TEvFetchResponse()
        {}
        Ydb::StatusIds::StatusCode Status;
        TString Message;
        NKikimrClient::TPersQueueFetchResponse Response;
    };

    struct TEvRegisterDirectReadSession : public TEventLocal<TEvRegisterDirectReadSession, EvRegisterDirectReadSession> {
        TEvRegisterDirectReadSession(const NPQ::TReadSessionKey& sessionKey, ui32 tabletGeneration)
            : Session(sessionKey)
            , Generation(tabletGeneration)
        {}
        NPQ::TReadSessionKey Session;
        ui32 Generation;
    };

    struct TEvDeregisterDirectReadSession : public TEventLocal<TEvDeregisterDirectReadSession, EvDeregisterDirectReadSession> {
        TEvDeregisterDirectReadSession(const NPQ::TReadSessionKey& sessionKey, ui32 tabletGeneration)
            : Session(sessionKey)
            , Generation(tabletGeneration)
        {}
        NPQ::TReadSessionKey Session;
        ui32 Generation;
    };

    struct TEvStageDirectReadData : public TEventLocal<TEvStageDirectReadData, EvStageDirectReadData> {
        TEvStageDirectReadData(const NPQ::TDirectReadKey& readKey, ui32 tabletGeneration,
                                   const std::shared_ptr<NKikimrClient::TResponse>& response)
            : TabletGeneration(tabletGeneration)
            , ReadKey(readKey)
            , Response(response)
        {}
        ui32 TabletGeneration;
        NPQ::TDirectReadKey ReadKey;
        std::shared_ptr<NKikimrClient::TResponse> Response;
    };

    struct TEvPublishDirectRead : public TEventLocal<TEvPublishDirectRead, EvCacheProxyPublishRead> {
        TEvPublishDirectRead(const NPQ::TDirectReadKey& readKey, ui32 tabletGeneration)
            : ReadKey(readKey)
            , TabletGeneration(tabletGeneration)
        {}
        NPQ::TDirectReadKey ReadKey;
        ui32 TabletGeneration;
    };

    struct TEvForgetDirectRead : public TEventLocal<TEvForgetDirectRead, EvCacheProxyForgetRead> {
        TEvForgetDirectRead(const NPQ::TDirectReadKey& readKey, ui32 tabletGeneration)
            : TabletGeneration(tabletGeneration)
            , ReadKey(readKey)
        {}
        ui32 TabletGeneration;
        NPQ::TDirectReadKey ReadKey;
    };

    struct TEvGetFullDirectReadData : public TEventLocal<TEvGetFullDirectReadData, EvGetFullDirectReadData> {
        TEvGetFullDirectReadData() = default;
        TEvGetFullDirectReadData(const NPQ::TReadSessionKey& key, ui32 generation)
            : ReadKey(key)
            , Generation(generation)
        {}

        NPQ::TReadSessionKey ReadKey;
        ui32 Generation;
        bool Error = false;
        TVector<std::pair<NPQ::TReadSessionKey, NPQ::TCacheServiceData>> Data;
    };

    struct TEvProvideDirectReadInfo : public TEventLocal<TEvProvideDirectReadInfo, EvProvideDirectReadInfo> {
    };

    struct TEvCheckPartitionStatusRequest : public TEventPB<TEvCheckPartitionStatusRequest, NKikimrPQ::TEvCheckPartitionStatusRequest, EvCheckPartitionStatusRequest> {
        TEvCheckPartitionStatusRequest() = default;

        TEvCheckPartitionStatusRequest(ui32 partitionId) {
            Record.SetPartition(partitionId);
        }
    };

    struct TEvCheckPartitionStatusResponse : public TEventPB<TEvCheckPartitionStatusResponse, NKikimrPQ::TEvCheckPartitionStatusResponse, EvCheckPartitionStatusResponse> {
    };

    struct TEvGetWriteInfoRequest : public TEventLocal<TEvGetWriteInfoRequest, EvGetWriteInfoRequest> {
        TActorId OriginalPartition;

        NWilson::TSpan Span;
    };

    struct TEvGetWriteInfoResponse : public TEventLocal<TEvGetWriteInfoResponse, EvGetWriteInfoResponse> {
        TEvGetWriteInfoResponse() = default;
        TEvGetWriteInfoResponse(ui32 cookie,
                                NPQ::TSourceIdMap&& srcIdInfo,
                                std::deque<NPQ::TDataKey>&& bodyKeys) :
            Cookie(cookie),
            SrcIdInfo(std::move(srcIdInfo)),
            BodyKeys(std::move(bodyKeys))
        {
        }

        ui32 Cookie; // InternalPartitionId
        TActorId SupportivePartition;

        NPQ::TSourceIdMap SrcIdInfo;
        std::deque<NPQ::TDataKey> BodyKeys;
        // SourceId->WritenBytes
        std::vector<std::pair<TString, ui64>> WrittenBytes;

        ui64 BytesWrittenTotal;
        ui64 BytesWrittenGrpc;
        ui64 BytesWrittenUncompressed;
        ui64 MessagesWrittenTotal;
        ui64 MessagesWrittenGrpc;
        TVector<ui64> MessagesSizes;
        THolder<NPQ::TMultiBucketCounter> InputLags;
    };

    struct TEvGetWriteInfoError : public TEventLocal<TEvGetWriteInfoError, EvGetWriteInfoError> {
        ui32 Cookie; // InternalPartitionId
        TString Message;
        TActorId SupportivePartition;

        TEvGetWriteInfoError(ui32 cookie, TString message) :
            Cookie(cookie),
            Message(std::move(message))
        {
        }
    };

    struct TEvTxBatchComplete : public TEventLocal<TEvTxBatchComplete, EvTxBatchComplete> {
        explicit TEvTxBatchComplete(ui64 batchSize)
            : BatchSize(batchSize)
        {}
        ui64 BatchSize;
    };

    struct TEvReadingPartitionStatusRequest : public TEventPB<TEvReadingPartitionStatusRequest, NKikimrPQ::TEvReadingPartitionStatusRequest, EvReadingPartitionStatusRequest> {
        TEvReadingPartitionStatusRequest() = default;

        TEvReadingPartitionStatusRequest(const TString& consumer, ui32 partitionId, ui32 generaion, ui64 cookie) {
            Record.SetConsumer(consumer);
            Record.SetPartitionId(partitionId);
            Record.SetGeneration(generaion);
            Record.SetCookie(cookie);
        }
    };

    struct TEvProcessChangeOwnerRequests : public TEventLocal<TEvProcessChangeOwnerRequests, EvProcessChangeOwnerRequests> {
    };

    struct TEvWakeupReleasePartition : TEventLocal<TEvWakeupReleasePartition, EvWakeupReleasePartition> {
        TEvWakeupReleasePartition(const TString& consumer, const ui32 partitionId, const ui64 cookie)
            : Consumer(consumer)
            , PartitionId(partitionId)
            , Cookie(cookie)
        {}

        TString Consumer;
        ui32 PartitionId;
        ui64 Cookie;
    };

    struct TEvPartitionScaleStatusChanged : public TEventPB<TEvPartitionScaleStatusChanged, NKikimrPQ::TEvPartitionScaleStatusChanged, EvPartitionScaleStatusChanged> {
        TEvPartitionScaleStatusChanged() = default;

        TEvPartitionScaleStatusChanged(ui32 partitionId, NKikimrPQ::EScaleStatus scaleStatus) {
            Record.SetPartitionId(partitionId);
            Record.SetScaleStatus(scaleStatus);
        }
    };

    struct TBroadcastPartitionError : public TEventPB<TBroadcastPartitionError,
            NKikimrPQ::TBroadcastPartitionError, EvBroadcastPartitionError> {
        TBroadcastPartitionError() = default;

        explicit TBroadcastPartitionError(TString message, NKikimrServices::EServiceKikimr service, TInstant timestamp) {
            auto* defaultGroup = Record.AddMessageGroups();
            auto* error = defaultGroup->AddErrors();
            error->SetMessage(std::move(message));
            error->SetService(service);
            error->SetTimestamp(timestamp.Seconds());
        }
    };

    struct TEvBalanceConsumer : TEventLocal<TEvBalanceConsumer, EvBalanceConsumer> {
        TEvBalanceConsumer(const TString& consumerName)
            : ConsumerName(consumerName)
        {}

        TString ConsumerName;
    };

    struct TEvDeletePartition : TEventLocal<TEvDeletePartition, EvDeletePartition> {
    };

    struct TEvDeletePartitionDone : TEventLocal<TEvDeletePartitionDone, EvDeletePartitionDone> {
        explicit TEvDeletePartitionDone(const NPQ::TPartitionId& partitionId) :
            PartitionId(partitionId)
        {
        }

        NPQ::TPartitionId PartitionId;
    };

    struct TEvTransactionCompleted : TEventLocal<TEvTransactionCompleted, EvTransactionCompleted> {
        explicit TEvTransactionCompleted(const TMaybe<NPQ::TWriteId>& writeId) :
            WriteId(writeId)
        {
        }

        TMaybe<NPQ::TWriteId> WriteId;
    };

    struct TEvListAllTopicsResponse : TEventLocal<TEvListAllTopicsResponse, EvListAllTopicsResponse> {
        explicit TEvListAllTopicsResponse() = default;
        explicit TEvListAllTopicsResponse(Ydb::StatusIds status, const TString& error);

        TVector<TString> Topics;
        bool HaveMoreTopics = false;
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        TString Error;
    };

    struct TEvAcquireExclusiveLock : public TEventLocal<TEvAcquireExclusiveLock, EvAcquireExclusiveLock> {
    };

    struct TEvExclusiveLockAcquired : public TEventLocal<TEvExclusiveLockAcquired, EvExclusiveLockAcquired> {
    };

    struct TEvReleaseExclusiveLock : public TEventLocal<TEvReleaseExclusiveLock, EvReleaseExclusiveLock> {
    };

    struct TEvRunCompaction : TEventLocal<TEvRunCompaction, EvRunCompaction> {
        explicit TEvRunCompaction(const ui64 blobsCount) :
            BlobsCount(blobsCount)
        {
        }

        ui64 BlobsCount = 0;
    };

    struct TEvForceCompaction : TEventLocal<TEvForceCompaction, EvForceCompaction> {
        explicit TEvForceCompaction(const ui32 partitionId) :
            PartitionId(partitionId)
        {
        }

        ui32 PartitionId = 0;
    };

    //
    // Request to the PQRB. It asks him which partition is ready for reading.
    //
    struct TEvMLPGetPartitionRequest : TEventPB<TEvMLPGetPartitionRequest, NKikimrPQ::TEvMLPGetPartitionRequest, EvMLPGetPartitionRequest> {
        TEvMLPGetPartitionRequest() = default;

        TEvMLPGetPartitionRequest(const TString& topic, const TString& consumer) {
            Record.SetTopic(topic);
            Record.SetConsumer(consumer);
        }

        const TString& GetTopic() const {
            return Record.GetTopic();
        }

        const TString& GetConsumer() const {
            return Record.GetConsumer();
        }
    };

    //
    // Response from the PQRB. It tells us which partition is ready for reading.
    //
    struct TEvMLPGetPartitionResponse : TEventPB<TEvMLPGetPartitionResponse, NKikimrPQ::TEvMLPGetPartitionResponse, EvMLPGetPartitionResponse> {
        TEvMLPGetPartitionResponse() = default;

        TEvMLPGetPartitionResponse(ui32 partitionId, ui64 tabletId) {
            Record.SetStatus(Ydb::StatusIds::SUCCESS);
            Record.SetPartitionId(partitionId);
            Record.SetTabletId(tabletId);
        }

        TEvMLPGetPartitionResponse(Ydb::StatusIds::StatusCode errorCode) {
            Record.SetStatus(errorCode);
        }

       Ydb::StatusIds::StatusCode GetStatus() const {
            return Record.GetStatus();
        }

        // The partition which is ready for reading.
        ui32 GetPartitionId() const {
            return Record.GetPartitionId();
        }

        // The tablet of the partition which is ready for reading. Return it because partition may not exist yet in local scheme cache.
        ui64 GetTabletId() const {
            return Record.GetTabletId();
        }
    };

    struct TEvMLPErrorResponse : TEventPB<TEvMLPErrorResponse, NKikimrPQ::TEvMLPErrorResponse, EvMLPErrorResponse> {
        TEvMLPErrorResponse() = default;

        TEvMLPErrorResponse(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage) {
            Record.SetStatus(errorCode);
            Record.SetErrorMessage(errorMessage);
        }

        TEvMLPErrorResponse(ui32 partitionId, Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage) {
            Record.SetPartitionId(partitionId);
            Record.SetStatus(errorCode);
            Record.SetErrorMessage(errorMessage);
        }

        ui32 GetPartitionId() const {
            return Record.GetPartitionId();
        }

        Ydb::StatusIds::StatusCode GetStatus() const {
            return Record.GetStatus();
        }

        TString& GetErrorMessage() {
            return *Record.MutableErrorMessage();
        }
    };

    //
    // Request to the PQ-tablet.
    //
    struct TEvMLPReadRequest : TEventPB<TEvMLPReadRequest, NKikimrPQ::TEvMLPReadRequest, EvMLPReadRequest> {
        TEvMLPReadRequest() = default;

        TEvMLPReadRequest(const TString& topic, const TString& consumer, ui32 partitionId, TInstant waitDeadline, TInstant visibilityDeadline, ui32 maxNumberOfMessages) {
            Record.SetTopic(topic);
            Record.SetConsumer(consumer);
            Record.SetPartitionId(partitionId);
            Record.SetWaitDeadlineMilliseconds(waitDeadline.MilliSeconds());
            Record.SetVisibilityDeadlineMilliseconds(visibilityDeadline.MilliSeconds());
            Record.SetMaxNumberOfMessages(maxNumberOfMessages);
        }

        const TString& GetTopic() const {
            return Record.GetTopic();
        }

        const TString& GetConsumer() const {
            return Record.GetConsumer();
        }

        ui32 GetPartitionId() const {
            return Record.GetPartitionId();
        }

        TInstant GetWaitDeadline() const {
            return TInstant::MilliSeconds(Record.GetWaitDeadlineMilliseconds());
        }

        TInstant GetVisibilityDeadline() const {
            return TInstant::MilliSeconds(Record.GetVisibilityDeadlineMilliseconds());
        }

        // The maximum number of messages to return.
        ui32 GetMaxNumberOfMessages() const {
            return Record.GetMaxNumberOfMessages();
        }
    };

    //
    // Response from the PQ-tablet.
    //
    struct TEvMLPReadResponse : TEventPB<TEvMLPReadResponse, NKikimrPQ::TEvMLPReadResponse, EvMLPReadResponse> {
        TEvMLPReadResponse() = default;
    };

    //
    // Request to the PQ-tablet. It is SQS DeleteMessageBatch request.
    //
    struct TEvMLPCommitRequest : TEventPB<TEvMLPCommitRequest, NKikimrPQ::TEvMLPCommitRequest, EvMLPCommitRequest> {
        TEvMLPCommitRequest() = default;

        TEvMLPCommitRequest(const TString& topic, const TString& consumer, ui32 partitionId, const std::vector<ui64>& offsets) {
            Record.SetTopic(topic);
            Record.SetConsumer(consumer);
            Record.SetPartitionId(partitionId);
            for (auto offset : offsets) {
                Record.AddOffset(offset);
            }
        }

        void AddMessage(ui64 offset) {
            Record.AddOffset(offset);
        }

        const TString& GetTopic() const {
            return Record.GetTopic();
        }

        const TString& GetConsumer() const {
            return Record.GetConsumer();
        }

        ui32 GetPartitionId() const {
            return Record.GetPartitionId();
        }
    };

    //
    // Response from the PQ-tablet.
    //
    struct TEvMLPCommitResponse : TEventPB<TEvMLPCommitResponse, NKikimrPQ::TEvMLPCommitResponse, EvMLPCommitResponse> {
        TEvMLPCommitResponse() = default;
    };

    //
    // Request to the PQ-tablet.
    //
    struct TEvMLPUnlockRequest : TEventPB<TEvMLPUnlockRequest, NKikimrPQ::TEvMLPUnlockRequest, EvMLPUnlockRequest> {
        TEvMLPUnlockRequest() = default;

        TEvMLPUnlockRequest(const TString& topic, const TString& consumer, ui32 partitionId, const std::vector<ui64>& offsets) {
            Record.SetTopic(topic);
            Record.SetConsumer(consumer);
            Record.SetPartitionId(partitionId);
            for (auto offset : offsets) {
                Record.AddOffset(offset);
            }
        }

        void AddMessage(ui64 offset) {
            Record.AddOffset(offset);
        }

        const TString& GetTopic() const {
            return Record.GetTopic();
        }

        const TString& GetConsumer() const {
            return Record.GetConsumer();
        }

        ui32 GetPartitionId() const {
            return Record.GetPartitionId();
        }
    };

    //
    // Response from the PQ-tablet.
    //
    struct TEvMLPUnlockResponse : TEventPB<TEvMLPUnlockResponse, NKikimrPQ::TEvMLPUnlockResponse, EvMLPUnlockResponse> {
        TEvMLPUnlockResponse() = default;
    };

    //
    // Request to the PQ-tablet.
    //
    struct TEvMLPChangeMessageDeadlineRequest : TEventPB<TEvMLPChangeMessageDeadlineRequest, NKikimrPQ::TEvMLPChangeMessageDeadlineRequest, EvMLPChangeMessageDeadlineRequest> {
        TEvMLPChangeMessageDeadlineRequest() = default;

        TEvMLPChangeMessageDeadlineRequest(const TString& topic, const TString& consumer, ui32 partitionId, const std::span<const ui64> offsets, const std::span<const TInstant> deadlineTimestamps);

        const TString& GetTopic() const {
            return Record.GetTopic();
        }

        const TString& GetConsumer() const {
            return Record.GetConsumer();
        }

        ui32 GetPartitionId() const {
            return Record.GetPartitionId();
        }
    };

    //
    // Response from the PQ-tablet.
    //
    struct TEvMLPChangeMessageDeadlineResponse : TEventPB<TEvMLPChangeMessageDeadlineResponse, NKikimrPQ::TEvMLPChangeMessageDeadlineResponse, EvMLPChangeMessageDeadlineResponse> {
        TEvMLPChangeMessageDeadlineResponse() = default;
    };

    //
    // Request to the PQ-tablet. Only for testing purposes.
    //
    struct TEvGetMLPConsumerStateRequest : TEventLocal<TEvGetMLPConsumerStateRequest, EvGetMLPConsumerStateRequest> {
        TEvGetMLPConsumerStateRequest(const TString& topic, const TString& consumer, ui32 partitionId)
            : Topic(topic)
            , Consumer(consumer)
            , PartitionId(partitionId) {
        }

        ui32 GetPartitionId() const {
            return PartitionId;
        }

        const TString Topic;
        const TString Consumer;
        const ui32 PartitionId;
    };

    //
    // Response from the MLP consumer. Only for testing purposes.
    //
    struct TEvGetMLPConsumerStateResponse : TEventLocal<TEvGetMLPConsumerStateResponse, EvGetMLPConsumerStateResponse> {

        NKikimrPQ::TPQTabletConfig::TConsumer Config;
        std::optional<TDuration> RetentionPeriod;

        struct TMessage {
            ui64 Offset;
            ui32 Status;
            ui32 ProcessingCount;
            TInstant ProcessingDeadline;
            TInstant WriteTimestamp;
        };

        std::vector<TMessage> Messages;
    };

    struct TEvMLPConsumerUpdateConfig : TEventLocal<TEvMLPConsumerUpdateConfig, EvMLPConsumerUpdateConfig> {

        TEvMLPConsumerUpdateConfig(
            const NKikimrPQ::TPQTabletConfig& topicConfig,
            const NKikimrPQ::TPQTabletConfig::TConsumer& config,
            std::optional<TDuration> retentionPeriod,
            NMonitoring::TDynamicCounterPtr detailedMetricsRoot
        )
            : TopicConfig(topicConfig)
            , Config(config)
            , RetentionPeriod(retentionPeriod)
            , DetailedMetricsRoot(std::move(detailedMetricsRoot))
        {
        }

        NKikimrPQ::TPQTabletConfig TopicConfig;
        NKikimrPQ::TPQTabletConfig::TConsumer Config;
        std::optional<TDuration> RetentionPeriod;
        NMonitoring::TDynamicCounterPtr DetailedMetricsRoot;
    };

    struct TEvMLPDLQMoverResponse : TEventLocal<TEvMLPDLQMoverResponse, EvMLPDLQMoverResponse> {

        TEvMLPDLQMoverResponse(Ydb::StatusIds::StatusCode status,
             std::vector<std::pair<ui64, ui64>>&& movedMessages, TString&& errorDescription = "")
            : Status(status)
            , MovedMessages(std::move(movedMessages))
            , ErrorDescription(std::move(errorDescription))
        {
        }

        Ydb::StatusIds::StatusCode Status;
        // offset->seqNo
        std::vector<std::pair<ui64, ui64>> MovedMessages;
        TString ErrorDescription;
    };

    struct TEvEndOffsetChanged : TEventLocal<TEvEndOffsetChanged, EvEndOffsetChanged> {
        TEvEndOffsetChanged(ui64 offset)
            : Offset(offset)
        {
        }

        ui64 Offset;
    };

    struct TEvMLPConsumerState : TEventLocal<TEvMLPConsumerState, EvMLPConsumerState> {
        TEvMLPConsumerState(bool useForReading, NKikimrPQ::TAggregatedCounters::TMLPConsumerCounters&& metrics)
            : UseForReading(useForReading)
            , Metrics(std::move(metrics))
        {
        }

        bool UseForReading;
        NKikimrPQ::TAggregatedCounters::TMLPConsumerCounters Metrics;
    };

    struct TEvMLPConsumerMonRequest : TEventLocal<TEvMLPConsumerMonRequest, EvMLPConsumerMonRequest> {
        TEvMLPConsumerMonRequest(TActorId replyTo, ui32 partitionId, const TString& consumer)
            : ReplyTo(replyTo)
            , PartitionId(partitionId)
            , Consumer(consumer)
        {
        }

        TActorId ReplyTo;
        ui32 PartitionId;
        TString Consumer;
    };

    struct TEvMLPPurgeRequest : TEventPB<TEvMLPPurgeRequest, NKikimrPQ::TEvMLPPurgeRequest, EvMLPPurgeRequest> {
        TEvMLPPurgeRequest() = default;

        TEvMLPPurgeRequest(const TString& topic, const TString& consumer, ui32 partitionId)
        {
            Record.SetTopic(topic);
            Record.SetConsumer(consumer);
            Record.SetPartitionId(partitionId);
        }

        const TString& GetTopic() const {
            return Record.GetTopic();
        }

        const TString& GetConsumer() const {
            return Record.GetConsumer();
        }

        ui32 GetPartitionId() const {
            return Record.GetPartitionId();
        }
    };

    struct TEvMLPPurgeResponse : TEventPB<TEvMLPPurgeResponse, NKikimrPQ::TEvMLPPurgeResponse, EvMLPPurgeResponse> {
        TEvMLPPurgeResponse() = default;

        TEvMLPPurgeResponse(ui32 partitionId) {
            Record.SetPartitionId(partitionId);
        }

        ui32 GetPartitionId() const {
            return Record.GetPartitionId();
        }
    };

    struct TEvMLPConsumerStatus : TEventPB<TEvMLPConsumerStatus, NKikimrPQ::TEvMLPConsumerStatus, EvMLPConsumerStatus> {
        TEvMLPConsumerStatus() = default;

        TEvMLPConsumerStatus(const TString& consumer, ui32 partitionId, ui64 messages, bool useForReading) {
            Record.SetConsumer(consumer);
            Record.SetPartitionId(partitionId);
            Record.SetMessages(messages);
            Record.SetUseForReading(useForReading);
        }
    };
};

} //NKikimr
