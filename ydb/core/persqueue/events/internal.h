#pragma once

#include "global.h"

#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/persqueue/key.h>
#include <ydb/core/persqueue/metering_sink.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/actorid.h>

#include <util/generic/maybe.h>

namespace NYdb {
    class ICredentialsProviderFactory;
}

namespace NKikimr {

namespace NPQ {

    struct TRequestedBlob {
        ui64 Offset;
        ui16 PartNo;
        ui32 Count;
        ui16 InternalPartsCount;
        ui32 Size;
        TString Value;
        bool Cached;
        TKey Key;

        TRequestedBlob() = delete;

        TRequestedBlob(ui64 offset, ui16 partNo, ui32 count, ui16 internalPartsCount, ui32 size, TString value, const TKey& key)
            : Offset(offset)
            , PartNo(partNo)
            , Count(count)
            , InternalPartsCount(internalPartsCount)
            , Size(size)
            , Value(value)
            , Cached(false)
            , Key(key)
        {}
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
}

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
        EvAccountReadQuotaRequest,
        EvAccountReadQuotaResponse,
        EvAccountReadQuotaConsumed,
        EvAccountReadQuotaCounters,
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
        EvApproveQuota,
        EvConsumed,
        EvQuotaUpdated,
        EvQuotaCountersUpdated,
        EvConsumerRemoved,
        EvEnd
    };

    struct TEvHandleWriteResponse : TEventLocal<TEvHandleWriteResponse, EvHandleWriteResponse> {
        TEvHandleWriteResponse()
        {}
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
        };

        TEvWrite(const ui64 cookie, const ui64 messageNo, const TString& ownerCookie, const TMaybe<ui64> offset, TVector<TMsg> &&msgs, bool isDirectWrite)
        : Cookie(cookie)
        , MessageNo(messageNo)
        , OwnerCookie(ownerCookie)
        , Offset(offset)
        , Msgs(std::move(msgs))
        , IsDirectWrite(isDirectWrite)
        {}

        ui64 Cookie;
        ui64 MessageNo;
        TString OwnerCookie;
        TMaybe<ui64> Offset;
        TVector<TMsg> Msgs;
        bool IsDirectWrite;

    };

    struct TEvReadTimeout : public TEventLocal<TEvReadTimeout, EvReadTimeout> {
        explicit TEvReadTimeout(const ui64 cookie)
        : Cookie(cookie)
        {}

        ui64 Cookie;
    };

    struct TEvRead : public TEventLocal<TEvRead, EvRead> {
        TEvRead(const ui64 cookie, const ui64 offset, const ui16 partNo, const ui32 count,
                const TString& sessionId, const TString& clientId, const ui32 timeout, const ui32 size,
                const ui32 maxTimeLagMs, const ui64 readTimestampMs, const TString& clientDC,
                bool externalOperation)
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
        TEvMonResponse(ui32 partition, const TVector<TString>& res, const TString& str)
        : Partition(partition)
        , Res(res)
        , Str(str)
        {}

        ui32 Partition;
        TVector<TString> Res;
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

        TEvSetClientInfo(const ui64 cookie, const TString& clientId, const ui64 offset, const TString& sessionId,
                            const ui32 generation, const ui32 step, ESetClientInfoType type = ESCI_OFFSET,
                            ui64 readRuleGeneration = 0, bool strict = false)
        : Cookie(cookie)
        , ClientId(clientId)
        , Offset(offset)
        , SessionId(sessionId)
        , Generation(generation)
        , Step(step)
        , Type(type)
        , ReadRuleGeneration(readRuleGeneration)
        , Strict(strict)
        {
        }

        ui64 Cookie;
        TString ClientId;
        ui64 Offset;
        TString SessionId;
        ui32 Generation;
        ui32 Step;
        ESetClientInfoType Type;
        ui64 ReadRuleGeneration;
        bool Strict;
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
        explicit TEvPartitionOffsetsResponse(NKikimrPQ::TOffsetsResponse::TPartResult& partResult)
        : PartResult(partResult)
        {}

        NKikimrPQ::TOffsetsResponse::TPartResult PartResult;
    };

    struct TEvPartitionStatus : public TEventLocal<TEvPartitionStatus, EvPartitionStatus> {
        explicit TEvPartitionStatus(const TActorId& sender, const TString& clientId, bool getStatForAllConsumers)
        : Sender(sender)
        , ClientId(clientId)
        , GetStatForAllConsumers(getStatForAllConsumers)
        {}

        TActorId Sender;
        TString ClientId;
        bool GetStatForAllConsumers;
    };

    struct TEvPartitionStatusResponse : public TEventLocal<TEvPartitionStatusResponse, EvPartitionStatusResponse> {
        explicit TEvPartitionStatusResponse(NKikimrPQ::TStatusResponse::TPartResult& partResult)
        : PartResult(partResult)
        {}

        NKikimrPQ::TStatusResponse::TPartResult PartResult;
    };


    struct TEvProxyResponse : public TEventLocal<TEvProxyResponse, EvProxyResponse> {
        TEvProxyResponse(ui64 cookie)
        : Cookie(cookie)
        {}
        ui64 Cookie;
        NKikimrClient::TResponse Response;
    };

    struct TEvInitComplete : public TEventLocal<TEvInitComplete, EvInitComplete> {
        explicit TEvInitComplete(const ui32 partition)
        : Partition(partition)
        {}

        ui32 Partition;
    };

    struct TEvError : public TEventLocal<TEvError, EvError> {
        TEvError(const NPersQueue::NErrorCode::EErrorCode errorCode, const TString& error, ui64 cookie)
        : ErrorCode(errorCode)
        , Error(error)
        , Cookie(cookie)
        {}

        NPersQueue::NErrorCode::EErrorCode ErrorCode;
        TString Error;
        ui64 Cookie;
    };

    struct TEvBlobRequest : public TEventLocal<TEvBlobRequest, EvBlobRequest> {
        TEvBlobRequest(const TString& user, const ui64 cookie, const ui32 partition, const ui64 readOffset,
                       TVector<NPQ::TRequestedBlob>&& blobs)
        : User(user)
        , Cookie(cookie)
        , Partition(partition)
        , ReadOffset(readOffset)
        , Blobs(std::move(blobs))
        {}

        TString User;
        ui64 Cookie;
        ui32 Partition;
        ui64 ReadOffset;
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
            Y_VERIFY(Error.HasError() || Blobs.empty() || !Blobs[0].Value.empty(),
                "Cookie %" PRIu64 " Error code: %" PRIu32 ", blobs count: %" PRIu64, Cookie, Error.ErrorCode, Blobs.size());
        }

    private:
        ui64 Cookie;
        TVector<NPQ::TRequestedBlob> Blobs;
    };

    struct TEvChangeOwner : public TEventLocal<TEvChangeOwner, EvChangeOwner> {
        explicit TEvChangeOwner(const ui64 cookie, const TString& owner, const TActorId& pipeClient, const TActorId& sender, const bool force)
        : Cookie(cookie)
        , Owner(owner)
        , PipeClient(pipeClient)
        , Sender(sender)
        , Force(force)
        {}

        ui64 Cookie;
        TString Owner;
        TActorId PipeClient;
        TActorId Sender;
        bool Force;
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
        explicit TEvPartitionConfigChanged(ui32 partition) :
            Partition(partition)
        {
        }

        ui32 Partition;
    };

    struct TEvChangeCacheConfig : public TEventLocal<TEvChangeCacheConfig, EvChangeCacheConfig> {
        explicit TEvChangeCacheConfig(ui32 maxSize)
        : MaxSize(maxSize)
        {}

        ui32 MaxSize;
    };

    struct TEvPartitionCounters : public TEventLocal<TEvPartitionCounters, EvPartitionCounters> {
        TEvPartitionCounters(const ui32 partition, const TTabletCountersBase& counters)
            : Partition(partition)
        {
            Counters.Populate(counters);
        }

        const ui32 Partition;
        TTabletCountersBase Counters;
    };

    struct TEvPartitionLabeledCounters : public TEventLocal<TEvPartitionLabeledCounters, EvPartitionLabeledCounters> {
        TEvPartitionLabeledCounters(const ui32 partition, const TTabletLabeledCountersBase& labeledCounters)
            : Partition(partition)
            , LabeledCounters(labeledCounters)
        {
        }

        const ui32 Partition;
        TTabletLabeledCountersBase LabeledCounters;
    };

    struct TEvPartitionLabeledCountersDrop : public TEventLocal<TEvPartitionLabeledCountersDrop, EvPartitionLabeledCountersDrop> {
        TEvPartitionLabeledCountersDrop(const ui32 partition, const TString& group)
            : Partition(partition)
            , Group(group)
        {
        }

        const ui32 Partition;
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

        void AddOperation(TString consumer, ui64 begin, ui64 end) {
            NKikimrPQ::TPartitionOperation operation;
            operation.SetBegin(begin);
            operation.SetEnd(end);
            operation.SetConsumer(std::move(consumer));

            Operations.push_back(std::move(operation));
        }

        ui64 Step;
        ui64 TxId;
        TVector<NKikimrPQ::TPartitionOperation> Operations;
    };

    struct TEvTxCalcPredicateResult : public TEventLocal<TEvTxCalcPredicateResult, EvTxCalcPredicateResult> {
        TEvTxCalcPredicateResult(ui64 step, ui64 txId, ui32 partition, bool predicate) :
            Step(step),
            TxId(txId),
            Partition(partition),
            Predicate(predicate)
        {
        }

        ui64 Step;
        ui64 TxId;
        ui32 Partition;
        bool Predicate = false;
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
        TEvProposePartitionConfigResult(ui64 step, ui64 txId, ui32 partition) :
            Step(step),
            TxId(txId),
            Partition(partition)
        {
        }

        ui64 Step;
        ui64 TxId;
        ui32 Partition;
    };

    struct TEvTxCommit : public TEventLocal<TEvTxCommit, EvTxCommit> {
        TEvTxCommit(ui64 step, ui64 txId) :
            Step(step),
            TxId(txId)
        {
        }

        ui64 Step;
        ui64 TxId;
    };

    struct TEvTxCommitDone : public TEventLocal<TEvTxCommitDone, EvTxCommitDone> {
        TEvTxCommitDone(ui64 step, ui64 txId, ui32 partition) :
            Step(step),
            TxId(txId),
            Partition(partition)
        {
        }

        ui64 Step;
        ui64 TxId;
        ui32 Partition;
    };

    struct TEvTxRollback : public TEventLocal<TEvTxRollback, EvTxRollback> {
        TEvTxRollback(ui64 step, ui64 txId) :
            Step(step),
            TxId(txId)
        {
        }

        ui64 Step;
        ui64 TxId;
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
        TEvRequestQuota(TEvPQ::TEvRead::TPtr readRequest)
            : 
            ReadRequest(std::move(readRequest))
        {}

        TEvPQ::TEvRead::TPtr ReadRequest;
    };

    struct TEvApproveQuota : public TEventLocal<TEvApproveQuota, EvApproveQuota> {
        TEvApproveQuota(TEvPQ::TEvRead::TPtr readRequest, TDuration waitTime) 
            : 
            ReadRequest(std::move(readRequest)),
            WaitTime(std::move(waitTime))
        {}

        TEvPQ::TEvRead::TPtr ReadRequest;
        TDuration WaitTime;
    };

    struct TEvConsumed : public TEventLocal<TEvConsumed, EvConsumed> {
        TEvConsumed(ui64 readBytes, ui64 readRequestCookie, const TString& consumer)
            : ReadBytes(readBytes),
              ReadRequestCookie(readRequestCookie),
              Consumer(consumer)
        {}

        ui64 ReadBytes;
        ui64 ReadRequestCookie;
        TString Consumer;
    };

    struct TEvConsumerRemoved : public TEventLocal<TEvConsumerRemoved, EvConsumerRemoved> {
        TEvConsumerRemoved(const TString& consumer)
            : Consumer(consumer)
        {}

        TString Consumer;
    };
};

} //NKikimr
