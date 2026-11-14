#pragma once
#include "defs.h"

#include <ydb/core/blobstorage/base/blobstorage_syncstate.h>
#include <ydb/core/blobstorage/contracts/blobstorage_proxy_config.h>
#include <ydb/core/blobstorage/base/blobstorage_oos_defs.h>
#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/blobstorage/base/vdisk_priorities.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_defs.h>
#include <ydb/core/blobstorage/vdisk/common/disk_part.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_histogram_latency.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_mon.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress_matrix.h>
#include <ydb/core/blobstorage/vdisk/protos/events.pb.h>
#include <ydb/core/blobstorage/vdisk/protos/space_report.pb.h>
#include <ydb/core/blobstorage/storagepoolmon/storagepool_counters.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/base/blobstorage_write_source.h>

#include <ydb/core/base/event_filter.h>
#include <ydb/core/base/interconnect_channels.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/blobstorage_disk.pb.h>

#include <ydb/core/util/pb.h>

#include <library/cpp/time_provider/time_provider.h>

#include <util/digest/multi.h>
#include <util/generic/maybe.h>
#include <optional>
#include <util/stream/str.h>
#include <util/string/escape.h>

// FIXME: this check is obsolete; TEvVGet message generators check expected reply size by their own
#ifndef BS_EVVGET_SIZE_VERIFY
#define BS_EVVGET_SIZE_VERIFY 0
#endif

#ifndef VDISK_SKELETON_TRACE
#define VDISK_SKELETON_TRACE 0
#endif

namespace NKikimr {

    namespace NBackpressure {


        enum class EQueueClientType : ui32 {
            None,
            ReplJob,
            DSProxy,
            VDiskLoad,
            VPatch,
            Balancing,
        };

        const char *EQueueClientType2String(EQueueClientType t);

        // backpressure queue client identifier; ui32 means NodeId of DS Proxy actor, TVDiskID -- replication job identifier.
        class TQueueClientId {
            EQueueClientType Type;
            ui64 Identifier;

        public:
            TQueueClientId()
                : Type(EQueueClientType::None)
                , Identifier(0)
            {}

            TQueueClientId(EQueueClientType type, ui64 identifier)
                : Type(type)
                , Identifier(identifier)
            {}

            explicit TQueueClientId(const NKikimrBlobStorage::TMsgQoS& msgQoS);

            EQueueClientType GetType() const {
                return Type;
            }

            ui64 GetIdentifier() const {
                return Identifier;
            }

            void Serialize(NKikimrBlobStorage::TMsgQoS *msgQoS) const;

            bool IsRepl() const {
                return Type == EQueueClientType::ReplJob;
            }

            size_t Hash() const {
                return MultiHash(Type, Identifier);
            }

            bool operator ==(const TQueueClientId& other) const {
                return Type == other.Type && Identifier == other.Identifier;
            }

            void Output(IOutputStream &str) const;

            TString ToString() const;
        };

        ////////////////////////////////////////////////////////////////////////////
        // Status for window op/feedback
        ////////////////////////////////////////////////////////////////////////////
        using EStatus = NKikimrBlobStorage::TWindowFeedback::EStatus;

        bool Good(EStatus s);

        ////////////////////////////////////////////////////////////////////////////
        // TMessageId -- Status for window op/feedback
        ////////////////////////////////////////////////////////////////////////////
        struct TMessageId {
            ui64 SequenceId;    // id of sequence, incremented by client after failure of expected TMessageId
            ui64 MsgId;         // message id in sequence, increments on each message

            TMessageId()
                : SequenceId(0)
                , MsgId(0)
            {}

            explicit TMessageId(ui64 sequenceId, ui64 msgId)
                : SequenceId(sequenceId)
                , MsgId(msgId)
            {}

            TMessageId(const NKikimrBlobStorage::TMessageId &from)
                : SequenceId(from.GetSequenceId())
                , MsgId(from.GetMsgId())
            {}

            void Serialize(NKikimrBlobStorage::TMessageId &to) const;

            bool operator ==(const TMessageId &other) const {
                return SequenceId == other.SequenceId && MsgId == other.MsgId;
            }

            bool Empty() const {
                return SequenceId == 0 && MsgId == 0;
            }

            TString ToString() const;

            void Out(IOutputStream &str) const {
                str << "[" << SequenceId << " " << MsgId << "]";
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // Current window status (after operation on window or layout change)
        ////////////////////////////////////////////////////////////////////////////
        template <class TClientId>
        struct TWindowStatus {
            TClientId ClientId;
            TActorId ActorId;
            EStatus Status;
            bool Notify;
            ui64 ActualWindowSize;
            ui64 MaxWindowSize;
            TMessageId ExpectedMsgId;
            TMessageId FailedMsgId;

            TWindowStatus()
                : ClientId()
                , ActorId()
                , Status(NKikimrBlobStorage::TWindowFeedback::Unknown)
                , Notify(false)
                , ActualWindowSize(0)
                , MaxWindowSize(0)
                , ExpectedMsgId()
                , FailedMsgId()
            {}

            void Set(const TClientId &clientId, const TActorId& actorId, EStatus status, bool notify,
                    ui64 actualWindowSize, ui64 maxWindowSize, const TMessageId &expectedMsgId,
                    const TMessageId &failedMsgId) {
                ClientId = clientId;
                ActorId = actorId;
                Status = status;
                Notify = notify;
                ActualWindowSize = actualWindowSize;
                MaxWindowSize = maxWindowSize;
                ExpectedMsgId = expectedMsgId;
                FailedMsgId = failedMsgId;
            }

            void WindowUpdate(const TWindowStatus &s) {
                Y_ABORT_UNLESS(ClientId == s.ClientId);
                // don't touch Status
                Notify = Notify || s.Notify;
                ActualWindowSize = s.ActualWindowSize;
                MaxWindowSize = s.MaxWindowSize;
                Y_ABORT_UNLESS(ExpectedMsgId == s.ExpectedMsgId);
                // don't touch FailedMsgId
            }

            TString ToString() const {
                TStringStream str;
                str << "{Status# " << ui32(Status) << " Notify# " << Notify
                    << " ActualWindowSize# " << ActualWindowSize << " MaxWindowSize# " << MaxWindowSize
                    << " ExpectedMsgId# " << ExpectedMsgId.ToString() << " FailedMsgId# " << FailedMsgId.ToString()
                    << "}";
                return str.Str();
            }

            void Serialize(NKikimrBlobStorage::TWindowFeedback &to) const {
                to.SetStatus(Status);
                to.SetActualWindowSize(ActualWindowSize);
                to.SetMaxWindowSize(MaxWindowSize);
                ExpectedMsgId.Serialize(*to.MutableExpectedMsgId());
                if (!FailedMsgId.Empty())
                    FailedMsgId.Serialize(*to.MutableFailedMsgId());
            }
        };

    } // NBackpressure

    struct TVDiskSkeletonTrace {
        static constexpr ui32 BufferSize = 32;
        const char * Marks[BufferSize];
        ui32 MarkCount = 0;
        std::shared_ptr<TVDiskSkeletonTrace> AdditionalTrace;

        TVDiskSkeletonTrace() = default;

        void Clear() {
            MarkCount = 0;
            AdditionalTrace.reset();
        }

        void AddMark(const char * const mark);

        TString ToString() const;
    };

    struct TVMsgContext {
        const NBackpressure::TQueueClientId ClientId;
        const ui32 RecByteSize = 0;
        const NBackpressure::TMessageId MsgId;
        const ui64 Cost = 0;
        const NKikimrBlobStorage::EVDiskQueueId ExtQueueId = NKikimrBlobStorage::EVDiskQueueId::Unknown;
        const NKikimrBlobStorage::EVDiskInternalQueueId IntQueueId = NKikimrBlobStorage::EVDiskInternalQueueId::IntUnknown;
        const TActorId ActorId;
        const ui64 InternalMessageId = 0;

        TVMsgContext() = default;

        TVMsgContext(ui32 recByteSize, const NKikimrBlobStorage::TMsgQoS& msgQoS)
            : ClientId(NBackpressure::TQueueClientId(msgQoS))
            , RecByteSize(recByteSize)
            , MsgId(msgQoS.GetMsgId())
            , Cost(msgQoS.GetCost())
            , ExtQueueId(msgQoS.GetExtQueueId())
            , IntQueueId(msgQoS.GetIntQueueId())
            , ActorId(ActorIdFromProto(msgQoS.GetSenderActorId()))
            , InternalMessageId(msgQoS.GetInternalMessageId())
        {}

        void Output(IOutputStream &str) const;

        TString ToString() const;
    };

    struct TEvVDiskRequestCompleted
            : public TEventLocal<TEvVDiskRequestCompleted, TEvBlobStorage::EvVDiskRequestCompleted> {
        TVMsgContext Ctx;
        std::unique_ptr<IEventHandle> Event;
        bool DoNotResend;

        TEvVDiskRequestCompleted(const TVMsgContext &ctx, std::unique_ptr<IEventHandle> event, bool doNotResend = false)
            : Ctx(ctx)
            , Event(std::move(event))
            , DoNotResend(doNotResend)
        {
            Y_DEBUG_ABORT_UNLESS(Ctx.ExtQueueId != NKikimrBlobStorage::EVDiskQueueId::Unknown);
            Y_DEBUG_ABORT_UNLESS(Ctx.IntQueueId != NKikimrBlobStorage::EVDiskInternalQueueId::IntUnknown);
        }
    };

    typedef std::shared_ptr<TActorId> TActorIDPtr;

    class TVDiskNonlocalResultBase {
        ui32 Channel = 0;
        bool ResultSent = false;

    public:
        TVDiskNonlocalResultBase()
            : ResultSent(true)
        {}

        TVDiskNonlocalResultBase(ui32 channel)
            : Channel(channel)
        {}

        ui32 GetChannelToSend();
    };

    // Base class for all VDisk result events
    class TEvVResultBase : public TVDiskNonlocalResultBase {
    public:
        TEvVResultBase() = default;

        TEvVResultBase(const TInstant &now, ui32 channel, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr)
            : TVDiskNonlocalResultBase(channel)
            , Start(now)
            , Size(0)
            , CounterPtr(counterPtr)
            , HistoPtr(histoPtr)
        {}

        virtual ~TEvVResultBase() {
            //Y_ABORT_UNLESS(Finalized);
        }

        virtual void FinalizeAndSend(const TActorContext& /*ctx*/, std::unique_ptr<IEventHandle> ev);

    protected:
        void IncrementSize(ui64 size) {
            Size += size;
        }

    private:
        const TInstant Start;
        ui64 Size = 0;
        ::NMonitoring::TDynamicCounters::TCounterPtr CounterPtr;
        NVDiskMon::TLtcHistoPtr HistoPtr;
        bool Finalized = false;
    };

    template<typename TEv, typename TRecord /*protobuf record*/, ui32 TEventType>
    class TEvVResultBasePB
        : public TEventPB<TEv, TRecord, TEventType>
        , public TEvVResultBase
    {
        using TEventPBBase = TEventPB<TEv, TRecord, TEventType>;

    public:
        TEvVResultBasePB() = default;

        TEvVResultBasePB(const TInstant &now, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel)
            : TEvVResultBase(now, channel, counterPtr, histoPtr)
        {}
    };

    template<typename TEv, typename TRecord /*protobuf record*/, ui32 TEventType>
    class TEvVResultBaseWithQoSPB : public TEvVResultBasePB<TEv, TRecord, TEventType> {
        using TBase = TEvVResultBasePB<TEv, TRecord, TEventType>;

        TVMsgContext MsgCtx;
        TActorIDPtr SkeletonFrontIDPtr;
        THPTimer ExecutionTimer;

    protected:
        bool DoNotResendFromSkeletonFront = false;

    public:
        TEvVResultBaseWithQoSPB() = default;

        template<typename TQueryRecord>
        static inline TInstant GetReceivedTimestamp(const TQueryRecord *queryRecord, TInstant now) {
            if (queryRecord && queryRecord->HasMsgQoS() && queryRecord->GetMsgQoS().HasExecTimeStats()
                    && queryRecord->GetMsgQoS().GetExecTimeStats().HasReceivedTimestamp()) {
                return TInstant::MicroSeconds(queryRecord->GetMsgQoS().GetExecTimeStats().GetReceivedTimestamp());
            }
            return now;
        }

        template<typename TQueryRecord>
        TEvVResultBaseWithQoSPB(TInstant now, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel,
                ui32 recByteSize, TQueryRecord *queryRecord, const TActorIDPtr &skeletonFrontIDPtr)
            : TBase(GetReceivedTimestamp(queryRecord, now), counterPtr, histoPtr, channel)
            , MsgCtx(queryRecord ? TVMsgContext(recByteSize, queryRecord->GetMsgQoS()) : TVMsgContext())
            , SkeletonFrontIDPtr(skeletonFrontIDPtr)
        {
            if (queryRecord) {
                Y_ABORT_UNLESS(queryRecord->HasMsgQoS());
                auto *resultQoS = TBase::Record.MutableMsgQoS();
                resultQoS->Swap(queryRecord->MutableMsgQoS());
                resultQoS->ClearDeadlineSeconds();
                resultQoS->ClearSendMeCostSettings();
                resultQoS->ClearInternalMessageId();
            } else {
                Y_ABORT_UNLESS(!SkeletonFrontIDPtr);
            }
        }

        void MarkHugeWriteTime() {
            if (auto *stats = GetExecTimeStats()) {
                TDuration hugeWriteTime = TDuration::Seconds(ExecutionTimer.Passed());
                stats->SetHugeWriteTime(hugeWriteTime.GetValue());
            }
        }

        void FinalizeAndSend(const TActorContext &ctx, std::unique_ptr<IEventHandle> ev) override {
            TBase::FinalizeAndSend(ctx, nullptr);

            // update execution time stats
            if (auto *stats = GetExecTimeStats()) {
                TDuration execution = TDuration::Seconds(ExecutionTimer.Passed());
                stats->SetExecution(execution.GetValue());
                if (stats->HasReceivedTimestamp()) {
                    TInstant started = TInstant::MicroSeconds(stats->GetReceivedTimestamp());
                    TInstant now = TAppData::TimeProvider->Now();
                    TDuration total = now - started;
                    stats->SetTotal(total.GetValue());
                }
            }

            size_t byteSize = TBase::Record.ByteSize();
            Y_ABORT_UNLESS(byteSize <= NActors::EventMaxByteSize,
                "event suspiciously large: %zu\n%s",
                byteSize, this->ToString().data());

            if (SkeletonFrontIDPtr && MsgCtx.IntQueueId != NKikimrBlobStorage::IntUnknown) {
                ctx.Send(*SkeletonFrontIDPtr, new TEvVDiskRequestCompleted(MsgCtx, std::move(ev), DoNotResendFromSkeletonFront));
            } else {
                TActivationContext::Send(ev.release());
            }
        }

    private:
        NKikimrBlobStorage::TExecTimeStats *GetExecTimeStats() {
            auto *msgQoS = TBase::Record.MutableMsgQoS();
            return msgQoS->HasExecTimeStats() ? msgQoS->MutableExecTimeStats() : nullptr;
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TEvVPut
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    struct TEventWithRelevanceTracker {
        std::optional<TMessageRelevance> MessageRelevanceTracker;
    };

    struct TEvBlobStorage::TEvVPut
        : TEventPB<TEvBlobStorage::TEvVPut, NKikimrBlobStorage::TEvVPut, TEvBlobStorage::EvVPut>
        , TEventWithRelevanceTracker
    {
        // In current realization it is intentionaly lost on event serialization since
        // LWTrace doesn't support distributed shuttels yet
        mutable NLWTrace::TOrbit Orbit;

        // set to true in writes generated by defrag/scrub actor
        bool RewriteBlob = false;

        bool IsInternal = false;

        TEvVPut()
        {}

        TEvVPut(const TLogoBlobID &logoBlobId, TRope buffer, const TVDiskID &vdisk,
                const bool ignoreBlock, const ui64 *cookie, TInstant deadline,
                NKikimrBlobStorage::EPutHandleClass cls, bool checksumming,
                TWriteSource writeSource = UnknownWriteSource(),
                NKikimrBlobStorage::TDataKind::E dataKind = NKikimrBlobStorage::TDataKind::USER)
        {
            InitWithoutBuffer(logoBlobId, vdisk, ignoreBlock, cookie, deadline, cls, writeSource, dataKind);
            StorePayload(std::move(buffer), checksumming);
        }

        void InitWithoutBuffer(const TLogoBlobID &logoBlobId, const TVDiskID &vdisk, const bool ignoreBlock,
                const ui64 *cookie, TInstant deadline, NKikimrBlobStorage::EPutHandleClass cls,
                TWriteSource writeSource = UnknownWriteSource(),
                NKikimrBlobStorage::TDataKind::E dataKind = NKikimrBlobStorage::TDataKind::USER);

        bool GetIgnoreBlock() const {
            return Record.GetIgnoreBlock();
        }

        TRope GetBuffer() const {
            Y_ABORT_UNLESS(GetPayloadCount() == 1);
            return GetPayload(0);
        }

        void StorePayload(TRope&& buffer, bool checksumming);

        ui64 GetBufferBytes() const {
            ui64 sizeBytes = 0;
            const ui32 size = GetPayloadCount();
            for (ui32 i = 0; i < size; ++i) {
                sizeBytes += GetPayload(i).GetSize();
            }
            return sizeBytes;
        }

        bool Validate(TString& errorReason);

        TString ToString() const override {
            return ToString(*this);
        }

        static void OutMsgQos(const NKikimrBlobStorage::TMsgQoS &qos, TStringStream &str);

        static TString ToString(const TEvBlobStorage::TEvVPut& ev);
    };

    struct TEvBlobStorage::TEvVPutResult
            : public TEvVResultBaseWithQoSPB<TEvBlobStorage::TEvVPutResult,
                NKikimrBlobStorage::TEvVPutResult, TEvBlobStorage::EvVPutResult> {

        // In current realization it is intentionaly lost on event serialization since
        // LWTrace doesn't support distributed shuttels yet
        mutable NLWTrace::TOrbit Orbit;

        TDiskPart WrittenLocation;

        TEvVPutResult();

        TEvVPutResult(const NKikimrProto::EReplyStatus status, const TLogoBlobID &logoBlobId, const TVDiskID &vdisk,
                const ui64 *cookie, TOutOfSpaceStatus oosStatus, const TInstant &now, ui32 recByteSize,
                NKikimrBlobStorage::TEvVPut *queryRecord, const TActorIDPtr &skeletonFrontIDPtr,
                const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr, const NVDiskMon::TLtcHistoPtr &histoPtr,
                const ui64 bufferSizeBytes, ui64 incarnationGuid, const TString& errorReason);

        TString ToString() const override {
            return ToString(Record);
        }

        void UpdateStatus(const NKikimrProto::EReplyStatus status, std::optional<TString> errorReason = std::nullopt) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
        }

        static TString ToString(const NKikimrBlobStorage::TEvVPutResult &record);

        void MakeError(NKikimrProto::EReplyStatus status, const TString& errorReason,
                const NKikimrBlobStorage::TEvVPut &request);
    };

    struct TEvBlobStorage::TEvVMultiPut
        : TEventPB<TEvBlobStorage::TEvVMultiPut, NKikimrBlobStorage::TEvVMultiPut, TEvBlobStorage::EvVMultiPut>
        , TEventWithRelevanceTracker
    {
        mutable NLWTrace::TOrbit Orbit;

        TEvVMultiPut() = default;

        TEvVMultiPut(const TVDiskID &vdisk, TInstant deadline, NKikimrBlobStorage::EPutHandleClass cls,
                     bool ignoreBlock, const ui64 *cookie = nullptr);

        ui64 GetBufferBytes(ui64 idx) const {
            Y_DEBUG_ABORT_UNLESS(idx < Record.ItemsSize());
            return GetPayload(idx).GetSize();
        }

        ui64 GetBufferBytes() const {
            ui64 bytes = 0;
            ui32 size = GetPayloadCount();
            for (ui32 i = 0; i < size; ++i) {
                bytes += GetPayload(i).GetSize();
            }
            return bytes;
        }

        ui64 GetSumBlobSize() const {
            ui64 sum = 0;
            for (auto &item : Record.GetItems()) {
                TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(item.GetBlobID());
                sum += blobId.BlobSize();
            }
            return sum;
        }

        void StorePayload(const TRcBuf &buffer, NKikimrBlobStorage::TVMultiPutItem *item, bool checksumming);

        TRope GetItemBuffer(ui64 itemIdx) const;

        void AddVPut(const TLogoBlobID &logoBlobId, const TRcBuf &buffer, ui64 *cookie, bool issueKeepFlag, bool ignoreBlock,
                bool isZeroEntry, std::vector<std::pair<ui64, ui32>> *extraBlockChecks, NWilson::TTraceId traceId,
                bool checksumming,
                TWriteSource writeSource = UnknownWriteSource(),
                NKikimrBlobStorage::TDataKind::E dataKind = NKikimrBlobStorage::TDataKind::USER);

        bool Validate(TString& errorReason);

        TString ToString() const override;
    };

    struct TEvBlobStorage::TEvVMultiPutResult
        : public TEvVResultBaseWithQoSPB<TEvBlobStorage::TEvVMultiPutResult,
                NKikimrBlobStorage::TEvVMultiPutResult,
                TEvBlobStorage::EvVMultiPutResult>
    {
        mutable NLWTrace::TOrbit Orbit;

        TEvVMultiPutResult() = default;

        TEvVMultiPutResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const ui64 *cookie,
                const TInstant &now, ui32 recByteSize, NKikimrBlobStorage::TEvVMultiPut *record,
                const TActorIDPtr &skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr, const ui64 bufferSizeBytes, ui64 incarnationGuid,
                const TString& errorReason);

        void AddVPutResult(NKikimrProto::EReplyStatus status, const TString& errorReason, const TLogoBlobID &logoBlobId,
                ui64 *cookie, ui32 statusFlags = 0, bool writtenBeyondBarrier = false);

        void MakeError(NKikimrProto::EReplyStatus status, const TString& errorReason,
                const NKikimrBlobStorage::TEvVMultiPut &request);

        TString ToString() const override {
            return ToString(Record);
        }

        static TString ToString(const NKikimrBlobStorage::TEvVMultiPutResult &record);
    };

    //////////////////////////////////////////////////////////////////////////////////////////////
    // VGet
    //////////////////////////////////////////////////////////////////////////////////////////////

    struct TEvBlobStorage::TEvVGet
        : TEventPB<TEvBlobStorage::TEvVGet, NKikimrBlobStorage::TEvVGet, TEvBlobStorage::EvVGet>
        , TEventWithRelevanceTracker
    {
        bool IsInternal = false;
        // Local read checks can report CORRUPTED without starting recovery. Not serialized.
        bool SuppressCorruptedBlobRestoration = false;

        TEvVGet() = default;

        enum class EFlags : ui32 {
            None = 0,
            NotifyIfNotReady = 1,
            ShowInternals = 2,
        };

        using TForceBlockTabletData = TEvBlobStorage::TEvGet::TForceBlockTabletData;

        struct TExtremeQuery : std::tuple<TLogoBlobID, ui32, ui32, const ui64*> {
            TExtremeQuery(const TLogoBlobID &logoBlobId, ui32 sh, ui32 sz, const ui64 *cookie = nullptr)
                : std::tuple<TLogoBlobID, ui32, ui32, const ui64*>(logoBlobId, sh, sz, cookie)
            {}

            TExtremeQuery(const TLogoBlobID &logoBlobId)
                : TExtremeQuery(logoBlobId, 0, 0)
            {}
        };

        static std::unique_ptr<TEvVGet> CreateExtremeIndexQuery(const TVDiskID &vdisk, TInstant deadline,
                NKikimrBlobStorage::EGetHandleClass cls, EFlags flags = EFlags::None, TMaybe<ui64> requestCookie = {},
                std::initializer_list<TExtremeQuery> queries = {}, std::optional<TForceBlockTabletData> forceBlockTabletData = {}) {
            std::unique_ptr<TEvVGet> res(new TEvVGet(vdisk, deadline, cls, bool(ui32(flags) & ui32(EFlags::NotifyIfNotReady)),
                    bool(ui32(flags) & ui32(EFlags::ShowInternals)), requestCookie, true, true, forceBlockTabletData));
            for (const auto &q : queries) {
                res->AddExtremeQuery(std::get<0>(q), std::get<1>(q), std::get<2>(q), std::get<3>(q));
            }
            return res;
        }

        static std::unique_ptr<TEvVGet> CreateExtremeDataQuery(const TVDiskID &vdisk, TInstant deadline,
                NKikimrBlobStorage::EGetHandleClass cls, EFlags flags = EFlags::None, TMaybe<ui64> requestCookie = {},
                std::initializer_list<TExtremeQuery> queries = {}, std::optional<TForceBlockTabletData> forceBlockTabletData = {}) {
            std::unique_ptr<TEvVGet> res(new TEvVGet(vdisk, deadline, cls, bool(ui32(flags) & ui32(EFlags::NotifyIfNotReady)),
                    bool(ui32(flags) & ui32(EFlags::ShowInternals)), requestCookie, false, true, forceBlockTabletData));
            for (const auto &q : queries) {
                res->AddExtremeQuery(std::get<0>(q), std::get<1>(q), std::get<2>(q), std::get<3>(q));
            }
            return res;
        }

        static std::unique_ptr<TEvVGet> CreateRangeIndexQuery(const TVDiskID &vdisk, TInstant deadline,
                NKikimrBlobStorage::EGetHandleClass cls, EFlags flags, TMaybe<ui64> requestCookie,
                const TLogoBlobID &fromId, const TLogoBlobID &toId, ui32 maxResults = 0, const ui64 *cookie = nullptr,
                std::optional<TForceBlockTabletData> forceBlockTabletData = {}) {
            std::unique_ptr<TEvVGet> res(new TEvVGet(vdisk, deadline, cls, bool(ui32(flags) & ui32(EFlags::NotifyIfNotReady)),
                    bool(ui32(flags) & ui32(EFlags::ShowInternals)), requestCookie, true, false, forceBlockTabletData));
            NKikimrBlobStorage::TRangeQuery *q = res->Record.MutableRangeQuery();
            LogoBlobIDFromLogoBlobID(fromId, q->MutableFrom());
            LogoBlobIDFromLogoBlobID(toId, q->MutableTo());
            if (maxResults) {
                q->SetMaxResults(maxResults);
            }
            if (cookie) {
                q->SetCookie(*cookie);
            }
            return res;
        }

        void AddExtremeQuery(const TLogoBlobID &logoBlobId, ui32 sh, ui32 sz, const ui64 *cookie = nullptr) {
            Y_ABORT_UNLESS(Extreme);

            NKikimrBlobStorage::TExtremeQuery *q = Record.AddExtremeQueries();
            LogoBlobIDFromLogoBlobID(logoBlobId, q->MutableId());
            if (sh != 0)
                q->SetShift(sh);
            if (sz != 0)
                q->SetSize(sz);
            if (cookie)
                q->SetCookie(*cookie);
#if BS_EVVGET_SIZE_VERIFY
            if (Record.HasIndexOnly() && Record.GetIndexOnly()) {
                ExpectedReplySize += BlobProtobufHeaderMaxSize;
                Y_ABORT_UNLESS(TBlobStorageGroupType::GetMaxDataSizeWithErasureOverhead(ExpectedReplySize) <= MaxProtobufSize,
                        "Possible protobuf overflow for indexOnly get request. ExpectedReplySize# %" PRIu64
                        " MaxProtobufSize# %" PRIu64 " ExtremeQueries# %" PRIu64, (ui64)ExpectedReplySize,
                        (ui64)MaxProtobufSize, (ui64)Record.ExtremeQueriesSize());
            } else {
                ui32 blobSize = logoBlobId.BlobSize();
                if (!blobSize) {
                    blobSize = MaxBlobSize;
                }

                ui32 payloadSize = sz ? sz : blobSize;
                if (payloadSize + sh > blobSize) {
                    payloadSize = blobSize > sh ? blobSize - sh : 0;
                }
                ExpectedReplySize += BlobProtobufHeaderMaxSize + payloadSize;
                Y_ABORT_UNLESS(TBlobStorageGroupType::GetMaxDataSizeWithErasureOverhead(ExpectedReplySize) <= MaxProtobufSize,
                    "Possible protobuf overflow for get request. ExpectedReplySize# %" PRIu64
                    " MaxProtobufSize# %" PRIu64 " ExtremeQueries# %" PRIu64, (ui64)ExpectedReplySize,
                    (ui64)MaxProtobufSize, (ui64)Record.ExtremeQueriesSize());
            }
#endif
        }

        bool Validate(TString& errorReason);

        TString ToString() const override {
            return ToString(Record);
        }

        static TString ToString(const NKikimrBlobStorage::TEvVGet &record);

        bool GetIsLocalMon() const {
            return IsLocalMon;
        }

        void SetIsLocalMon() {
            IsLocalMon = true;
        }

    private:
        bool IsLocalMon = false;
        const bool Extreme = false;

#if BS_EVVGET_SIZE_VERIFY
        ui64 ExpectedReplySize = 0;
#endif

    private:
        TEvVGet(const TVDiskID &vdisk,
                TInstant deadline,
                NKikimrBlobStorage::EGetHandleClass cls,
                bool notifyIfNotReady,
                bool showInternals,
                TMaybe<ui64> requestCookie,
                bool indexOnly,
                bool extreme,
                std::optional<TForceBlockTabletData> forceBlockTabletData);
    };

    struct TEvBlobStorage::TEvVGetResult
        : public TEvVResultBaseWithQoSPB<TEvBlobStorage::TEvVGetResult,
                NKikimrBlobStorage::TEvVGetResult,
                TEvBlobStorage::EvVGetResult> {
        const bool EnablePayload = false;

        TEvVGetResult() = default;

        TEvVGetResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const TInstant &now,
                ui32 recByteSize, NKikimrBlobStorage::TEvVGet *queryRecord, const TActorIDPtr &skeletonFrontIDPtr,
                const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr, const NVDiskMon::TLtcHistoPtr &histoPtr,
                TMaybe<ui64> cookie, ui32 channel, ui64 incarnationGuid);

        void MarkRangeOverflow() {
            Record.SetIsRangeOverflow(true);
        }

        void AddResult(NKikimrProto::EReplyStatus status, const TLogoBlobID &logoBlobId, ui64 sh,
                       std::variant<TRope, ui32> dataOrSize, const ui64 *cookie = nullptr,
                       const ui64 *ingress = nullptr, bool keep = false, bool doNotKeep = false,
                       const ui64 *checksum = nullptr,
                       NKikimrBlobStorage::TChecksumType checksumType = NKikimrBlobStorage::TChecksumType::NoChecksum);

        void SetBlobData(NKikimrBlobStorage::TQueryResult& item, TRope&& data) {
            if (EnablePayload && data.size() >= 128) {
                item.SetPayloadId(AddPayload(std::move(data)));
            } else {
                item.SetBufferData(data.ConvertToString());
            }
        }

        void AddResult(NKikimrProto::EReplyStatus status, const TLogoBlobID &logoBlobId, const ui64 *cookie = nullptr,
                       const ui64 *ingress = nullptr, const NMatrix::TVectorType *local = nullptr, bool keep = false,
                       bool doNotKeep = false);

        bool HasBlob(const NKikimrBlobStorage::TQueryResult& result) const {
            return result.HasBufferData() || result.HasPayloadId();
        }

        ui32 GetBlobSize(const NKikimrBlobStorage::TQueryResult& result) const {
            return result.HasBufferData() ? result.GetBufferData().size()
                : result.HasPayloadId() ? GetPayload(result.GetPayloadId()).size()
                : 0;
        }

        TRope GetBlobData(const NKikimrBlobStorage::TQueryResult& result) const {
            return result.HasBufferData() ? TRope(result.GetBufferData())
                : result.HasPayloadId() ? GetPayload(result.GetPayloadId())
                : TRope();
        }

        TString ToString() const override;

        void MakeError(NKikimrProto::EReplyStatus status, const TString& errorReason,
                const NKikimrBlobStorage::TEvVGet &request);
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // VPatch
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    template <typename TEv, typename TRecord, ui32 EventType>
    struct TEvVSpecialPatchBase
        : TEventPB<TEv, TRecord, EventType>
        , TEventWithRelevanceTracker
    {
        mutable NLWTrace::TOrbit Orbit;
#if VDISK_SKELETON_TRACE
        std::shared_ptr<TVDiskSkeletonTrace> VDiskSkeletonTrace;
#endif

        TEvVSpecialPatchBase() = default;

        TEvVSpecialPatchBase(ui32 originalGroupId, ui32 patchedGroupId, const TLogoBlobID &originalBlobId,
                const TLogoBlobID &patchedBlobId, const TVDiskID &vdisk, bool ignoreBlock, TMaybe<ui64> cookie,
                TInstant deadline)
        {
            InitWithoutBuffer(originalGroupId, patchedGroupId, originalBlobId, patchedBlobId, vdisk, ignoreBlock,
                    cookie, deadline);
        }

        void InitWithoutBuffer(ui32 originalGroupId, ui32 patchedGroupId, const TLogoBlobID &originalBlobId,
                const TLogoBlobID &patchedBlobId, const TVDiskID &vdisk, bool ignoreBlock, TMaybe<ui64> cookie,
                TInstant deadline)
        {
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&originalBlobId, sizeof(originalBlobId));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&patchedBlobId, sizeof(patchedBlobId));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&vdisk, sizeof(vdisk));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&ignoreBlock, sizeof(ignoreBlock));

            this->Record.SetOriginalGroupId(originalGroupId);
            this->Record.SetPatchedGroupId(patchedGroupId);

            LogoBlobIDFromLogoBlobID(originalBlobId, this->Record.MutableOriginalBlobId());
            LogoBlobIDFromLogoBlobID(patchedBlobId, this->Record.MutablePatchedBlobId());

            VDiskIDFromVDiskID(vdisk, this->Record.MutableVDiskID());
            if (ignoreBlock) {
                this->Record.SetIgnoreBlock(ignoreBlock);
            }
            if (cookie) {
                this->Record.SetCookie(*cookie);
            }
            if (deadline != TInstant::Max()) {
                this->Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
            }
            this->Record.MutableMsgQoS()->SetExtQueueId(NKikimrBlobStorage::PutAsyncBlob);
        }

        bool GetIgnoreBlock() const {
            return this->Record.GetIgnoreBlock();
        }

        void AddDiff(ui64 startIdx, const TString &buffer) {
            TLogoBlobID id = LogoBlobIDFromLogoBlobID(this->Record.GetOriginalBlobId());
            Y_ABORT_UNLESS(startIdx < id.BlobSize());
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&buffer, sizeof(buffer));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(buffer.data(), buffer.size() + 1);
            Y_ABORT_UNLESS(startIdx + buffer.size() <= id.BlobSize());

            NKikimrBlobStorage::TDiffBlock *diffBlock = this->Record.AddDiffs();
            diffBlock->SetOffset(startIdx);
            diffBlock->SetBuffer(buffer);
        }

        void AddDiff(ui64 startIdx, const TRcBuf &buffer) {
            TLogoBlobID id = LogoBlobIDFromLogoBlobID(this->Record.GetOriginalBlobId());
            Y_ABORT_UNLESS(startIdx < id.BlobSize());
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&buffer, sizeof(buffer));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(buffer.data(), buffer.size() + 1);
            Y_ABORT_UNLESS(startIdx + buffer.size() <= id.BlobSize());

            NKikimrBlobStorage::TDiffBlock *diffBlock = this->Record.AddDiffs();
            diffBlock->SetOffset(startIdx);
            diffBlock->SetBuffer(buffer.data(), buffer.size());
        }

        TString ToString() const override {
            return ToString(this->Record);
        }

        static TString ToString(const TRecord &record) {
            TStringStream str;
            TLogoBlobID originalId = LogoBlobIDFromLogoBlobID(record.GetOriginalBlobId());
            TLogoBlobID patchedId = LogoBlobIDFromLogoBlobID(record.GetPatchedBlobId());
            str << "{TEvVMovedPatch";
            str << " OriginalBlobId# " << originalId.ToString();
            str << " PatchedBlobId# " << patchedId.ToString();
            str << " OriginalGroupId# " << record.GetOriginalBlobId();
            str << " PatchedGroupId# " << record.GetPatchedBlobId();
            if (record.GetIgnoreBlock()) {
                str << " IgnoreBlock";
            }
            if (record.HasMsgQoS()) {
                str << " ";
                TEvBlobStorage::TEvVPut::OutMsgQos(record.GetMsgQoS(), str);
            }
            str << " DiffCount# " << record.DiffsSize();
            str << "}";
            return str.Str();
        }

        ui32 DiffSizeSum() const {
            ui32 result = 0;
            for (auto &diff : this->Record.GetDiffs()) {
                result += diff.GetBuffer().size();
            }
            return result;
        }
    };

    template <typename TEv, typename TRecord, ui32 EventType, typename TReqRecord>
    struct TEvVSpecialPatchBaseResult
            : public TEvVResultBaseWithQoSPB<TEv, TRecord, EventType> {
        using TBase = TEvVResultBaseWithQoSPB<TEv, TRecord, EventType>;
        mutable NLWTrace::TOrbit Orbit;

        TEvVSpecialPatchBaseResult() = default;

        TEvVSpecialPatchBaseResult(const NKikimrProto::EReplyStatus status, const TLogoBlobID &originalBlobId,
                const TLogoBlobID &patchedBlobId, const TVDiskID &vdisk, TMaybe<ui64> cookie,
                TOutOfSpaceStatus oosStatus, const TInstant &now, ui32 recByteSize,
                TReqRecord *record, const TActorIDPtr &skeletonFrontIDPtr,
                const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr, const NVDiskMon::TLtcHistoPtr &histoPtr,
                ui64 incarnationGuid, const TString& errorReason)
            : TBase(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG, recByteSize, record, skeletonFrontIDPtr)
        {
            this->Record.SetStatus(status);
            LogoBlobIDFromLogoBlobID(originalBlobId, this->Record.MutableOriginalBlobId());
            LogoBlobIDFromLogoBlobID(patchedBlobId, this->Record.MutablePatchedBlobId());
            VDiskIDFromVDiskID(vdisk, this->Record.MutableVDiskID());
            if (cookie) {
                this->Record.SetCookie(*cookie);
            }
            this->Record.SetStatusFlags(oosStatus.Flags);
            this->Record.SetApproximateFreeSpaceShare(oosStatus.ApproximateFreeSpaceShare);
            if (record && record->HasTimestamps()) {
                this->Record.MutableTimestamps()->CopyFrom(record->GetTimestamps());
            }
            if (status == NKikimrProto::OK) {
                this->Record.SetIncarnationGuid(incarnationGuid);
            }
            if (errorReason && status != NKikimrProto::OK) {
                this->Record.SetErrorReason(errorReason);
            }
        }

        void UpdateStatus(const NKikimrProto::EReplyStatus status) {
            this->Record.SetStatus(status);
        }

        TString ToString() const override {
            return ToString(this->Record);
        }

        static TString ToString(const TRecord &record) {
            TStringStream str;
            TLogoBlobID originalId = LogoBlobIDFromLogoBlobID(record.GetOriginalBlobId());
            TLogoBlobID patchedId = LogoBlobIDFromLogoBlobID(record.GetPatchedBlobId());
            TVDiskID vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
            str << "{EvVMovedPatchResult Status# " << NKikimrProto::EReplyStatus_Name(record.GetStatus()).data();
            if (record.HasErrorReason()) {
                str << " ErrorReason# " << '"' << EscapeC(record.GetErrorReason()) << '"';
            }
            str << " OriginalBlobId# " << originalId;
            str << " PatchedBlobId# " << patchedId;
            str << " VDiskId# " << vdiskId;
            if (record.HasCookie()) {
                str << " Cookie# " << record.GetCookie();
            }
            if (record.HasMsgQoS()) {
                str << " ";
                TEvBlobStorage::TEvVPut::OutMsgQos(record.GetMsgQoS(), str);
            }
            str << "}";

            return str.Str();
        }

        void MakeError(NKikimrProto::EReplyStatus status, const TString& errorReason,
                const TReqRecord &request) {
            this->Record.SetStatus(status);
            if (status != NKikimrProto::OK && errorReason) {
                this->Record.SetErrorReason(errorReason);
            }
            Y_ABORT_UNLESS(request.HasOriginalBlobId());
            Y_ABORT_UNLESS(request.HasPatchedBlobId());
            TLogoBlobID originalBlobId = LogoBlobIDFromLogoBlobID(request.GetOriginalBlobId());
            TLogoBlobID patchedBlobId = LogoBlobIDFromLogoBlobID(request.GetPatchedBlobId());
            LogoBlobIDFromLogoBlobID(originalBlobId, this->Record.MutableOriginalBlobId());
            LogoBlobIDFromLogoBlobID(patchedBlobId, this->Record.MutablePatchedBlobId());

            Y_ABORT_UNLESS(request.HasVDiskID());
            TVDiskID vDiskId = VDiskIDFromVDiskID(request.GetVDiskID());
            VDiskIDFromVDiskID(vDiskId, this->Record.MutableVDiskID());
            if (request.HasCookie()) {
                this->Record.SetCookie(request.GetCookie());
            }
            if (request.HasTimestamps()) {
                this->Record.MutableTimestamps()->CopyFrom(request.GetTimestamps());
            }
        }
    };


    struct TEvBlobStorage::TEvVMovedPatch
        : TEvVSpecialPatchBase<
                TEvBlobStorage::TEvVMovedPatch,
                NKikimrBlobStorage::TEvVMovedPatch,
                TEvBlobStorage::EvVMovedPatch>
    {
        using TBase = TEvVSpecialPatchBase<
                TEvBlobStorage::TEvVMovedPatch,
                NKikimrBlobStorage::TEvVMovedPatch,
                TEvBlobStorage::EvVMovedPatch>;

        TEvVMovedPatch() = default;

        TEvVMovedPatch(ui32 originalGroupId, ui32 patchedGroupId, const TLogoBlobID &originalBlobId,
                const TLogoBlobID &patchedBlobId, const TVDiskID &vdisk, bool ignoreBlock, TMaybe<ui64> cookie,
                TInstant deadline)
            : TBase(originalGroupId, patchedGroupId, originalBlobId, patchedBlobId, vdisk, ignoreBlock,
                    cookie, deadline)
        {}
    };

    struct TEvBlobStorage::TEvVMovedPatchResult
        : TEvVSpecialPatchBaseResult<
                TEvBlobStorage::TEvVMovedPatchResult,
                NKikimrBlobStorage::TEvVMovedPatchResult,
                TEvBlobStorage::EvVMovedPatchResult,
                NKikimrBlobStorage::TEvVMovedPatch>
    {
        using TBase = TEvVSpecialPatchBaseResult<
                TEvBlobStorage::TEvVMovedPatchResult,
                NKikimrBlobStorage::TEvVMovedPatchResult,
                TEvBlobStorage::EvVMovedPatchResult,
                NKikimrBlobStorage::TEvVMovedPatch>;

        TEvVMovedPatchResult() = default;

        TEvVMovedPatchResult(const NKikimrProto::EReplyStatus status, const TLogoBlobID &originalBlobId,
                const TLogoBlobID &patchedBlobId, const TVDiskID &vdisk, TMaybe<ui64> cookie,
                TOutOfSpaceStatus oosStatus, const TInstant &now, ui32 recByteSize,
                NKikimrBlobStorage::TEvVMovedPatch *record, const TActorIDPtr &skeletonFrontIDPtr,
                const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr, const NVDiskMon::TLtcHistoPtr &histoPtr,
                ui64 incarnationGuid, const TString& errorReason)
            : TBase(status, originalBlobId, patchedBlobId, vdisk, cookie, oosStatus, now, recByteSize,
                    record, skeletonFrontIDPtr, counterPtr, histoPtr, incarnationGuid, errorReason)
        {}
    };


    struct TEvBlobStorage::TEvVInplacePatch
        : TEvVSpecialPatchBase<
                TEvBlobStorage::TEvVInplacePatch,
                NKikimrBlobStorage::TEvVInplacePatch,
                TEvBlobStorage::EvVInplacePatch>
    {
        using TBase = TEvVSpecialPatchBase<
                TEvBlobStorage::TEvVInplacePatch,
                NKikimrBlobStorage::TEvVInplacePatch,
                TEvBlobStorage::EvVInplacePatch>;

        TEvVInplacePatch() = default;

        TEvVInplacePatch(ui32 originalGroupId, ui32 patchedGroupId, const TLogoBlobID &originalBlobId,
                const TLogoBlobID &patchedBlobId, const TVDiskID &vdisk, bool ignoreBlock, TMaybe<ui64> cookie,
                TInstant deadline)
            : TBase(originalGroupId, patchedGroupId, originalBlobId, patchedBlobId, vdisk, ignoreBlock,
                    cookie, deadline)
        {}
    };

    struct TEvBlobStorage::TEvVInplacePatchResult
        : TEvVSpecialPatchBaseResult<
                TEvBlobStorage::TEvVInplacePatchResult,
                NKikimrBlobStorage::TEvVInplacePatchResult,
                TEvBlobStorage::EvVInplacePatchResult,
                NKikimrBlobStorage::TEvVInplacePatch>
    {
        using TBase = TEvVSpecialPatchBaseResult<
                TEvBlobStorage::TEvVInplacePatchResult,
                NKikimrBlobStorage::TEvVInplacePatchResult,
                TEvBlobStorage::EvVInplacePatchResult,
                NKikimrBlobStorage::TEvVInplacePatch>;

        TEvVInplacePatchResult() = default;

        TEvVInplacePatchResult(const NKikimrProto::EReplyStatus status, const TLogoBlobID &originalBlobId,
                const TLogoBlobID &patchedBlobId, const TVDiskID &vdisk, TMaybe<ui64> cookie,
                TOutOfSpaceStatus oosStatus, const TInstant &now, ui32 recByteSize,
                NKikimrBlobStorage::TEvVInplacePatch *record, const TActorIDPtr &skeletonFrontIDPtr,
                const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr, const NVDiskMon::TLtcHistoPtr &histoPtr,
                ui64 incarnationGuid, const TString& errorReason)
            : TBase(status, originalBlobId, patchedBlobId, vdisk, cookie, oosStatus, now, recByteSize,
                    record, skeletonFrontIDPtr, counterPtr, histoPtr, incarnationGuid, errorReason)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // VBLOCK
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    struct TEvBlobStorage::TEvVBlock
        : TEventPB<TEvBlobStorage::TEvVBlock, NKikimrBlobStorage::TEvVBlock, TEvBlobStorage::EvVBlock>
        , TEventWithRelevanceTracker
    {
        TEvVBlock()
        {}

        TEvVBlock(ui64 tabletId, ui32 generation, const TVDiskID &vdisk, TInstant deadline,
                TWriteSource writeSource = UnknownWriteSource(), ui64 issuerGuid = 0,
                std::optional<ui32> version = std::nullopt);
    };

    struct TEvBlobStorage::TEvVBlockResult
        : public TEvVResultBaseWithQoSPB<TEvBlobStorage::TEvVBlockResult,
                    NKikimrBlobStorage::TEvVBlockResult,
                    TEvBlobStorage::EvVBlockResult> {
        struct TTabletActGen {
            ui64 TabletId = 0;
            ui32 ActualGen = 0;

            TTabletActGen(ui64 tabletId, ui32 actualGen)
                : TabletId(tabletId)
                , ActualGen(actualGen)
            {}
        };
        TEvVBlockResult() = default;

        TEvVBlockResult(NKikimrProto::EReplyStatus status, const TTabletActGen *actual, const TVDiskID &vdisk,
                const TInstant &now, ui32 recByteSize, NKikimrBlobStorage::TEvVBlock *queryRecord,
                const TActorIDPtr &skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr, ui64 incarnationGuid);

        void UpdateStatus(NKikimrProto::EReplyStatus status, ui64 tabletId, ui32 actualGen) {
            Record.SetStatus(status);
            Record.SetTabletId(tabletId);
            Record.SetGeneration(actualGen);
        }

        TString ToString() const override;

        void MakeError(NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                const NKikimrBlobStorage::TEvVBlock &request);
    };

    //////////////////////////////////////////////////////////////////////////////////////////////
    // VPatch
    //////////////////////////////////////////////////////////////////////////////////////////////

    struct TEvBlobStorage::TEvVPatchStart
        : TEventPB<TEvBlobStorage::TEvVPatchStart, NKikimrBlobStorage::TEvVPatchStart, TEvBlobStorage::EvVPatchStart>
        , TEventWithRelevanceTracker
    {
        mutable NLWTrace::TOrbit Orbit;
#if VDISK_SKELETON_TRACE
        std::shared_ptr<TVDiskSkeletonTrace> VDiskSkeletonTrace;
#endif

        TEvVPatchStart() = default;

        TEvVPatchStart(const TLogoBlobID &originalBlobId, const TLogoBlobID &patchedBlobId, const TVDiskID &vDiskId,
                TInstant deadline, TMaybe<ui64> cookie, bool notifyIfNotReady);

        TString ToString() const {
            return ToString(this->Record);
        }

        static TString ToString(const NKikimrBlobStorage::TEvVPatchStart &record);
    };

    struct TEvBlobStorage::TEvVPatchFoundParts
        : public TEvVResultBaseWithQoSPB<TEvBlobStorage::TEvVPatchFoundParts,
                NKikimrBlobStorage::TEvVPatchFoundParts,
                TEvBlobStorage::EvVPatchFoundParts>
    {
        mutable NLWTrace::TOrbit Orbit;

        TEvVPatchFoundParts() = default;

        TEvVPatchFoundParts(NKikimrProto::EReplyStatus status, const TLogoBlobID &originalBlobId,
                const TLogoBlobID &patchedBlobId, const TVDiskID &vDiskId, TMaybe<ui64> cookie, const TInstant &now,
                const TString &errorReason, NKikimrBlobStorage::TEvVPatchStart *record,
                const TActorIDPtr &skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr, ui64 incarnationGuid);

        void AddPart(ui8 part) {
            Record.AddOriginalParts(part);
        }

        void SetStatus(NKikimrProto::EReplyStatus status) {
            Record.SetStatus(status);
        }

        TString ToString() const {
            return ToString(this->Record);
        }

        static TString ToString(const NKikimrBlobStorage::TEvVPatchFoundParts &record);

        void MakeError(NKikimrProto::EReplyStatus status, const TString& errorReason,
                const NKikimrBlobStorage::TEvVPatchStart &request);
    };

    struct TEvBlobStorage::TEvVPatchDiff
        : TEventPB<TEvBlobStorage::TEvVPatchDiff, NKikimrBlobStorage::TEvVPatchDiff, TEvBlobStorage::EvVPatchDiff>
        , TEventWithRelevanceTracker
    {
        mutable NLWTrace::TOrbit Orbit;
#if VDISK_SKELETON_TRACE
        std::shared_ptr<TVDiskSkeletonTrace> VDiskSkeletonTrace;
#endif

        TEvVPatchDiff() = default;

        TEvVPatchDiff(const TLogoBlobID &originalPartBlobId, const TLogoBlobID &patchedPartBlobId, const TVDiskID &vDiskId,
                ui32 expectedXorDiffs, TInstant deadline, TMaybe<ui64> cookie);

        void AddDiff(ui64 startIdx, const TString &buffer);

        void AddDiff(ui64 startIdx, const TRcBuf &buffer);

        void AddXorReceiver(const TVDiskID &vDiskId, ui8 partId);

        void SetForceEnd() {
            Record.SetForceEnd(true);
        }

        bool IsForceEnd() const {
            return Record.HasForceEnd() && Record.GetForceEnd();
        }

        bool IsXorReceiver() const {
            return Record.HasExpectedXorDiffs() && Record.GetExpectedXorDiffs();
        }

        ui32 GetExpectedXorDiffs() const {
            return Record.HasExpectedXorDiffs() ? Record.GetExpectedXorDiffs() : 0;
        }

        ui32 DiffSizeSum() const {
            ui32 result = 0;
            for (auto &diff : Record.GetDiffs()) {
                result += diff.GetBuffer().size();
            }
            return result;
        }

        TString ToString() const {
            return ToString(this->Record);
        }

        static TString ToString(const NKikimrBlobStorage::TEvVPatchDiff &record);
    };


    struct TEvBlobStorage::TEvVPatchXorDiff
        : TEventPB<TEvBlobStorage::TEvVPatchXorDiff, NKikimrBlobStorage::TEvVPatchXorDiff, TEvBlobStorage::EvVPatchXorDiff>
        , TEventWithRelevanceTracker
    {
        mutable NLWTrace::TOrbit Orbit;
#if VDISK_SKELETON_TRACE
        std::shared_ptr<TVDiskSkeletonTrace> VDiskSkeletonTrace;
#endif

        TEvVPatchXorDiff() = default;

        TEvVPatchXorDiff(const TLogoBlobID &originalPartBlobId, const TLogoBlobID &patchedPartBlobId, const TVDiskID &vDiskId,
                ui8 partId, TInstant deadline, TMaybe<ui64> cookie);

        void AddDiff(ui64 startIdx, const TRcBuf &buffer);

        ui32 DiffSizeSum() const {
            ui32 result = 0;
            for (auto &diff : Record.GetDiffs()) {
                result += diff.GetBuffer().size();
            }
            return result;
        }

        TString ToString() const {
            return ToString(this->Record);
        }

        static TString ToString(const NKikimrBlobStorage::TEvVPatchXorDiff &record);
    };

    struct TEvBlobStorage::TEvVPatchXorDiffResult
        : public TEvVResultBaseWithQoSPB<TEvBlobStorage::TEvVPatchXorDiffResult,
                NKikimrBlobStorage::TEvVPatchXorDiffResult,
                TEvBlobStorage::EvVPatchXorDiffResult>
    {
        mutable NLWTrace::TOrbit Orbit;

        TEvVPatchXorDiffResult() = default;

        TEvVPatchXorDiffResult(NKikimrProto::EReplyStatus status, TInstant now,
                NKikimrBlobStorage::TEvVPatchXorDiff *record, const TActorIDPtr &skeletonFrontIDPtr,
                const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr);

        void MakeError(NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                const NKikimrBlobStorage::TEvVPatchXorDiff &request);
    };

    struct TEvBlobStorage::TEvVPatchResult
        : public TEvVResultBaseWithQoSPB<TEvBlobStorage::TEvVPatchResult,
                NKikimrBlobStorage::TEvVPatchResult,
                TEvBlobStorage::EvVPatchResult>
    {
        mutable NLWTrace::TOrbit Orbit;

        TEvVPatchResult() = default;

        TEvVPatchResult(NKikimrProto::EReplyStatus status, const TLogoBlobID &originalPartBlobId,
                const TLogoBlobID &patchedPartBlobId, const TVDiskID &vDiskId, TMaybe<ui64> cookie, TInstant now,
                NKikimrBlobStorage::TEvVPatchDiff *record, const TActorIDPtr &skeletonFrontIDPtr,
                const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr, const NVDiskMon::TLtcHistoPtr &histoPtr,
                ui64 incarnationGuid);

        void SetStatus(NKikimrProto::EReplyStatus status, const TString &errorReason) {
            Record.SetStatus(status);
            Record.SetErrorReason(errorReason);
        }

        void SetStatusFlagsAndFreeSpace(ui32 statusFlags, float approximateFreeSpaceShare) {
            Record.SetStatusFlags(statusFlags);
            Record.SetApproximateFreeSpaceShare(approximateFreeSpaceShare);
        }

        void SetForceEndResponse() {
            DoNotResendFromSkeletonFront = true;
        }

        void MakeError(NKikimrProto::EReplyStatus status, const TString &errorReason,
                const NKikimrBlobStorage::TEvVPatchDiff &request);
    };

    struct TEvBlobStorage::TEvVGetBlock
        : TEventPB<TEvBlobStorage::TEvVGetBlock, NKikimrBlobStorage::TEvVGetBlock, TEvBlobStorage::EvVGetBlock>
        , TEventWithRelevanceTracker
    {
        TEvVGetBlock()
        {}

        TEvVGetBlock(ui64 tabletId, const TVDiskID &vdisk, TInstant deadline);
    };

    struct TEvBlobStorage::TEvVGetBlockResult
        : public TEvVResultBaseWithQoSPB<TEvBlobStorage::TEvVGetBlockResult,
                        NKikimrBlobStorage::TEvVGetBlockResult,
                        TEvBlobStorage::EvVGetBlockResult> {
        TEvVGetBlockResult() = default;

        TEvVGetBlockResult(NKikimrProto::EReplyStatus status, ui64 tabletId, const TVDiskID &vdisk,
                const TInstant &now, ui32 recByteSize, NKikimrBlobStorage::TEvVGetBlock *queryRecord,
                const TActorIDPtr &skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr);

        TEvVGetBlockResult(NKikimrProto::EReplyStatus status, ui64 tabletId, ui32 generation, const TVDiskID &vdisk,
                const TInstant &now, ui32 recByteSize, NKikimrBlobStorage::TEvVGetBlock *queryRecord,
                const TActorIDPtr &skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr);

        TString ToString() const override;

        void MakeError(NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                const NKikimrBlobStorage::TEvVGetBlock &request);
    };


    struct TEvBlobStorage::TEvVCollectGarbage
        : TEventPB<TEvBlobStorage::TEvVCollectGarbage, NKikimrBlobStorage::TEvVCollectGarbage, TEvBlobStorage::EvVCollectGarbage>
        , TEventWithRelevanceTracker
    {
        TEvVCollectGarbage()
        {}

        explicit
        TEvVCollectGarbage(ui64 tabletId, ui32 recordGeneration, ui32 perGenerationCounter, ui32 channel, bool collect,
            ui32 collectGeneration, ui32 collectStep, bool hard,
            const TVector<TLogoBlobID> *keep, const TVector<TLogoBlobID> *doNotKeep,
            const TVDiskID &vdisk, TInstant deadline,
            TWriteSource writeSource = UnknownWriteSource());

        TString ToString() const override;
    };

    struct TEvBlobStorage::TEvVCollectGarbageResult
        : public TEvVResultBaseWithQoSPB<TEvBlobStorage::TEvVCollectGarbageResult,
                    NKikimrBlobStorage::TEvVCollectGarbageResult,
                    TEvBlobStorage::EvVCollectGarbageResult>
    {
        TEvVCollectGarbageResult() = default;

        TEvVCollectGarbageResult(NKikimrProto::EReplyStatus status, ui64 tabletId, ui32 recordGeneration, ui32 channel,
                const TVDiskID &vdisk, const TInstant &now, ui32 recByteSize,
                NKikimrBlobStorage::TEvVCollectGarbage *queryRecord, const TActorIDPtr &skeletonFrontIDPtr,
                const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr, const NVDiskMon::TLtcHistoPtr &histoPtr,
                ui64 incarnationGuid);

        void UpdateStatus(NKikimrProto::EReplyStatus status) {
            Record.SetStatus(status);
        }

        TString ToString() const override;

        void MakeError(NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                const NKikimrBlobStorage::TEvVCollectGarbage &request);
    };

    struct TKeyBarrier;
    struct TMemRecBarrier;

    struct TEvBlobStorage::TEvVGetBarrier
        : TEventPB<TEvBlobStorage::TEvVGetBarrier, NKikimrBlobStorage::TEvVGetBarrier, TEvBlobStorage::EvVGetBarrier>
        , TEventWithRelevanceTracker
    {
        TEvVGetBarrier()
        {}

        TEvVGetBarrier(const TVDiskID &vdisk, const TKeyBarrier &from, const TKeyBarrier &to, ui32 *maxResults,
                bool showInternals);

        TString ToString() const override;
    };

    struct TEvBlobStorage::TEvVGetBarrierResult
        : public TEvVResultBaseWithQoSPB<TEvBlobStorage::TEvVGetBarrierResult,
                        NKikimrBlobStorage::TEvVGetBarrierResult,
                        TEvBlobStorage::EvVGetBarrierResult> {
        TEvVGetBarrierResult() = default;

        TEvVGetBarrierResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const TInstant &now,
                ui32 recByteSize, NKikimrBlobStorage::TEvVGetBarrier *queryRecord,
                const TActorIDPtr &skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr);

        void AddResult(const TKeyBarrier &key, const TMemRecBarrier &memRec, bool showInternals);

        void MakeError(NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                const NKikimrBlobStorage::TEvVGetBarrier &request);
    };

    struct TEvBlobStorage::TEvVCheckReadiness
        : TEventPB<TEvBlobStorage::TEvVCheckReadiness, NKikimrBlobStorage::TEvVCheckReadiness, TEvBlobStorage::EvVCheckReadiness>
    {
        TEvVCheckReadiness() = default;

        TEvVCheckReadiness(bool notifyIfNotReady) {
            Record.SetNotifyIfNotReady(notifyIfNotReady);
        }
    };

    struct TEvBlobStorage::TEvVCheckReadinessResult
        : public TEventPB<TEvBlobStorage::TEvVCheckReadinessResult,
                    NKikimrBlobStorage::TEvVCheckReadinessResult,
                    EvVCheckReadinessResult>
    {
        TEvVCheckReadinessResult() = default;

        TEvVCheckReadinessResult(NKikimrProto::EReplyStatus status, bool checksumming);
    };

    struct TEvBlobStorage::TEvVCompact
        : public TEventPB<TEvBlobStorage::TEvVCompact, NKikimrBlobStorage::TEvVCompact, TEvBlobStorage::EvVCompact>
    {
        TEvVCompact() = default;

        TEvVCompact(const TVDiskID &vdisk, NKikimrBlobStorage::TEvVCompact::EOpType opType);
    };

    struct TEvBlobStorage::TEvVCompactResult
        : public TEventPB<TEvBlobStorage::TEvVCompactResult,
                    NKikimrBlobStorage::TEvVCompactResult,
                    TEvBlobStorage::EvVCompactResult>
    {
        TEvVCompactResult() = default;

        TEvVCompactResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk);

        TEvVCompactResult(NKikimrProto::EReplyStatus status, const NKikimrBlobStorage::TVDiskID &vdisk);
    };

    struct TEvBlobStorage::TEvVDefrag
        : public TEventPB<TEvBlobStorage::TEvVDefrag, NKikimrBlobStorage::TEvVDefrag, TEvBlobStorage::EvVDefrag>
    {
        TEvVDefrag() = default;

        TEvVDefrag(const TVDiskID &vdisk, bool full);
    };

    struct TEvBlobStorage::TEvVDefragResult
        : public TEventPB<TEvBlobStorage::TEvVDefragResult,
                    NKikimrBlobStorage::TEvVDefragResult,
                    TEvBlobStorage::EvVDefragResult>
    {
        TEvVDefragResult() = default;

        TEvVDefragResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk);

        TEvVDefragResult(NKikimrProto::EReplyStatus status, const NKikimrBlobStorage::TVDiskID &vdisk);
    };

    struct TEvBlobStorage::TEvVBaldSyncLog
        : public TEventPB<TEvBlobStorage::TEvVBaldSyncLog,
                    NKikimrBlobStorage::TEvVBaldSyncLog,
                    TEvBlobStorage::EvVBaldSyncLog>
    {
        TEvVBaldSyncLog() = default;

        TEvVBaldSyncLog(const TVDiskID &vdisk, bool dropChunksExplicitly = false);
    };

    struct TEvBlobStorage::TEvVBaldSyncLogResult
        : public TEventPB<TEvBlobStorage::TEvVBaldSyncLogResult,
                    NKikimrBlobStorage::TEvVBaldSyncLogResult,
                    TEvBlobStorage::EvVBaldSyncLogResult>
    {
        TEvVBaldSyncLogResult()
        {}

        TEvVBaldSyncLogResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk);

        TEvVBaldSyncLogResult(NKikimrProto::EReplyStatus status, const NKikimrBlobStorage::TVDiskID &vdisk);
    };

    struct TEvBlobStorage::TEvVDbStat
        : public TEventPB<TEvBlobStorage::TEvVDbStat,
                    NKikimrBlobStorage::TEvVDbStat,
                    TEvBlobStorage::EvVDbStat>
    {
        TEvVDbStat()
        {}

        // stat or dump db
        TEvVDbStat(const TVDiskID &vdisk, NKikimrBlobStorage::EDbStatAction action,
                   NKikimrBlobStorage::EDbStatType dbStatType, bool pretty);

        // stat tablet
        TEvVDbStat(const TVDiskID &vdisk, ui64 tabletId, bool pretty);
    };

    struct TEvBlobStorage::TEvVDbStatResult
        : public TEvVResultBasePB<TEvBlobStorage::TEvVDbStatResult,
                    NKikimrBlobStorage::TEvVDbStatResult,
                    TEvBlobStorage::EvVDbStatResult>
    {
        TEvVDbStatResult() = default;

        TEvVDbStatResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const TInstant &now,
                const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr, const NVDiskMon::TLtcHistoPtr &histoPtr);

        void SetError() {
            Record.SetStatus(NKikimrProto::ERROR);
        }

        void SetResult(const TString &data) {
            Record.SetData(data);
        }
    };

    struct TEvBlobStorage::TEvVAssimilate : TEventPB<TEvVAssimilate, NKikimrBlobStorage::TEvVAssimilate, EvVAssimilate> {
        TEvVAssimilate() = default;

        TEvVAssimilate(const TVDiskID& vdiskId, std::optional<ui64> skipBlocksUpTo,
                std::optional<std::tuple<ui64, ui8>> skipBarriersUpTo, std::optional<TLogoBlobID> skipBlobsUpTo,
                bool ignoreDecommitState, bool reverse);
    };

    struct TEvBlobStorage::TEvVAssimilateResult : TEventPB<TEvVAssimilateResult, NKikimrBlobStorage::TEvVAssimilateResult, EvVAssimilateResult>
    {
        TEvVAssimilateResult() = default;

        TEvVAssimilateResult(NKikimrProto::EReplyStatus status, TString errorReason, TVDiskID vdiskId);
    };

    struct TEvBlobStorage::TEvVReadyNotify
        : public TEventPB<TEvBlobStorage::TEvVReadyNotify,
                NKikimrBlobStorage::TEvVReadyNotify,
                TEvBlobStorage::EvVReadyNotify>
    {
    };

    //////////////////////////////////////////////////////////////////////////////////////////////
    // VSyncGuid
    //////////////////////////////////////////////////////////////////////////////////////////////
    struct TEvBlobStorage::TEvVSyncGuid
        : public TEventPB<TEvBlobStorage::TEvVSyncGuid,
                    NKikimrBlobStorage::TEvVSyncGuid,
                    TEvBlobStorage::EvVSyncGuid>
    {
        TEvVSyncGuid()
        {}

        // read request
        TEvVSyncGuid(const TVDiskID &sourceVDisk, const TVDiskID &targetVDisk);

        // write request
        TEvVSyncGuid(const TVDiskID &sourceVDisk,
                     const TVDiskID &targetVDisk,
                     ui64 guid,
                     NKikimrBlobStorage::TSyncGuidInfo::EState state);
    };

    struct TEvBlobStorage::TEvVSyncGuidResult
        : public TEvVResultBasePB<TEvBlobStorage::TEvVSyncGuidResult,
                    NKikimrBlobStorage::TEvVSyncGuidResult,
                    TEvBlobStorage::EvVSyncGuidResult>
    {
        // from protobuf
        using EState = NKikimrBlobStorage::TSyncGuidInfo::EState;

        TEvVSyncGuidResult() = default;

        // read response
        TEvVSyncGuidResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const TInstant &now,
                ui64 guid, EState state, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel);

        // write or error response
        TEvVSyncGuidResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const TInstant &now,
                const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel);

        void Output(IOutputStream &str) const;

        TString ToString() const override;
    };

    //////////////////////////////////////////////////////////////////////////////////////////////
    // VSync
    //////////////////////////////////////////////////////////////////////////////////////////////
    struct TEvBlobStorage::TEvVSync
        : public TEventPB<TEvBlobStorage::TEvVSync,
                    NKikimrBlobStorage::TEvVSync,
                    TEvBlobStorage::EvVSync> {
        TEvVSync()
        {}

        TEvVSync(const TSyncState &syncState, const TVDiskID &sourceVDisk, const TVDiskID &targetVDisk);
    };

    struct TEvBlobStorage::TEvVSyncResult
        : public TEvVResultBasePB<TEvBlobStorage::TEvVSyncResult,
                    NKikimrBlobStorage::TEvVSyncResult,
                    TEvBlobStorage::EvVSyncResult> {
        TEvVSyncResult() = default;

        TEvVSyncResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk,
                       const TSyncState &newSyncState, bool finished, NPDisk::TStatusFlags flags, const TInstant &now,
                       const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                       const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel);

        TEvVSyncResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, NPDisk::TStatusFlags flags,
                const TInstant &now, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel);

        TString ToString() const override;
    };

    struct TEvBlobStorage::TEvVSyncFull
        : public TEventPB<TEvBlobStorage::TEvVSyncFull,
                    NKikimrBlobStorage::TEvVSyncFull,
                    TEvBlobStorage::EvVSyncFull>
    {
        TEvVSyncFull()
        {}

        TEvVSyncFull(const TSyncState &syncState, const TVDiskID &sourceVDisk, const TVDiskID &targetVDisk,
                ui64 cookie, NKikimrBlobStorage::ESyncFullStage stage,
                const TLogoBlobID &logoBlobFrom, ui64 blockTabletFrom, const TKeyBarrier &barrierFrom,
                NKikimrBlobStorage::EFullSyncProtocol protocol = NKikimrBlobStorage::EFullSyncProtocol::Legacy);

        bool IsInitial() const {
            return Record.GetCookie() == 0;
        }

        NKikimrBlobStorage::EFullSyncProtocol GetProtocol();
    };

    struct TEvBlobStorage::TEvVSyncFullResult
        : public TEvVResultBasePB<TEvBlobStorage::TEvVSyncFullResult,
                    NKikimrBlobStorage::TEvVSyncFullResult,
                    TEvBlobStorage::EvVSyncFullResult>
    {
        TEvVSyncFullResult() = default;

        TEvVSyncFullResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const TSyncState &syncState,
                ui64 cookie, const TInstant &now, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel, NKikimrBlobStorage::EFullSyncProtocol protocol);

        TEvVSyncFullResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, ui64 cookie,
                const TInstant &now, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel, NKikimrBlobStorage::EFullSyncProtocol protocol);

        TString ToString() const override;
    };

    struct TEvBlobStorage::TEvVWindowChange
        : public TEventPB<TEvBlobStorage::TEvVWindowChange,
                    NKikimrBlobStorage::TEvVWindowChange,
                    TEvBlobStorage::EvVWindowChange>
    {
        static constexpr struct TDropConnection {} DropConnection{};

        TEvVWindowChange()
        {}

        template<typename TClientId>
        TEvVWindowChange(NKikimrBlobStorage::EVDiskQueueId queueId,
                const typename NBackpressure::TWindowStatus<TClientId> &wstatus) {
            Record.SetFrontQueueId(queueId);
            wstatus.Serialize(*Record.MutableWindow());
        }

        TEvVWindowChange(NKikimrBlobStorage::EVDiskQueueId queueId, TDropConnection);

        TString ToString() const override {
            return SingleLineProto(Record);
        }
    };

    struct TEvVGenerationChange: public TEventLocal<TEvVGenerationChange, TEvBlobStorage::EvVGenerationChange> {
        TVDiskID NewVDiskId;
        TIntrusivePtr<TBlobStorageGroupInfo> NewInfo;

        TEvVGenerationChange(const TVDiskID &newVDiskId, const TIntrusivePtr<TBlobStorageGroupInfo> &newInfo)
            : NewVDiskId(newVDiskId)
            , NewInfo(newInfo)
        {}

        TEvVGenerationChange *Clone() {
            return new TEvVGenerationChange(NewVDiskId, NewInfo);
        }

        TString ToString() const override {
            return TStringBuilder() << "{TEvVGenerationChange NewVDiskId# " << NewVDiskId << " NewInfo# "
                << NewInfo->ToString() << "}" ;
        }
    };

    struct TEvBlobStorage::TEvUpdateGroupInfo : TEventLocal<TEvUpdateGroupInfo, EvUpdateGroupInfo> {
        const TGroupId GroupId;
        const ui32 GroupGeneration;
        std::optional<NKikimrBlobStorage::TGroupInfo> GroupInfo;

        TEvUpdateGroupInfo(TGroupId groupId, ui32 groupGeneration, std::optional<NKikimrBlobStorage::TGroupInfo> groupInfo)
            : GroupId(groupId)
            , GroupGeneration(groupGeneration)
            , GroupInfo(std::move(groupInfo))
        {}
    };

    struct TEvBlobStorage::TEvEnrichNotYet : TEventLocal<TEvEnrichNotYet, EvEnrichNotYet> {
        TEvVGet::TPtr Query;
        std::unique_ptr<TEvVGetResult> Result;

        TEvEnrichNotYet(TEvVGet::TPtr query, std::unique_ptr<TEvVGetResult> result)
            : Query(std::move(query))
            , Result(std::move(result))
        {}
    };

    struct TEvBlobStorage::TEvCaptureVDiskLayout : TEventLocal<TEvCaptureVDiskLayout, EvCaptureVDiskLayout>
    {};

    struct TDiskPart;

    struct TEvBlobStorage::TEvCaptureVDiskLayoutResult : TEventLocal<TEvCaptureVDiskLayoutResult, EvCaptureVDiskLayoutResult> {
        enum class EDatabase {
            LogoBlobs,
            Blocks,
            Barriers
        };

        enum class ERecordType {
            HugeBlob,
            InplaceBlob,
            IndexRecord,
        };

        struct TLayoutRecord {
            TDiskPart Location;
            EDatabase Database;
            ERecordType RecordType;
            TLogoBlobID BlobId; // for HugeBlob/InplaceBlob
            ui64 SstId; // for IndexRecord
            ui32 Level; // for IndexRecord

            TString ToString() const;
        };

        std::vector<TLayoutRecord> Layout;
    };

    struct TEvBlobStorage::TEvVTakeSnapshot : TEventPB<TEvVTakeSnapshot, NKikimrBlobStorage::TEvVTakeSnapshot, EvVTakeSnapshot> {
        TEvVTakeSnapshot() = default;

        TEvVTakeSnapshot(TVDiskID vdiskId, const TString& snapshotId, ui32 timeToLiveSec);
    };

    struct TEvBlobStorage::TEvVTakeSnapshotResult : TEventPB<TEvVTakeSnapshotResult, NKikimrBlobStorage::TEvVTakeSnapshotResult, EvVTakeSnapshotResult> {
        TEvVTakeSnapshotResult() = default;

        TEvVTakeSnapshotResult(NKikimrProto::EReplyStatus status, const TString& errorReason, TVDiskID vdiskId);
    };

    struct TEvBlobStorage::TEvVReleaseSnapshot : TEventPB<TEvVReleaseSnapshot, NKikimrBlobStorage::TEvVReleaseSnapshot, EvVReleaseSnapshot> {
        TEvVReleaseSnapshot() = default;

        TEvVReleaseSnapshot(TVDiskID vdiskId, const TString& snapshotId);
    };

    struct TEvBlobStorage::TEvVReleaseSnapshotResult : TEventPB<TEvVReleaseSnapshotResult, NKikimrBlobStorage::TEvVReleaseSnapshotResult, EvVReleaseSnapshotResult> {
        TEvVReleaseSnapshotResult() = default;

        TEvVReleaseSnapshotResult(NKikimrProto::EReplyStatus status, const TString& errorReason, TVDiskID vdiskId);
    };

    //////////////////////////////////////////////////////////////////////////////////////////////
    // FOR JSON
    //////////////////////////////////////////////////////////////////////////////////////////////
    struct TEvVDiskStatRequest
        : public TEventPB<TEvVDiskStatRequest,
                    NKikimrVDisk::VDiskStatRequest,
                    TEvBlobStorage::EvVDiskStatRequest>
    {
        TEvVDiskStatRequest() = default;
    };

    struct TEvVDiskStatResponse
        : public TEvVResultBasePB<TEvVDiskStatResponse,
                    NKikimrVDisk::VDiskStatResponse,
                    TEvBlobStorage::EvVDiskStatResponse>
    {
        TEvVDiskStatResponse() = default;
    };

    struct TEvGetLogoBlobRequest
        : public TEventPB<TEvGetLogoBlobRequest,
                    NKikimrVDisk::GetLogoBlobRequest,
                    TEvBlobStorage::EvGetLogoBlobRequest>
    {
        TEvGetLogoBlobRequest() = default;
    };

    struct TEvGetLogoBlobResponse
        : public TEvVResultBasePB<TEvGetLogoBlobResponse,
                    NKikimrVDisk::GetLogoBlobResponse,
                    TEvBlobStorage::EvGetLogoBlobResponse>
    {
        TEvGetLogoBlobResponse() = default;
    };

    struct TEvGetLogoBlobIndexStatRequest
        : public TEventPB<TEvGetLogoBlobIndexStatRequest,
                    NKikimrVDisk::GetLogoBlobIndexStatRequest,
                    TEvBlobStorage::EvGetLogoBlobIndexStatRequest>
    {
        TEvGetLogoBlobIndexStatRequest() = default;
    };

    struct TEvGetLogoBlobIndexStatResponse
        : public TEvVResultBasePB<TEvGetLogoBlobIndexStatResponse,
                    NKikimrVDisk::GetLogoBlobIndexStatResponse,
                    TEvBlobStorage::EvGetLogoBlobIndexStatResponse>
    {
        TEvGetLogoBlobIndexStatResponse() = default;

        TEvGetLogoBlobIndexStatResponse(NKikimrProto::EReplyStatus status, const TVDiskID &, const TInstant &now,
                const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr, const NVDiskMon::TLtcHistoPtr &histoPtr)
            : TEvVResultBasePB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG)
        {
            Record.set_status(NKikimrProto::EReplyStatus_Name(status));
        }

        void SetError() {
            Record.set_status(NKikimrProto::EReplyStatus_Name(NKikimrProto::ERROR));
        }
    };

    struct TEvGetLogoBlobIndexStatResponseAck
        : public TEventPB<TEvGetLogoBlobIndexStatResponseAck,
                    NKikimrVDisk::GetLogoBlobIndexStatResponseAck,
                    TEvBlobStorage::EvGetLogoBlobIndexStatResponseAck>
    {
        TEvGetLogoBlobIndexStatResponseAck() = default;

        TEvGetLogoBlobIndexStatResponseAck(ui64 sequenceId, bool cancel = false);
    };

    struct TEvGetVDiskSpaceReportRequest
        : public TEventPB<TEvGetVDiskSpaceReportRequest,
                    NKikimrVDisk::TGetVDiskSpaceReportRequest,
                    TEvBlobStorage::EvGetVDiskSpaceReportRequest>
    {
        TEvGetVDiskSpaceReportRequest();
    };

    struct TEvGetVDiskSpaceReportResponse
        : public TEvVResultBasePB<TEvGetVDiskSpaceReportResponse,
                    NKikimrVDisk::TGetVDiskSpaceReportResponse,
                    TEvBlobStorage::EvGetVDiskSpaceReportResponse>
    {
        TEvGetVDiskSpaceReportResponse();

        TEvGetVDiskSpaceReportResponse(NKikimrProto::EReplyStatus status, const TString& errorReason,
                const TInstant& now, const ::NMonitoring::TDynamicCounters::TCounterPtr& counterPtr,
                const NVDiskMon::TLtcHistoPtr& histoPtr);
    };

    struct TEvPermitGarbageCollection : TEventLocal<TEvPermitGarbageCollection, TEvBlobStorage::EvPermitGarbageCollection> {};

    ////////////////////////////////////////////////////////////////////////////
    // TEvMinHugeBlobSizeUpdate
    ////////////////////////////////////////////////////////////////////////////
    class TEvMinHugeBlobSizeUpdate : public TEventLocal<TEvMinHugeBlobSizeUpdate, TEvBlobStorage::EvMinHugeBlobSizeUpdate> {
    public:
        ui32 MinHugeBlobInBytes;

        TEvMinHugeBlobSizeUpdate(ui32 minHugeBlobInBytes) : MinHugeBlobInBytes(minHugeBlobInBytes) {
        };
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvGetSkeletonState
    ////////////////////////////////////////////////////////////////////////////
    // Accesses internal state of the Skeleton actor, for test purposes
    ////////////////////////////////////////////////////////////////////////////

    class TEvGetSkeletonState : public TEventLocal<TEvGetSkeletonState,
            TEvBlobStorage::EvGetSkeletonState> {};

    ////////////////////////////////////////////////////////////////////////////
    // TEvGetSkeletonStateResult
    ////////////////////////////////////////////////////////////////////////////
    class TEvGetSkeletonStateResult : public TEventLocal<TEvGetSkeletonStateResult,
            TEvBlobStorage::EvGetSkeletonStateResult> {
    public:
        TActorId ChunkKeeperActorId;

        TEvGetSkeletonStateResult(TActorId chunkKeeperActorId);
    };
} // NKikimr
