#pragma once
#include "defs.h"

#include <ydb/core/blobstorage/base/blobstorage_syncstate.h>
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
#include <ydb/core/blobstorage/storagepoolmon/storagepool_counters.h>
#include <ydb/core/base/blobstorage_common.h>

#include <ydb/core/base/event_filter.h>
#include <ydb/core/base/interconnect_channels.h>
#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/core/protos/blobstorage_disk.pb.h>

#include <ydb/core/util/pb.h>

#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/digest/multi.h>
#include <util/generic/maybe.h>
#include <util/stream/str.h>
#include <util/string/escape.h>
#include <util/generic/overloaded.h>

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

        inline const char *EQueueClientType2String(EQueueClientType t) {
            switch (t) {
                case EQueueClientType::None:        return "None";
                case EQueueClientType::ReplJob:     return "ReplJob";
                case EQueueClientType::DSProxy:     return "DSProxy";
                case EQueueClientType::VDiskLoad:   return "VDiskLoad";
                case EQueueClientType::VPatch:      return "VPatch";
                case EQueueClientType::Balancing:   return "Balancing";
            }

            Y_ABORT("unexpected EQueueClientType");
        }

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

            explicit TQueueClientId(const NKikimrBlobStorage::TMsgQoS& msgQoS)
            {
                switch (msgQoS.ClientId_case()) {
                    case NKikimrBlobStorage::TMsgQoS::ClientIdCase::kProxyNodeId:
                        Type = EQueueClientType::DSProxy;
                        Identifier = msgQoS.GetProxyNodeId();
                        break;

                    case NKikimrBlobStorage::TMsgQoS::ClientIdCase::kReplVDiskId:
                        Type = EQueueClientType::ReplJob;
                        Identifier = msgQoS.GetReplVDiskId();
                        break;

                    case NKikimrBlobStorage::TMsgQoS::ClientIdCase::kVDiskLoadId:
                        Type = EQueueClientType::VDiskLoad;
                        Identifier = msgQoS.GetVDiskLoadId();
                        break;

                    case NKikimrBlobStorage::TMsgQoS::ClientIdCase::kVPatchVDiskId:
                        Type = EQueueClientType::VPatch;
                        Identifier = msgQoS.GetVPatchVDiskId();
                        break;

                    case NKikimrBlobStorage::TMsgQoS::ClientIdCase::CLIENTID_NOT_SET:
                        Type = EQueueClientType::None;
                        Identifier = 0;
                        break;

                    case NKikimrBlobStorage::TMsgQoS::ClientIdCase::kBalancingVDiskId:
                        Type = EQueueClientType::Balancing;
                        Identifier = msgQoS.GetBalancingVDiskId();
                        break;

                    default:
                        Y_ABORT("unexpected case");
                }
            }

            EQueueClientType GetType() const {
                return Type;
            }

            ui64 GetIdentifier() const {
                return Identifier;
            }

            void Serialize(NKikimrBlobStorage::TMsgQoS *msgQoS) const {
                switch (Type) {
                    case EQueueClientType::None:
                        break;

                    case EQueueClientType::DSProxy:
                        msgQoS->SetProxyNodeId(Identifier);
                        break;

                    case EQueueClientType::ReplJob:
                        msgQoS->SetReplVDiskId(Identifier);
                        break;

                    case EQueueClientType::VDiskLoad:
                        msgQoS->SetVDiskLoadId(Identifier);
                        break;

                    case EQueueClientType::VPatch:
                        msgQoS->SetVPatchVDiskId(Identifier);
                        break;

                    case EQueueClientType::Balancing:
                        msgQoS->SetBalancingVDiskId(Identifier);
                        break;
                }
            }

            bool IsRepl() const {
                return Type == EQueueClientType::ReplJob;
            }

            size_t Hash() const {
                return MultiHash(Type, Identifier);
            }

            bool operator ==(const TQueueClientId& other) const {
                return Type == other.Type && Identifier == other.Identifier;
            }

            void Output(IOutputStream &str) const {
                str << "{Type# " << EQueueClientType2String(Type) << " Identifier# " << Identifier << "}";
            }

            TString ToString() const {
                TStringStream str;
                Output(str);
                return str.Str();
            }
        };

        ////////////////////////////////////////////////////////////////////////////
        // Status for window op/feedback
        ////////////////////////////////////////////////////////////////////////////
        using EStatus = NKikimrBlobStorage::TWindowFeedback::EStatus;

        inline bool Good(EStatus s) {
            return s == NKikimrBlobStorage::TWindowFeedback::Success ||
                s == NKikimrBlobStorage::TWindowFeedback::WindowUpdate ||
                s == NKikimrBlobStorage::TWindowFeedback::Processed;
        }

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

            void Serialize(NKikimrBlobStorage::TMessageId &to) const {
                to.SetSequenceId(SequenceId);
                to.SetMsgId(MsgId);
            }

            bool operator ==(const TMessageId &other) const {
                return SequenceId == other.SequenceId && MsgId == other.MsgId;
            }

            bool Empty() const {
                return SequenceId == 0 && MsgId == 0;
            }

            TString ToString() const {
                TStringStream str;
                Out(str);
                return str.Str();
            }

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

        void AddMark(const char * const mark) {
            if (MarkCount < BufferSize) {
                Marks[MarkCount++] = mark;
            }
        }

        TString ToString() const {
            TStringBuilder msg;
            msg << "[";
            for (ui32 idx = 0; idx < MarkCount; ++idx) {
                if (idx) {
                    msg << ',';
                }
                msg << '"' << Marks[idx] << '"';
            }
            msg << "]";
            if (AdditionalTrace) {
                msg << '+' << AdditionalTrace->ToString();
            }
            return msg;
        }
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

        void Output(IOutputStream &str) const {
            str << "{ClientId# " << ClientId
                << " RecByteSize# " << RecByteSize
                << " MsgId# " << MsgId
                << " Cost# " << Cost
                << " ExtQueueId# " << ExtQueueId
                << " IntQueueId# " << IntQueueId
                << " InternalMessageId# " << InternalMessageId
                << "}";
        }

        TString ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }
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

        ui32 GetChannelToSend() {
            Y_ABORT_UNLESS(!ResultSent);
            ResultSent = true;
            return Channel;
        }
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

        virtual void FinalizeAndSend(const TActorContext& /*ctx*/, std::unique_ptr<IEventHandle> ev) {
            Y_ABORT_UNLESS(!Finalized); // call Finalize only once
            Finalized = true;

            if (CounterPtr) {
                CounterPtr->Inc();
            }

            if (HistoPtr) {
                HistoPtr->Collect(TAppData::TimeProvider->Now() - Start, Size);
            }

            TActivationContext::Send(ev.release());
        }

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
            if (queryRecord && queryRecord->HasMsgQoS() && queryRecord->GetMsgQoS().HasExecTimeStats()) {
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

    struct TMessageRelevanceTracker {};

    struct TEventWithRelevanceTracker {
        std::optional<std::weak_ptr<TMessageRelevanceTracker>> MessageRelevanceTracker;
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
                NKikimrBlobStorage::EPutHandleClass cls)
        {
            InitWithoutBuffer(logoBlobId, vdisk, ignoreBlock, cookie, deadline, cls);
            StorePayload(std::move(buffer));
        }

        void InitWithoutBuffer(const TLogoBlobID &logoBlobId, const TVDiskID &vdisk, const bool ignoreBlock,
                const ui64 *cookie, TInstant deadline, NKikimrBlobStorage::EPutHandleClass cls)
        {
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&logoBlobId, sizeof(logoBlobId));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&vdisk, sizeof(vdisk));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&ignoreBlock, sizeof(ignoreBlock));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&cookie, sizeof(cookie));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&deadline, sizeof(deadline));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&cls, sizeof(cls));

            LogoBlobIDFromLogoBlobID(logoBlobId, Record.MutableBlobID());
            Record.SetFullDataSize(logoBlobId.BlobSize());
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            if (ignoreBlock) {
                Record.SetIgnoreBlock(ignoreBlock);
            }
            if (cookie) {
                Record.SetCookie(*cookie);
            }
            if (deadline != TInstant::Max()) {
                Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
            }
            Record.SetHandleClass(cls);
            Record.MutableMsgQoS()->SetExtQueueId(HandleClassToQueueId(cls));
        }

        bool GetIgnoreBlock() const {
            return Record.GetIgnoreBlock();
        }

        TRope GetBuffer() const {
            return Record.HasBuffer() ? TRope(Record.GetBuffer()) : GetPayload(0);
        }

        void StorePayload(TRope&& buffer);

        ui64 GetBufferBytes() const {
            ui64 sizeBytes = 0;
            const ui32 size = GetPayloadCount();
            for (ui32 i = 0; i < size; ++i) {
                sizeBytes += GetPayload(i).GetSize();
            }
            return sizeBytes;
        }

        bool Validate(TString& errorReason) {
            if (!Record.HasBlobID()) {
                errorReason = "TEvVPut rejected by VDisk. It has no query";
            } else if (!Record.HasVDiskID()) {
                errorReason = "TEvVPut rejected by VDisk. It has no VDiskID";
            } else if (!Record.MutableVDiskID()->HasGroupID()) {
                errorReason = "TEvVPut rejected by VDisk. It has no VDiskID::GroupID";
            } else if (Record.MutableMsgQoS() == nullptr) {
                errorReason = "TEvVPut rejected by VDisk. MsgQoS is undefined";
            } else if (!Record.MutableMsgQoS()->HasExtQueueId()) {
                errorReason = "TEvVPut rejected by VDisk. ExtQueueId is undefined";
            } else if (GetPayloadCount() == 0 && !Record.HasBuffer()) {
                errorReason = "TEvVPut rejected by VDisk. Payload empty and no buffer provided";
            } else {
                return true;
            }

            return false;
        }

        TString ToString() const override {
            return ToString(Record);
        }

        static void OutMsgQos(const NKikimrBlobStorage::TMsgQoS &qos, TStringStream &str) {
            str << "{MsgQoS";
            if (qos.HasDeadlineSeconds()) {
                str << " DeadlineSeconds# " << qos.GetDeadlineSeconds();
            }
            if (qos.HasMsgId()) {
                str << " MsgId# " << qos.GetMsgId();
            }
            if (qos.HasCost()) {
                str << " Cost# " << qos.GetCost();
            }
            if (qos.HasExtQueueId()) {
                str << " ExtQueueId# " << NKikimrBlobStorage::EVDiskQueueId_Name(qos.GetExtQueueId()).data();
            }
            if (qos.HasIntQueueId()) {
                str << " IntQueueId# " << NKikimrBlobStorage::EVDiskInternalQueueId_Name(qos.GetIntQueueId()).data();
            }
            if (qos.HasCostSettings()) {
                str << " CostSettings# {";
                const NKikimrBlobStorage::TVDiskCostSettings &costSettings = qos.GetCostSettings();
                if (costSettings.HasSeekTimeUs()) {
                    str << " SeekTimeUs# " << costSettings.GetSeekTimeUs();
                }
                if (costSettings.HasReadSpeedBps()) {
                    str << " ReadSpeedBps# " << costSettings.GetReadSpeedBps();
                }
                if (costSettings.HasWriteSpeedBps()) {
                    str << " WriteSpeedBps# " << costSettings.GetWriteSpeedBps();
                }
                if (costSettings.HasReadBlockSize()) {
                    str << " ReadBlockSize# " << costSettings.GetReadBlockSize();
                }
                if (costSettings.HasWriteBlockSize()) {
                    str << " WriteBlockSize# " << costSettings.GetWriteBlockSize();
                }
                if (costSettings.HasMinREALHugeBlobInBytes()) {
                    str << " MinREALHugeBlobInBytes# " << costSettings.GetMinREALHugeBlobInBytes();
                }
                str << "}";
            }

            if (qos.HasSendMeCostSettings()) {
                str << " SendMeCostSettings# " << qos.GetSendMeCostSettings();
            }
            if (qos.HasWindow()) {
                str << " Window# {";
                const NKikimrBlobStorage::TWindowFeedback &window = qos.GetWindow();
                if (window.HasStatus()) {
                    str << " Status# " << NKikimrBlobStorage::TWindowFeedback::EStatus_Name(window.GetStatus()).data();
                }
                if (window.HasActualWindowSize()) {
                    str << " ActualWindowSize# " << window.GetActualWindowSize();
                }
                if (window.HasMaxWindowSize()) {
                    str << " MaxWindowSize# " << window.GetMaxWindowSize();
                }
                if (window.HasExpectedMsgId()) {
                    str << " ExpectedMsgId# " << window.GetExpectedMsgId();
                }
                if (window.HasFailedMsgId()) {
                    str << " FailedMsgId# " << window.GetFailedMsgId();
                }
                str << "}";
            }
            str << "}";
        }

        static TString ToString(const NKikimrBlobStorage::TEvVPut &record) {
            TStringStream str;
            TLogoBlobID id = LogoBlobIDFromLogoBlobID(record.GetBlobID());
            const TString &data = record.GetBuffer();
            str << "{ID# " << id.ToString() << " FDS# " << record.GetFullDataSize();
            if (record.GetIgnoreBlock()) {
                str << " IgnoreBlock";
            }
            if (record.HasHandleClass()) {
                str << " HandleClass# " << record.GetHandleClass();
            }
            if (record.HasMsgQoS()) {
                str << " ";
                TEvBlobStorage::TEvVPut::OutMsgQos(record.GetMsgQoS(), str);
            }
            str << " DataSize# " << data.size();
            if (data.size() > 16) {
                str << " Data# <too_large>";
            } else {
                TString encoded;
                Base64Encode(data, encoded);
                str << " Data# " << encoded;
            }
            str << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvVPutResult
            : public TEvVResultBaseWithQoSPB<TEvBlobStorage::TEvVPutResult,
                NKikimrBlobStorage::TEvVPutResult, TEvBlobStorage::EvVPutResult> {

        // In current realization it is intentionaly lost on event serialization since
        // LWTrace doesn't support distributed shuttels yet
        mutable NLWTrace::TOrbit Orbit;

        TEvVPutResult();

        TEvVPutResult(const NKikimrProto::EReplyStatus status, const TLogoBlobID &logoBlobId, const TVDiskID &vdisk,
                const ui64 *cookie, TOutOfSpaceStatus oosStatus, const TInstant &now, ui32 recByteSize,
                NKikimrBlobStorage::TEvVPut *queryRecord, const TActorIDPtr &skeletonFrontIDPtr,
                const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr, const NVDiskMon::TLtcHistoPtr &histoPtr,
                const ui64 bufferSizeBytes, ui64 incarnationGuid, const TString& errorReason);

        TString ToString() const override {
            return ToString(Record);
        }

        void UpdateStatus(const NKikimrProto::EReplyStatus status) {
            Record.SetStatus(status);
        }

        static TString ToString(const NKikimrBlobStorage::TEvVPutResult &record) {
            TStringStream str;
            TLogoBlobID id = LogoBlobIDFromLogoBlobID(record.GetBlobID());
            str << "{EvVPutResult Status# " << NKikimrProto::EReplyStatus_Name(record.GetStatus()).data();
            if (record.HasErrorReason()) {
                str << " ErrorReason# " << '"' << EscapeC(record.GetErrorReason()) << '"';
            }
            str << " ID# " << id.ToString();
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
                const NKikimrBlobStorage::TEvVPut &request) {
            Record.SetStatus(status);
            if (status != NKikimrProto::OK && errorReason) {
                Record.SetErrorReason(errorReason);
            }
            if (request.HasBlobID()) {
                Record.MutableBlobID()->CopyFrom(request.GetBlobID());
            }

            if (request.HasVDiskID()) {
                Record.MutableVDiskID()->CopyFrom(request.GetVDiskID());
            }
            if (request.HasCookie()) {
                Record.SetCookie(request.GetCookie());
            }
            if (request.HasTimestamps()) {
                Record.MutableTimestamps()->CopyFrom(request.GetTimestamps());
            }
        }
    };

    struct TEvBlobStorage::TEvVMultiPut
        : TEventPB<TEvBlobStorage::TEvVMultiPut, NKikimrBlobStorage::TEvVMultiPut, TEvBlobStorage::EvVMultiPut>
        , TEventWithRelevanceTracker
    {
        mutable NLWTrace::TOrbit Orbit;

        TEvVMultiPut() = default;

        TEvVMultiPut(const TVDiskID &vdisk, TInstant deadline, NKikimrBlobStorage::EPutHandleClass cls,
                     bool ignoreBlock, const ui64 *cookie = nullptr)
        {
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&vdisk, sizeof(vdisk));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&ignoreBlock, sizeof(ignoreBlock));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&cookie, sizeof(cookie));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&deadline, sizeof(deadline));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&cls, sizeof(cls));

            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            Record.SetIgnoreBlock(ignoreBlock);
            if (cookie) {
                Record.SetCookie(*cookie);
            }
            if (deadline != TInstant::Max()) {
                Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
            }
            Record.SetHandleClass(cls);
            Record.MutableMsgQoS()->SetExtQueueId(HandleClassToQueueId(cls));
        }

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

        void StorePayload(const TRcBuf &buffer);


        TRope GetItemBuffer(ui64 itemIdx) const;

        void AddVPut(const TLogoBlobID &logoBlobId, const TRcBuf &buffer, ui64 *cookie,
                std::vector<std::pair<ui64, ui32>> *extraBlockChecks, NWilson::TTraceId traceId) {
            NKikimrBlobStorage::TVMultiPutItem *item = Record.AddItems();
            LogoBlobIDFromLogoBlobID(logoBlobId, item->MutableBlobID());
            item->SetFullDataSize(logoBlobId.BlobSize());
            StorePayload(buffer);
            item->SetFullDataSize(logoBlobId.BlobSize());
            if (cookie) {
                item->SetCookie(*cookie);
            }
            if (extraBlockChecks) {
                for (const auto& [tabletId, generation] : *extraBlockChecks) {
                    auto *p = item->AddExtraBlockChecks();
                    p->SetTabletId(tabletId);
                    p->SetGeneration(generation);
                }
            }
            if (traceId) {
                traceId.Serialize(item->MutableTraceId());
            }
        }

        bool Validate(TString& errorReason) {
            if (Record.ItemsSize() == 0) {
                errorReason =  "TEvVMultiPut rejected by VDisk. It has 0 blobs to put";
            } else if (!Record.HasVDiskID()) {
                errorReason = "TEvVMultiPut rejected by VDisk. VDiskID is undefined";
            } else if (!Record.MutableVDiskID()->HasGroupID()) {
                errorReason = "TEvVMultiPut rejected by VDisk. It has no VDiskID::GroupID";
            } else if (Record.MutableMsgQoS() == nullptr) {
                errorReason = "TEvVMultiPut rejected by VDisk. MsgQoS is undefined";
            } else if (!Record.MutableMsgQoS()->HasExtQueueId()) {
                errorReason =  "TEvVMultiPut rejected by VDisk. ExtQueueId is undefined";
            } else {
                return true;
            }

            return false;
        }

        TString ToString() const override {
            TStringStream str;
            str << "{EvVMultiPut";
            ui64 size = Record.ItemsSize();
            for (ui64 itemIdx = 0; itemIdx < size; ++itemIdx) {
                const NKikimrBlobStorage::TVMultiPutItem &item = Record.GetItems(itemIdx);
                str << " Item# {VMultiPutItem";
                TLogoBlobID id = LogoBlobIDFromLogoBlobID(item.GetBlobID());
                str << " ID# " << id.ToString();
                str << " FullDataSize# " << item.GetFullDataSize();
                TString data = GetItemBuffer(itemIdx).ConvertToString();
                str << " DataSize# " << data.size();
                if (data.size() > 16) {
                    str << " Data# <too_large>";
                } else {
                    str << " Data# " << data;
                }
                if (item.HasCookie()) {
                    str << " Cookie# " << item.GetCookie();
                }
                str << "}";
            }

            str << " VDiskId# " << VDiskIDFromVDiskID(Record.GetVDiskID());

            if (Record.GetIgnoreBlock()) {
                str << " IgnoreBlock";
            }
            if (Record.HasHandleClass()) {
                str << " HandleClass# " << Record.GetHandleClass();
            }
            if (Record.HasMsgQoS()) {
                str << " ";
                TEvBlobStorage::TEvVPut::OutMsgQos(Record.GetMsgQoS(), str);
            }
            if (Record.HasCookie()) {
                str << " Cookie# " << Record.GetCookie();
            }
            str << "}";
            return str.Str();
        }
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
                const TString& errorReason)
            : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG,
                    recByteSize, record, skeletonFrontIDPtr)
        {
            IncrementSize(bufferSizeBytes);
            Record.SetStatus(status);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            if (cookie) {
                Record.SetCookie(*cookie);
            }
            if (record && record->HasTimestamps()) {
                Record.MutableTimestamps()->CopyFrom(record->GetTimestamps());
            }
            if (status == NKikimrProto::OK) {
                Record.SetIncarnationGuid(incarnationGuid);
            }
            if (errorReason && status != NKikimrProto::OK) {
                Record.SetErrorReason(errorReason);
            }
        }

        void AddVPutResult(NKikimrProto::EReplyStatus status, const TString& errorReason, const TLogoBlobID &logoBlobId,
                ui64 *cookie, ui32 statusFlags = 0, bool writtenBeyondBarrier = false)
        {
            NKikimrBlobStorage::TVMultiPutResultItem *item = Record.AddItems();
            item->SetStatus(status);
            if (errorReason && status != NKikimrProto::OK) {
                item->SetErrorReason(errorReason);
            }
            LogoBlobIDFromLogoBlobID(logoBlobId, item->MutableBlobID());
            if (cookie) {
                item->SetCookie(*cookie);
            }
            item->SetStatusFlags(statusFlags);
            if (writtenBeyondBarrier) {
                item->SetWrittenBeyondBarrier(true);
            }
        }

        void MakeError(NKikimrProto::EReplyStatus status, const TString& errorReason,
                const NKikimrBlobStorage::TEvVMultiPut &request) {
            Record.SetStatus(status);
            if (errorReason && status != NKikimrProto::OK) {
                Record.SetErrorReason(errorReason);
            }
            if (status == NKikimrProto::NOTREADY) {
                status = NKikimrProto::ERROR; // treat BS_QUEUE internally generated NOTREADY as ERROR
            }

            for (auto &item : request.GetItems()) {
                Y_ABORT_UNLESS(item.HasBlobID());
                TLogoBlobID logoBlobId = LogoBlobIDFromLogoBlobID(item.GetBlobID());
                ui64 cookieValue = 0;
                ui64 *cookie = nullptr;
                if (item.HasCookie()) {
                    cookieValue = item.GetCookie();
                    cookie = &cookieValue;
                }
                AddVPutResult(status, errorReason, logoBlobId, cookie);
            }

            if (request.HasVDiskID()) {
                Record.MutableVDiskID()->CopyFrom(request.GetVDiskID());
            }
            if (request.HasCookie()) {
                Record.SetCookie(request.GetCookie());
            }
            if (request.HasTimestamps()) {
                Record.MutableTimestamps()->CopyFrom(request.GetTimestamps());
            }
        }

        TString ToString() const override {
            return ToString(Record);
        }

        static TString ToString(const NKikimrBlobStorage::TEvVMultiPutResult &record) {
            TStringStream str;
            str << "{EvVMultiPutResult Status# " << NKikimrProto::EReplyStatus_Name(record.GetStatus()).data();
            if (record.HasErrorReason()) {
                str << " ErrorReason# " << '"' << EscapeC(record.GetErrorReason()) << '"';
            }
            ui64 size = record.ItemsSize();
            for (ui64 itemIdx = 0; itemIdx < size; ++itemIdx) {
                const NKikimrBlobStorage::TVMultiPutResultItem &item = record.GetItems(itemIdx);
                str << " Item# {VMultiPutResultItem";
                str << " Status# " << NKikimrProto::EReplyStatus_Name(item.GetStatus()).data();
                if (item.HasErrorReason()) {
                    str << " ErrorReason# " << '"' << EscapeC(item.GetErrorReason()) << '"';
                }
                TLogoBlobID id = LogoBlobIDFromLogoBlobID(item.GetBlobID());
                str << " ID# " << id.ToString();
                if (item.HasCookie()) {
                    str << " Cookie# " << item.GetCookie();
                }
                str << "}";
            }
            if (record.HasMsgQoS()) {
                str << " ";
                TEvBlobStorage::TEvVPut::OutMsgQos(record.GetMsgQoS(), str);
            }
            if (record.HasCookie()) {
                str << " Cookie# " << record.GetCookie();
            }
            str << "}";
            return str.Str();
        }
    };

    //////////////////////////////////////////////////////////////////////////////////////////////
    // VGet
    //////////////////////////////////////////////////////////////////////////////////////////////

    struct TEvBlobStorage::TEvVGet
        : TEventPB<TEvBlobStorage::TEvVGet, NKikimrBlobStorage::TEvVGet, TEvBlobStorage::EvVGet>
        , TEventWithRelevanceTracker
    {
        bool IsInternal = false;

        TEvVGet() = default;

        enum class EFlags : ui32 {
            None = 0,
            NotifyIfNotReady = 1,
            ShowInternals = 2,
        };

        using TForceBlockTabletData = TEvBlobStorage::TEvGet::TTabletData;

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

        bool Validate(TString& errorReason) {
            if (!Record.HasRangeQuery() && Record.ExtremeQueriesSize() == 0) {
                errorReason = "TEvVGet rejected by VDisk. It has no query";
            } else if (!Record.HasVDiskID()) {
                errorReason = "TEvVGet rejected by VDisk. It has no VDiskID";
            } else if (!Record.MutableVDiskID()->HasGroupID()) {
                errorReason = "TEvVGet rejected by VDisk. It has no VDiskID::GroupID";
            } else if (Record.MutableMsgQoS() == nullptr) {
                errorReason = "TEvVGet rejected by VDisk. MsgQoS is undefined";
            } else if (! Record.MutableMsgQoS()->HasExtQueueId()) {
                errorReason = "TEvVGet rejected by VDisk. ExtQueueId is undefined";
            } else {
                return true;
            }
            return false;
        }

        TString ToString() const override {
            return ToString(Record);
        }

        static TString ToString(const NKikimrBlobStorage::TEvVGet &record) {
            TStringStream str;
            if (record.HasRangeQuery()) {
                const NKikimrBlobStorage::TRangeQuery &query = record.GetRangeQuery();
                str << "{RangeQuery# ";
                TLogoBlobID from = LogoBlobIDFromLogoBlobID(query.GetFrom());
                TLogoBlobID to = LogoBlobIDFromLogoBlobID(query.GetTo());
                str << from.ToString();
                str << " ";
                str << to.ToString();
                if (query.HasMaxResults()) {
                    str << " MaxResults# " << query.GetMaxResults();
                }
                if (query.HasCookie()) {
                    str << " c# " << query.GetCookie();
                }
                str << "}";
            }
            size_t size = record.ExtremeQueriesSize();
            for (unsigned i = 0; i < size; i++) {
                const NKikimrBlobStorage::TExtremeQuery &query = record.GetExtremeQueries(i);
                str << "{ExtrQuery# ";
                TLogoBlobID id = LogoBlobIDFromLogoBlobID(query.GetId());
                str << id.ToString();
                str << " sh# " << query.GetShift() << " sz# " << query.GetSize();
                if (query.HasCookie()) {
                    str << " c# " << query.GetCookie();
                }
                str << "}";
            }
            if (record.GetIndexOnly())
                str << " IndexOnly";
            if (record.HasMsgQoS()) {
                TEvBlobStorage::TEvVPut::OutMsgQos(record.GetMsgQoS(), str);
            }
            str << " Notify# " << record.GetNotifyIfNotReady()
                << " Internals# " << record.GetShowInternals()
                << " TabletId# " << record.GetTabletId()
                << " AcquireBlockedGeneration# " << record.GetAcquireBlockedGeneration()
                << " ForceBlockedGeneration# " << record.GetForceBlockedGeneration()
                << "}";
            return str.Str();
        }

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
                std::optional<TForceBlockTabletData> forceBlockTabletData)
            : Extreme(extreme)
        {
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            Record.SetHandleClass(cls);
            if (notifyIfNotReady) {
                Record.SetNotifyIfNotReady(true);
            }
            if (showInternals) {
                Record.SetShowInternals(true);
            }
            if (requestCookie) {
                Record.SetCookie(*requestCookie);
            }
            if (indexOnly) {
                Record.SetIndexOnly(true);
            }
            if (deadline != TInstant::Max()) {
                Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
            }
            if (forceBlockTabletData) {
                Record.MutableForceBlockTabletData()->SetId(forceBlockTabletData->Id);
                Record.MutableForceBlockTabletData()->SetGeneration(forceBlockTabletData->Generation);
            }
            Record.MutableMsgQoS()->SetExtQueueId(HandleClassToQueueId(cls));
            Record.SetEnablePayload(true);
        }
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
                TMaybe<ui64> cookie, ui32 channel, ui64 incarnationGuid)
            : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr, channel, recByteSize, queryRecord, skeletonFrontIDPtr)
            , EnablePayload(queryRecord && queryRecord->GetEnablePayload())
        {
            Record.SetStatus(status);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            if (queryRecord && queryRecord->HasTimestamps()) {
                Record.MutableTimestamps()->CopyFrom(queryRecord->GetTimestamps());
            }

            // copy cookie if it was set in initial query
            if (cookie)
                Record.SetCookie(*cookie);

            if (status == NKikimrProto::OK) {
                Record.SetIncarnationGuid(incarnationGuid);
            }
        }

        void MarkRangeOverflow() {
            Record.SetIsRangeOverflow(true);
        }

        void AddResult(NKikimrProto::EReplyStatus status, const TLogoBlobID &logoBlobId, ui64 sh,
                       std::variant<TRope, ui32> dataOrSize, const ui64 *cookie = nullptr,
                       const ui64 *ingress = nullptr, bool keep = false, bool doNotKeep = false) {
            TRope *data = nullptr;
            ui32 size = 0;

            std::visit(TOverloaded{
                [&](TRope& x) { data = &x; size = x.size(); },
                [&](ui32& x) { size = x; }
            }, dataOrSize);

            IncrementSize(size);
            NKikimrBlobStorage::TQueryResult *r = Record.AddResult();
            r->SetStatus(status);
            LogoBlobIDFromLogoBlobID(logoBlobId, r->MutableBlobID());
            if (sh != 0) {
                r->SetShift(sh);
            }
            if (data) {
                SetBlobData(*r, std::move(*data));
            }
            r->SetSize(size);
            r->SetFullDataSize(logoBlobId.BlobSize());
            if (cookie) {
                r->SetCookie(*cookie);
            }
            if (ingress)
                r->SetIngress(*ingress);
            if (keep) {
                r->SetKeep(true);
            }
            if (doNotKeep) {
                r->SetDoNotKeep(true);
            }
            Y_DEBUG_ABORT_UNLESS(keep + doNotKeep <= 1);
        }

        void SetBlobData(NKikimrBlobStorage::TQueryResult& item, TRope&& data) {
            if (EnablePayload && data.size() >= 128) {
                item.SetPayloadId(AddPayload(std::move(data)));
            } else {
                item.SetBufferData(data.ConvertToString());
            }
        }

        void AddResult(NKikimrProto::EReplyStatus status, const TLogoBlobID &logoBlobId, const ui64 *cookie = nullptr,
                       const ui64 *ingress = nullptr, const NMatrix::TVectorType *local = nullptr, bool keep = false,
                       bool doNotKeep = false) {
            NKikimrBlobStorage::TQueryResult *r = Record.AddResult();
            r->SetStatus(status);
            LogoBlobIDFromLogoBlobID(logoBlobId, r->MutableBlobID());
            if (cookie)
                r->SetCookie(*cookie);
            if (ingress)
                r->SetIngress(*ingress);
            if (local) {
                for (ui8 i = local->FirstPosition(); i != local->GetSize(); i = local->NextPosition(i)) {
                    r->AddParts(i + 1);
                }
            }
            if (keep) {
                r->SetKeep(true);
            }
            if (doNotKeep) {
                r->SetDoNotKeep(true);
            }
            Y_DEBUG_ABORT_UNLESS(keep + doNotKeep <= 1);
        }

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

        TString ToString() const override {
            TStringStream str;
            str << "{EvVGetResult QueryResult Status# " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
            if (Record.HasErrorReason()) {
                str << " ErrorReason# '" << EscapeC(Record.GetErrorReason()) << '\'';
            }
            for (const auto& result : Record.GetResult()) {
                str << " {";
                str << (result.HasBlobID() ? LogoBlobIDFromLogoBlobID(result.GetBlobID()).ToString() : "?")
                    << " " << (result.HasStatus() ? NKikimrProto::EReplyStatus_Name(result.GetStatus()) : "?");
                if (result.HasShift()) {
                    str << " Shift# " << result.GetShift();
                }
                if (result.HasSize()) {
                    str << " Size# " << result.GetSize();
                }
                if (result.HasFullDataSize()) {
                    str << " FullDataSize# " << result.GetFullDataSize();
                }
                if (result.HasBufferData()) {
                    const TString& data = result.GetBufferData();
                    str << " BufferData# "
                        << (data.size() <= 16 ? Base64Encode(data) : (TStringBuilder() << "<too_large>" << data.size() << 'b'));
                }
                if (result.HasPayloadId()) {
                    const TRope& data = GetPayload(result.GetPayloadId());
                    str << " PayloadId# " << result.GetPayloadId()
                        << " Data# " << (data.size() <= 16 ? Base64Encode(data.ConvertToString()) : (TStringBuilder() << "<too_large>" << data.size() << 'b'));
                }
                if (result.HasCookie()) {
                    str << " Cookie# " << result.GetCookie();
                }
                if (result.HasIngress()) {
                    ui64 ingress = result.GetIngress();
                    str << " Ingress# " << ingress;
                }
                if (const auto& v = result.GetParts(); !v.empty()) {
                    str << " Parts# " << FormatList(v);
                }
                str << "}";
            }
            str << " BlockedGeneration# " << Record.GetBlockedGeneration();
            str << "}";
            return str.Str();
        }

        void MakeError(NKikimrProto::EReplyStatus status, const TString& errorReason,
                const NKikimrBlobStorage::TEvVGet &request) {
            NKikimrProto::EReplyStatus messageStatus = status;
            NKikimrProto::EReplyStatus queryStatus = status != NKikimrProto::NOTREADY ? status : NKikimrProto::ERROR;

            Record.SetStatus(messageStatus);
            // Ignore RangeQuery (pretend there are no results)

            size_t size = request.ExtremeQueriesSize();
            for (unsigned i = 0; i < size; i++) {
                const NKikimrBlobStorage::TExtremeQuery &query = request.GetExtremeQueries(i);
                Y_ABORT_UNLESS(request.HasVDiskID());
                TLogoBlobID id = LogoBlobIDFromLogoBlobID(query.GetId());
                ui64 shift = (query.HasShift() ? query.GetShift() : 0);
                ui64 *cookie = nullptr;
                ui64 cookieValue = 0;
                if (query.HasCookie()) {
                    cookieValue = query.GetCookie();
                    cookie = &cookieValue;
                }
                AddResult(queryStatus, id, shift, 0u, cookie);
            }

            if (request.HasVDiskID()) {
                TVDiskID vDiskId = VDiskIDFromVDiskID(request.GetVDiskID());
                VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());
            }
            if (request.HasCookie()) {
                Record.SetCookie(request.GetCookie());
            }
            if (request.HasTimestamps()) {
                Record.MutableTimestamps()->CopyFrom(request.GetTimestamps());
            }
            if (errorReason) {
                Record.SetErrorReason(errorReason);
            }
        }
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
            this->Record.MutableMsgQoS()->SetExtQueueId(HandleClassToQueueId(NKikimrBlobStorage::AsyncBlob));
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

        TEvVBlock(ui64 tabletId, ui32 generation, const TVDiskID &vdisk, TInstant deadline, ui64 issuerGuid = 0)
        {
            Record.SetTabletId(tabletId);
            Record.SetGeneration(generation);
            if (issuerGuid) {
                Record.SetIssuerGuid(issuerGuid);
            }
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            if (deadline != TInstant::Max()) {
                Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
            }
            Record.MutableMsgQoS()->SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId::PutTabletLog);
        }
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
                const NVDiskMon::TLtcHistoPtr &histoPtr, ui64 incarnationGuid)
            : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG,
                    recByteSize, queryRecord, skeletonFrontIDPtr)
        {
            Record.SetStatus(status);
            if (actual) {
                Record.SetTabletId(actual->TabletId);
                Record.SetGeneration(actual->ActualGen);
            }
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            if (status == NKikimrProto::OK) {
                Record.SetIncarnationGuid(incarnationGuid);
            }
        }

        void UpdateStatus(NKikimrProto::EReplyStatus status, ui64 tabletId, ui32 actualGen) {
            Record.SetStatus(status);
            Record.SetTabletId(tabletId);
            Record.SetGeneration(actualGen);
        }

        TString ToString() const override {
            TStringStream str;
            str << "{EvVBlockResult Status# " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
            if (Record.HasTabletId()) {
                str << "TabletId# " << Record.GetTabletId();
            }
            if (Record.HasGeneration()) {
                str << "Generation# " << Record.GetGeneration();
            }
            if (Record.HasVDiskID()) {
                str << "VDisk# " << VDiskIDFromVDiskID(Record.GetVDiskID()).ToString().c_str();
            }
            str << "}";
            return str.Str();
        }

        void MakeError(NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                const NKikimrBlobStorage::TEvVBlock &request) {
            Record.SetStatus(status);
            Y_ABORT_UNLESS(request.HasTabletId());
            Record.SetTabletId(request.GetTabletId());
            Y_ABORT_UNLESS(request.HasGeneration());
            Record.SetGeneration(request.GetGeneration());
            Y_ABORT_UNLESS(request.HasVDiskID());
            TVDiskID vDiskId = VDiskIDFromVDiskID(request.GetVDiskID());
            VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());
        }
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
                TInstant deadline, TMaybe<ui64> cookie, bool notifyIfNotReady)
        {
            LogoBlobIDFromLogoBlobID(originalBlobId, Record.MutableOriginalBlobId());
            LogoBlobIDFromLogoBlobID(patchedBlobId, Record.MutablePatchedBlobId());
            VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());
            if (cookie) {
                Record.SetCookie(*cookie);
            }
            if (deadline != TInstant::Max()) {
                Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
            }
            if (notifyIfNotReady) {
                Record.SetNotifyIfNotReady(true);
            }
            Record.MutableMsgQoS()->SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId::GetFastRead);
        }
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
                const NVDiskMon::TLtcHistoPtr &histoPtr, ui64 incarnationGuid)
            : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr,
                    TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG, record->GetCachedSize(),
                    record, skeletonFrontIDPtr)
        {
            Record.SetStatus(status);
            LogoBlobIDFromLogoBlobID(originalBlobId, Record.MutableOriginalBlobId());
            LogoBlobIDFromLogoBlobID(patchedBlobId, Record.MutablePatchedBlobId());
            VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());

            if (cookie) {
                Record.SetCookie(*cookie);
            }
            if (record && record->HasTimestamps()) {
                Record.MutableTimestamps()->CopyFrom(record->GetTimestamps());
            }
            if (status == NKikimrProto::OK) {
                Record.SetIncarnationGuid(incarnationGuid);
            }
            Record.SetErrorReason(errorReason);
        }

        void AddPart(ui8 part) {
            Record.AddOriginalParts(part);
        }

        void SetStatus(NKikimrProto::EReplyStatus status) {
            Record.SetStatus(status);
        }

        void MakeError(NKikimrProto::EReplyStatus status, const TString& errorReason,
                const NKikimrBlobStorage::TEvVPatchStart &request) {
            Record.SetErrorReason(errorReason);
            Record.SetStatus(status);
            Y_ABORT_UNLESS(request.HasOriginalBlobId());
            TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(request.GetOriginalBlobId());
            LogoBlobIDFromLogoBlobID(blobId, Record.MutableOriginalBlobId());
            Y_ABORT_UNLESS(request.HasVDiskID());
            TVDiskID vDiskId = VDiskIDFromVDiskID(request.GetVDiskID());
            VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());
            Y_ABORT_UNLESS(request.HasCookie());
            Record.SetCookie(request.GetCookie());
        }
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
                ui32 expectedXorDiffs, TInstant deadline, TMaybe<ui64> cookie)
        {
            LogoBlobIDFromLogoBlobID(originalPartBlobId, Record.MutableOriginalPartBlobId());
            LogoBlobIDFromLogoBlobID(patchedPartBlobId, Record.MutablePatchedPartBlobId());
            VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());
            if (expectedXorDiffs) {
                Record.SetExpectedXorDiffs(expectedXorDiffs);
            }
            if (cookie) {
                Record.SetCookie(*cookie);
            }
            if (deadline != TInstant::Max()) {
                Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
            }
            Record.MutableMsgQoS()->SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob);
        }

        void AddDiff(ui64 startIdx, const TString &buffer) {
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(buffer.data(), buffer.size());

            NKikimrBlobStorage::TDiffBlock *r = Record.AddDiffs();
            r->SetOffset(startIdx);
            r->SetBuffer(buffer.data(), buffer.size());
        }

        void AddDiff(ui64 startIdx, const TRcBuf &buffer) {
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(buffer.data(), buffer.size());

            NKikimrBlobStorage::TDiffBlock *r = Record.AddDiffs();
            r->SetOffset(startIdx);
            r->SetBuffer(buffer.data(), buffer.size());
        }

        void AddXorReceiver(const TVDiskID &vDiskId, ui8 partId) {
            NKikimrBlobStorage::TXorDiffReceiver *r = Record.AddXorReceivers();
            VDiskIDFromVDiskID(vDiskId, r->MutableVDiskID());
            r->SetPartId(partId);
        }

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
                ui8 partId, TInstant deadline, TMaybe<ui64> cookie)
        {
            LogoBlobIDFromLogoBlobID(originalPartBlobId, Record.MutableOriginalPartBlobId());
            LogoBlobIDFromLogoBlobID(patchedPartBlobId, Record.MutablePatchedPartBlobId());
            VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());
            Record.SetFromPartId(partId);
            if (cookie) {
                Record.SetCookie(*cookie);
            }
            if (deadline != TInstant::Max()) {
                Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
            }
            Record.MutableMsgQoS()->SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob);
        }

        void AddDiff(ui64 startIdx, const TRcBuf &buffer) {
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(buffer.data(), buffer.size());

            NKikimrBlobStorage::TDiffBlock *r = Record.AddDiffs();
            r->SetOffset(startIdx);
            r->SetBuffer(buffer.data(), buffer.size());
        }

        ui32 DiffSizeSum() const {
            ui32 result = 0;
            for (auto &diff : Record.GetDiffs()) {
                result += diff.GetBuffer().size();
            }
            return result;
        }
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
                const NVDiskMon::TLtcHistoPtr &histoPtr)
            : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr,
                    TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG, record->GetCachedSize(),
                    record, skeletonFrontIDPtr)
        {
            Record.SetStatus(status);
            if (record && record->HasTimestamps()) {
                Record.MutableTimestamps()->CopyFrom(record->GetTimestamps());
            }
        }

        void MakeError(NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                const NKikimrBlobStorage::TEvVPatchXorDiff &request)
        {
            Record.SetStatus(status);
            if (request.HasTimestamps()) {
                Record.MutableTimestamps()->CopyFrom(request.GetTimestamps());
            }
        }
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
                ui64 incarnationGuid)
            : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr,
                    TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG, record->GetCachedSize(),
                    record, skeletonFrontIDPtr)
        {
            Record.SetStatus(status);
            LogoBlobIDFromLogoBlobID(originalPartBlobId, Record.MutableOriginalPartBlobId());
            LogoBlobIDFromLogoBlobID(patchedPartBlobId, Record.MutablePatchedPartBlobId());
            VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());

            if (cookie) {
                Record.SetCookie(*cookie);
            }
            if (record && record->HasTimestamps()) {
                Record.MutableTimestamps()->CopyFrom(record->GetTimestamps());
            }
            if (status == NKikimrProto::OK) {
                Record.SetIncarnationGuid(incarnationGuid);
            }
        }

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
                const NKikimrBlobStorage::TEvVPatchDiff &request)
        {
            Record.SetErrorReason(errorReason);
            Record.SetStatus(status);

            Y_ABORT_UNLESS(request.HasOriginalPartBlobId());
            TLogoBlobID originalPartBlobId = LogoBlobIDFromLogoBlobID(request.GetOriginalPartBlobId());
            LogoBlobIDFromLogoBlobID(originalPartBlobId, Record.MutableOriginalPartBlobId());

            Y_ABORT_UNLESS(request.HasPatchedPartBlobId());
            TLogoBlobID patchedPartBlobId = LogoBlobIDFromLogoBlobID(request.GetPatchedPartBlobId());
            LogoBlobIDFromLogoBlobID(patchedPartBlobId, Record.MutablePatchedPartBlobId());

            Y_ABORT_UNLESS(request.HasVDiskID());
            Record.MutableVDiskID()->CopyFrom(request.GetVDiskID());
            Y_ABORT_UNLESS(request.HasCookie());
            Record.SetCookie(request.GetCookie());
        }
    };

    struct TEvBlobStorage::TEvVGetBlock
        : TEventPB<TEvBlobStorage::TEvVGetBlock, NKikimrBlobStorage::TEvVGetBlock, TEvBlobStorage::EvVGetBlock>
        , TEventWithRelevanceTracker
    {
        TEvVGetBlock()
        {}

        TEvVGetBlock(ui64 tabletId, const TVDiskID &vdisk, TInstant deadline)
        {
            Record.SetTabletId(tabletId);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            if (deadline != TInstant::Max()) {
                Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
            }
            Record.MutableMsgQoS()->SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId::GetFastRead);
        }
    };

    struct TEvBlobStorage::TEvVGetBlockResult
        : public TEvVResultBaseWithQoSPB<TEvBlobStorage::TEvVGetBlockResult,
                        NKikimrBlobStorage::TEvVGetBlockResult,
                        TEvBlobStorage::EvVGetBlockResult> {
        TEvVGetBlockResult() = default;

        TEvVGetBlockResult(NKikimrProto::EReplyStatus status, ui64 tabletId, const TVDiskID &vdisk,
                const TInstant &now, ui32 recByteSize, NKikimrBlobStorage::TEvVGetBlock *queryRecord,
                const TActorIDPtr &skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr)
            : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG,
                    recByteSize, queryRecord, skeletonFrontIDPtr)
        {
            Record.SetStatus(status);
            Record.SetTabletId(tabletId);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
        }

        TEvVGetBlockResult(NKikimrProto::EReplyStatus status, ui64 tabletId, ui32 generation, const TVDiskID &vdisk,
                const TInstant &now, ui32 recByteSize, NKikimrBlobStorage::TEvVGetBlock *queryRecord,
                const TActorIDPtr &skeletonFrontIDPtr, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr)
            : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG,
                    recByteSize, queryRecord, skeletonFrontIDPtr)
        {
            Record.SetStatus(status);
            Record.SetTabletId(tabletId);
            Record.SetGeneration(generation);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
        }

        TString ToString() const override {
            TStringStream str;
            str << "{EvVGetBlockResult Status# " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
            if (Record.HasTabletId()) {
                str << "TabletId# " << Record.GetTabletId();
            }
            if (Record.HasGeneration()) {
                str << "Generation# " << Record.GetGeneration();
            }
            if (Record.HasVDiskID()) {
                str << "VDisk# " << VDiskIDFromVDiskID(Record.GetVDiskID()).ToString().c_str();
            }
            str << "}";
            return str.Str();
        }

        void MakeError(NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                const NKikimrBlobStorage::TEvVGetBlock &request) {
            Record.SetStatus(status);
            Y_ABORT_UNLESS(request.HasTabletId());
            Record.SetTabletId(request.GetTabletId());
            // WARNING: Generation is not set!
            Y_ABORT_UNLESS(request.HasVDiskID());
            TVDiskID vDiskId = VDiskIDFromVDiskID(request.GetVDiskID());
            VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());
        }
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
            const TVDiskID &vdisk, TInstant deadline)
        {
            Record.SetTabletId(tabletId);
            Record.SetRecordGeneration(recordGeneration);
            Record.SetPerGenerationCounter(perGenerationCounter);
            Record.SetChannel(channel);
            if (collect) {
                Record.SetCollectGeneration(collectGeneration);
                Record.SetCollectStep(collectStep);
                Record.SetHard(hard);
            }
            if (keep) {
                for (ui64 i = 0; i < keep->size(); ++i) {
                    LogoBlobIDFromLogoBlobID(keep->at(i), Record.AddKeep());
                }
            }
            if (doNotKeep) {
                for (ui64 i = 0; i < doNotKeep->size(); ++i) {
                    LogoBlobIDFromLogoBlobID(doNotKeep->at(i), Record.AddDoNotKeep());
                }
            }
            if (deadline != TInstant::Max()) {
                Record.MutableMsgQoS()->SetDeadlineSeconds((ui32)deadline.Seconds());
            }
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            Record.MutableMsgQoS()->SetExtQueueId(NKikimrBlobStorage::EVDiskQueueId::PutTabletLog);
        }

        TString ToString() const override {
            TStringStream str;
            str << "{TEvVCollectGarbage for [tablet:gen:cnt:channel]=[" << Record.GetTabletId() << ":"
                << Record.GetRecordGeneration() << ":" << Record.GetPerGenerationCounter() << ":"
                << Record.GetChannel() << "]";
            if (Record.HasCollectGeneration() || Record.HasCollectStep()) {
                str << " collect=[" << Record.GetCollectGeneration() << ":" << Record.GetCollectStep() << "]";
            }

            if (Record.KeepSize() > 0) {
                str << " Keep:";
                for (const auto &item : Record.GetKeep())
                    str << " " << LogoBlobIDFromLogoBlobID(item);
            }

            if (Record.DoNotKeepSize() > 0) {
                str << " DoNotKeep:";
                for (const auto &item : Record.GetDoNotKeep())
                    str << LogoBlobIDFromLogoBlobID(item);
            }
            return str.Str();
        }
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
                ui64 incarnationGuid)
            : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG,
                    recByteSize, queryRecord, skeletonFrontIDPtr)
        {
            Record.SetStatus(status);
            Record.SetTabletId(tabletId);
            Record.SetRecordGeneration(recordGeneration);
            Record.SetChannel(channel);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            if (status == NKikimrProto::OK) {
                Record.SetIncarnationGuid(incarnationGuid);
            }
        }

        void UpdateStatus(NKikimrProto::EReplyStatus status) {
            Record.SetStatus(status);
        }

        TString ToString() const override {
            TStringStream str;
            str << "{EvVCollectGarbageResult Status# " << NKikimrProto::EReplyStatus_Name(Record.GetStatus()).data();
            if (Record.HasTabletId()) {
                str << " TabletId# " << Record.GetTabletId();
            }
            if (Record.HasRecordGeneration()) {
                str << " RecordGeneration# " << Record.GetRecordGeneration();
            }
            if (Record.HasChannel()) {
                str << " Channel# " << Record.GetChannel();
            }
            if (Record.HasVDiskID()) {
                str << " VDisk# " << VDiskIDFromVDiskID(Record.GetVDiskID()).ToString().c_str();
            }
            str << "}";
            return str.Str();
        }

        void MakeError(NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                const NKikimrBlobStorage::TEvVCollectGarbage &request) {
            Record.SetStatus(status);
            Y_ABORT_UNLESS(request.HasTabletId());
            Record.SetTabletId(request.GetTabletId());
            Y_ABORT_UNLESS(request.HasRecordGeneration());
            Record.SetRecordGeneration(request.GetRecordGeneration());
            Y_ABORT_UNLESS(request.HasChannel());
            Record.SetChannel(request.GetChannel());
            Y_ABORT_UNLESS(request.HasVDiskID());
            TVDiskID vDiskId = VDiskIDFromVDiskID(request.GetVDiskID());
            VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskID());
        }
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
                const NVDiskMon::TLtcHistoPtr &histoPtr)
            : TEvVResultBaseWithQoSPB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG,
                    recByteSize, queryRecord, skeletonFrontIDPtr)
        {
            Record.SetStatus(status);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
        }

        void AddResult(const TKeyBarrier &key, const TMemRecBarrier &memRec, bool showInternals);

        void MakeError(NKikimrProto::EReplyStatus status, const TString& /*errorReason*/,
                const NKikimrBlobStorage::TEvVGetBarrier &request) {
            Record.SetStatus(status);
            Record.MutableVDiskID()->CopyFrom(request.GetVDiskID());
        }
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

        TEvVCheckReadinessResult(NKikimrProto::EReplyStatus status) {
            Record.SetStatus(status);
            Record.SetExtraBlockChecksSupport(true);
        }
    };

    struct TEvBlobStorage::TEvVCompact
        : public TEventPB<TEvBlobStorage::TEvVCompact, NKikimrBlobStorage::TEvVCompact, TEvBlobStorage::EvVCompact>
    {
        TEvVCompact() = default;

        TEvVCompact(const TVDiskID &vdisk, NKikimrBlobStorage::TEvVCompact::EOpType opType) {
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            Record.SetOpType(opType);
        }
    };

    struct TEvBlobStorage::TEvVCompactResult
        : public TEventPB<TEvBlobStorage::TEvVCompactResult,
                    NKikimrBlobStorage::TEvVCompactResult,
                    TEvBlobStorage::EvVCompactResult>
    {
        TEvVCompactResult() = default;

        TEvVCompactResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk) {
            Record.SetStatus(status);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
        }

        TEvVCompactResult(NKikimrProto::EReplyStatus status, const NKikimrBlobStorage::TVDiskID &vdisk) {
            Record.SetStatus(status);
            Record.MutableVDiskID()->CopyFrom(vdisk);
        }
    };

    struct TEvBlobStorage::TEvVDefrag
        : public TEventPB<TEvBlobStorage::TEvVDefrag, NKikimrBlobStorage::TEvVDefrag, TEvBlobStorage::EvVDefrag>
    {
        TEvVDefrag() = default;

        TEvVDefrag(const TVDiskID &vdisk, bool full) {
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            Record.SetFull(full);
        }
    };

    struct TEvBlobStorage::TEvVDefragResult
        : public TEventPB<TEvBlobStorage::TEvVDefragResult,
                    NKikimrBlobStorage::TEvVDefragResult,
                    TEvBlobStorage::EvVDefragResult>
    {
        TEvVDefragResult() = default;

        TEvVDefragResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk) {
            Record.SetStatus(status);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
        }

        TEvVDefragResult(NKikimrProto::EReplyStatus status, const NKikimrBlobStorage::TVDiskID &vdisk) {
            Record.SetStatus(status);
            Record.MutableVDiskID()->CopyFrom(vdisk);
        }
    };

    struct TEvBlobStorage::TEvVBaldSyncLog
        : public TEventPB<TEvBlobStorage::TEvVBaldSyncLog,
                    NKikimrBlobStorage::TEvVBaldSyncLog,
                    TEvBlobStorage::EvVBaldSyncLog>
    {
        TEvVBaldSyncLog() = default;

        TEvVBaldSyncLog(const TVDiskID &vdisk) {
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
        }
    };

    struct TEvBlobStorage::TEvVBaldSyncLogResult
        : public TEventPB<TEvBlobStorage::TEvVBaldSyncLogResult,
                    NKikimrBlobStorage::TEvVBaldSyncLogResult,
                    TEvBlobStorage::EvVBaldSyncLogResult>
    {
        TEvVBaldSyncLogResult()
        {}

        TEvVBaldSyncLogResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk) {
            Record.SetStatus(status);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
        }

        TEvVBaldSyncLogResult(NKikimrProto::EReplyStatus status, const NKikimrBlobStorage::TVDiskID &vdisk) {
            Record.SetStatus(status);
            Record.MutableVDiskID()->CopyFrom(vdisk);
        }
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
                   NKikimrBlobStorage::EDbStatType dbStatType, bool pretty) {
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            Record.SetAction(action);
            Record.SetType(dbStatType);
            Record.SetPrettyPrint(pretty);
        }

        // stat tablet
        TEvVDbStat(const TVDiskID &vdisk, ui64 tabletId, bool pretty) {
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            Record.SetAction(NKikimrBlobStorage::StatTabletAction);
            Record.SetType(NKikimrBlobStorage::StatTabletType);
            Record.SetTabletId(tabletId);
            Record.SetPrettyPrint(pretty);
        }
    };

    struct TEvBlobStorage::TEvVDbStatResult
        : public TEvVResultBasePB<TEvBlobStorage::TEvVDbStatResult,
                    NKikimrBlobStorage::TEvVDbStatResult,
                    TEvBlobStorage::EvVDbStatResult>
    {
        TEvVDbStatResult() = default;

        TEvVDbStatResult(NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const TInstant &now,
                const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr, const NVDiskMon::TLtcHistoPtr &histoPtr)
            : TEvVResultBasePB(now, counterPtr, histoPtr, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG)
        {
            Record.SetStatus(status);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
        }

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
                std::optional<std::tuple<ui64, ui8>> skipBarriersUpTo, std::optional<TLogoBlobID> skipBlobsUpTo) {
            VDiskIDFromVDiskID(vdiskId, Record.MutableVDiskID());
            if (skipBlocksUpTo) {
                Record.SetSkipBlocksUpTo(*skipBlocksUpTo);
            }
            if (skipBarriersUpTo) {
                auto *barrier = Record.MutableSkipBarriersUpTo();
                barrier->SetTabletId(std::get<0>(*skipBarriersUpTo));
                barrier->SetChannel(std::get<1>(*skipBarriersUpTo));
            }
            if (skipBlobsUpTo) {
                LogoBlobIDFromLogoBlobID(*skipBlobsUpTo, Record.MutableSkipBlobsUpTo());
            }
        }
    };

    struct TEvBlobStorage::TEvVAssimilateResult : TEventPB<TEvVAssimilateResult, NKikimrBlobStorage::TEvVAssimilateResult, EvVAssimilateResult>
    {
        TEvVAssimilateResult() = default;

        TEvVAssimilateResult(NKikimrProto::EReplyStatus status, TString errorReason, TVDiskID vdiskId) {
            Record.SetStatus(status);
            if (status != NKikimrProto::OK) {
                Record.SetErrorReason(errorReason);
            }
            VDiskIDFromVDiskID(vdiskId, Record.MutableVDiskID());
        }
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
        TEvVSyncGuid(const TVDiskID &sourceVDisk, const TVDiskID &targetVDisk) {
            VDiskIDFromVDiskID(sourceVDisk, Record.MutableSourceVDiskID());
            VDiskIDFromVDiskID(targetVDisk, Record.MutableTargetVDiskID());
        }

        // write request
        TEvVSyncGuid(const TVDiskID &sourceVDisk,
                     const TVDiskID &targetVDisk,
                     ui64 guid,
                     NKikimrBlobStorage::TSyncGuidInfo::EState state) {
            VDiskIDFromVDiskID(sourceVDisk, Record.MutableSourceVDiskID());
            VDiskIDFromVDiskID(targetVDisk, Record.MutableTargetVDiskID());
            auto w = Record.MutableInfo();
            w->SetGuid(guid);
            w->SetState(state);
        }
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
                const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel)
            : TEvVResultBasePB(now, counterPtr, histoPtr, channel)
        {
            Record.SetStatus(status);
            auto guidInfo = Record.MutableReadInfo();
            guidInfo->SetGuid(guid);
            guidInfo->SetState(state);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
        }

        // write or error response
        TEvVSyncGuidResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const TInstant &now,
                const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel)
            : TEvVResultBasePB(now, counterPtr, histoPtr, channel)
        {
            Record.SetStatus(status);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
        }

        void Output(IOutputStream &str) const {
            auto vd = VDiskIDFromVDiskID(Record.GetVDiskID());
            str << "{TEvVSyncGuidResult: Status# " << Record.GetStatus();
            str << " VDiskId# " << vd.ToString() << "}";
        }

        TString ToString() const override {
            TStringStream str;
            Output(str);
            return str.Str();
        }
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

        TEvVSync(const TSyncState &syncState, const TVDiskID &sourceVDisk, const TVDiskID &targetVDisk) {
            SyncStateFromSyncState(syncState, Record.MutableSyncState());
            VDiskIDFromVDiskID(sourceVDisk, Record.MutableSourceVDiskID());
            VDiskIDFromVDiskID(targetVDisk, Record.MutableTargetVDiskID());
        }
    };

    struct TEvBlobStorage::TEvVSyncResult
        : public TEvVResultBasePB<TEvBlobStorage::TEvVSyncResult,
                    NKikimrBlobStorage::TEvVSyncResult,
                    TEvBlobStorage::EvVSyncResult> {
        TEvVSyncResult() = default;

        TEvVSyncResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk,
                       const TSyncState &newSyncState, bool finished, NPDisk::TStatusFlags flags, const TInstant &now,
                       const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                       const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel)
            : TEvVResultBasePB(now, counterPtr, histoPtr, channel)
        {
            Record.SetStatus(status);
            SyncStateFromSyncState(newSyncState, Record.MutableNewSyncState());
            Record.SetFinished(finished);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            Record.SetStatusFlags(flags);
        }

        TEvVSyncResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, NPDisk::TStatusFlags flags,
                const TInstant &now, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel)
            : TEvVResultBasePB(now, counterPtr, histoPtr, channel)
        {
            Record.SetStatus(status);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            Record.SetStatusFlags(flags);
        }

        TString ToString() const override {
            TStringStream str;
            str << "{Status:" << Record.GetStatus()
                << " NewSyncState# " << SyncStateFromSyncState(Record.GetNewSyncState())
                << " Finished# " << Record.GetFinished()
                << " VDiskID# " << VDiskIDFromVDiskID(Record.GetVDiskID())
                << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvVSyncFull
        : public TEventPB<TEvBlobStorage::TEvVSyncFull,
                    NKikimrBlobStorage::TEvVSyncFull,
                    TEvBlobStorage::EvVSyncFull>
    {
        TEvVSyncFull()
        {}

        TEvVSyncFull(const TSyncState &syncState, const TVDiskID &sourceVDisk, const TVDiskID &targetVDisk,
                ui64 cookie, NKikimrBlobStorage::ESyncFullStage stage, const TLogoBlobID &logoBlobFrom,
                ui64 blockTabletFrom, const TKeyBarrier &barrierFrom);

        bool IsInitial() const {
            return Record.GetCookie() == 0;
        }
    };

    struct TEvBlobStorage::TEvVSyncFullResult
        : public TEvVResultBasePB<TEvBlobStorage::TEvVSyncFullResult,
                    NKikimrBlobStorage::TEvVSyncFullResult,
                    TEvBlobStorage::EvVSyncFullResult>
    {
        TEvVSyncFullResult() = default;

        TEvVSyncFullResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, const TSyncState &syncState,
                ui64 cookie, const TInstant &now, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel)
            : TEvVResultBasePB(now, counterPtr, histoPtr, channel)
        {
            Record.SetStatus(status);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            Record.SetCookie(cookie);
            SyncStateFromSyncState(syncState, Record.MutableSyncState());
        }

        TEvVSyncFullResult(const NKikimrProto::EReplyStatus status, const TVDiskID &vdisk, ui64 cookie,
                const TInstant &now, const ::NMonitoring::TDynamicCounters::TCounterPtr &counterPtr,
                const NVDiskMon::TLtcHistoPtr &histoPtr, ui32 channel)
            : TEvVResultBasePB(now, counterPtr, histoPtr, channel)
        {
            Record.SetStatus(status);
            VDiskIDFromVDiskID(vdisk, Record.MutableVDiskID());
            Record.SetCookie(cookie);
        }

        TString ToString() const override {
            TStringStream str;
            str << "{Status:" << Record.GetStatus()
                << " SyncState# " << SyncStateFromSyncState(Record.GetSyncState())
                << " Finished# " << Record.GetFinished()
                << " VDiskID# " << VDiskIDFromVDiskID(Record.GetVDiskID())
                << " Cookie# " << Record.GetCookie()
                << " Stage# " << Record.GetStage()
                << "}";
            return str.Str();
        }
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

        TEvVWindowChange(NKikimrBlobStorage::EVDiskQueueId queueId, TDropConnection) {
            Record.SetFrontQueueId(queueId);
            Record.SetDropConnection(true);
        }

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

    struct TEvBlobStorage::TEvConfigureProxy
        : public TEventLocal<TEvBlobStorage::TEvConfigureProxy, TEvBlobStorage::EvConfigureProxy>
    {
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        TIntrusivePtr<TStoragePoolCounters> StoragePoolCounters;

        TEvConfigureProxy(TIntrusivePtr<TBlobStorageGroupInfo> info, TIntrusivePtr<TStoragePoolCounters> storagePoolCounters = nullptr)
            : Info(std::move(info))
            , StoragePoolCounters(std::move(storagePoolCounters))
        {}

        TString ToString() const override {
            TStringStream str;
            str << "{TEvConfigureProxy Info# ";
            if (Info) {
                str << Info->ToString();
            } else {
                str << "nullptr";
            }
            str << " StoragePoolCounters# ";
            if (StoragePoolCounters) {
                str << "specified";
            } else {
                str << "nullptr";
            }
            str << "}";
            return str.Str();
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
        };

        std::vector<TLayoutRecord> Layout;
    };

    struct TEvBlobStorage::TEvVTakeSnapshot : TEventPB<TEvVTakeSnapshot, NKikimrBlobStorage::TEvVTakeSnapshot, EvVTakeSnapshot> {
        TEvVTakeSnapshot() = default;

        TEvVTakeSnapshot(TVDiskID vdiskId, const TString& snapshotId, ui32 timeToLiveSec) {
            VDiskIDFromVDiskID(vdiskId, Record.MutableVDiskID());
            Record.SetSnapshotId(snapshotId);
            Record.SetTimeToLiveSec(timeToLiveSec);
        }
    };

    struct TEvBlobStorage::TEvVTakeSnapshotResult : TEventPB<TEvVTakeSnapshotResult, NKikimrBlobStorage::TEvVTakeSnapshotResult, EvVTakeSnapshotResult> {
        TEvVTakeSnapshotResult() = default;

        TEvVTakeSnapshotResult(NKikimrProto::EReplyStatus status, const TString& errorReason, TVDiskID vdiskId) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(errorReason);
            }
            VDiskIDFromVDiskID(vdiskId, Record.MutableVDiskID());
        }
    };

    struct TEvBlobStorage::TEvVReleaseSnapshot : TEventPB<TEvVReleaseSnapshot, NKikimrBlobStorage::TEvVReleaseSnapshot, EvVReleaseSnapshot> {
        TEvVReleaseSnapshot() = default;

        TEvVReleaseSnapshot(TVDiskID vdiskId, const TString& snapshotId) {
            VDiskIDFromVDiskID(vdiskId, Record.MutableVDiskID());
            Record.SetSnapshotId(snapshotId);
        }
    };

    struct TEvBlobStorage::TEvVReleaseSnapshotResult : TEventPB<TEvVReleaseSnapshotResult, NKikimrBlobStorage::TEvVReleaseSnapshotResult, EvVReleaseSnapshotResult> {
        TEvVReleaseSnapshotResult() = default;

        TEvVReleaseSnapshotResult(NKikimrProto::EReplyStatus status, const TString& errorReason, TVDiskID vdiskId) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(errorReason);
            }
            VDiskIDFromVDiskID(vdiskId, Record.MutableVDiskID());
        }
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

    struct TEvPermitGarbageCollection : TEventLocal<TEvPermitGarbageCollection, TEvBlobStorage::EvPermitGarbageCollection> {};

    ////////////////////////////////////////////////////////////////////////////
    // TEvMinHugeBlobSizeUpdate
    ////////////////////////////////////////////////////////////////////////////
    class TEvMinHugeBlobSizeUpdate : public TEventLocal<TEvMinHugeBlobSizeUpdate, TEvBlobStorage::EvMinHugeBlobSizeUpdate> {
    public:
        ui32 MinREALHugeBlobInBytes;

        TEvMinHugeBlobSizeUpdate(ui32 minREALHugeBlobInBytes) : MinREALHugeBlobInBytes(minREALHugeBlobInBytes) {  
        };
    };
} // NKikimr
