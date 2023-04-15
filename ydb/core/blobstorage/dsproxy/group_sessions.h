#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_iter.h>

#include <ydb/core/blobstorage/backpressure/queue_backpressure_common.h>

#include <ydb/core/blobstorage/vdisk/common/capnp/enums_conversions.h>

namespace NKikimr {

    constexpr ui32 TypicalDisksInGroup = 32;
    constexpr ui32 TypicalFailDomainsInGroup = 8;
    constexpr ui32 TypicalDisksInFailDomain = 4;

    // TGroupQueues is a set of ActorIds for queues representing each queue for each disk in the group, also the shared
    // FlowRecord for every queue
    struct TGroupQueues : TThrRefBase {
        struct TVDisk {
            struct TQueues {
                struct TQueue {
                    TActorId ActorId;
                    TIntrusivePtr<NBackpressure::TFlowRecord> FlowRecord;
                    std::optional<bool> ExtraBlockChecksSupport;
                };
                TQueue PutTabletLog;
                TQueue PutAsyncBlob;
                TQueue PutUserData;
                TQueue GetAsyncRead;
                TQueue GetFastRead;
                TQueue GetDiscover;
                TQueue GetLowRead;

                template<typename T>
                void ForEachQueue(T&& callback) const {
                    for (const TQueue *q : {&PutTabletLog, &PutAsyncBlob, &PutUserData, &GetAsyncRead, &GetFastRead,
                            &GetDiscover, &GetLowRead}) {
                        callback(*q);
                    }
                }

                template<typename T>
                static NKikimrBlobStorage::EVDiskQueueId VDiskQueueId(const T& event) {
                    Y_VERIFY(event.Record.HasMsgQoS());
                    const auto &msgQoS = event.Record.GetMsgQoS();
                    Y_VERIFY(msgQoS.HasExtQueueId());
                    auto queueId = NKikimrCapnProtoUtil::convertToProtobuf(msgQoS.GetExtQueueId());
                    Y_VERIFY(queueId != NKikimrBlobStorage::EVDiskQueueId::Unknown);
                    return queueId;
                }

                static NKikimrBlobStorage::EVDiskQueueId VDiskQueueId(const TEvBlobStorage::TEvVStatus&) {
                    return NKikimrBlobStorage::EVDiskQueueId::GetFastRead;
                }

                static NKikimrBlobStorage::EVDiskQueueId VDiskQueueId(const TEvBlobStorage::TEvVAssimilate&) {
                    return NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead;
                }

                TQueue& GetQueue(NKikimrBlobStorage::EVDiskQueueId queueId) {
                    switch (queueId) {
                        case NKikimrBlobStorage::EVDiskQueueId::PutTabletLog: return PutTabletLog;
                        case NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob: return PutAsyncBlob;
                        case NKikimrBlobStorage::EVDiskQueueId::PutUserData:  return PutUserData;
                        case NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead: return GetAsyncRead;
                        case NKikimrBlobStorage::EVDiskQueueId::GetFastRead:  return GetFastRead;
                        case NKikimrBlobStorage::EVDiskQueueId::GetDiscover:  return GetDiscover;
                        case NKikimrBlobStorage::EVDiskQueueId::GetLowRead:   return GetLowRead;
                        default:                                              Y_FAIL("unexpected EVDiskQueueId");
                    }
                }

                TIntrusivePtr<NBackpressure::TFlowRecord>& FlowRecordForQueueId(NKikimrBlobStorage::EVDiskQueueId queueId) {
                    return GetQueue(queueId).FlowRecord;
                }

                ui64 PredictedDelayNsForQueueId(NKikimrBlobStorage::EVDiskQueueId queueId) {
                    const auto& flowRecord = FlowRecordForQueueId(queueId);
                    return flowRecord ? flowRecord->GetPredictedDelayNs() : 0;
                }

                template<typename T>
                static void ValidateEvent(TQueue& /*queue*/, const T& /*event*/)
                {}

                static void ValidateEvent(TQueue& queue, const TEvBlobStorage::TEvVPut& event) {
                    Y_VERIFY(!event.Record.ExtraBlockChecksSize() || queue.ExtraBlockChecksSupport.value_or(true));
                }

                static void ValidateEvent(TQueue& queue, const TEvBlobStorage::TEvVMultiPut& event) {
                    for (const auto& item : event.Record.GetItems()) {
                        Y_VERIFY(!item.ExtraBlockChecksSize() || queue.ExtraBlockChecksSupport.value_or(true));
                    }
                }

                template<typename TEvent>
                TActorId QueueForEvent(const TEvent& event) {
                    TQueue& queue = GetQueue(VDiskQueueId(event));
                    ValidateEvent(queue, event);
                    return queue.ActorId;
                }

                TString ToString() const {
                    TStringStream s;
                    s << "{PutTabletLog# " << PutTabletLog.ActorId
                      << " PutAsyncBlob# " << PutAsyncBlob.ActorId
                      << " PutUserData# " << PutUserData.ActorId
                      << " GetAsyncRead# " << GetAsyncRead.ActorId
                      << " GetFastRead# " << GetAsyncRead.ActorId
                      << " GetDiscover# " << GetDiscover.ActorId
                      << " GetLowRead# " << GetLowRead.ActorId

                      << " PutTabletLog_PredictedDelayNs# "
                      << (PutTabletLog.FlowRecord ? PutTabletLog.FlowRecord->GetPredictedDelayNs() : 0)
                      << " PutAsyncBlob_PredictedDelayNs# "
                      << (PutAsyncBlob.FlowRecord ? PutAsyncBlob.FlowRecord->GetPredictedDelayNs() : 0)
                      << " PutUserData_PredictedDelayNs# "
                      << (PutUserData.FlowRecord ? PutUserData.FlowRecord->GetPredictedDelayNs() : 0)
                      << " GetAsyncRead_PredictedDelayNs# "
                      << (GetAsyncRead.FlowRecord ? GetAsyncRead.FlowRecord->GetPredictedDelayNs() : 0)
                      << " GetFastRead_PredictedDelayNs# "
                      << (GetAsyncRead.FlowRecord ? GetAsyncRead.FlowRecord->GetPredictedDelayNs() : 0)
                      << " GetDiscover_PredictedDelayNs# "
                      << (GetDiscover.FlowRecord ? GetDiscover.FlowRecord->GetPredictedDelayNs() : 0)
                      << " GetLowRead_PredictedDelayNs# "
                      << (GetLowRead.FlowRecord ? GetLowRead.FlowRecord->GetPredictedDelayNs() : 0)

                      << "}";
                    return s.Str();
                }
            };

            TQueues Queues;

            TString ToString() const {
                return TStringBuilder() << "{Queues# " << Queues.ToString() << "}";
            }
        };

        struct TFailDomain {
            TStackVec<TVDisk, TypicalDisksInFailDomain> VDisks;

            // Ill-formed because TVDisk is not assignable.
            TFailDomain(const TFailDomain& other) = default;
            TFailDomain() {};
            void operator=(const TFailDomain&) = delete;

            TString ToString() const {
                TStringStream str;
                str << "{VDisks# {";
                for (ui32 i = 0; i < VDisks.size(); ++i) {
                    str << (i ? " " : "") << i << ":{" << VDisks[i].ToString() << "}";
                }
                str << "}}";
                return str.Str();
            }
        };

        TStackVec<TFailDomain, TypicalFailDomainsInGroup> FailDomains;
        TStackVec<TVDisk*, TypicalDisksInGroup> DisksByOrderNumber;

        TGroupQueues(const TBlobStorageGroupInfo::TTopology& topology)
            : FailDomains(topology.GetTotalFailDomainsNum())
            , DisksByOrderNumber(topology.GetTotalVDisksNum())
        {
            // create group-like structure, but with plain fail domains as an array indexed by fail domain order number
            for (TFailDomain& domain : FailDomains) {
                domain.VDisks.resize(topology.GetNumVDisksPerFailDomain());
            }
            for (const auto& vdisk : topology.GetVDisks()) {
                DisksByOrderNumber[vdisk.OrderNumber] =
                    &FailDomains[vdisk.FailDomainOrderNumber].VDisks[vdisk.VDiskIdShort.VDisk];
            }
        }

        template<typename TEvent>
        void SetUpSubmitTimestamp(TEvent& event) {
            TInstant now = TAppData::TimeProvider->Now();
            auto& record = event.Record;
            auto& msgQoS = *record.MutableMsgQoS();
            auto& execTimeStats = *msgQoS.MutableExecTimeStats();
            execTimeStats.SetSubmitTimestamp(now.GetValue());
        }

        void SetUpSubmitTimestamp(TEvBlobStorage::TEvVStatus& /*event*/) {}
        void SetUpSubmitTimestamp(TEvBlobStorage::TEvVAssimilate& /*event*/) {}

        template<typename TEvent>
        TActorId Send(const IActor& actor, const TBlobStorageGroupInfo::TTopology& topology, std::unique_ptr<TEvent> event,
                ui64 cookie, NWilson::TTraceId traceId, bool timeStatsEnabled) {
            if (timeStatsEnabled) {
                SetUpSubmitTimestamp(*event);
            }
            const TVDiskID& vdiskId = VDiskIDFromVDiskID(event->Record.GetVDiskID());
            auto& queues = FailDomains[topology.GetFailDomainOrderNumber(vdiskId)].VDisks[vdiskId.VDisk].Queues;
            TActorId queueId = queues.QueueForEvent(*event);
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Send to queueId# " << queueId
                << " " << TypeName<TEvent>() << "# " << event->ToString() << " cookie# " << cookie);
            TActivationContext::Send(new IEventHandle(queueId, actor.SelfId(), event.release(), 0, cookie, nullptr, std::move(traceId)));
            return queueId;
        }

        ui64 GetPredictedDelayNsByOrderNumber(ui32 orderNumber, NKikimrBlobStorage::EVDiskQueueId queueId) {
            return DisksByOrderNumber[orderNumber]->Queues.PredictedDelayNsForQueueId(queueId);
        }

        template<typename TEvent>
        ui64 GetPredictedDelayNsForEvent(const TEvent& event, const TBlobStorageGroupInfo::TTopology& topology) {
            return GetPredictedDelayNsByOrderNumber(topology.GetOrderNumber(VDiskIDFromVDiskID(event.Record.GetVDiskID())),
                TVDisk::TQueues::VDiskQueueId(event));
        }

        TString ToString() const {
            TStringStream str;
            str << "{FailDomains# {";
            for (ui32 i = 0; i < FailDomains.size(); ++i) {
                str << (i ? " " : "") << i << ":{" << FailDomains[i].ToString() << "}";
            }
            str << "}}";
            return str.Str();
        }
    };

    struct TGroupSessions : TThrRefBase {
        const ui8 AllQueuesMask =
            1 << NKikimrBlobStorage::EVDiskQueueId::PutTabletLog |
            1 << NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob |
            1 << NKikimrBlobStorage::EVDiskQueueId::PutUserData |
            1 << NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead |
            1 << NKikimrBlobStorage::EVDiskQueueId::GetFastRead |
            1 << NKikimrBlobStorage::EVDiskQueueId::GetDiscover |
            1 << NKikimrBlobStorage::EVDiskQueueId::GetLowRead;

        TIntrusivePtr<TGroupQueues> GroupQueues;
        TStackVec<ui8, TypicalDisksInGroup> ConnectedQueuesMask;
        TActorId MonActor;
        TActorId ProxyActor;

        TGroupSessions(const TIntrusivePtr<TBlobStorageGroupInfo>& info, const TBSProxyContextPtr& bspctx,
            const TActorId& monActor, const TActorId& proxyActor);
        void Poison();
        bool GoodToGo(const TBlobStorageGroupInfo::TTopology& topology, bool waitForAllVDisks);
        void QueueConnectUpdate(ui32 orderNumber, NKikimrBlobStorage::EVDiskQueueId queueId, bool connected,
            bool extraBlockChecksSupport, const TBlobStorageGroupInfo::TTopology& topology);
        ui32 GetNumUnconnectedDisks();
    };

    struct TEvRequestProxySessionsState : TEventLocal<TEvRequestProxySessionsState, TEvBlobStorage::EvRequestProxySessionsState>
    {};

    struct TEvProxySessionsState : TEventLocal<TEvProxySessionsState, TEvBlobStorage::EvProxySessionsState> {
        TIntrusivePtr<TGroupQueues> GroupQueues;

        TEvProxySessionsState(const TIntrusivePtr<TGroupQueues>& groupQueues)
            : GroupQueues(groupQueues)
        {}
    };

} // NKikimr
