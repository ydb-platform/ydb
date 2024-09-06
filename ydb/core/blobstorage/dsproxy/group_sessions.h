#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_iter.h>

#include <ydb/core/blobstorage/backpressure/queue_backpressure_common.h>

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
                    std::shared_ptr<const TCostModel> CostModel = nullptr;
                    volatile bool IsConnected = false;
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
                    Y_ABORT_UNLESS(event.Record.HasMsgQoS());
                    auto &msgQoS = event.Record.GetMsgQoS();
                    Y_ABORT_UNLESS(msgQoS.HasExtQueueId());
                    NKikimrBlobStorage::EVDiskQueueId queueId = msgQoS.GetExtQueueId();
                    Y_ABORT_UNLESS(queueId != NKikimrBlobStorage::EVDiskQueueId::Unknown);
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
                        default:                                              Y_ABORT("unexpected EVDiskQueueId");
                    }
                }

                TIntrusivePtr<NBackpressure::TFlowRecord>& FlowRecordForQueueId(NKikimrBlobStorage::EVDiskQueueId queueId) {
                    return GetQueue(queueId).FlowRecord;
                }

                ui64 PredictedDelayNsForQueueId(NKikimrBlobStorage::EVDiskQueueId queueId) {
                    const auto& flowRecord = FlowRecordForQueueId(queueId);
                    return flowRecord ? flowRecord->GetPredictedDelayNs() : 0;
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
            std::shared_ptr<const TCostModel> CostModel;

            TString ToString() const {
                return TStringBuilder() << "{Queues# " << Queues.ToString() << "}";
            }
        };

        struct TFailDomain {
            TStackVec<TVDisk, TypicalDisksInFailDomain> VDisks;
            std::shared_ptr<const TCostModel> CostModel;

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
        std::shared_ptr<const TCostModel> CostModel;

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

        void Send(const IActor& actor, const TBlobStorageGroupInfo::TTopology& topology, std::unique_ptr<IEventBase> event,
                ui64 cookie, NWilson::TTraceId traceId, const TVDiskID vdiskId, NKikimrBlobStorage::EVDiskQueueId queueId) {
            auto& queues = FailDomains[topology.GetFailDomainOrderNumber(vdiskId)].VDisks[vdiskId.VDisk].Queues;
            TActorId queueActorId = queues.GetQueue(queueId).ActorId;
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Send to queueActorId# " << queueActorId
                << " " << TypeName(*event) << "# " << event->ToString() << " cookie# " << cookie);
            TActivationContext::Send(new IEventHandle(queueActorId, actor.SelfId(), event.release(), 0, cookie, nullptr,
                std::move(traceId)));
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
            const TActorId& monActor, const TActorId& proxyActor, bool useActorSystemTimeInBSQueue);
        void Poison();
        bool GoodToGo(const TBlobStorageGroupInfo::TTopology& topology, bool waitForAllVDisks);
        void QueueConnectUpdate(ui32 orderNumber, NKikimrBlobStorage::EVDiskQueueId queueId, bool connected,
            bool extraBlockChecksSupport, std::shared_ptr<const TCostModel> costModel, const TBlobStorageGroupInfo::TTopology& topology);
        ui32 GetNumUnconnectedDisks();
        ui32 GetMinREALHugeBlobInBytes() const;
    };

    struct TEvRequestProxySessionsState : TEventLocal<TEvRequestProxySessionsState, TEvBlobStorage::EvRequestProxySessionsState>
    {};

    struct TEvProxySessionsState : TEventLocal<TEvProxySessionsState, TEvBlobStorage::EvProxySessionsState> {
        TIntrusivePtr<TGroupQueues> GroupQueues;

        TEvProxySessionsState(const TIntrusivePtr<TGroupQueues>& groupQueues)
            : GroupQueues(groupQueues)
        {}
    };

    struct TEvGetQueuesInfo : public TEventLocal<TEvGetQueuesInfo, TEvBlobStorage::EvGetQueuesInfo> {
        NKikimrBlobStorage::EVDiskQueueId QueueId;

        TEvGetQueuesInfo(NKikimrBlobStorage::EVDiskQueueId queueId)
            : QueueId(queueId)
        {}
    };

    struct TEvQueuesInfo : public TEventLocal<TEvQueuesInfo, TEvBlobStorage::EvQueuesInfo> {
        struct TQueueInfo {
            TActorId ActorId;
            TIntrusivePtr<NBackpressure::TFlowRecord> FlowRecord;
        };

        TEvQueuesInfo(ui32 groupSize) {
            Queues.resize(groupSize);
        }

        void AddInfoForQueue(ui32 orderNumber, TActorId actorId, const TIntrusivePtr<NBackpressure::TFlowRecord>& flowRecord) {
            Queues[orderNumber].emplace(TQueueInfo{
                .ActorId = actorId,
                .FlowRecord = flowRecord
            });
        }

        TString ToString() const override {
            TStringStream str;
            str << "{ TEvQueuesInfo";
            str << " Queues [";
            for (ui32 orderNum = 0; orderNum < Queues.size(); ++orderNum) {
                const std::optional<TQueueInfo>& queue = Queues[orderNum];
                if (queue) {
                    str << " { OrderNumber# " << orderNum
                        << " ActorId# " << queue->ActorId.ToString() << " },";
                } else {
                    str << " {}";
                }
            }
            str << " ] }";
            return str.Str();
        }

        TStackVec<std::optional<TQueueInfo>, TypicalDisksInGroup> Queues;
    };

} // NKikimr
