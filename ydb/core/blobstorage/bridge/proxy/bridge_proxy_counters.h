#pragma once

#include <ydb/core/blobstorage/dsproxy/dsproxy.h>

#include <map>

namespace NKikimr {

    // Shared by requests admitted under this configuration, including ones restarted through another actor.
    struct TBridgeProxyPoolCounters {
        ::NMonitoring::TDynamicCounterPtr Pool;
        ::NMonitoring::TDynamicCounters::TCounterPtr RoutingRestarts;
        ::NMonitoring::TDynamicCounters::TCounterPtr RoutingTimeouts;

        explicit TBridgeProxyPoolCounters(const TString& name)
            : Pool(GetServiceCounters(AppData()->Counters, "dsproxynode")->GetSubgroup("subsystem", "bridge_request")
                ->GetSubgroup("storagePool", name))
            , RoutingRestarts(Pool->GetCounter("RoutingRestarts", true))
            , RoutingTimeouts(Pool->GetCounter("RoutingTimeouts", true))
        {}
    };

    class TBridgeProxyCounters {
        using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
        using EHandleClass = TStoragePoolCounters::EHandleClass;

    public:
        struct TRequestCounters {
            ::NMonitoring::TDynamicCounterPtr Group;
            ::NMonitoring::THistogramPtr ResponseTime;
            TCounterPtr RequestBytes;
            TCounterPtr ResponseBytes;
            THashMap<NKikimrProto::EReplyStatus, TCounterPtr> Completed;

            void CountReply(ui64 bytes, const IEventBase& response, NKikimrProto::EReplyStatus status, TDuration latency) {
                ResponseTime->Collect(latency.MicroSeconds() / 1000.0);
                if (RequestBytes) {
                    *RequestBytes += bytes;
                }
                if (response.Type() == TEvBlobStorage::EvGetResult) {
                    *ResponseBytes += static_cast<const TEvBlobStorage::TEvGetResult&>(response).PayloadSizeBytes();
                }
                auto& completed = Completed[status];
                if (!completed) {
                    completed = Group->GetSubgroup("status", NKikimrProto::EReplyStatus_Name(status))
                        ->GetCounter("completedRequests", true);
                }
                ++*completed;
            }
        };

    private:
        ::NMonitoring::TDynamicCounterPtr Pool;
        // Cache the handles used by this actor. The sensors themselves are shared by all proxies in the pool.
        std::map<std::tuple<ui32, EHandleClass, ui32>, TRequestCounters> Requests;

        TRequestCounters& GetRequestCounters(ui32 type, EHandleClass handleClass, ui64 bytes) {
            const ui32 sizeClass = TStoragePoolCounters::GetSizeClass(handleClass, bytes);
            const auto [it, inserted] = Requests.try_emplace({type, handleClass, sizeClass});
            auto& counters = it->second;
            if (inserted) {
                if (type == TEvBlobStorage::EvPut || type == TEvBlobStorage::EvGet) {
                    counters.Group = Pool->GetSubgroup("handleClass", TStoragePoolCounters::GetHandleClassName(handleClass))
                        ->GetSubgroup("sizeClass", TStoragePoolCounters::GetSizeClassName(handleClass, sizeClass));
                    counters.RequestBytes = counters.Group->GetCounter("bridgeRequestBytes", true);
                    if (type == TEvBlobStorage::EvGet) {
                        counters.ResponseBytes = counters.Group->GetCounter("bridgeResponseBytes", true);
                    }
                } else {
                    counters.Group = Pool->GetSubgroup("operation", TEvBlobStorage::TEvRequestCommon::GetRequestName(type));
                }
                counters.ResponseTime = counters.Group->GetHistogram("bridgeResponseTimeMs",
                    ::NMonitoring::ExplicitHistogram(GetCommonLatencyHistBounds(NPDisk::DEVICE_TYPE_UNKNOWN)));
            }
            return counters;
        }

    public:
        std::pair<TRequestCounters&, ui64> ForRequest(const TBridgeProxyPoolCounters& admitted, const IEventBase& request) {
            if (Pool != admitted.Pool) {
                Pool = admitted.Pool;
                Requests.clear();
            }
            EHandleClass handleClass = TStoragePoolCounters::HcCount;
            ui64 bytes = 0;
            if (request.Type() == TEvBlobStorage::EvPut) {
                const auto& put = static_cast<const TEvBlobStorage::TEvPut&>(request);
                handleClass = HandleClassToHandleClass(put.HandleClass);
                bytes = put.Buffer.size();
            } else if (request.Type() == TEvBlobStorage::EvGet) {
                const auto& get = static_cast<const TEvBlobStorage::TEvGet&>(request);
                handleClass = HandleClassToHandleClass(get.GetHandleClass);
                bytes = get.CountRequestBytes({get.Queries.Get(), get.QuerySize});
            }
            return {GetRequestCounters(request.Type(), handleClass, bytes), bytes};
        }
    };

} // NKikimr
