#include "blob_depot_tablet.h"
#include "data.h"
#include "space_monitor.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::DoGroupMetricsExchange() {
        std::set<ui32> groups;

        for (const auto& channel : Info()->Channels) {
            if (auto *entry = channel.LatestEntry()) {
                groups.insert(entry->GroupID);
            }
        }

        auto ev = std::make_unique<TEvBlobStorage::TEvControllerGroupMetricsExchange>();
        auto& record = ev->Record;
        for (const ui32 groupId : groups) {
            record.AddGroupsToQuery(groupId);
        }

        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), ev.release());
        TActivationContext::Schedule(TDuration::Seconds(10), new IEventHandle(TEvPrivate::EvDoGroupMetricsExchange, 0,
            SelfId(), {}, nullptr, 0));
    }

    void TBlobDepot::Handle(TEvBlobStorage::TEvControllerGroupMetricsExchange::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT58, "TEvControllerGroupMetricsExchange", (Id, GetLogId()), (Msg, ev->Get()->Record));

        if (Config.HasVirtualGroupId()) {
            auto response = std::make_unique<TEvBlobStorage::TEvControllerGroupMetricsExchange>();
            auto& record = response->Record;
            auto *m = record.AddGroupMetrics();
            m->SetGroupId(Config.GetVirtualGroupId());
            auto *params = m->MutableGroupParameters();

            NKikimrBlobStorage::TPDiskSpaceColor::E systemColor = NKikimrBlobStorage::TPDiskSpaceColor::GREEN;
            NKikimrBlobStorage::TPDiskSpaceColor::E dataColor = NKikimrBlobStorage::TPDiskSpaceColor::BLACK;

            std::unordered_map<ui32, const NKikimrBlobStorage::TGroupMetrics*> metrics;
            for (const auto& m : ev->Get()->Record.GetGroupMetrics()) {
                metrics.emplace(m.GetGroupId(), &m);
            }

            auto addResources = [](auto *to, const auto& from) {
                to->SetSpace(to->GetSpace() + from.GetSpace());
                to->SetIOPS(to->GetIOPS() + from.GetIOPS());
                to->SetReadThroughput(to->GetReadThroughput() + from.GetReadThroughput());
                to->SetWriteThroughput(to->GetWriteThroughput() + from.GetWriteThroughput());
            };

            std::unordered_set<ui32> processedDataGroups;

            for (const auto& channel : Info()->Channels) {
                if (auto *entry = channel.LatestEntry(); entry && channel.Channel < Channels.size()) {
                    TChannelInfo& ch = Channels[channel.Channel];
                    if (const auto it = metrics.find(entry->GroupID); it != metrics.end()) {
                        const auto& p = it->second->GetGroupParameters();
                        switch (ch.ChannelKind) {
                            case NKikimrBlobDepot::TChannelKind::System:
                                systemColor = Max(systemColor, p.GetSpaceColor());
                                break;

                            case NKikimrBlobDepot::TChannelKind::Data:
                                if (processedDataGroups.insert(entry->GroupID).second) { // count each data group exactly once
                                    dataColor = Min(dataColor, p.GetSpaceColor());
                                    params->SetAvailableSize(params->GetAvailableSize() + p.GetAvailableSize());
                                    if (p.HasAssuredResources()) {
                                        addResources(params->MutableAssuredResources(), p.GetAssuredResources());
                                    }
                                    if (p.HasCurrentResources()) {
                                        addResources(params->MutableCurrentResources(), p.GetCurrentResources());
                                    }
                                }
                                break;

                            case NKikimrBlobDepot::TChannelKind::Log:
                                break;
                        }
                    }
                }
            }

            auto *wb = record.MutableWhiteboardUpdate();
            wb->SetGroupID(Config.GetVirtualGroupId());
            wb->SetAllocatedSize(Data->GetTotalStoredDataSize());
            wb->SetAvailableSize(params->GetAvailableSize());
            wb->SetReadThroughput(ReadThroughput);
            wb->SetWriteThroughput(WriteThroughput);

            if (ReadyForAgentQueries()) {
                wb->SetBlobDepotOnlineTime(TInstant::Now().MilliSeconds());
            }

            params->SetAllocatedSize(Data->GetTotalStoredDataSize());
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), response.release());

            // TODO(alexvru): use a better approach
            const double approximateFreeSpaceShare = (double)params->GetAvailableSize() / (params->GetAvailableSize() +
                params->GetAllocatedSize());

            SpaceMonitor->SetSpaceColor(dataColor, approximateFreeSpaceShare); // the best data channel space color works for the whole depot
        }
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvPushMetrics::TPtr ev) {
        const auto& record = ev->Get()->Record;
        BytesRead += record.GetBytesRead();
        BytesWritten += record.GetBytesWritten();
        if (Config.HasVirtualGroupId()) {
            MetricsQ.emplace_back(TActivationContext::Monotonic(), BytesRead, BytesWritten);
        }

        auto inc = [&](NKikimrBlobDepot::ECumulativeCounters counter, ui64 value) {
            if (value) {
                TabletCounters->Cumulative()[counter] += value;
            }
        };

        inc(NKikimrBlobDepot::COUNTER_S3_GETS_OK, record.GetS3GetsOk());
        inc(NKikimrBlobDepot::COUNTER_S3_GETS_ERROR, record.GetS3GetsError());
        inc(NKikimrBlobDepot::COUNTER_S3_GETS_BYTES, record.GetS3GetsBytes());
        inc(NKikimrBlobDepot::COUNTER_S3_GETS_SLOW_DOWN, record.GetS3GetsSlowDown());
        inc(NKikimrBlobDepot::COUNTER_S3_GET_THROTTLE_ACTIVATIONS, record.GetS3GetThrottleActivations());

        if (record.HasNodeId()) {
            if (const auto it = Agents.find(record.GetNodeId()); it != Agents.end()) {
                ApplyAgentS3GetGauges(it->second,
                    record.GetS3GetsInFlight(),
                    record.GetS3GetsMaxInFlight(),
                    record.GetS3GetsPendingQueueSize());
            }
        }

        UpdateThroughputs(false);
    }

    void TBlobDepot::ApplyAgentS3GetGauges(TAgent& agent, ui64 inFlight, ui64 maxInFlight, ui64 pendingQueueSize) {
        S3GetsInFlightTotal = S3GetsInFlightTotal - agent.S3GetsInFlight + inFlight;
        S3GetsMaxInFlightTotal = S3GetsMaxInFlightTotal - agent.S3GetsMaxInFlight + maxInFlight;
        S3GetsPendingQueueSizeTotal = S3GetsPendingQueueSizeTotal - agent.S3GetsPendingQueueSize + pendingQueueSize;
        agent.S3GetsInFlight = inFlight;
        agent.S3GetsMaxInFlight = maxInFlight;
        agent.S3GetsPendingQueueSize = pendingQueueSize;
        TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_GET_READS_IN_FLIGHT] = S3GetsInFlightTotal;
        TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_GET_MAX_READS_IN_FLIGHT] = S3GetsMaxInFlightTotal;
        TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_GET_PENDING_QUEUE_SIZE] = S3GetsPendingQueueSizeTotal;
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvPushS3RouterMetrics::TPtr ev) {
        const auto& record = ev->Get()->Record;

        auto inc = [&](NKikimrBlobDepot::ECumulativeCounters counter, ui64 value) {
            if (value) {
                TabletCounters->Cumulative()[counter] += value;
            }
        };

        inc(NKikimrBlobDepot::COUNTER_S3_ROUTER_BALANCER_REQUESTS, record.GetBalancerRequests());
        inc(NKikimrBlobDepot::COUNTER_S3_ROUTER_BALANCER_ERRORS, record.GetBalancerErrors());
        inc(NKikimrBlobDepot::COUNTER_S3_ROUTER_NON_BALANCER_REQUESTS, record.GetNonBalancerRequests());
        inc(NKikimrBlobDepot::COUNTER_S3_ROUTER_NON_BALANCER_ERRORS, record.GetNonBalancerErrors());
        inc(NKikimrBlobDepot::COUNTER_S3_ROUTER_NON_BALANCER_BYTES_READ, record.GetNonBalancerBytesRead());
        inc(NKikimrBlobDepot::COUNTER_S3_ROUTER_NON_BALANCER_BYTES_WRITTEN, record.GetNonBalancerBytesWritten());
        inc(NKikimrBlobDepot::COUNTER_S3_ROUTER_BALANCER_RESOLVE_REQUESTS, record.GetBalancerResolveRequests());
        inc(NKikimrBlobDepot::COUNTER_S3_ROUTER_BALANCER_RESOLVE_SUCCESSES, record.GetBalancerResolveSuccesses());
        inc(NKikimrBlobDepot::COUNTER_S3_ROUTER_BALANCER_RESOLVE_FAILURES, record.GetBalancerResolveFailures());
        inc(NKikimrBlobDepot::COUNTER_S3_ROUTER_ENDPOINT_SWITCHES, record.GetEndpointSwitches());
        inc(NKikimrBlobDepot::COUNTER_S3_ROUTER_FIVE_XX_REFRESH_TRIGGERS, record.GetFiveXxRefreshTriggers());
        inc(NKikimrBlobDepot::COUNTER_S3_ROUTER_PENDING_REJECTS, record.GetPendingRejects());
        inc(NKikimrBlobDepot::COUNTER_S3_ROUTER_RETIRING_WRAPPERS_ABORTED, record.GetRetiringWrappersAborted());

        auto applyLatencyHistogram = [&](NKikimrBlobDepot::EPercentileCounters counter, const auto& buckets) {
            if (buckets.empty()) {
                return;
            }

            auto& hist = TabletCounters->Percentile()[counter];
            const ui32 n = Min<ui32>(hist.GetRangeCount(), buckets.size());
            for (ui32 i = 0; i < n; ++i) {
                if (const ui64 count = buckets.Get(i)) {
                    hist.AddFor(hist.GetRangeBound(i), count);
                }
            }
        };

        applyLatencyHistogram(NKikimrBlobDepot::COUNTER_S3_ROUTER_BALANCER_LATENCY_MS, record.GetBalancerLatencyHistogram());
        applyLatencyHistogram(NKikimrBlobDepot::COUNTER_S3_ROUTER_NON_BALANCER_LATENCY_MS, record.GetNonBalancerLatencyHistogram());
        applyLatencyHistogram(NKikimrBlobDepot::COUNTER_S3_ROUTER_BALANCER_RESOLVE_LATENCY_MS, record.GetBalancerResolveLatencyHistogram());
        applyLatencyHistogram(NKikimrBlobDepot::COUNTER_S3_ROUTER_PENDING_LATENCY_MS, record.GetPendingLatencyHistogram());

        if (record.HasIsUsingProxy()) {
            const ui32 nodeId = record.GetNodeId();
            const bool isUsingProxyByNode = record.GetIsUsingProxy();
            auto it = S3RouterIsUsingProxyByNode.find(nodeId);
            if (it == S3RouterIsUsingProxyByNode.end()) {
                ++S3RouterNodeCount;
                it = S3RouterIsUsingProxyByNode.emplace(nodeId, false).first;
            }
            if (isUsingProxyByNode != it->second) {
                if (isUsingProxyByNode) {
                    ++S3RouterNodesWithUsingProxy;
                } else if (S3RouterNodesWithUsingProxy) {
                    --S3RouterNodesWithUsingProxy;
                }

                it->second = isUsingProxyByNode;
            }

            const bool isUsingProxy = S3RouterNodeCount && S3RouterNodesWithUsingProxy == S3RouterNodeCount;
            TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_S3_ROUTER_IS_USING_PROXY] = isUsingProxy ? 1 : 0;
        }
    }

    void TBlobDepot::UpdateThroughputs(bool reschedule) {
        static constexpr TDuration Window = TDuration::Seconds(3);

        if (Config.HasVirtualGroupId() && !MetricsQ.empty()) {
            const TMonotonic now = TActivationContext::Monotonic();
            const TMonotonic left = now - Window;
            const auto comp = [](TMonotonic x, const auto& y) { return x < std::get<0>(y); };
            if (const auto it = std::upper_bound(MetricsQ.begin(), MetricsQ.end(), left, comp); it != MetricsQ.begin()) {
                MetricsQ.erase(MetricsQ.begin(), std::prev(it)); // remove all obsolete entries
                if (MetricsQ.size() >= 2) {
                    auto& [xTimestamp, xRead, xWritten] = MetricsQ[0];
                    const auto& [yTimestamp, yRead, yWritten] = MetricsQ[1];
                    Y_ABORT_UNLESS(xTimestamp <= left && left < yTimestamp);
                    static constexpr ui64 scale = 1'000'000;
                    const ui64 factor = (left - xTimestamp).MicroSeconds() * scale / (yTimestamp - xTimestamp).MicroSeconds();
                    xTimestamp = left;
                    xRead += (yRead - xRead) * factor / scale;
                    xWritten += (yWritten - xWritten) * factor / scale;
                }
            }

            const auto& [ts, read, written] = MetricsQ.front();
            if (ts + TDuration::Seconds(1) < now) {
                ReadThroughput = (BytesRead - read) * 1'000'000 / (now - ts).MicroSeconds();
                WriteThroughput = (BytesWritten - written) * 1'000'000 / (now - ts).MicroSeconds();
            } else {
                ReadThroughput = WriteThroughput = 0;
            }
        }

        if (reschedule) {
            TActivationContext::Schedule(Window, new IEventHandle(TEvPrivate::EvUpdateThroughputs, 0,
                SelfId(), {}, nullptr, 0));
        }
    }

} // NKikimr::NBlobDepot
