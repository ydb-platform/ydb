#include "interconnect_counters.h"

#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/monlib/metrics/metric_sub_registry.h>

#include <unordered_map>

namespace NActors {

namespace {

    class TInterconnectCounters: public IInterconnectMetrics {
    public:
        struct TOutputChannel {
            NMonitoring::TDynamicCounters::TCounterPtr Traffic;
            NMonitoring::TDynamicCounters::TCounterPtr Events;
            NMonitoring::TDynamicCounters::TCounterPtr OutgoingTraffic;
            NMonitoring::TDynamicCounters::TCounterPtr OutgoingEvents;

            TOutputChannel() = default;

            TOutputChannel(const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
                    NMonitoring::TDynamicCounters::TCounterPtr traffic,
                    NMonitoring::TDynamicCounters::TCounterPtr events)
                : Traffic(std::move(traffic))
                , Events(std::move(events))
                , OutgoingTraffic(counters->GetCounter("OutgoingTraffic", true))
                , OutgoingEvents(counters->GetCounter("OutgoingEvents", true))
            {}

            TOutputChannel(const TOutputChannel&) = default;
            TOutputChannel &operator=(const TOutputChannel& other) = default;
        };

        struct TInputChannel {
            NMonitoring::TDynamicCounters::TCounterPtr Traffic;
            NMonitoring::TDynamicCounters::TCounterPtr Events;
            NMonitoring::TDynamicCounters::TCounterPtr ScopeErrors;
            NMonitoring::TDynamicCounters::TCounterPtr IncomingTraffic;
            NMonitoring::TDynamicCounters::TCounterPtr IncomingEvents;

            TInputChannel() = default;

            TInputChannel(const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
                    NMonitoring::TDynamicCounters::TCounterPtr traffic,
                    NMonitoring::TDynamicCounters::TCounterPtr events,
                    NMonitoring::TDynamicCounters::TCounterPtr scopeErrors)
                : Traffic(std::move(traffic))
                , Events(std::move(events))
                , ScopeErrors(std::move(scopeErrors))
                , IncomingTraffic(counters->GetCounter("IncomingTraffic", true))
                , IncomingEvents(counters->GetCounter("IncomingEvents", true))
            {}

            TInputChannel(const TInputChannel&) = default;
            TInputChannel &operator=(const TInputChannel& other) = default;
        };

        struct TInputChannels : std::unordered_map<ui16, TInputChannel> {
            TInputChannel OtherInputChannel;

            TInputChannels() = default;

            TInputChannels(const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
                    const std::unordered_map<ui16, TString>& names,
                    NMonitoring::TDynamicCounters::TCounterPtr traffic,
                    NMonitoring::TDynamicCounters::TCounterPtr events,
                    NMonitoring::TDynamicCounters::TCounterPtr scopeErrors)
                : OtherInputChannel(counters->GetSubgroup("channel", "other"), traffic, events, scopeErrors)
            {
                for (const auto& [id, name] : names) {
                    try_emplace(id, counters->GetSubgroup("channel", name), traffic, events, scopeErrors);
                }
            }

            TInputChannels(const TInputChannels&) = default;
            TInputChannels &operator=(const TInputChannels& other) = default;

            const TInputChannel& Get(ui16 id) const {
                const auto it = find(id);
                return it != end() ? it->second : OtherInputChannel;
            }
        };

    private:
        const TInterconnectProxyCommon::TPtr Common;
        const bool MergePerDataCenterCounters;
        const bool MergePerPeerCounters;
        NMonitoring::TDynamicCounterPtr Counters;
        NMonitoring::TDynamicCounterPtr PerSessionCounters;
        NMonitoring::TDynamicCounterPtr PerDataCenterCounters;
        NMonitoring::TDynamicCounterPtr& AdaptiveCounters;

        bool Initialized = false;

        NMonitoring::TDynamicCounters::TCounterPtr Traffic;
        NMonitoring::TDynamicCounters::TCounterPtr Events;
        NMonitoring::TDynamicCounters::TCounterPtr ScopeErrors;

    public:
        TInterconnectCounters(const TInterconnectProxyCommon::TPtr& common)
            : Common(common)
            , MergePerDataCenterCounters(common->Settings.MergePerDataCenterCounters)
            , MergePerPeerCounters(common->Settings.MergePerPeerCounters)
            , Counters(common->MonCounters)
            , AdaptiveCounters(MergePerDataCenterCounters
                    ? PerDataCenterCounters :
                    MergePerPeerCounters ? Counters : PerSessionCounters)
        {}

        void AddInflightDataAmount(ui64 value) override {
            *InflightDataAmount += value;
        }

        void SubInflightDataAmount(ui64 value) override {
            *InflightDataAmount -= value;
        }

        void AddTotalBytesWritten(ui64 value) override {
            *TotalBytesWritten += value;
        }

        void SetClockSkewMicrosec(i64 value) override {
            *ClockSkewMicrosec = value;
        }

        void IncSessionDeaths() override {
            ++*SessionDeaths;
        }

        void IncHandshakeFails() override {
            ++*HandshakeFails;
        }

        void SetConnected(ui32 value) override {
            *Connected = value;
        }

        void IncSubscribersCount() override {
            ++*SubscribersCount;
        }

        void SubSubscribersCount(ui32 value) override {
            *SubscribersCount -= value;
        }

        void SubOutputBuffersTotalSize(ui64 value) override {
            *OutputBuffersTotalSize -= value;
        }

        void AddOutputBuffersTotalSize(ui64 value) override {
            *OutputBuffersTotalSize += value;
        }

        ui64 GetOutputBuffersTotalSize() const override {
            return *OutputBuffersTotalSize;
        }

        void IncDisconnections() override {
            ++*Disconnections;
        }

        void IncUsefulWriteWakeups() override {
            ++*UsefulWriteWakeups;
        }

        void IncSpuriousWriteWakeups() override {
            ++*SpuriousWriteWakeups;
        }

        void IncSendSyscalls(ui64 ns) override {
            ++*SendSyscalls;
            *SendSyscallsNs += ns;
        }

        void IncInflyLimitReach() override {
            ++*InflyLimitReach;
        }

        void IncUsefulReadWakeups() override {
            ++*UsefulReadWakeups;
        }

        void IncSpuriousReadWakeups() override {
            ++*SpuriousReadWakeups;
        }

        void IncDisconnectByReason(const TString& s) override {
            if (auto it = DisconnectByReason.find(s); it != DisconnectByReason.end()) {
                it->second->Inc();
            }
        }

        void AddInputChannelsIncomingTraffic(ui16 channel, ui64 incomingTraffic) override {
            auto& ch = InputChannels.Get(channel);
            *ch.IncomingTraffic += incomingTraffic;
        }

        void IncInputChannelsIncomingEvents(ui16 channel) override {
            auto& ch = InputChannels.Get(channel);
            ++*ch.IncomingEvents;
        }

        void IncRecvSyscalls(ui64 ns) override {
            ++*RecvSyscalls;
            *RecvSyscallsNs += ns;
        }

        void AddTotalBytesRead(ui64 value) override {
            *TotalBytesRead += value;
        }

        void UpdatePingTimeHistogram(ui64 value) override {
            PingTimeHistogram->Collect(value);
        }

        void UpdateOutputChannelTraffic(ui16 channel, ui64 value) override {
            auto& ch = GetOutputChannel(channel);
            if (ch.OutgoingTraffic) {
                *ch.OutgoingTraffic += value;
            }
            if (ch.Traffic) {
                *ch.Traffic += value;
            }
        }

        void UpdateOutputChannelEvents(ui16 channel) override {
            auto& ch = GetOutputChannel(channel);
            if (ch.OutgoingEvents) {
                ++*ch.OutgoingEvents;
            }
            if (ch.Events) {
                ++*ch.Events;
            }
        }

        void SetPeerInfo(const TString& name, const TString& dataCenterId) override {
            if (name != std::exchange(HumanFriendlyPeerHostName, name)) {
                PerSessionCounters.Reset();
            }
            VALGRIND_MAKE_READABLE(&DataCenterId, sizeof(DataCenterId));
            if (dataCenterId != std::exchange(DataCenterId, dataCenterId)) {
                PerDataCenterCounters.Reset();
            }

            const bool updatePerDataCenter = !PerDataCenterCounters && MergePerDataCenterCounters;
            if (updatePerDataCenter) {
                PerDataCenterCounters = Counters->GetSubgroup("dataCenterId", *DataCenterId);
            }

            const bool updatePerSession = !PerSessionCounters || updatePerDataCenter;
            if (updatePerSession) {
                auto base = MergePerDataCenterCounters ? PerDataCenterCounters : Counters;
                PerSessionCounters = base->GetSubgroup("peer", *HumanFriendlyPeerHostName);
            }

            const bool updateGlobal = !Initialized;

            const bool updateAdaptive =
                &AdaptiveCounters == &Counters              ? updateGlobal        :
                &AdaptiveCounters == &PerSessionCounters    ? updatePerSession    :
                &AdaptiveCounters == &PerDataCenterCounters ? updatePerDataCenter :
                false;

            if (updatePerSession) {
                Connected = PerSessionCounters->GetCounter("Connected");
                Disconnections = PerSessionCounters->GetCounter("Disconnections", true);
                ClockSkewMicrosec = PerSessionCounters->GetCounter("ClockSkewMicrosec");
                Traffic = PerSessionCounters->GetCounter("Traffic", true);
                Events = PerSessionCounters->GetCounter("Events", true);
                ScopeErrors = PerSessionCounters->GetCounter("ScopeErrors", true);

                for (const auto& [id, name] : Common->ChannelName) {
                    OutputChannels.try_emplace(id, Counters->GetSubgroup("channel", name), Traffic, Events);
                }
                OtherOutputChannel = TOutputChannel(Counters->GetSubgroup("channel", "other"), Traffic, Events);

                InputChannels = TInputChannels(Counters, Common->ChannelName, Traffic, Events, ScopeErrors);
            }

            if (updateAdaptive) {
                SessionDeaths = AdaptiveCounters->GetCounter("Session_Deaths", true);
                HandshakeFails = AdaptiveCounters->GetCounter("Handshake_Fails", true);
                InflyLimitReach = AdaptiveCounters->GetCounter("InflyLimitReach", true);
                InflightDataAmount = AdaptiveCounters->GetCounter("Inflight_Data");

                PingTimeHistogram = AdaptiveCounters->GetHistogram(
                    "PingTimeUs", NMonitoring::ExponentialHistogram(18, 2, 125));
            }

            if (updateGlobal) {
                OutputBuffersTotalSize = Counters->GetCounter("OutputBuffersTotalSize");
                SendSyscalls = Counters->GetCounter("SendSyscalls", true);
                SendSyscallsNs = Counters->GetCounter("SendSyscallsNs", true);
                RecvSyscalls = Counters->GetCounter("RecvSyscalls", true);
                RecvSyscallsNs = Counters->GetCounter("RecvSyscallsNs", true);
                SpuriousReadWakeups = Counters->GetCounter("SpuriousReadWakeups", true);
                UsefulReadWakeups = Counters->GetCounter("UsefulReadWakeups", true);
                SpuriousWriteWakeups = Counters->GetCounter("SpuriousWriteWakeups", true);
                UsefulWriteWakeups = Counters->GetCounter("UsefulWriteWakeups", true);
                SubscribersCount = AdaptiveCounters->GetCounter("SubscribersCount");
                TotalBytesWritten = Counters->GetCounter("TotalBytesWritten", true);
                TotalBytesRead = Counters->GetCounter("TotalBytesRead", true);

                auto disconnectReasonGroup = Counters->GetSubgroup("subsystem", "disconnectReason");
                for (const char *reason : TDisconnectReason::Reasons) {
                    DisconnectByReason[reason] = disconnectReasonGroup->GetCounter(reason, true);
                }
            }

            Initialized = true;
        }

        const TOutputChannel& GetOutputChannel(ui16 index) const {
            Y_ABORT_UNLESS(Initialized);
            const auto it = OutputChannels.find(index);
            return it != OutputChannels.end() ? it->second : OtherOutputChannel;
        }

    private:
        NMonitoring::TDynamicCounters::TCounterPtr SessionDeaths;
        NMonitoring::TDynamicCounters::TCounterPtr HandshakeFails;
        NMonitoring::TDynamicCounters::TCounterPtr Connected;
        NMonitoring::TDynamicCounters::TCounterPtr Disconnections;
        NMonitoring::TDynamicCounters::TCounterPtr InflightDataAmount;
        NMonitoring::TDynamicCounters::TCounterPtr InflyLimitReach;
        NMonitoring::TDynamicCounters::TCounterPtr OutputBuffersTotalSize;
        NMonitoring::TDynamicCounters::TCounterPtr QueueUtilization;
        NMonitoring::TDynamicCounters::TCounterPtr SubscribersCount;
        NMonitoring::TDynamicCounters::TCounterPtr SendSyscalls;
        NMonitoring::TDynamicCounters::TCounterPtr SendSyscallsNs;
        NMonitoring::TDynamicCounters::TCounterPtr ClockSkewMicrosec;
        NMonitoring::TDynamicCounters::TCounterPtr RecvSyscalls;
        NMonitoring::TDynamicCounters::TCounterPtr RecvSyscallsNs;
        NMonitoring::TDynamicCounters::TCounterPtr UsefulReadWakeups;
        NMonitoring::TDynamicCounters::TCounterPtr SpuriousReadWakeups;
        NMonitoring::TDynamicCounters::TCounterPtr UsefulWriteWakeups;
        NMonitoring::TDynamicCounters::TCounterPtr SpuriousWriteWakeups;
        NMonitoring::THistogramPtr PingTimeHistogram;

        std::unordered_map<ui16, TOutputChannel> OutputChannels;
        TOutputChannel OtherOutputChannel;
        TInputChannels InputChannels;
        THashMap<TString, NMonitoring::TDynamicCounters::TCounterPtr> DisconnectByReason;

        NMonitoring::TDynamicCounters::TCounterPtr TotalBytesWritten, TotalBytesRead;
    };

    class TInterconnectMetrics: public IInterconnectMetrics {
    public:
        struct TOutputChannel {
            NMonitoring::IRate* Traffic;
            NMonitoring::IRate* Events;
            NMonitoring::IRate* OutgoingTraffic;
            NMonitoring::IRate* OutgoingEvents;

            TOutputChannel() = default;

            TOutputChannel(const std::shared_ptr<NMonitoring::IMetricRegistry>& metrics,
                           NMonitoring::IRate* traffic,
                           NMonitoring::IRate* events)
                    : Traffic(traffic)
                    , Events(events)
                    , OutgoingTraffic(metrics->Rate(NMonitoring::MakeLabels({{"sensor", "interconnect.outgoing_traffic"}})))
                    , OutgoingEvents(metrics->Rate(NMonitoring::MakeLabels({{"sensor", "interconnect.outgoing_events"}})))
            {}

            TOutputChannel(const TOutputChannel&) = default;
            TOutputChannel &operator=(const TOutputChannel& other) = default;
        };

        struct TInputChannel {
            NMonitoring::IRate* Traffic;
            NMonitoring::IRate* Events;
            NMonitoring::IRate* ScopeErrors;
            NMonitoring::IRate* IncomingTraffic;
            NMonitoring::IRate* IncomingEvents;

            TInputChannel() = default;

            TInputChannel(const std::shared_ptr<NMonitoring::IMetricRegistry>& metrics,
                          NMonitoring::IRate* traffic, NMonitoring::IRate* events,
                          NMonitoring::IRate* scopeErrors)
                    : Traffic(traffic)
                    , Events(events)
                    , ScopeErrors(scopeErrors)
                    , IncomingTraffic(metrics->Rate(NMonitoring::MakeLabels({{"sensor", "interconnect.incoming_traffic"}})))
                    , IncomingEvents(metrics->Rate(NMonitoring::MakeLabels({{"sensor", "interconnect.incoming_events"}})))
            {}

            TInputChannel(const TInputChannel&) = default;
            TInputChannel &operator=(const TInputChannel& other) = default;
        };

        struct TInputChannels : std::unordered_map<ui16, TInputChannel> {
            TInputChannel OtherInputChannel;

            TInputChannels() = default;

            TInputChannels(const std::shared_ptr<NMonitoring::IMetricRegistry>& metrics,
                           const std::unordered_map<ui16, TString>& names,
                           NMonitoring::IRate* traffic, NMonitoring::IRate* events,
                           NMonitoring::IRate* scopeErrors)
                    : OtherInputChannel(std::make_shared<NMonitoring::TMetricSubRegistry>(
                            NMonitoring::TLabels{{"channel", "other"}}, metrics), traffic, events, scopeErrors)
            {
                for (const auto& [id, name] : names) {
                    try_emplace(id, std::make_shared<NMonitoring::TMetricSubRegistry>(NMonitoring::TLabels{{"channel", name}}, metrics),
                                traffic, events, scopeErrors);
                }
            }

            TInputChannels(const TInputChannels&) = default;
            TInputChannels &operator=(const TInputChannels& other) = default;

            const TInputChannel& Get(ui16 id) const {
                const auto it = find(id);
                return it != end() ? it->second : OtherInputChannel;
            }
        };

        TInterconnectMetrics(const TInterconnectProxyCommon::TPtr& common)
            : Common(common)
            , MergePerDataCenterMetrics_(common->Settings.MergePerDataCenterCounters)
            , MergePerPeerMetrics_(common->Settings.MergePerPeerCounters)
            , Metrics_(common->Metrics)
            , AdaptiveMetrics_(MergePerDataCenterMetrics_
                               ? PerDataCenterMetrics_ :
                               MergePerPeerMetrics_ ? Metrics_ : PerSessionMetrics_)
        {}

        void AddInflightDataAmount(ui64 value) override {
            InflightDataAmount_->Add(value);
        }

        void SubInflightDataAmount(ui64 value) override {
            InflightDataAmount_->Add(-value);
        }

        void AddTotalBytesWritten(ui64 value) override {
            TotalBytesWritten_->Add(value);
        }

        void SetClockSkewMicrosec(i64 value) override {
            ClockSkewMicrosec_->Set(value);
        }

        void IncSessionDeaths() override {
            SessionDeaths_->Inc();
        }

        void IncHandshakeFails() override {
            HandshakeFails_->Inc();
        }

        void SetConnected(ui32 value) override {
            Connected_->Set(value);
        }

        void IncSubscribersCount() override {
            SubscribersCount_->Inc();
        }

        void SubSubscribersCount(ui32 value) override {
            SubscribersCount_->Add(-value);
        }

        void SubOutputBuffersTotalSize(ui64 value) override {
            OutputBuffersTotalSize_->Add(-value);
        }

        void AddOutputBuffersTotalSize(ui64 value) override {
            OutputBuffersTotalSize_->Add(value);
        }

        ui64 GetOutputBuffersTotalSize() const override {
            return OutputBuffersTotalSize_->Get();
        }

        void IncDisconnections() override {
            Disconnections_->Inc();
        }

        void IncUsefulWriteWakeups() override {
            UsefulWriteWakeups_->Inc();
        }

        void IncSpuriousWriteWakeups() override {
            SpuriousWriteWakeups_->Inc();
        }

        void IncSendSyscalls(ui64 /*ns*/) override {
            SendSyscalls_->Inc();
        }

        void IncInflyLimitReach() override {
            InflyLimitReach_->Inc();
        }

        void IncUsefulReadWakeups() override {
            UsefulReadWakeups_->Inc();
        }

        void IncSpuriousReadWakeups() override {
            SpuriousReadWakeups_->Inc();
        }

        void IncDisconnectByReason(const TString& s) override {
            if (auto it = DisconnectByReason_.find(s); it != DisconnectByReason_.end()) {
                it->second->Inc();
            }
        }

        void AddInputChannelsIncomingTraffic(ui16 channel, ui64 incomingTraffic) override {
            auto& ch = InputChannels_.Get(channel);
            ch.IncomingTraffic->Add(incomingTraffic);
        }

        void IncInputChannelsIncomingEvents(ui16 channel) override {
            auto& ch = InputChannels_.Get(channel);
            ch.IncomingEvents->Inc();
        }

        void IncRecvSyscalls(ui64 /*ns*/) override {
            RecvSyscalls_->Inc();
        }

        void AddTotalBytesRead(ui64 value) override {
            TotalBytesRead_->Add(value);
        }

        void UpdatePingTimeHistogram(ui64 value) override {
            PingTimeHistogram_->Record(value);
        }

        void UpdateOutputChannelTraffic(ui16 channel, ui64 value) override {
            auto& ch = GetOutputChannel(channel);
            if (ch.OutgoingTraffic) {
                ch.OutgoingTraffic->Add(value);
            }
            if (ch.Traffic) {
                ch.Traffic->Add(value);
            }
        }

        void UpdateOutputChannelEvents(ui16 channel) override {
            auto& ch = GetOutputChannel(channel);
            if (ch.OutgoingEvents) {
                ch.OutgoingEvents->Inc();
            }
            if (ch.Events) {
                ch.Events->Inc();
            }
        }

        void SetPeerInfo(const TString& name, const TString& dataCenterId) override {
            if (name != std::exchange(HumanFriendlyPeerHostName, name)) {
                PerSessionMetrics_.reset();
            }
            VALGRIND_MAKE_READABLE(&DataCenterId, sizeof(DataCenterId));
            if (dataCenterId != std::exchange(DataCenterId, dataCenterId)) {
                PerDataCenterMetrics_.reset();
            }

            const bool updatePerDataCenter = !PerDataCenterMetrics_ && MergePerDataCenterMetrics_;
            if (updatePerDataCenter) {
                PerDataCenterMetrics_ = std::make_shared<NMonitoring::TMetricSubRegistry>(
                        NMonitoring::TLabels{{"datacenter_id", *DataCenterId}}, Metrics_);
            }

            const bool updatePerSession = !PerSessionMetrics_ || updatePerDataCenter;
            if (updatePerSession) {
                auto base = MergePerDataCenterMetrics_ ? PerDataCenterMetrics_ : Metrics_;
                PerSessionMetrics_ = std::make_shared<NMonitoring::TMetricSubRegistry>(
                        NMonitoring::TLabels{{"peer", *HumanFriendlyPeerHostName}}, base);
            }

            const bool updateGlobal = !Initialized_;

            const bool updateAdaptive =
                &AdaptiveMetrics_ == &Metrics_              ? updateGlobal        :
                &AdaptiveMetrics_ == &PerSessionMetrics_    ? updatePerSession    :
                &AdaptiveMetrics_ == &PerDataCenterMetrics_ ? updatePerDataCenter :
                false;

            auto createRate = [](std::shared_ptr<NMonitoring::IMetricRegistry> metrics, TStringBuf name) mutable {
                return metrics->Rate(NMonitoring::MakeLabels(NMonitoring::TLabels{{"sensor", name}}));
            };
            auto createIntGauge = [](std::shared_ptr<NMonitoring::IMetricRegistry> metrics, TStringBuf name) mutable {
                return metrics->IntGauge(NMonitoring::MakeLabels(NMonitoring::TLabels{{"sensor", name}}));
            };

            if (updatePerSession) {
                Connected_ = createIntGauge(PerSessionMetrics_, "interconnect.connected");
                Disconnections_ = createRate(PerSessionMetrics_, "interconnect.disconnections");
                ClockSkewMicrosec_ = createIntGauge(PerSessionMetrics_, "interconnect.clock_skew_microsec");
                Traffic_ = createRate(PerSessionMetrics_, "interconnect.traffic");
                Events_ = createRate(PerSessionMetrics_, "interconnect.events");
                ScopeErrors_ = createRate(PerSessionMetrics_, "interconnect.scope_errors");

                for (const auto& [id, name] : Common->ChannelName) {
                    OutputChannels_.try_emplace(id, std::make_shared<NMonitoring::TMetricSubRegistry>(
                            NMonitoring::TLabels{{"channel", name}}, Metrics_), Traffic_, Events_);
                }
                OtherOutputChannel_ = TOutputChannel(std::make_shared<NMonitoring::TMetricSubRegistry>(
                        NMonitoring::TLabels{{"channel", "other"}}, Metrics_), Traffic_, Events_);

                InputChannels_ = TInputChannels(Metrics_, Common->ChannelName, Traffic_, Events_, ScopeErrors_);
            }

            if (updateAdaptive) {
                SessionDeaths_ = createRate(AdaptiveMetrics_, "interconnect.session_deaths");
                HandshakeFails_ = createRate(AdaptiveMetrics_, "interconnect.handshake_fails");
                InflyLimitReach_ = createRate(AdaptiveMetrics_, "interconnect.infly_limit_reach");
                InflightDataAmount_ = createRate(AdaptiveMetrics_, "interconnect.inflight_data");
                PingTimeHistogram_ = AdaptiveMetrics_->HistogramRate(
                        NMonitoring::MakeLabels({{"sensor", "interconnect.ping_time_us"}}), NMonitoring::ExponentialHistogram(18, 2, 125));
            }

            if (updateGlobal) {
                OutputBuffersTotalSize_ = createRate(Metrics_, "interconnect.output_buffers_total_size");
                SendSyscalls_ = createRate(Metrics_, "interconnect.send_syscalls");
                RecvSyscalls_ = createRate(Metrics_, "interconnect.recv_syscalls");
                SpuriousReadWakeups_ = createRate(Metrics_, "interconnect.spurious_read_wakeups");
                UsefulReadWakeups_ = createRate(Metrics_, "interconnect.useful_read_wakeups");
                SpuriousWriteWakeups_ = createRate(Metrics_, "interconnect.spurious_write_wakeups");
                UsefulWriteWakeups_ = createRate(Metrics_, "interconnect.useful_write_wakeups");
                SubscribersCount_ = createIntGauge(AdaptiveMetrics_, "interconnect.subscribers_count");
                TotalBytesWritten_ = createRate(Metrics_, "interconnect.total_bytes_written");
                TotalBytesRead_ = createRate(Metrics_, "interconnect.total_bytes_read");

                for (const char *reason : TDisconnectReason::Reasons) {
                    DisconnectByReason_[reason] = Metrics_->Rate(
                            NMonitoring::MakeLabels({
                                {"sensor", "interconnect.disconnect_reason"},
                                {"reason", reason},
                            }));
                }
            }

            Initialized_ = true;
        }

        const TOutputChannel& GetOutputChannel(ui16 index) const {
            Y_ABORT_UNLESS(Initialized_);
            const auto it = OutputChannels_.find(index);
            return it != OutputChannels_.end() ? it->second : OtherOutputChannel_;
        }

    private:
        const TInterconnectProxyCommon::TPtr Common;
        const bool MergePerDataCenterMetrics_;
        const bool MergePerPeerMetrics_;
        std::shared_ptr<NMonitoring::IMetricRegistry> Metrics_;
        std::shared_ptr<NMonitoring::IMetricRegistry> PerSessionMetrics_;
        std::shared_ptr<NMonitoring::IMetricRegistry> PerDataCenterMetrics_;
        std::shared_ptr<NMonitoring::IMetricRegistry>& AdaptiveMetrics_;
        bool Initialized_ = false;

        NMonitoring::IRate* Traffic_;

        NMonitoring::IRate* Events_;
        NMonitoring::IRate* ScopeErrors_;
        NMonitoring::IRate* Disconnections_;
        NMonitoring::IIntGauge* Connected_;

        NMonitoring::IRate* SessionDeaths_;
        NMonitoring::IRate* HandshakeFails_;
        NMonitoring::IRate* InflyLimitReach_;
        NMonitoring::IRate* InflightDataAmount_;
        NMonitoring::IRate* OutputBuffersTotalSize_;
        NMonitoring::IIntGauge* SubscribersCount_;
        NMonitoring::IRate* SendSyscalls_;
        NMonitoring::IRate* RecvSyscalls_;
        NMonitoring::IRate* SpuriousWriteWakeups_;
        NMonitoring::IRate* UsefulWriteWakeups_;
        NMonitoring::IRate* SpuriousReadWakeups_;
        NMonitoring::IRate* UsefulReadWakeups_;
        NMonitoring::IIntGauge* ClockSkewMicrosec_;

        NMonitoring::IHistogram* PingTimeHistogram_;

        THashMap<ui16, TOutputChannel> OutputChannels_;
        TOutputChannel OtherOutputChannel_;
        TInputChannels InputChannels_;

        THashMap<TString, NMonitoring::IRate*> DisconnectByReason_;

        NMonitoring::IRate* TotalBytesWritten_;
        NMonitoring::IRate* TotalBytesRead_;
    };

} // namespace

std::unique_ptr<IInterconnectMetrics> CreateInterconnectCounters(const TInterconnectProxyCommon::TPtr& common) {
    return std::make_unique<TInterconnectCounters>(common);
}

std::unique_ptr<IInterconnectMetrics> CreateInterconnectMetrics(const TInterconnectProxyCommon::TPtr& common) {
    return std::make_unique<TInterconnectMetrics>(common);
}

} // NActors
