#pragma once

#include "private.h"
#include "dispatcher.h"
#include "config.h"

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/library/syncmap/map.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/mpsc_stack.h>

#include <library/cpp/yt/threading/atomic_object.h>
#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/fork_aware_rw_spin_lock.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <atomic>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

NNet::TNetworkAddress GetLocalBusAddress(int port);
bool IsLocalBusTransportEnabled();

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcher::TImpl
    : public NProfiling::ISensorProducer
{
public:
    static const TIntrusivePtr<TImpl>& Get();

    const TBusNetworkCountersPtr& GetCounters(const std::string& networkName, bool encrypted);

    void DisableNetworking();
    bool IsNetworkingDisabled();

    const std::string& GetNetworkNameForAddress(const NNet::TNetworkAddress& address);

    TTosLevel GetTosLevelForBand(EMultiplexingBand band);

    int GetMultiplexingParallelism(EMultiplexingBand band, int multiplexingParallelism);

    NConcurrency::IPollerPtr GetAcceptorPoller();
    NConcurrency::IPollerPtr GetXferPoller();

    void Configure(const TTcpDispatcherConfigPtr& config);

    void RegisterConnection(TTcpConnectionPtr connection);

    void CollectSensors(NProfiling::ISensorWriter* writer) override;

    NYTree::IYPathServicePtr GetOrchidService();

    std::optional<TString> GetBusCertsDirectoryPath() const;

    void RegisterLocalMessageHandler(int port, const ILocalMessageHandlerPtr& handler);
    void UnregisterLocalMessageHandler(int port);
    ILocalMessageHandlerPtr FindLocalBypassMessageHandler(const NNet::TNetworkAddress& address);

private:
    friend class TTcpDispatcher;

    DECLARE_NEW_FRIEND()

    void StartPeriodicExecutors();
    void OnPeriodicCheck();

    NConcurrency::IPollerPtr GetOrCreatePoller(
        NConcurrency::IThreadPoolPollerPtr* poller,
        bool isXfer,
        std::string threadNamePrefix);

    std::vector<TTcpConnectionPtr> GetConnections();
    void BuildOrchid(NYson::IYsonConsumer* consumer);

    TAtomicIntrusivePtr<TTcpDispatcherConfig> Config_{New<TTcpDispatcherConfig>()};

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, PollersLock_);
    NConcurrency::IThreadPoolPollerPtr AcceptorPoller_;
    NConcurrency::IThreadPoolPollerPtr XferPoller_;

    TMpscStack<TWeakPtr<TTcpConnection>> ConnectionsToRegister_;
    NThreading::TAtomicObject<std::vector<TWeakPtr<TTcpConnection>>> ConnectionList_;
    int CurrentConnectionListIndex_ = 0;

    struct TNetworkStatistics
    {
        const TBusNetworkCountersPtr Counters = New<TBusNetworkCounters>();
    };

    NConcurrency::TSyncMap<std::string, std::array<TNetworkStatistics, 2>> NetworkStatistics_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, PeriodicExecutorsLock_);
    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;
    NConcurrency::TPeriodicExecutorPtr PeriodicCheckExecutor_;

    std::atomic<bool> NetworkingDisabled_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TForkAwareReaderWriterSpinLock, NetworksLock_);
    std::vector<std::pair<NNet::TIP6Network, std::string>> Networks_;

    struct TBandDescriptor
    {
        std::atomic<TTosLevel> TosLevel = DefaultTosLevel;
        std::atomic<int> MinMultiplexingParallelism = DefaultMinMultiplexingParallelism;
        std::atomic<int> MaxMultiplexingParallelism = DefaultMaxMultiplexingParallelism;
    };

    TEnumIndexedArray<EMultiplexingBand, TBandDescriptor> BandToDescriptor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, LocalMessageHandlersLock_);
    THashMap<int, ILocalMessageHandlerPtr> LocalMessageHandlers_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
