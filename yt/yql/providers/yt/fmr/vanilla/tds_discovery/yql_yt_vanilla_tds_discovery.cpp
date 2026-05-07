#include "yql_yt_vanilla_tds_discovery.h"

#include <yt/yql/providers/yt/fmr/vanilla/common/yql_yt_vanilla_common.h>

#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/generic/string.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>
#include <util/string/builder.h>
#include <util/system/guard.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>

namespace NYql::NFmr {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TVanillaTdsDiscovery : public ITableDataServiceDiscovery {
public:
    explicit TVanillaTdsDiscovery(const IVanillaExternalPeerTracker& peerTracker, TVanillaTdsDiscoverySettings settings);

    void Start() override;
    void Stop() override;
    ui64 GetHostCount() const override;
    TTableDataServiceServerConnection GetHost(ui64 index) const override;

    const TVanillaTdsDiscoverySettings& GetSettings() const;

private:
    void RefreshLoop();
    void Refresh();

    const IVanillaExternalPeerTracker& PeerTracker_;
    TVanillaTdsDiscoverySettings Settings_;
    mutable TMutex Mutex_;
    std::vector<TTableDataServiceServerConnection> Hosts_;
    std::atomic<bool> Shutdown_{false};
    THolder<TThread> RefreshThread_;
};

TVanillaTdsDiscovery::TVanillaTdsDiscovery(const IVanillaExternalPeerTracker& peerTracker,
    TVanillaTdsDiscoverySettings settings)
    : PeerTracker_(peerTracker)
    , Settings_(std::move(settings))
{
    Hosts_.resize(PeerTracker_.GetPeerCount(), {TString(), Settings_.TdsPort});
}

void TVanillaTdsDiscovery::Start() {
    Shutdown_.store(false);
    RefreshThread_ = MakeHolder<TThread>([this]() { RefreshLoop(); });
    RefreshThread_->Start();
}

void TVanillaTdsDiscovery::Stop() {
    Shutdown_.store(true);
    if (RefreshThread_) {
        RefreshThread_->Join();
    }
}

ui64 TVanillaTdsDiscovery::GetHostCount() const {
    return PeerTracker_.GetPeerCount();
}

TTableDataServiceServerConnection TVanillaTdsDiscovery::GetHost(ui64 index) const {
    TGuard guard(Mutex_);
    Y_ENSURE(index < Hosts_.size(),
        "TDS host index " << index << " is out of range [0, " << Hosts_.size() << ")");
    return Hosts_[index];
}


const TVanillaTdsDiscoverySettings& TVanillaTdsDiscovery::GetSettings() const {
    return Settings_;
}

void TVanillaTdsDiscovery::RefreshLoop() {
    YQL_CLOG(TRACE, FastMapReduce) << "refresh loop started";
    while (!Shutdown_.load()) {
        Refresh();
        Sleep(Settings_.RefreshInterval);
    }
    YQL_CLOG(TRACE, FastMapReduce) << "refresh loop stopped";
}

void TVanillaTdsDiscovery::Refresh() {
    YQL_CLOG(TRACE, FastMapReduce) << "scan jobs";
    try {
        std::vector<TTableDataServiceServerConnection> newHosts(
            PeerTracker_.GetPeerCount(), {TString(), Settings_.TdsPort});

        auto ips = PeerTracker_.GetPeerAddresses();
        for (ui32 i = 0; i < ips.size(); ++i) {
            newHosts[i].Host = ips[i];
        }

        TGuard guard(Mutex_);
        bool changed = false;
        for (ui64 i = 0; i < newHosts.size(); ++i) {
            if (newHosts[i].Host != Hosts_[i].Host) {
                changed = true;
                break;
            }
        }
        Hosts_ = std::move(newHosts);

        if (changed) {
            TStringBuilder sb;
            for (ui64 i = 0; i < Hosts_.size(); ++i) {
                if (i > 0) {
                    sb << ", ";
                }
                sb << i << "=" << (Hosts_[i].Host.empty() ? "<unknown>" : Hosts_[i].Host);
            }
            YQL_CLOG(INFO, FastMapReduce) << "peer addresses changed ["
                << sb << "]";
        }
    } catch (...) {
        YQL_CLOG(ERROR, FastMapReduce) << "failed to refresh TDS hosts: "
            << CurrentExceptionMessage();
    }
}

}

////////////////////////////////////////////////////////////////////////////////

ITableDataServiceDiscovery::TPtr MakeVanillaTdsDiscovery(
    const IVanillaExternalPeerTracker& peerTracker,
    const TVanillaTdsDiscoverySettings& settings)
{
    return MakeIntrusive<TVanillaTdsDiscovery>(peerTracker, settings);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYql::NFmr
