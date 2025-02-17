#pragma once

#include <contrib/libs/curl/include/curl/curl.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/scope.h>
#include <util/network/address.h>
#include <util/network/socket.h>
#include <util/string/builder.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>

#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace NYql {
namespace {

struct TDNSResolver {
    std::vector<NAddr::TOpaqueAddr> Resolve(const TString& host, ui16 port) const {
        TNetworkAddress na(host, port);
        std::vector<NAddr::TOpaqueAddr> result;
        for (auto i = na.Begin(); i != na.End(); ++i) {
            result.emplace_back(NAddr::TOpaqueAddr((*i).ai_addr));
        }
        return result;
    }
};

struct TResolutionTask {
    TString Host;
    ui16 Port = 0;
    TString ExpectedIP;
    TExplicitDNSRecord::ProtocolVersion ProtocolVersion = TExplicitDNSRecord_ProtocolVersion_ANY;
};

} // namespace

template<typename Resolver = TDNSResolver>
class TDNSGateway {
public:
    using TDNSCurlListPtr = std::shared_ptr<curl_slist>;
    using TDNSConstCurlListPtr = const std::shared_ptr<const curl_slist>;
    using TDNSResolutionTable = std::unordered_map<TString, std::vector<TString>>;

    static constexpr ui32 DEFAULT_UPDATE_INTERVAL = 60000;

    TDNSGateway(
        const TDnsResolverConfig& dnsResolverConfig,
        ::NMonitoring::TDynamicCounterPtr counters)
        : IsStopped(false)
        , UpdateInterval(std::chrono::milliseconds(dnsResolverConfig.GetRefreshMs()))
        , TotalResolutionCounter(counters->GetCounter("TotalResolutions", true))
        , ResolutionSuccessCounter(counters->GetCounter("ResolutionSuccesses", true))
        , ResolutionErrorCounter(counters->GetCounter("ResolutionErrors", true))
        , ResolvedToNotExpectedIP(counters->GetCounter("ResolvedToNotExpectedIP", true)) {
        if (dnsResolverConfig.HasRefreshMs()) {
            UpdateInterval = std::chrono::milliseconds(dnsResolverConfig.GetRefreshMs());
        }

        for (auto& dnsResolverRecord: dnsResolverConfig.GetExplicitDNSRecord()) {
            auto address = dnsResolverRecord.GetAddress();
            if (address) {
                auto task = TResolutionTask{
                    address,
                    static_cast<ui16>(dnsResolverRecord.GetPort()),
                    dnsResolverRecord.GetExpectedIP(),
                    dnsResolverRecord.GetProtocol()};

                HostAddressedToResolve.emplace_back(task);

                if (task.ExpectedIP) {
                    auto hostname = TStringBuilder()
                                    << task.Host << ":" << task.Port;
                    DnsResolutionTable.emplace(
                        std::move(hostname),
                        std::vector<TString>{task.ExpectedIP});
                }
            }
        }

        DnsCurlList = ConvertDNSTableToCurlList();
        YQL_CLOG(INFO, HttpGateway)
            << "Filled DNS resolution table based on provided configuration";

        UpdateResolutionTable();

        Thread = std::thread([this]() {
            auto lock = std::unique_lock{Sync};
            YQL_CLOG(DEBUG, HttpGateway)
                << "DNS Gateway thread is going to start";
            while (true) {
                if (IsStoppedConditionalVariable.wait_for(
                        lock, UpdateInterval, [this] { return IsStopped; })) {
                    YQL_CLOG(DEBUG, HttpGateway)
                        << "DNS Gateway thread is going to stop";
                    break;
                }
                lock.unlock();
                Y_DEFER {
                    lock.lock();
                };

                UpdateResolutionTable();
            }
            YQL_CLOG(DEBUG, HttpGateway) << "DNS Gateway thread stopped";
        });
    }

    TDNSConstCurlListPtr GetDNSCurlList() {
        auto lock = std::lock_guard{Sync};
        return DnsCurlList;
    }

    ~TDNSGateway() { StopThread(); }

private:
    void SetDNSCurlList(TDNSCurlListPtr&& newDnsCurlList) {
        auto lock = std::lock_guard{Sync};
        DnsCurlList = std::move(newDnsCurlList);
    }

    void UpdateResolutionTable() {
        YQL_CLOG(INFO, HttpGateway) << "Started DNS table update";

        TDNSResolutionTable newResolutionTable;
        for (const auto& record: HostAddressedToResolve) {
            auto resolvedAddress = ResolveHostname(
                record.Host,
                record.Port,
                record.ProtocolVersion);

            auto hostname = TStringBuilder()
                            << record.Host << ":" << record.Port;
            if (!resolvedAddress.empty()) {
                if (record.ExpectedIP &&
                    std::find(
                        resolvedAddress.begin(),
                        resolvedAddress.end(),
                        record.ExpectedIP) == resolvedAddress.end()) {
                    ResolvedToNotExpectedIP->Inc();
                }
                newResolutionTable.emplace(
                    std::move(hostname), std::move(resolvedAddress));
            } else {
                auto it = DnsResolutionTable.find(hostname);
                if (it != DnsResolutionTable.end()) {
                    newResolutionTable.emplace(std::move(hostname), it->second);
                }
            }
        }

        if (newResolutionTable != DnsResolutionTable) {
            YQL_CLOG(INFO, HttpGateway)
                << "New resolution table contains changes compared current one. "
                   "Going to generate new Curl list";
            DnsResolutionTable = std::move(newResolutionTable);
            SetDNSCurlList(ConvertDNSTableToCurlList());
        } else {
            YQL_CLOG(DEBUG, HttpGateway)
                << "New resolution table same as current "
                   "one. No update is required";
        }
    }

    std::vector<TString> ResolveHostname(
        const TString& host,
        ui16 port,
        TExplicitDNSRecord::ProtocolVersion protocolVersion) const {

        TotalResolutionCounter->Inc();
        std::vector<TString> result;
        try {
            const std::vector<NAddr::TOpaqueAddr> addresses = DnsResolver.Resolve(host, port);
            for (auto& addr: addresses) {
                switch (addr.Addr()->sa_family) {
                    case AF_INET:
                        if (protocolVersion ==
                                TExplicitDNSRecord_ProtocolVersion_ANY ||
                            protocolVersion ==
                                TExplicitDNSRecord_ProtocolVersion_IPV4) {
                            result.emplace_back(NAddr::PrintHost(addr));
                        } else {
                            YQL_CLOG(WARN, HttpGateway)
                                << "Discarding IPV4 address as other protocol version was "
                                   "configured for hostname: "
                                << host << ":" << port;
                        }
                        break;

                    case AF_INET6:
                        if (protocolVersion ==
                                TExplicitDNSRecord_ProtocolVersion_ANY ||
                            protocolVersion ==
                                TExplicitDNSRecord_ProtocolVersion_IPV6) {
                            result.emplace_back(NAddr::PrintHost(addr));
                        } else {
                            YQL_CLOG(WARN, HttpGateway)
                                << "Discarding IPV6 address as other protocol version was "
                                   "configured for hostname: "
                                << host << ":" << port;
                        }
                        break;
                }
            }
        } catch (const TNetworkResolutionError& e) {
            YQL_CLOG(ERROR, HttpGateway)
                << "An exception was raised during DNS "
                   "resolution for hostname: '"
                << host << ":" << port << "' with error:" << e.AsStrBuf();
            ResolutionErrorCounter->Inc();
            return result;
        }

        if (result.empty()) {
            YQL_CLOG(WARN, HttpGateway)
                << "No IPV4 or IPV6 address was recieved as a result of DNS "
                   "resolution for hostname: "
                << host << ":" << port;
            ResolutionErrorCounter->Inc();
        } else {
            ResolutionSuccessCounter->Inc();
        }

        return result;
    }

    void StopThread() {
        {
            auto lock = std::lock_guard{Sync};
            if (IsStopped) {
                return;
            }
            IsStopped = true;
        }

        IsStoppedConditionalVariable.notify_all();
        YQL_CLOG(DEBUG, HttpGateway)
            << "Requested DNS gateway thread termination";
        if (Thread.joinable()) {
            Thread.join();
        }
        YQL_CLOG(DEBUG, HttpGateway)
            << "DNS Gateway thread termination finished";
    }

    std::shared_ptr<curl_slist> ConvertDNSTableToCurlList() const {
        curl_slist* dnsRecords = nullptr;
        for (const auto& [hostname, addresses]: DnsResolutionTable) {
            auto curlDnsRecord = TStringBuilder() << hostname << ":";

            bool isFirst = true;
            for (const auto& address: addresses) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    curlDnsRecord << ",";
                }
                curlDnsRecord << address;
            }

            YQL_CLOG(INFO, HttpGateway)
                << "Adding new DNS entry: " << curlDnsRecord;

            dnsRecords = curl_slist_append(dnsRecords, curlDnsRecord.c_str());
        }
        return std::shared_ptr<curl_slist>{dnsRecords, &curl_slist_free_all};
    }

private:
    std::mutex Sync;
    std::thread Thread;
    std::condition_variable IsStoppedConditionalVariable;
    bool IsStopped = false;

    std::chrono::milliseconds UpdateInterval;
    std::vector<TResolutionTask> HostAddressedToResolve;

    Resolver DnsResolver;
    TDNSResolutionTable DnsResolutionTable;
    TDNSCurlListPtr DnsCurlList;

    const ::NMonitoring::TDynamicCounters::TCounterPtr TotalResolutionCounter;
    const ::NMonitoring::TDynamicCounters::TCounterPtr ResolutionSuccessCounter;
    const ::NMonitoring::TDynamicCounters::TCounterPtr ResolutionErrorCounter;
    const ::NMonitoring::TDynamicCounters::TCounterPtr ResolvedToNotExpectedIP;
};

} // namespace NYql
