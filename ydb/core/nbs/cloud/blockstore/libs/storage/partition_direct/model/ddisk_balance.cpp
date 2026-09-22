#include "ddisk_balance.h"

#include <util/generic/hash_set.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

// Distribute the remainder to the most loaded hosts to minimize moves.
std::array<size_t, MaxHostCount> CalculateTargetDDiskCountByHost(
    THostMask allowedForBalancing,
    const std::array<size_t, MaxHostCount>& ddiskCountByHost)
{
    const size_t balanceHostCount = allowedForBalancing.Count();
    size_t ddiskCount = 0;
    for (THostIndex host: allowedForBalancing) {
        ddiskCount += ddiskCountByHost[host];
    }

    std::array<size_t, MaxHostCount> targetCounts{};
    std::array<bool, MaxHostCount> hasExtra{};
    const size_t baseCount = ddiskCount / balanceHostCount;
    for (THostIndex host: allowedForBalancing) {
        targetCounts[host] = baseCount;
    }
    for (size_t extra = ddiskCount % balanceHostCount; extra != 0; --extra) {
        THostIndex selected = InvalidHostIndex;
        for (THostIndex host: allowedForBalancing) {
            if (!hasExtra[host] &&
                (selected == InvalidHostIndex ||
                 ddiskCountByHost[host] > ddiskCountByHost[selected]))
            {
                selected = host;
            }
        }
        hasExtra[selected] = true;
        ++targetCounts[selected];
    }
    return targetCounts;
}

// Choose the least loaded eligible target, including requests already planned.
THostIndex FindTargetHost(
    const TVChunkConfig& config,
    THostMask allowedForBalancing,
    const std::array<size_t, MaxHostCount>& ddiskCountByHost,
    const std::array<size_t, MaxHostCount>& targetCounts,
    const std::array<size_t, MaxHostCount>& requestedIncoming)
{
    const auto ddisks = config.GetDDisks();
    const auto disabledHosts = config.GetDisabledHosts();
    THostIndex targetHost = InvalidHostIndex;
    for (THostIndex host: allowedForBalancing) {
        if (host >= config.GetHostCount() || disabledHosts.Get(host) ||
            ddisks.Get(host) ||
            ddiskCountByHost[host] + requestedIncoming[host] >=
                targetCounts[host])
        {
            continue;
        }
        if (targetHost == InvalidHostIndex ||
            ddiskCountByHost[host] + requestedIncoming[host] <
                ddiskCountByHost[targetHost] + requestedIncoming[targetHost])
        {
            targetHost = host;
        }
    }
    return targetHost;
}

std::array<TVector<const TVChunkConfig*>, MaxHostCount> CollectVChunksByHost(
    const TVector<const TVChunkConfig*>& vChunks,
    const std::array<size_t, MaxHostCount>& toMove)
{
    std::array<TVector<const TVChunkConfig*>, MaxHostCount> vchunksByHost;

    // Keep registration order within each priority group.
    for (size_t priority = 0; priority < 2; ++priority) {
        for (const auto* config: vChunks) {
            if ((config->GetEnabledDDisks().Count() >= 4) != (priority == 1)) {
                continue;
            }

            for (THostIndex host: config->GetEnabledDDisks()) {
                if (toMove[host] != 0) {
                    vchunksByHost[host].push_back(config);
                }
            }
        }
    }

    return vchunksByHost;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVector<TDDiskBalanceRequest> PlanDDiskBalance(
    const TVector<const TVChunkConfig*>& vChunks,
    THostMask allowedForBalancing,
    const std::array<size_t, MaxHostCount>& ddiskCountByHost)
{
    TVector<TDDiskBalanceRequest> requests;
    if (allowedForBalancing.Empty()) {
        return requests;
    }

    const auto targetCounts =
        CalculateTargetDDiskCountByHost(allowedForBalancing, ddiskCountByHost);

    std::array<size_t, MaxHostCount> toMove{};
    for (THostIndex host: allowedForBalancing) {
        if (ddiskCountByHost[host] > targetCounts[host]) {
            toMove[host] = ddiskCountByHost[host] - targetCounts[host];
        }
    }

    const auto vchunksByHost = CollectVChunksByHost(vChunks, toMove);

    THashSet<ui32> requestedVChunks;
    std::array<size_t, MaxHostCount> requestedIncoming{};
    for (THostIndex source: allowedForBalancing) {
        for (const auto* config: vchunksByHost[source]) {
            if (toMove[source] == 0) {
                break;
            }

            if (requestedVChunks.contains(config->GetVChunkIndex())) {
                continue;
            }

            const THostIndex targetHost = FindTargetHost(
                *config,
                allowedForBalancing,
                ddiskCountByHost,
                targetCounts,
                requestedIncoming);
            if (targetHost == InvalidHostIndex) {
                continue;
            }

            requests.push_back(
                {.VChunkId = config->GetVChunkIndex(),
                 .SourceHost = source,
                 .TargetHost = targetHost});
            requestedVChunks.insert(config->GetVChunkIndex());
            ++requestedIncoming[targetHost];
            --toMove[source];
        }
    }
    return requests;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
