#include "ddisk_balance.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>

#include <cmath>

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
            const auto enabledDDisks = config->GetEnabledDDisks();
            const size_t enabledDDiskCount = enabledDDisks.Count();
            if (enabledDDiskCount < QuorumDirectBlockGroupHostCount) {
                continue;
            }

            const bool hasExcessReplica =
                enabledDDiskCount >= QuorumDirectBlockGroupHostCount + 1;
            if (hasExcessReplica != (priority == 1)) {
                continue;
            }

            for (THostIndex host: enabledDDisks) {
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

TDDiskImbalance CalculateDDiskImbalance(
    const std::array<size_t, MaxHostCount>& ddiskCountByHost,
    THostMask allowedForBalancing)
{
    const size_t hostCount = allowedForBalancing.Count();
    if (hostCount == 0) {
        return {};
    }

    size_t ddiskCount = 0;
    for (THostIndex host: allowedForBalancing) {
        ddiskCount += ddiskCountByHost[host];
    }
    if (ddiskCount == 0) {
        return {};
    }

    const size_t baseCount = ddiskCount / hostCount;
    const size_t extraHosts = ddiskCount % hostCount;
    size_t excess = 0;
    size_t hostsAboveBase = 0;
    for (THostIndex host: allowedForBalancing) {
        const size_t count = ddiskCountByHost[host];
        if (count > baseCount) {
            excess += count - baseCount;
            ++hostsAboveBase;
        }
    }

    // Keep the extra DDisks on already loaded hosts to minimize moves.
    const size_t moves = excess - Min(extraHosts, hostsAboveBase);
    return {
        .Moves = moves,
        .TotalDDiskCount = ddiskCount,
        .Percent = static_cast<ui32>(std::lround(100.0 * moves / ddiskCount)),
    };
}

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
