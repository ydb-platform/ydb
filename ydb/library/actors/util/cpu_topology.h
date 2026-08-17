#pragma once

#include "cpumask.h"
#include "defs.h"

#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <expected>
#include <limits>

inline constexpr ui32 UnknownCpuTopologyId = std::numeric_limits<ui32>::max();

class ICpuAffinityBackend {
public:
    virtual ~ICpuAffinityBackend() = default;

    virtual std::expected<TCpuMask, TString> GetAffinity(size_t pid = 0) const = 0;
    virtual std::expected<void, TString> SetAffinity(const TCpuMask& mask, size_t pid = 0) const = 0;
};

class TSchedCpuAffinityBackend final: public ICpuAffinityBackend {
public:
    std::expected<TCpuMask, TString> GetAffinity(size_t pid = 0) const override;
    std::expected<void, TString> SetAffinity(const TCpuMask& mask, size_t pid = 0) const override;
};

struct TCpuTopologyGroup {
    ui32 Id = UnknownCpuTopologyId;
    TCpuMask Cpus;
};

struct TLogicalCpuInfo {
    TCpuId CpuId = 0;
    bool Online = false;
    ui64 Capacity = 0;

    ui32 CoreId = UnknownCpuTopologyId;
    TCpuMask CoreCpus;
    TCpuMask ThreadSiblings;

    ui32 ClusterId = UnknownCpuTopologyId;
    TCpuMask ClusterCpus;

    ui32 L3CacheId = UnknownCpuTopologyId;
    TCpuMask L3CacheCpus;

    ui32 DieId = UnknownCpuTopologyId;
    TCpuMask DieCpus;

    ui32 PackageId = UnknownCpuTopologyId;
    TCpuMask PackageCpus;

    ui32 NumaNodeId = UnknownCpuTopologyId;
    TCpuMask NumaNodeCpus;
};

struct TCpuTopology {
    // Source CPU information
    TVector<TLogicalCpuInfo> Cpus;
    TCpuMask AllCpus;

    // Derived groupings
    TVector<TCpuTopologyGroup> Clusters;
    TVector<TCpuTopologyGroup> L3CacheGroups;
    TVector<TCpuTopologyGroup> Dies;
    TVector<TCpuTopologyGroup> Packages;
    TVector<TCpuTopologyGroup> NumaNodes;

    // PlacementGroups use shared L3 cache boundaries when available.
    // CPUs without L3 cache data are collected into one extra group.
    // Id is assigned by the parser after sorting (matches index)
    TVector<TCpuTopologyGroup> PlacementGroups;

    const TLogicalCpuInfo* FindCpu(TCpuId cpuId) const;
};

std::expected<TCpuTopology, TString> ParseSysfsCpuTopology(const TFsPath& root);
std::expected<TCpuTopology, TString> ParseCpuTopology();
