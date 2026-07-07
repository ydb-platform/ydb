#include "cpu_topology.h"

#include "affinity.h"

#include <util/folder/path.h>
#include <util/generic/algorithm.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/strip.h>
#include <util/system/error.h>

#include <algorithm>
#include <cctype>
#include <optional>
#include <utility>

namespace {

constexpr TStringBuf CpuDir = "cpu";
constexpr TStringBuf NodeDir = "node";

TFsPath CpuRootPath(const TFsPath& root) {
    return root / CpuDir;
}

TFsPath NodeRootPath(const TFsPath& root) {
    return root / NodeDir;
}

TFsPath CpuPath(const TFsPath& root, TCpuId cpuId) {
    return CpuRootPath(root) / (TString("cpu") + ToString(cpuId));
}

bool IsNumber(TStringBuf value) {
    return !value.empty() && std::all_of(value.begin(), value.end(), [](auto ch) {
        return std::isdigit(ch);
    });
}

std::optional<TCpuId> TryParseCpuName(TStringBuf name) {
    if (!name.StartsWith("cpu")) {
        return std::nullopt;
    }
    TStringBuf id = name.SubStr(3);
    if (!IsNumber(id)) {
        return std::nullopt;
    }
    TCpuId cpuId = 0;
    if (!TryFromString(id, cpuId)) {
        return std::nullopt;
    }
    return cpuId;
}

std::optional<ui32> TryParseNodeName(TStringBuf name) {
    if (!name.StartsWith("node")) {
        return std::nullopt;
    }
    TStringBuf id = name.SubStr(4);
    if (!IsNumber(id)) {
        return std::nullopt;
    }
    ui32 nodeId = UnknownCpuTopologyId;
    if (!TryFromString(id, nodeId)) {
        return std::nullopt;
    }
    return nodeId;
}

std::expected<std::optional<TString>, TString> ReadFile(const TFsPath& path) {
    try {
        if (!path.IsFile()) {
            return std::nullopt;
        }
        return TFileInput(path.GetPath()).ReadAll();
    } catch (...) {
        return std::unexpected("failed to read file " + path.GetPath() + ": " + CurrentExceptionMessage());
    }
}

std::expected<TVector<TString>, TString> ListDirectory(const TFsPath& path) {
    try {
        if (!path.IsDirectory()) {
            return TVector<TString>();
        }

        TVector<TString> children;
        path.ListNames(children);
        std::sort(children.begin(), children.end());
        return children;
    } catch (...) {
        return std::unexpected("failed to list directory " + path.GetPath() + ": " + CurrentExceptionMessage());
    }
}

std::expected<std::optional<TString>, TString> ReadValue(const TFsPath& path) {
    auto data = ReadFile(path);
    if (!data) {
        return std::unexpected{std::move(data).error()};
    }
    if (!*data) {
        return std::nullopt;
    }
    return StripString(**data);
}

std::expected<ui64, TString> ReadUi64(const TFsPath& path, ui64 defaultValue = 0) {
    auto data = ReadValue(path);
    if (!data) {
        return std::unexpected{std::move(data).error()};
    }
    if (!*data || (*data)->empty()) {
        return defaultValue;
    }
    ui64 value = defaultValue;
    if (!TryFromString(**data, value)) {
        return std::unexpected("failed to parse unsigned integer field " + path.GetPath() + ": '" + **data + "'");
    }
    return value;
}

std::expected<ui32, TString> ReadTopologyId(const TFsPath& path, ui32 defaultValue = UnknownCpuTopologyId) {
    auto data = ReadValue(path);
    if (!data) {
        return std::unexpected{std::move(data).error()};
    }
    if (!*data || (*data)->empty()) {
        return defaultValue;
    }
    if (**data == "-1") {
        return UnknownCpuTopologyId;
    }

    auto value = ReadUi64(path, defaultValue);
    if (!value) {
        return std::unexpected{std::move(value).error()};
    }
    if (*value > UnknownCpuTopologyId) {
        return std::unexpected("id field is too large " + path.GetPath() + ": '" + **data + "'");
    }
    return static_cast<ui32>(*value);
}

std::expected<TCpuMask, TString> ReadCpuMask(const TFsPath& path) {
    auto data = ReadValue(path);
    if (!data) {
        return std::unexpected{std::move(data).error()};
    }
    if (!*data) {
        return TCpuMask();
    }
    if ((*data)->empty() || **data == "(null)") {
        return TCpuMask();
    }
    try {
        return TCpuMask(**data);
    } catch (...) {
        return std::unexpected("failed to parse CPU list field " + path.GetPath() + ": " + CurrentExceptionMessage());
    }
}

std::expected<std::optional<TCpuTopologyGroup>, TString> ParseL3CacheGroup(const TFsPath& root, TCpuId cpuId) {
    const TFsPath cacheRoot = CpuPath(root, cpuId) / "cache";
    auto indexNames = ListDirectory(cacheRoot);
    if (!indexNames) {
        return std::unexpected{std::move(indexNames).error()};
    }
    for (const TString& indexName : *indexNames) {
        if (!indexName.StartsWith("index")) {
            continue;
        }

        const TFsPath indexPath = cacheRoot / indexName;
        auto level = ReadUi64(indexPath / "level", 0);
        if (!level) {
            return std::unexpected{std::move(level).error()};
        }
        if (*level != 3) {
            continue;
        }

        auto id = ReadTopologyId(indexPath / "id");
        if (!id) {
            return std::unexpected{std::move(id).error()};
        }
        auto cpus = ReadCpuMask(indexPath / "shared_cpu_list");
        if (!cpus) {
            return std::unexpected{std::move(cpus).error()};
        }
        return TCpuTopologyGroup{*id, std::move(*cpus)};
    }

    return std::nullopt;
}

std::expected<TVector<TLogicalCpuInfo>, TString> ParseCpus(const TFsPath& root) {
    TVector<TLogicalCpuInfo> cpus;
    auto cpuNames = ListDirectory(CpuRootPath(root));
    if (!cpuNames) {
        return std::unexpected{std::move(cpuNames).error()};
    }
    for (const TString& name : *cpuNames) {
        auto cpuId = TryParseCpuName(name);
        if (!cpuId) {
            continue;
        }

        const TFsPath cpuPath = CpuPath(root, *cpuId);
        TLogicalCpuInfo cpu;
        cpu.CpuId = *cpuId;
        auto online = ReadUi64(cpuPath / "online", 1);
        if (!online) {
            return std::unexpected{std::move(online).error()};
        }
        cpu.Online = *online != 0;
        auto capacity = ReadUi64(cpuPath / "cpu_capacity");
        if (!capacity) {
            return std::unexpected{std::move(capacity).error()};
        }
        cpu.Capacity = *capacity;

        const TFsPath topologyRoot = cpuPath / "topology";
        auto coreId = ReadTopologyId(topologyRoot / "core_id");
        if (!coreId) {
            return std::unexpected{std::move(coreId).error()};
        }
        cpu.CoreId = *coreId;
        auto coreCpus = ReadCpuMask(topologyRoot / "core_cpus_list");
        if (coreCpus && coreCpus->IsEmpty()) {
            coreCpus = ReadCpuMask(topologyRoot / "thread_siblings_list");
        }
        if (!coreCpus) {
            return std::unexpected{std::move(coreCpus).error()};
        }
        cpu.CoreCpus = std::move(*coreCpus);
        auto threadSiblings = ReadCpuMask(topologyRoot / "thread_siblings_list");
        if (!threadSiblings) {
            return std::unexpected{std::move(threadSiblings).error()};
        }
        cpu.ThreadSiblings = std::move(*threadSiblings);

        auto clusterId = ReadTopologyId(topologyRoot / "cluster_id");
        if (!clusterId) {
            return std::unexpected{std::move(clusterId).error()};
        }
        cpu.ClusterId = *clusterId;
        auto clusterCpus = ReadCpuMask(topologyRoot / "cluster_cpus_list");
        if (!clusterCpus) {
            return std::unexpected{std::move(clusterCpus).error()};
        }
        cpu.ClusterCpus = std::move(*clusterCpus);

        auto l3Cache = ParseL3CacheGroup(root, *cpuId);
        if (!l3Cache) {
            return std::unexpected{std::move(l3Cache).error()};
        }
        if (*l3Cache) {
            cpu.L3CacheId = (*l3Cache)->Id;
            cpu.L3CacheCpus = std::move((*l3Cache)->Cpus);
        }

        auto dieId = ReadTopologyId(topologyRoot / "die_id");
        if (!dieId) {
            return std::unexpected{std::move(dieId).error()};
        }
        cpu.DieId = *dieId;
        auto dieCpus = ReadCpuMask(topologyRoot / "die_cpus_list");
        if (!dieCpus) {
            return std::unexpected{std::move(dieCpus).error()};
        }
        cpu.DieCpus = std::move(*dieCpus);

        auto packageId = ReadTopologyId(topologyRoot / "physical_package_id");
        if (!packageId) {
            return std::unexpected{std::move(packageId).error()};
        }
        cpu.PackageId = *packageId;
        auto packageCpus = ReadCpuMask(topologyRoot / "package_cpus_list");
        if (packageCpus && packageCpus->IsEmpty()) {
            packageCpus = ReadCpuMask(topologyRoot / "core_siblings_list");
        }
        if (!packageCpus) {
            return std::unexpected{std::move(packageCpus).error()};
        }
        cpu.PackageCpus = std::move(*packageCpus);

        cpus.push_back(std::move(cpu));
    }

    std::sort(cpus.begin(), cpus.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.CpuId < rhs.CpuId;
    });
    return cpus;
}

void SortGroups(TVector<TCpuTopologyGroup>& groups) {
    std::sort(groups.begin(), groups.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.Id < rhs.Id;
    });
}

std::expected<TVector<TCpuTopologyGroup>, TString> ParseNumaNodes(const TFsPath& root) {
    TVector<TCpuTopologyGroup> nodes;
    const TFsPath nodeRoot = NodeRootPath(root);
    auto nodeNames = ListDirectory(nodeRoot);
    if (!nodeNames) {
        return std::unexpected{std::move(nodeNames).error()};
    }
    for (const TString& name : *nodeNames) {
        auto nodeId = TryParseNodeName(name);
        if (!nodeId) {
            continue;
        }

        TCpuTopologyGroup node;
        node.Id = *nodeId;
        const TFsPath nodePath = nodeRoot / name;
        auto cpus = ReadCpuMask(nodePath / "cpulist");
        if (!cpus) {
            return std::unexpected{std::move(cpus).error()};
        }
        node.Cpus = std::move(*cpus);

        nodes.push_back(std::move(node));
    }

    SortGroups(nodes);
    return nodes;
}

bool MaskEquals(const TCpuMask& lhs, const TCpuMask& rhs) {
    const TCpuId size = Max(lhs.Size(), rhs.Size());
    for (TCpuId cpu = 0; cpu < size; ++cpu) {
        if (lhs.IsSet(cpu) != rhs.IsSet(cpu)) {
            return false;
        }
    }
    return true;
}

void AddUniqueGroup(TVector<TCpuTopologyGroup>& groups, TCpuTopologyGroup group) {
    if (group.Cpus.IsEmpty()) {
        return;
    }
    for (const auto& existing : groups) {
        if (MaskEquals(existing.Cpus, group.Cpus)) {
            return;
        }
    }
    groups.push_back(std::move(group));
}

void BuildDerivedGroups(TCpuTopology& topology) {
    for (const auto& cpu : topology.Cpus) {
        AddUniqueGroup(topology.Clusters, TCpuTopologyGroup{cpu.ClusterId, cpu.ClusterCpus});
        AddUniqueGroup(topology.L3CacheGroups, TCpuTopologyGroup{cpu.L3CacheId, cpu.L3CacheCpus});
        AddUniqueGroup(topology.Dies, TCpuTopologyGroup{cpu.DieId, cpu.DieCpus});
        AddUniqueGroup(topology.Packages, TCpuTopologyGroup{cpu.PackageId, cpu.PackageCpus});
    }
    SortGroups(topology.Clusters);
    SortGroups(topology.L3CacheGroups);
    SortGroups(topology.Dies);
    SortGroups(topology.Packages);

    topology.PlacementGroups = topology.L3CacheGroups;
    ui32 placementGroupId = 0;
    for (auto& group : topology.PlacementGroups) {
        group.Id = placementGroupId++;
    }
}

} // namespace

std::expected<TCpuMask, TString> TSchedCpuAffinityBackend::GetAffinity(size_t pid) const {
    try {
        TAffinity affinity;
        affinity.Current(pid);
        return TCpuMask(affinity);
    } catch (...) {
        return std::unexpected("failed to get CPU affinity: " + CurrentExceptionMessage());
    }
}

std::expected<void, TString> TSchedCpuAffinityBackend::SetAffinity(const TCpuMask& mask, size_t pid) const {
    try {
        TAffinity affinity(mask);
        affinity.Set(pid);
        return {};
    } catch (...) {
        return std::unexpected("failed to set CPU affinity: " + CurrentExceptionMessage());
    }
}

const TLogicalCpuInfo* TCpuTopology::FindCpu(TCpuId cpuId) const {
    const auto it = std::lower_bound(Cpus.begin(), Cpus.end(), cpuId, [](const TLogicalCpuInfo& lhs, TCpuId rhs) {
        return lhs.CpuId < rhs;
    });
    if (it != Cpus.end() && it->CpuId == cpuId) {
        return &*it;
    }
    return nullptr;
}

std::expected<TCpuTopology, TString> ParseSysfsCpuTopology(const TFsPath& root) {
    TCpuTopology result;

    auto cpus = ParseCpus(root);
    if (!cpus) {
        return std::unexpected{std::move(cpus).error()};
    }
    result.Cpus = std::move(*cpus);
    if (result.Cpus.empty()) {
        return std::unexpected("no CPU topology data found under " + CpuRootPath(root).GetPath());
    }

    auto numaNodes = ParseNumaNodes(root);
    if (!numaNodes) {
        return std::unexpected{std::move(numaNodes).error()};
    }
    result.NumaNodes = std::move(*numaNodes);
    for (auto& cpu : result.Cpus) {
        for (const auto& node : result.NumaNodes) {
            if (node.Cpus.IsSet(cpu.CpuId)) {
                cpu.NumaNodeId = node.Id;
                cpu.NumaNodeCpus = node.Cpus;
                break;
            }
        }
    }

    BuildDerivedGroups(result);
    return result;
}
