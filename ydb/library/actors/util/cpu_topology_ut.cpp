#include "cpu_topology.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/str.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/system/info.h>

#include <map>

namespace {

TString ToCpuListString(const TCpuMask& mask) {
    TStringStream out;
    bool firstRange = true;

    for (TCpuId cpu = 0; cpu < mask.Size();) {
        if (!mask.IsSet(cpu)) {
            ++cpu;
            continue;
        }

        const TCpuId begin = cpu;
        TCpuId end = cpu;
        while (end + 1 < mask.Size() && mask.IsSet(end + 1)) {
            ++end;
        }

        if (!firstRange) {
            out << ",";
        }
        firstRange = false;

        if (begin == end) {
            out << begin;
        } else {
            out << begin << "-" << end;
        }

        cpu = end + 1;
    }

    return out.Str();
}

TFsPath JoinPath(const TFsPath& root, const TString& path) {
    return root / path;
}

void WriteFile(const TFsPath& root, const TString& path, const TString& data) {
    const TFsPath fullPath(JoinPath(root, path));
    fullPath.Parent().MkDirs();
    TFileOutput(fullPath.GetPath()).Write(data);
}

void WriteCpu(const TFsPath& root, TCpuId cpuId, ui32 dieId, const TString& dieCpus, ui32 l3CacheId, const TString& l3CacheCpus) {
    const TString cpuPath = "cpu/cpu" + ToString(cpuId);
    WriteFile(root, cpuPath + "/topology/core_id", ToString(cpuId) + "\n");
    WriteFile(root, cpuPath + "/topology/core_cpus_list", ToString(cpuId) + "\n");
    WriteFile(root, cpuPath + "/topology/thread_siblings_list", ToString(cpuId) + "\n");
    WriteFile(root, cpuPath + "/topology/die_id", ToString(dieId) + "\n");
    WriteFile(root, cpuPath + "/topology/die_cpus_list", dieCpus + "\n");
    WriteFile(root, cpuPath + "/topology/physical_package_id", "0\n");
    WriteFile(root, cpuPath + "/topology/package_cpus_list", "0-1\n");
    WriteFile(root, cpuPath + "/cache/index3/level", "3\n");
    WriteFile(root, cpuPath + "/cache/index3/id", ToString(l3CacheId) + "\n");
    WriteFile(root, cpuPath + "/cache/index3/shared_cpu_list", l3CacheCpus + "\n");
}

class TMockAffinityBackend final: public ICpuAffinityBackend {
public:
    mutable std::map<size_t, TCpuMask> Affinity;

    std::expected<TCpuMask, TString> GetAffinity(size_t pid = 0) const override {
        auto it = Affinity.find(pid);
        if (it != Affinity.end()) {
            return it->second;
        }
        return TCpuMask();
    }

    std::expected<void, TString> SetAffinity(const TCpuMask& mask, size_t pid = 0) const override {
        Affinity[pid] = mask;
        return {};
    }
};

TFsPath SnapshotSystemRoot(const TString& name) {
    return TFsPath(ArcadiaSourceRoot()) / "ydb/library/actors/util/testdata/cpu_topology" / name / "sys/devices/system";
}

TCpuTopology LoadSnapshot(const TString& name) {
    auto topology = ParseSysfsCpuTopology(SnapshotSystemRoot(name));
    UNIT_ASSERT_C(topology.has_value(), topology.error());
    return std::move(topology).value();
}

void AssertGroup(const TVector<TCpuTopologyGroup>& groups, size_t index, ui32 id, const TString& cpus) {
    UNIT_ASSERT_C(index < groups.size(), "missing CPU topology group at index " << index);
    UNIT_ASSERT_VALUES_EQUAL(groups[index].Id, id);
    UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(groups[index].Cpus), cpus);
}

void AssertSequentialPlacementGroupIds(const TCpuTopology& topology) {
    ui32 placementGroupId = 0;
    for (const auto& group : topology.PlacementGroups) {
        UNIT_ASSERT_VALUES_EQUAL(group.Id, placementGroupId++);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(CpuTopology) {
    Y_UNIT_TEST(CpuListParsingAndFormatting) {
        TCpuMask parsed(TString("0,2-4,8"));
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(parsed), "0,2-4,8");

        TCpuMask empty;
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(empty), "");
    }

    Y_UNIT_TEST(AffinityBackendIsMockable) {
        TMockAffinityBackend affinity;
        TCpuMask mask(TString("1,3-4"));
        auto set = affinity.SetAffinity(mask, 42);
        UNIT_ASSERT_C(set.has_value(), set.error());

        auto got = affinity.GetAffinity(42);
        UNIT_ASSERT_C(got.has_value(), got.error());
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(got.value()), "1,3-4");

        auto missing = affinity.GetAffinity(43);
        UNIT_ASSERT_C(missing.has_value(), missing.error());
        UNIT_ASSERT(missing->IsEmpty());
    }

    Y_UNIT_TEST(EmptySourceReturnsError) {
        TTempDir tempDir;

        auto topology = ParseSysfsCpuTopology(tempDir.Name());
        UNIT_ASSERT(!topology);
        UNIT_ASSERT_STRING_CONTAINS(topology.error(), "no CPU topology data");
    }

    Y_UNIT_TEST(InvalidSnapshotFieldReturnsError) {
        TTempDir tempDir;
        WriteFile(tempDir.Name(), "cpu/cpu0/topology/core_id", "bad\n");

        auto topology = ParseSysfsCpuTopology(tempDir.Name());
        UNIT_ASSERT(!topology);
        UNIT_ASSERT_STRING_CONTAINS(topology.error(), "cpu/cpu0/topology/core_id");
    }

    Y_UNIT_TEST(LegacyTopologyMaskNamesAreUsed) {
        TTempDir tempDir;
        WriteFile(tempDir.Name(), "cpu/cpu0/topology/core_id", "0\n");
        WriteFile(tempDir.Name(), "cpu/cpu0/topology/thread_siblings_list", "0,4\n");
        WriteFile(tempDir.Name(), "cpu/cpu0/topology/physical_package_id", "7\n");
        WriteFile(tempDir.Name(), "cpu/cpu0/topology/core_siblings_list", "0-7\n");

        auto topology = ParseSysfsCpuTopology(tempDir.Name());
        UNIT_ASSERT_C(topology.has_value(), topology.error());
        UNIT_ASSERT_VALUES_EQUAL(topology->Cpus.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(topology->AllCpus), "0");

        const TLogicalCpuInfo& cpu = topology->Cpus[0];
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu.CoreCpus), "0,4");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu.ThreadSiblings), "0,4");
        UNIT_ASSERT_VALUES_EQUAL(cpu.PackageId, 7u);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu.PackageCpus), "0-7");

        UNIT_ASSERT_VALUES_EQUAL(topology->Packages.size(), 1);
        AssertGroup(topology->Packages, 0, 7, "0-7");
    }

    Y_UNIT_TEST(InvalidLegacyPackageMaskReturnsError) {
        TTempDir tempDir;
        WriteFile(tempDir.Name(), "cpu/cpu0/topology/core_id", "0\n");
        WriteFile(tempDir.Name(), "cpu/cpu0/topology/thread_siblings_list", "0\n");
        WriteFile(tempDir.Name(), "cpu/cpu0/topology/physical_package_id", "0\n");
        WriteFile(tempDir.Name(), "cpu/cpu0/topology/core_siblings_list", "bad\n");

        auto topology = ParseSysfsCpuTopology(tempDir.Name());
        UNIT_ASSERT(!topology);
        UNIT_ASSERT_STRING_CONTAINS(topology.error(), "cpu/cpu0/topology/core_siblings_list");
    }

    Y_UNIT_TEST(PlacementGroupsUseL3CacheLocality) {
        TTempDir tempDir;
        WriteCpu(tempDir.Name(), 0, 0, "0", 10, "0-1");
        WriteCpu(tempDir.Name(), 1, 1, "1", 10, "0-1");

        auto topology = ParseSysfsCpuTopology(tempDir.Name());
        UNIT_ASSERT_C(topology.has_value(), topology.error());
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(topology->AllCpus), "0-1");

        UNIT_ASSERT_VALUES_EQUAL(topology->Dies.size(), 2);
        AssertGroup(topology->Dies, 0, 0, "0");
        AssertGroup(topology->Dies, 1, 1, "1");
        UNIT_ASSERT_VALUES_EQUAL(topology->L3CacheGroups.size(), 1);
        AssertGroup(topology->L3CacheGroups, 0, 10, "0-1");
        UNIT_ASSERT_VALUES_EQUAL(topology->PlacementGroups.size(), 1);
        AssertGroup(topology->PlacementGroups, 0, 0, "0-1");
    }

    Y_UNIT_TEST(AmdEpyc9654SnapshotUsesL3PlacementGroups) {
        const TCpuTopology topology = LoadSnapshot("amd-epyc-9654");

        UNIT_ASSERT_VALUES_EQUAL(topology.Cpus.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(topology.AllCpus), "0,104,192");
        UNIT_ASSERT_VALUES_EQUAL(topology.NumaNodes.size(), 2);
        AssertGroup(topology.NumaNodes, 0, 0, "0-95,192-287");
        AssertGroup(topology.NumaNodes, 1, 1, "96-191,288-383");

        const TLogicalCpuInfo* cpu0 = topology.FindCpu(0);
        UNIT_ASSERT(cpu0);
        UNIT_ASSERT(cpu0->Online);
        UNIT_ASSERT_VALUES_EQUAL(cpu0->CoreId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu0->ClusterId, 65535u);
        UNIT_ASSERT_VALUES_EQUAL(cpu0->DieId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu0->PackageId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu0->NumaNodeId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu0->L3CacheId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu0->CoreCpus), "0,192");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu0->ThreadSiblings), "0,192");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu0->ClusterCpus), "0,192");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu0->DieCpus), "0-7,192-199");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu0->PackageCpus), "0-95,192-287");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu0->L3CacheCpus), "0-7,192-199");

        const TLogicalCpuInfo* cpu104 = topology.FindCpu(104);
        UNIT_ASSERT(cpu104);
        UNIT_ASSERT_VALUES_EQUAL(cpu104->CoreId, 32u);
        UNIT_ASSERT_VALUES_EQUAL(cpu104->ClusterId, 65535u);
        UNIT_ASSERT_VALUES_EQUAL(cpu104->DieId, 20u);
        UNIT_ASSERT_VALUES_EQUAL(cpu104->PackageId, 1u);
        UNIT_ASSERT_VALUES_EQUAL(cpu104->NumaNodeId, 1u);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu104->NumaNodeCpus), "96-191,288-383");
        UNIT_ASSERT_VALUES_EQUAL(cpu104->L3CacheId, 20u);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu104->CoreCpus), "104,296");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu104->ThreadSiblings), "104,296");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu104->ClusterCpus), "104,296");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu104->DieCpus), "104-111,296-303");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu104->PackageCpus), "96-191,288-383");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu104->L3CacheCpus), "104-111,296-303");

        const TLogicalCpuInfo* cpu192 = topology.FindCpu(192);
        UNIT_ASSERT(cpu192);
        UNIT_ASSERT_VALUES_EQUAL(cpu192->CoreId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu192->ClusterId, 65535u);
        UNIT_ASSERT_VALUES_EQUAL(cpu192->DieId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu192->PackageId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu192->NumaNodeId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu192->L3CacheId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu192->CoreCpus), "0,192");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu192->ThreadSiblings), "0,192");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu192->ClusterCpus), "0,192");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu192->DieCpus), "0-7,192-199");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu192->PackageCpus), "0-95,192-287");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu192->L3CacheCpus), "0-7,192-199");

        AssertGroup(topology.Packages, 0, 0, "0-95,192-287");
        AssertGroup(topology.Packages, 1, 1, "96-191,288-383");
        UNIT_ASSERT_VALUES_EQUAL(topology.Packages.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(topology.Dies.size(), 2);
        AssertGroup(topology.Dies, 0, 0, "0-7,192-199");
        AssertGroup(topology.Dies, 1, 20, "104-111,296-303");
        UNIT_ASSERT_VALUES_EQUAL(topology.L3CacheGroups.size(), 2);
        AssertGroup(topology.L3CacheGroups, 0, 0, "0-7,192-199");
        AssertGroup(topology.L3CacheGroups, 1, 20, "104-111,296-303");
        UNIT_ASSERT_VALUES_EQUAL(topology.PlacementGroups.size(), 2);
        AssertSequentialPlacementGroupIds(topology);
        AssertGroup(topology.PlacementGroups, 0, 0, "0-7,192-199");
        AssertGroup(topology.PlacementGroups, 1, 1, "104-111,296-303");
    }

    Y_UNIT_TEST(IntelXeonGold6230SnapshotUsesL3PlacementGroups) {
        const TCpuTopology topology = LoadSnapshot("intel-xeon-gold-6230");

        UNIT_ASSERT_VALUES_EQUAL(topology.Cpus.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(topology.AllCpus), "0,20,40");
        UNIT_ASSERT_VALUES_EQUAL(topology.NumaNodes.size(), 2);
        AssertGroup(topology.NumaNodes, 0, 0, "0-19,40-59");
        AssertGroup(topology.NumaNodes, 1, 1, "20-39,60-79");

        const TLogicalCpuInfo* cpu0 = topology.FindCpu(0);
        UNIT_ASSERT(cpu0);
        UNIT_ASSERT(cpu0->Online);
        UNIT_ASSERT_VALUES_EQUAL(cpu0->CoreId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu0->ClusterId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu0->DieId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu0->PackageId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu0->NumaNodeId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu0->L3CacheId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu0->CoreCpus), "0,40");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu0->ThreadSiblings), "0,40");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu0->ClusterCpus), "0,40");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu0->DieCpus), "0-19,40-59");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu0->PackageCpus), "0-19,40-59");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu0->L3CacheCpus), "0-19,40-59");

        const TLogicalCpuInfo* cpu20 = topology.FindCpu(20);
        UNIT_ASSERT(cpu20);
        UNIT_ASSERT_VALUES_EQUAL(cpu20->CoreId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu20->ClusterId, 64u);
        UNIT_ASSERT_VALUES_EQUAL(cpu20->DieId, 1u);
        UNIT_ASSERT_VALUES_EQUAL(cpu20->PackageId, 1u);
        UNIT_ASSERT_VALUES_EQUAL(cpu20->NumaNodeId, 1u);
        UNIT_ASSERT_VALUES_EQUAL(cpu20->L3CacheId, 1u);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu20->CoreCpus), "20,60");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu20->ThreadSiblings), "20,60");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu20->ClusterCpus), "20,60");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu20->DieCpus), "20-39,60-79");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu20->PackageCpus), "20-39,60-79");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu20->L3CacheCpus), "20-39,60-79");

        const TLogicalCpuInfo* cpu40 = topology.FindCpu(40);
        UNIT_ASSERT(cpu40);
        UNIT_ASSERT_VALUES_EQUAL(cpu40->CoreId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu40->ClusterId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu40->DieId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu40->PackageId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu40->NumaNodeId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(cpu40->L3CacheId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu40->CoreCpus), "0,40");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu40->ThreadSiblings), "0,40");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu40->ClusterCpus), "0,40");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu40->DieCpus), "0-19,40-59");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu40->PackageCpus), "0-19,40-59");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu40->L3CacheCpus), "0-19,40-59");

        AssertGroup(topology.Packages, 0, 0, "0-19,40-59");
        AssertGroup(topology.Packages, 1, 1, "20-39,60-79");
        AssertGroup(topology.Dies, 0, 0, "0-19,40-59");
        AssertGroup(topology.Dies, 1, 1, "20-39,60-79");
        AssertGroup(topology.L3CacheGroups, 0, 0, "0-19,40-59");
        AssertGroup(topology.L3CacheGroups, 1, 1, "20-39,60-79");
        UNIT_ASSERT_VALUES_EQUAL(topology.Packages.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(topology.Dies.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(topology.L3CacheGroups.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(topology.PlacementGroups.size(), 2);
        AssertSequentialPlacementGroupIds(topology);
        AssertGroup(topology.PlacementGroups, 0, 0, "0-19,40-59");
        AssertGroup(topology.PlacementGroups, 1, 1, "20-39,60-79");
    }

    Y_UNIT_TEST(IntelMeteorLakeSnapshotUsesL3AndRemainingPlacementGroups) {
        const TCpuTopology topology = LoadSnapshot("intel-meteor-lake");

        UNIT_ASSERT_VALUES_EQUAL(topology.Cpus.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(topology.AllCpus), "0,10,20");
        UNIT_ASSERT_VALUES_EQUAL(topology.NumaNodes.size(), 1);
        AssertGroup(topology.NumaNodes, 0, 0, "0-21");

        const TLogicalCpuInfo* cpu0 = topology.FindCpu(0);
        UNIT_ASSERT(cpu0);
        UNIT_ASSERT_VALUES_EQUAL(cpu0->Capacity, 1024u);
        UNIT_ASSERT_VALUES_EQUAL(cpu0->CoreId, 16u);
        UNIT_ASSERT_VALUES_EQUAL(cpu0->ClusterId, 32u);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu0->ThreadSiblings), "0,5");
        UNIT_ASSERT_VALUES_EQUAL(cpu0->L3CacheId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu0->L3CacheCpus), "0-19");

        const TLogicalCpuInfo* cpu20 = topology.FindCpu(20);
        UNIT_ASSERT(cpu20);
        UNIT_ASSERT_VALUES_EQUAL(cpu20->CoreId, 32u);
        UNIT_ASSERT_VALUES_EQUAL(cpu20->ClusterId, 64u);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu20->ThreadSiblings), "20");
        UNIT_ASSERT_VALUES_EQUAL(cpu20->L3CacheId, UnknownCpuTopologyId);
        UNIT_ASSERT(cpu20->L3CacheCpus.IsEmpty());

        UNIT_ASSERT_VALUES_EQUAL(topology.Packages.size(), 1);
        AssertGroup(topology.Packages, 0, 0, "0-21");
        UNIT_ASSERT_VALUES_EQUAL(topology.Dies.size(), 1);
        AssertGroup(topology.Dies, 0, 0, "0-21");
        UNIT_ASSERT_VALUES_EQUAL(topology.L3CacheGroups.size(), 1);
        AssertGroup(topology.L3CacheGroups, 0, 0, "0-19");
        UNIT_ASSERT_VALUES_EQUAL(topology.PlacementGroups.size(), 2);
        AssertSequentialPlacementGroupIds(topology);
        AssertGroup(topology.PlacementGroups, 0, 0, "0-19");
        AssertGroup(topology.PlacementGroups, 1, 1, "20");
    }

#if defined(_win_)
    Y_UNIT_TEST(ParseCpuTopologyUsesSinglePlacementGroupOnWindows) {
        auto topology = ParseCpuTopology();
        UNIT_ASSERT_C(topology.has_value(), topology.error());

        const TCpuId cpuCount = static_cast<TCpuId>(NSystemInfo::CachedNumberOfCpus());
        UNIT_ASSERT_VALUES_EQUAL(topology->Cpus.size(), cpuCount);

        const TString expectedCpus = cpuCount == 1
            ? TString("0")
            : TString("0-") + ToString(cpuCount - 1);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(topology->AllCpus), expectedCpus);

        UNIT_ASSERT_VALUES_EQUAL(topology->NumaNodes.size(), 1);
        AssertGroup(topology->NumaNodes, 0, 0, expectedCpus);
        UNIT_ASSERT_VALUES_EQUAL(topology->L3CacheGroups.size(), 1);
        AssertGroup(topology->L3CacheGroups, 0, 0, expectedCpus);
        UNIT_ASSERT_VALUES_EQUAL(topology->PlacementGroups.size(), 1);
        AssertGroup(topology->PlacementGroups, 0, 0, expectedCpus);

        const TLogicalCpuInfo* cpu0 = topology->FindCpu(0);
        UNIT_ASSERT(cpu0);
        UNIT_ASSERT(cpu0->Online);
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu0->CoreCpus), "0");
        UNIT_ASSERT_VALUES_EQUAL(ToCpuListString(cpu0->L3CacheCpus), expectedCpus);
    }
#endif

}
