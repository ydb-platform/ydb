#include <ydb/services/persqueue_cluster_discovery/cluster_ordering/weighed_ordering.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <algorithm>
#include <util/generic/string.h>
#include <vector>

namespace {

struct TCluster {
    TString Name;
    ui64 Weight = 0;
    ui64 OriginalIndex = 0;
};

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    std::vector<TCluster> clusters;
    clusters.reserve(fdp.ConsumeIntegralInRange<size_t>(0, 32));
    while (fdp.remaining_bytes() && clusters.size() < clusters.capacity()) {
        TCluster cluster;
        cluster.Name = TString(fdp.ConsumeRandomLengthString(32));
        cluster.Weight = fdp.ConsumeIntegralInRange<ui64>(0, 1'000'000);
        cluster.OriginalIndex = clusters.size();
        clusters.push_back(std::move(cluster));
    }

    if (clusters.empty()) {
        return 0;
    }

    if (clusters.size() > 1) {
        for (auto& cluster : clusters) {
            if (cluster.Weight == 0) {
                cluster.Weight = 1;
            }
        }
    }

    const ui64 hashValue = fdp.ConsumeIntegral<ui64>();

    auto selected = NKikimr::NPQ::NClusterDiscovery::NClusterOrdering::SelectByHashAndWeight(
        clusters.begin(),
        clusters.end(),
        hashValue,
        [](const TCluster& cluster) { return cluster.Weight; });
    if (selected != clusters.end()) {
        (void)selected->Name;
        (void)selected->Weight;
    }

    auto ordered = clusters;
    NKikimr::NPQ::NClusterDiscovery::NClusterOrdering::OrderByHashAndWeight(
        ordered.begin(),
        ordered.end(),
        hashValue,
        [](const TCluster& cluster) { return cluster.Weight; });

    if (ordered.size() > 1) {
        std::sort(
            ordered.begin(),
            ordered.end(),
            [](const TCluster& lhs, const TCluster& rhs) {
                return lhs.OriginalIndex < rhs.OriginalIndex;
            });
    }

    return 0;
}
