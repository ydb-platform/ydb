#pragma once

#include <util/generic/hash.h>
#include <util/generic/yexception.h>
#include <util/system/types.h>

#include <optional>
#include <span>
#include <vector>

namespace NKikimr::NKqp {

// Each consumer occurs once; producer i keeps degree M/N + (i < M%N).
// Node-local choices come first, without changing the endpoint budget on any node.
inline std::vector<std::vector<size_t>> MakeLocalScatterTopology(
    std::span<const std::optional<ui64>> producerNodes,
    std::span<const std::optional<ui64>> consumerNodes)
{
    const size_t producers = producerNodes.size();
    const size_t consumers = consumerNodes.size();
    Y_ENSURE(producers && consumers >= producers);

    struct TLocalConsumers {
        std::vector<size_t> Indices;
        size_t Next = 0;
    };
    THashMap<ui64, TLocalConsumers> byNode;
    for (size_t consumer = 0; consumer < consumers; ++consumer) {
        if (consumerNodes[consumer]) {
            byNode[*consumerNodes[consumer]].Indices.push_back(consumer);
        }
    }

    std::vector<std::vector<size_t>> result(producers);
    std::vector<bool> assigned(consumers, false);
    const auto degree = [&](size_t producer) {
        return consumers / producers + (producer < consumers % producers);
    };
    const auto takeLocal = [&](size_t producer, size_t limit) {
        if (!producerNodes[producer]) {
            return;
        }
        const auto it = byNode.find(*producerNodes[producer]);
        if (it == byNode.end()) {
            return;
        }
        auto& local = it->second;
        auto& targets = result[producer];
        while (targets.size() < limit && local.Next < local.Indices.size()) {
            const size_t consumer = local.Indices[local.Next++];
            targets.push_back(consumer);
            assigned[consumer] = true;
        }
    };

    // Give every producer a chance at a local first channel before filling its remaining fan-out.
    for (size_t producer = 0; producer < producers; ++producer) {
        result[producer].reserve(degree(producer));
        takeLocal(producer, 1);
    }
    for (size_t producer = 0; producer < producers; ++producer) {
        takeLocal(producer, degree(producer));
    }

    // Reserve all local matches before taking remote or unplaced consumers.
    size_t nextConsumer = 0;
    for (size_t producer = 0; producer < producers; ++producer) {
        auto& targets = result[producer];
        while (targets.size() < degree(producer)) {
            while (nextConsumer < consumers && assigned[nextConsumer]) {
                ++nextConsumer;
            }
            Y_ENSURE(nextConsumer < consumers);
            targets.push_back(nextConsumer++);
        }
    }
    return result;
}

} // namespace NKikimr::NKqp
