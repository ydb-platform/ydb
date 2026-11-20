#include "bind_queue.h"
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/defs.h>

using namespace NKikimr;
using namespace NKikimr::NStorage;

Y_UNIT_TEST_SUITE(BindQueue) {
    Y_UNIT_TEST(PostponedPeerSurvivesUpdatesAndEnable) {
        TBindQueue bindQueue;
        bindQueue.Update({1, 2});
        const TMonotonic now = TMonotonic::Seconds(100);
        const TMonotonic retryAt = now + TDuration::Seconds(60);
        bindQueue.Postpone(1, retryAt);
        TMonotonic closest;
        UNIT_ASSERT_VALUES_EQUAL(bindQueue.Pick(now, &closest), std::make_optional<ui32>(2));
        bindQueue.Disable(1);
        bindQueue.Update({1});
        bindQueue.Enable(1);
        UNIT_ASSERT(!bindQueue.Pick(retryAt - TDuration::MicroSeconds(1), &closest));
        UNIT_ASSERT_VALUES_EQUAL(closest, retryAt);
        UNIT_ASSERT_VALUES_EQUAL(bindQueue.Pick(retryAt, &closest), std::make_optional<ui32>(1));
        UNIT_ASSERT(!bindQueue.Pick(retryAt, &closest));
        UNIT_ASSERT_VALUES_EQUAL(closest, retryAt + TDuration::Seconds(1));
        bindQueue.Postpone(1, retryAt + TDuration::Seconds(90));
        bindQueue.Postpone(1, retryAt + TDuration::Seconds(10));
        UNIT_ASSERT(!bindQueue.Pick(retryAt + TDuration::Seconds(60), &closest));
        UNIT_ASSERT_VALUES_EQUAL(closest, retryAt + TDuration::Seconds(90));
    }

    Y_UNIT_TEST(Basic) {
        TBindQueue bindQueue;

        std::vector<ui32> nodes;
        for (ui32 i = 1; i <= 100; ++i) {
            nodes.push_back(i);
        }

        ui32 nextNodeId = nodes.size() + 1;

        std::vector<ui32> enabled(nodes.begin(), nodes.end());
        std::vector<ui32> disabled;

        THashSet<ui32> enabledSet(enabled.begin(), enabled.end());

        bindQueue.Update(nodes);

        TMonotonic now;

        for (ui32 iter = 0; iter < 100000; ++iter) {
            const bool canEnable = !disabled.empty();
            const bool canDisable = !enabled.empty();
            const bool canAddNode = nodes.size() < 100;
            const bool canDeleteNode = nodes.size() >= 10;
            const bool canPick = true;

            const ui32 w = canEnable + canDisable + canAddNode + canDeleteNode + canPick;

            ui32 i = RandomNumber(w);

            if (canEnable && !i--) {
                const size_t index = RandomNumber(disabled.size());
                const ui32 nodeId = disabled[index];
                Cerr << "Enable nodeId# " << nodeId << Endl;
                std::swap(disabled[index], disabled.back());
                disabled.pop_back();
                enabled.push_back(nodeId);
                enabledSet.insert(nodeId);
                bindQueue.Enable(nodeId);
            } else if (canDisable && !i--) {
                const size_t index = RandomNumber(enabled.size());
                const ui32 nodeId = enabled[index];
                Cerr << "Disable nodeId# " << nodeId << Endl;
                std::swap(enabled[index], enabled.back());
                enabled.pop_back();
                enabledSet.erase(nodeId);
                disabled.push_back(nodeId);
                bindQueue.Disable(nodeId);
            } else if (canAddNode && !i--) {
                Cerr << "Add nodeId# " << nextNodeId << Endl;
                nodes.push_back(nextNodeId);
                enabled.push_back(nextNodeId);
                enabledSet.insert(nextNodeId);
                bindQueue.Update(nodes);
                ++nextNodeId;
            } else if (canDeleteNode && !i--) {
                const size_t index = RandomNumber(nodes.size());
                const ui32 nodeId = nodes[index];
                Cerr << "Delete nodeId# " << nodeId << Endl;
                std::swap(nodes[index], nodes.back());
                nodes.pop_back();
                if (const auto it = std::find(enabled.begin(), enabled.end(), nodeId); it != enabled.end()) {
                    std::swap(*it, enabled.back());
                    enabled.pop_back();
                    enabledSet.erase(nodeId);
                } else if (const auto it = std::find(disabled.begin(), disabled.end(), nodeId); it != disabled.end()) {
                    std::swap(*it, disabled.back());
                    disabled.pop_back();
                } else {
                    UNIT_FAIL("unexpected case");
                }
                bindQueue.Update(nodes);
            } else if (canPick && !i--) {
                Cerr << "Pick" << Endl;
                THashSet<ui32> picked;
                TMonotonic closest;
                while (const auto res = bindQueue.Pick(now, &closest)) {
                    UNIT_ASSERT(picked.insert(*res).second);
                }
                UNIT_ASSERT_VALUES_EQUAL(picked, enabledSet);
                now += TDuration::Seconds(1);
            }
        }
    }
}
