
#include "domain_info.h"

#include <ydb/core/testlib/actor_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NHive;

Y_UNIT_TEST_SUITE(TargetTrackingScaleRecommenderPolicy) {
    Y_UNIT_TEST(ScaleOut) {
        TActorSystemStub stub;
        NKikimrConfig::THiveConfig config;
        config.SetScaleOutWindowSize(3);
        
        std::deque<double> history;

        history = {};
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.8 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.8, 0.8 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.8, 0.8, 0.8 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 4);
    }

    Y_UNIT_TEST(ScaleIn) {
        TActorSystemStub stub;
        NKikimrConfig::THiveConfig config;
        config.SetScaleInWindowSize(3);
        
        std::deque<double> history;

        history = {};
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.3 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.3, 0.3 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.3, 0.3, 0.3 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 2);
    }

    Y_UNIT_TEST(BigNumbersScaleOut) {
        TActorSystemStub stub;
        NKikimrConfig::THiveConfig config;
        config.SetScaleOutWindowSize(3);
        
        std::deque<double> history;

        history = {};
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(1000, config), 1000);

        history = { 0.8 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(1000, config), 1000);

        history = { 0.8, 0.8 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(1000, config), 1000);

        history = { 0.8, 0.8, 0.8 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(1000, config), 1334);
    }

    Y_UNIT_TEST(BigNumbersScaleIn) {
        TActorSystemStub stub;
        NKikimrConfig::THiveConfig config;
        config.SetScaleInWindowSize(3);
        
        std::deque<double> history;

        history = {};
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(1000, config), 1000);

        history = { 0.3 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(1000, config), 1000);

        history = { 0.3, 0.3 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(1000, config), 1000);

        history = { 0.3, 0.3, 0.3 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(1000, config), 500);
    }

    Y_UNIT_TEST(SpikeResistance) {
        TActorSystemStub stub;
        NKikimrConfig::THiveConfig config;
        config.SetScaleOutWindowSize(3);
        
        std::deque<double> history;

        history = {};
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.3, };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.3, 0.9 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.3, 0.9, 0.3 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);
    }

    Y_UNIT_TEST(NearTarget) {
        TActorSystemStub stub;
        NKikimrConfig::THiveConfig config;
        config.SetScaleOutWindowSize(3);
        config.SetScaleInWindowSize(3);
        
        std::deque<double> history;

        history = {};
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.55, };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.55, 0.55 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.55, 0.55, 0.55 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);
    }

    Y_UNIT_TEST(AtTarget) {
        TActorSystemStub stub;
        NKikimrConfig::THiveConfig config;
        config.SetScaleOutWindowSize(3);
        config.SetScaleInWindowSize(3);
        
        std::deque<double> history;

        history = {};
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.6, };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.6, 0.6 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.6, 0.6, 0.6 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);
    }

    void TestFluctuations(ui32 initialNodes) {
        NKikimrConfig::THiveConfig config;
        config.SetScaleOutWindowSize(1);
        config.SetScaleInWindowSize(1);

        for (double avgInitialLoad = 0; avgInitialLoad < 1.1; avgInitialLoad += 0.1) {
            const double totalLoad = avgInitialLoad * initialNodes;

            for (double avgTargetLoad = 0.1; avgTargetLoad < 1.0; avgTargetLoad += 0.1) {
                ui32 currentNodes = initialNodes;
                std::set<ui32> uniqueCurrentNodes = { currentNodes };
                std::vector<ui32> currentNodesHistory = { currentNodes };

                std::deque<double> history;
                for (size_t i = 0; i < 10; ++i) {
                    history.push_back(totalLoad / currentNodes);
                    currentNodes = TTargetTrackingPolicy(avgTargetLoad, history).MakeScaleRecommendation(currentNodes, config);
                    uniqueCurrentNodes.insert(currentNodes);
                    currentNodesHistory.push_back(currentNodes);
                }

                UNIT_ASSERT_C(uniqueCurrentNodes.size() <= 2,
                    TStringBuilder() << "Fluctuations detected: target=" << avgTargetLoad
                    << ", currentNodesHistory=" << "[" << JoinSeq(',', currentNodesHistory) << "]"
                    << ", history=" << "[" << JoinSeq(',', history) << "]");
            }
        }
    }

    Y_UNIT_TEST(Fluctuations) {
        TActorSystemStub stub;

        TestFluctuations(1);
        TestFluctuations(2);
        TestFluctuations(3);
        TestFluctuations(4);
        TestFluctuations(5);
        TestFluctuations(6);
        TestFluctuations(7);
        TestFluctuations(8);
        TestFluctuations(9);
        TestFluctuations(10);
    }

    Y_UNIT_TEST(FluctuationsBigNumbers) {
        TActorSystemStub stub;

        TestFluctuations(1001);
        TestFluctuations(1002);
        TestFluctuations(1003);
        TestFluctuations(1004);
        TestFluctuations(1005);
        TestFluctuations(1006);
        TestFluctuations(1007);
        TestFluctuations(1008);
        TestFluctuations(1009);
        TestFluctuations(1010);

        TestFluctuations(1000);
        TestFluctuations(2000);
        TestFluctuations(3000);
        TestFluctuations(4000);
        TestFluctuations(5000);
        TestFluctuations(6000);
        TestFluctuations(7000);
        TestFluctuations(8000);
        TestFluctuations(9000);
        TestFluctuations(10000);
    }

    Y_UNIT_TEST(ScaleInToMaxSeen) {
        TActorSystemStub stub;
        NKikimrConfig::THiveConfig config;
        config.SetScaleInWindowSize(3);
        
        std::deque<double> history;

        history = {};
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.3, };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.3, 0.1 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.3, 0.1, 0.1 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 2);
    }

    Y_UNIT_TEST(Idle) {
        TActorSystemStub stub;
        NKikimrConfig::THiveConfig config;
        config.SetScaleInWindowSize(3);
        
        std::deque<double> history;

        history = {};
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.01, };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.01, 0.01 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 3);

        history = { 0.01, 0.01, 0.01 };
        UNIT_ASSERT_VALUES_EQUAL(TTargetTrackingPolicy(0.6, history).MakeScaleRecommendation(3, config), 1);
    }
}
