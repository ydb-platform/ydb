#include <ydb/core/testlib/audit_helpers/audit_helper.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <util/generic/algorithm.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TPqGroupTests) {
    NKikimrSchemeOp::TPersQueueGroupDescription TopicWithChannels(ui32 count) {
        NKikimrSchemeOp::TPersQueueGroupDescription topic;
        topic.SetName("Topic");
        topic.SetTotalGroupCount(1);
        topic.SetPartitionPerTablet(1);
        auto* config = topic.MutablePQTabletConfig()->MutablePartitionConfig();
        config->SetLifetimeSeconds(10);
        for (ui32 i = 0; i < count; ++i) {
            config->AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
        }
        return topic;
    }

    Y_UNIT_TEST(TopicChannelErrorsHaveReason) {
        std::vector<std::string> auditLines;
        TTestBasicRuntime runtime;
        runtime.AuditLogBackends = Tests::CreateTestAuditLogBackends(auditLines);
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreatePQGroup(runtime, ++txId, "/MyRoot", TopicWithChannels(4).DebugString());
        env.TestWaitNotification(runtime, txId);

        const TVector<std::pair<ui32, TString>> cases = {
            {2, "ExplicitChannelProfiles has 2 channels, should be [3 .. 256]"},
            {257, "ExplicitChannelProfiles has 257 channels, should be [3 .. 256]"},
            {3, "ExplicitChannelProfiles has 3 channels, should be at least 4 (current config)"},
        };
        for (const auto& [count, reason] : cases) {
            TestAlterPQGroup(runtime, ++txId, "/MyRoot", TopicWithChannels(count).DebugString(),
                {{NKikimrScheme::StatusInvalidParameter, reason}});

            const std::string marker = "tx_id=" + std::to_string(txId);
            runtime.WaitFor("scheme audit record", [&] {
                return AnyOf(auditLines, [&](const auto& line) { return line.find(marker) != std::string::npos; });
            }, TDuration::Seconds(10));
            const auto line = Tests::FindAuditLine(auditLines, marker);
            UNIT_ASSERT_STRING_CONTAINS(line, "component=schemeshard");
            UNIT_ASSERT_STRING_CONTAINS(line, "operation=ALTER PERSISTENT QUEUE");
            UNIT_ASSERT_STRING_CONTAINS(line, "detailed_status=StatusInvalidParameter");
            UNIT_ASSERT_STRING_CONTAINS(line, "status=ERROR");
            UNIT_ASSERT_STRING_CONTAINS(line, TStringBuilder() << "reason=" << reason);
        }
    }
}
