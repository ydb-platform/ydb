#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/queue_client/config.h>

namespace NYT::NQueueClient {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TQueueStaticExportConfigTest
    : public ::testing::TestWithParam<std::tuple<
        TYsonString,
        bool>>
{ };

TEST_P(TQueueStaticExportConfigTest, ScheduleInvariants)
{
    const auto& [ysonConfig, shouldThrow] = GetParam();

    if (shouldThrow) {
        ASSERT_ANY_THROW(ConvertTo<TQueueStaticExportConfigPtr>(ysonConfig));
    } else {
        ASSERT_NO_THROW(ConvertTo<TQueueStaticExportConfigPtr>(ysonConfig));
    }
}

INSTANTIATE_TEST_SUITE_P(
    TQueueStaticExportConfigTest,
    TQueueStaticExportConfigTest,
    ::testing::Values(
        std::tuple(
            R"({"export_directory"="/tmp/foo"; "export_period"=1000;})"_sb,
            false
        ),
        std::tuple(
            R"({"export_directory"="/tmp/foo"; "export_cron_schedule"="* * * * *";})"_sb,
            false
        ),
        std::tuple(
            R"({"export_directory"="/tmp/foo";})"_sb,
            true
        ),
        std::tuple(
            R"({"export_directory"="/tmp/foo"; "export_period"=1000; "export_cron_schedule"="* * * * *";})"_sb,
            true
        )
));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
