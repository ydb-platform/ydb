#include <yt/yt/client/api/chaos_lease.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/connection.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NApi {
namespace {

using namespace NConcurrency;

using ::testing::_;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

TEST(TChaosLeaseTest, FailedAbortIsReportedAndRetried)
{
    static const NLogging::TLoggingTagList LoggingTags;
    auto connection = New<StrictMock<TMockConnection>>();
    EXPECT_CALL(*connection, GetLoggingTags())
        .WillRepeatedly(ReturnRef(LoggingTags));

    auto client = New<StrictMock<TMockClient>>();
    EXPECT_CALL(*client, GetConnection())
        .WillRepeatedly(Return(connection));
    EXPECT_CALL(*client, RemoveNode(_, _))
        .WillOnce(Return(MakeFuture(TError("Chaos cell is unavailable"))))
        .WillOnce(Return(OKFuture));

    auto lease = CreateChaosLease(
        client,
        NChaosClient::TChaosLeaseId(TGuid::Create()),
        TDuration::Minutes(1),
        /*pingAncestors*/ false,
        NLogging::TLogger("Test"));

    bool aborted = false;
    lease->SubscribeAborted(BIND([&] (const TError& /*error*/) {
        aborted = true;
    }));

    EXPECT_FALSE(WaitFor(lease->Abort()).IsOK());
    EXPECT_FALSE(aborted);

    EXPECT_TRUE(WaitFor(lease->Abort()).IsOK());
    EXPECT_TRUE(aborted);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NApi
