#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/assert/assert.h>

#include <util/generic/yexception.h>

namespace NYT {
namespace {

using ::testing::_;
using ::testing::A;
using ::testing::NiceMock;
using ::testing::ReturnArg;
using ::testing::Throw;

////////////////////////////////////////////////////////////////////////////////

class TCallee
{
public:
    virtual ~TCallee() = default;
    virtual bool F(bool passThrough, const char* comment) = 0;
};

class TMockCallee
    : public TCallee
{
public:
    TMockCallee()
    {
        ON_CALL(*this, F(A<bool>(), _))
            .WillByDefault(ReturnArg<0>());
    }

    MOCK_METHOD(bool, F, (bool passThrough, const char* comment), (override));
};

TEST(TVerifyDeathTest, DISABLED_NoCrashForTruthExpression)
{
    TMockCallee callee;
    EXPECT_CALL(callee, F(true, _)).Times(1);

    YT_VERIFY(callee.F(true, "This should be okay."));
    SUCCEED();
}

TEST(TVerifyDeathTest, DISABLED_CrashForFalseExpression)
{
    NiceMock<TMockCallee> callee;

    ASSERT_DEATH(
        { YT_VERIFY(callee.F(false, "Cheshire Cat")); },
        ".*Cheshire Cat.*");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
