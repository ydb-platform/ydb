#include "utils.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NGrpc {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TUtilsTest)
{
    Y_UNIT_TEST(ShouldExtractFdFromGrpcPeerString)
    {
        const TStringBuf b{"fd:12345"};
        ui32 fd{0};
        UNIT_ASSERT(TryParseSourceFd(b, &fd));
        UNIT_ASSERT_VALUES_EQUAL(12345, fd);
    }

    Y_UNIT_TEST(ShouldFailIfPeerStringIsIncorrect)
    {
        {
            const TStringBuf b{"fd12345"};
            ui32 fd{0};
            UNIT_ASSERT(!TryParseSourceFd(b, &fd));
        }

        {
            const TStringBuf b{"fd:abcd"};
            ui32 fd{0};
            UNIT_ASSERT(!TryParseSourceFd(b, &fd));
        }
    }
}

}   // namespace NYdb::NBS::NGrpc
