#include "direct_access.h"

#include <library/cpp/yt/global/mock_modules/module1_public/test_tag.h>

#include <library/cpp/yt/global/variable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_TRACKED_GLOBAL(int, TestVariable1, TestTag1, 42);
YT_DEFINE_TRACKED_THREAD_LOCAL(int, TlsVariable, ThreadLocalTag, 0);

////////////////////////////////////////////////////////////////////////////////

int GetTestVariable1()
{
    return TestVariable1.Get();
}

void SetTestVariable1(int val)
{
    TestVariable1.Set(val);
}

int GetTlsVariable()
{
    return TlsVariable();
}

void SetTlsVariable(int val)
{
    TlsVariable() = val;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
