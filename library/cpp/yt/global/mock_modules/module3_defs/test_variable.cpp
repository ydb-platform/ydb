#include "direct_access.h"

#include <library/cpp/yt/global/mock_modules/module3_public/test_tag.h>

#include <library/cpp/yt/global/variable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

NGlobal::TErasedStorage GetGlobal3() noexcept;

////////////////////////////////////////////////////////////////////////////////

static NGlobal::TVariable<std::array<int, 2>> TestVariable3{
    TestTag3,
    &GetGlobal3,
    std::array{11, 22}};

////////////////////////////////////////////////////////////////////////////////

NGlobal::TErasedStorage GetGlobal3() noexcept
{
    return NGlobal::TErasedStorage{TestVariable3.Get()};
}

////////////////////////////////////////////////////////////////////////////////

std::array<int, 2> GetTestVariable3()
{
    return TestVariable3.Get();
}

void SetTestVariable3(std::array<int, 2> val)
{
    TestVariable3.Set(val);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
