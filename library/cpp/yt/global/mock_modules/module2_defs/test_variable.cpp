#include "direct_access.h"

#include <library/cpp/yt/global/mock_modules/module2_public/test_tag.h>

#include <library/cpp/yt/global/variable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

NGlobal::TErasedStorage GetGlobal2() noexcept;

NGlobal::TErasedStorage GetGlobal3() noexcept;

////////////////////////////////////////////////////////////////////////////////

static NGlobal::TVariable<TGuid> TestVariable2{TestTag2, &GetGlobal2, TGuid{42, 77}};

static NGlobal::TVariable<std::array<int, 4>> TestVariable3{
    TestTag3,
    &GetGlobal3,
    std::array{0, 0, 42, 77}};

////////////////////////////////////////////////////////////////////////////////

NGlobal::TErasedStorage GetGlobal2() noexcept
{
    return NGlobal::TErasedStorage{TestVariable2.Get()};
}

NGlobal::TErasedStorage GetGlobal3() noexcept
{
    return NGlobal::TErasedStorage{TestVariable3.Get()};
}

////////////////////////////////////////////////////////////////////////////////

TGuid GetTestVariable2()
{
    return TestVariable2.Get();
}

void SetTestVariable2(TGuid val)
{
    TestVariable2.Set(val);
}

std::array<int, 4> GetTestVariable3()
{
    return TestVariable3.Get();
}

void SetTestVariable3(std::array<int, 4> val)
{
    TestVariable3.Set(val);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
