#pragma once

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NBus::NTests {

////////////////////////////////////////////////////////////////////////////////

template <class TTraits>
class TBusTest
    : public ::testing::Test
{
protected:
    TTraits Traits_;
};

TYPED_TEST_SUITE_P(TBusTest);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTests

#define BUS_UT_INL_H_
#include "bus_ut-inl.h"
#undef BUS_UT_INL_H_
