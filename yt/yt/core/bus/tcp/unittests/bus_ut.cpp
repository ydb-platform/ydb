#include "traits.h"

#include <yt/yt/core/bus/unittests/lib/bus_ut.h>

namespace NYT::NBus::NTcp::NTests {

using NTcp::NTests::TBusTraits;

////////////////////////////////////////////////////////////////////////////////

// INSTANTIATE_TYPED_TEST_SUITE_P expands to references to gtest-internal names
// (gtest_suite_TBusTest_, gtest_typed_test_suite_p_state_TBusTest_, ...) that
// gtest declares in the same namespace as the suite. The suite is registered
// in NYT::NBus::NTests by the shared bus_ut-inl.h; importing that namespace
// here lets the unqualified lookup find those internals.
using namespace NBus::NTests;

INSTANTIATE_TYPED_TEST_SUITE_P(Tcp, TBusTest, TBusTraits);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTcp::NTests
