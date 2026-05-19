#include "traits.h"

#include <yt/yt/core/bus/unittests/lib/bus_ut.h>

namespace NYT::NBus::NTests {
namespace {

using NTcp::NTests::TBusTraits;

////////////////////////////////////////////////////////////////////////////////

INSTANTIATE_TYPED_TEST_SUITE_P(Tcp, TBusTest, TBusTraits);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NBus::NTests
