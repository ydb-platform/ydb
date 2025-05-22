#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/string/format.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

static_assert(!CFormattable<TIntrusivePtr<TRefCounted>>);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
