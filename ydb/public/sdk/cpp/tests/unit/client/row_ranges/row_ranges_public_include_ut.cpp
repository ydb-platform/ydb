#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/rows.h>

#include <library/cpp/testing/unittest/registar.h>

#include "row_ranges_test_helpers.h"

#include <concepts>
#include <initializer_list>
#include <iterator>
#include <string_view>
#include <tuple>
#include <utility>

using namespace NYdb;
namespace {

static_assert(std::is_same_v<
    std::iterator_traits<TRowIterator>::iterator_category,
    std::input_iterator_tag>);
static_assert(!std::copy_constructible<TRowIterator>);
static_assert(!std::assignable_from<TRowIterator&, const TRowIterator&>);
static_assert(std::movable<TRowIterator>);
static_assert(std::sentinel_for<TRowIterEnd, TRowIterator>);

using Int32Columns = decltype(
    std::declval<TRowRange&>().Get<int32_t>(std::declval<std::initializer_list<std::string_view>>()));
using TupleColumns = decltype(
    std::declval<TRowRange&>().Get<int32_t, std::optional<std::string>>(
        std::declval<std::initializer_list<std::string_view>>()));

static_assert(std::same_as<
    typename Int32Columns::Iterator::value_type,
    std::tuple<int32_t>>);
static_assert(std::same_as<
    typename TupleColumns::Iterator::value_type,
    std::tuple<int32_t, std::optional<std::string>>>);

} // namespace

Y_UNIT_TEST_SUITE(TRowRangePublicIncludeTest) {
    Y_UNIT_TEST(SmokeIterateAndGet) {
        TRowRange range(NRowRangesTest::MakeSingleInt32ResultSet(42));
        int32_t sum = 0;
        for (auto it = range.begin(); it != range.end(); ++it) {
            sum += static_cast<int32_t>(it->ColumnParser("v").GetInt32());
        }
        UNIT_ASSERT_VALUES_EQUAL(sum, 42);

        TRowRange range2(NRowRangesTest::MakeSingleInt32ResultSet(42));
        for (auto [v] : range2.Get<int32_t>({"v"})) {
            UNIT_ASSERT_VALUES_EQUAL(v, 42);
        }
    }
}
