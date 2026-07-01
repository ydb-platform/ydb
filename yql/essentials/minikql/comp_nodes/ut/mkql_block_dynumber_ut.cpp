#include "mkql_block_test_helper.h"
#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/mkql_type_ops.h>

namespace NKikimr::NMiniKQL {

using namespace NTest;

namespace {

TVector<TDyNumberLiteral> MakeDyNumberVector(std::initializer_list<const char*> values) {
    TVector<TDyNumberLiteral> result;
    result.reserve(values.size());
    for (const char* value : values) {
        result.push_back(TDyNumberLiteral{.Value = value});
    }
    return result;
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockDyNumberTest) {

Y_UNIT_TEST(DyNumberListRoundtrip) {
    const auto data = MakeDyNumberVector({"0", "123.45", "-42"});
    TBlockHelper helper;
    auto [graph, listValue] = helper.BuildAndRunListFuzzied(data);
    Y_UNUSED(graph);

    auto iterator = listValue.GetListIterator();
    NUdf::TUnboxedValue val;
    for (const auto& item : data) {
        UNIT_ASSERT(iterator.Next(val));
        const auto expected = ValueFromString(NUdf::EDataSlot::DyNumber, item.Value);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(val.AsStringRef()), TStringBuf(expected.AsStringRef()));
    }
    UNIT_ASSERT(!iterator.Next(val));
}

} // Y_UNIT_TEST_SUITE(TMiniKQLBlockDyNumberTest)

} // namespace NKikimr::NMiniKQL
