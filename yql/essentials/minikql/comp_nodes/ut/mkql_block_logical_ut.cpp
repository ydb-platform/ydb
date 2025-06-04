#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename Op>
TMaybe<bool> ScalarOp(TMaybe<bool> arg1, TMaybe<bool> arg2, const Op& op) {
    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;
    auto arg1val = arg1 ? pb.NewDataLiteral<bool>(*arg1) : pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id);
    auto arg2val = arg2 ? pb.NewDataLiteral<bool>(*arg2) : pb.NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id);
    TRuntimeNode root = op(pb, pb.AsScalar(arg1val), pb.AsScalar(arg2val));
    const auto graph = setup.BuildGraph(root);
    NYql::NUdf::TUnboxedValue result = graph->GetValue();
    auto datum = TArrowBlock::From(result).GetDatum();
    UNIT_ASSERT(datum.is_scalar());
    const auto& scalar = datum.scalar_as<arrow::UInt8Scalar>();
    if (!scalar.is_valid) {
        return {};
    }
    UNIT_ASSERT(scalar.value == 0 || scalar.value == 1);
    return bool(scalar.value);
}

} //namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlockLogicalTest) {

Y_UNIT_TEST(ScalarAnd) {
    auto op = [](TProgramBuilder& pb, TRuntimeNode arg1, TRuntimeNode arg2) {
        return pb.BlockAnd(arg1, arg2);
    };

    UNIT_ASSERT_VALUES_EQUAL(ScalarOp(false, false, op), false);
    UNIT_ASSERT_VALUES_EQUAL(ScalarOp(false, true, op), false);
    UNIT_ASSERT_VALUES_EQUAL(ScalarOp(true, false, op), false);
    UNIT_ASSERT_VALUES_EQUAL(ScalarOp(true, true, op), true);

    UNIT_ASSERT_VALUES_EQUAL(ScalarOp({}, false, op), false);
    UNIT_ASSERT_VALUES_EQUAL(ScalarOp(false, {}, op), false);

    UNIT_ASSERT(ScalarOp({}, {}, op).Empty());
    UNIT_ASSERT(ScalarOp(true, {}, op).Empty());
    UNIT_ASSERT(ScalarOp({}, true, op).Empty());
}

Y_UNIT_TEST(ScalarOr) {
    auto op = [](TProgramBuilder& pb, TRuntimeNode arg1, TRuntimeNode arg2) {
        return pb.BlockOr(arg1, arg2);
    };

    UNIT_ASSERT_VALUES_EQUAL(ScalarOp(false, false, op), false);
    UNIT_ASSERT_VALUES_EQUAL(ScalarOp(false, true, op), true);
    UNIT_ASSERT_VALUES_EQUAL(ScalarOp(true, false, op), true);
    UNIT_ASSERT_VALUES_EQUAL(ScalarOp(true, true, op), true);

    UNIT_ASSERT_VALUES_EQUAL(ScalarOp({}, true, op), true);
    UNIT_ASSERT_VALUES_EQUAL(ScalarOp(true, {}, op), true);

    UNIT_ASSERT(ScalarOp({}, {}, op).Empty());
    UNIT_ASSERT(ScalarOp(false, {}, op).Empty());
    UNIT_ASSERT(ScalarOp({}, false, op).Empty());
}

Y_UNIT_TEST(ScalarXor) {
    auto op = [](TProgramBuilder& pb, TRuntimeNode arg1, TRuntimeNode arg2) {
        return pb.BlockXor(arg1, arg2);
    };

    UNIT_ASSERT_VALUES_EQUAL(ScalarOp(false, false, op), false);
    UNIT_ASSERT_VALUES_EQUAL(ScalarOp(false, true, op), true);
    UNIT_ASSERT_VALUES_EQUAL(ScalarOp(true, false, op), true);
    UNIT_ASSERT_VALUES_EQUAL(ScalarOp(true, true, op), false);

    UNIT_ASSERT(ScalarOp({}, {}, op).Empty());
    UNIT_ASSERT(ScalarOp(false, {}, op).Empty());
    UNIT_ASSERT(ScalarOp({}, false, op).Empty());
    UNIT_ASSERT(ScalarOp(true, {}, op).Empty());
    UNIT_ASSERT(ScalarOp({}, true, op).Empty());
}

Y_UNIT_TEST(ScalarNot) {
    auto op = [](TProgramBuilder& pb, TRuntimeNode arg1, TRuntimeNode arg2) {
        Y_UNUSED(arg2);
        return pb.BlockNot(arg1);
    };

    UNIT_ASSERT_VALUES_EQUAL(ScalarOp(false, {}, op), true);
    UNIT_ASSERT_VALUES_EQUAL(ScalarOp(true, {}, op), false);
    UNIT_ASSERT(ScalarOp({}, {}, op).Empty());
}

} // Y_UNIT_TEST_SUITE

} // namespace NMiniKQL
} // namespace NKikimr
