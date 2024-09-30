#include <library/cpp/testing/unittest/registar.h>

#include "dq_type_ann.h"

namespace NYql::NDq {

namespace {

TVector<TExprNode::TPtr> ConnectionTypes(const TVector<TString>& names, TExprContext& ctx) {
    TVector<TExprNode::TPtr> ret;
    for (const auto& name : names) {
        ret.emplace_back(ctx.NewCallable(TPositionHandle{}, name, {}));
    }
    return ret;
}

} // namespace {

Y_UNIT_TEST_SUITE(DqTypeAnnTests) {

Y_UNIT_TEST(TestEmpty) {
    TExprContext ctx;
    UNIT_ASSERT(CheckConnectionTypes({}, ctx) == true);
}

Y_UNIT_TEST(TestSingleInput) {
    TExprContext ctx;
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({"FakeConnection"}, ctx), ctx) == true);
}

Y_UNIT_TEST(TestBroadcastAsFirstInput) {
    TExprContext ctx;
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnBroadcast", "DqCnUnionAll"}, ctx), ctx) == false);

    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnUnionAll", "DqCnUnionAll"}, ctx), ctx) == true);
}

Y_UNIT_TEST(TestMapAsSecondInput) {
    TExprContext ctx;
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnHashShuffle", "DqCnMap"}, ctx), ctx) == false);
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnHashShuffle", "DqCnHashShuffle", "DqCnMap"}, ctx), ctx) == false);
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnMap", "DqCnHashShuffle"}, ctx), ctx) == true);
}

Y_UNIT_TEST(TestGoodSecondInputs) {
    TExprContext ctx;
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnHashShuffle", "DqCnHashShuffle"}, ctx), ctx) == true);
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnHashShuffle", "DqCnBroadcast"}, ctx), ctx) == true);
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnHashShuffle", "DqPhyPrecompute"}, ctx), ctx) == true);
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnHashShuffle", "KqpCnStreamLookup"}, ctx), ctx) == true);
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnHashShuffle", "DqCnBroadcast"}, ctx), ctx) == true);

    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnHashShuffle", "DqCnHashShuffle", "DqCnHashShuffle"}, ctx), ctx) == true);
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnHashShuffle", "DqCnHashShuffle", "DqCnBroadcast"}, ctx), ctx) == true);
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnHashShuffle", "DqCnHashShuffle", "DqPhyPrecompute"}, ctx), ctx) == true);
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnHashShuffle", "DqCnHashShuffle", "KqpCnStreamLookup"}, ctx), ctx) == true);
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnHashShuffle", "DqCnHashShuffle", "DqCnBroadcast"}, ctx), ctx) == true);

}

Y_UNIT_TEST(TestUnionAllAsSecondInput) {
    TExprContext ctx;
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnHashShuffle", "DqCnUnionAll"}, ctx), ctx) == true);
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnHashShuffle", "DqCnMerge"}, ctx), ctx) == true);

    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnMap", "DqCnUnionAll"}, ctx), ctx) == false);
    UNIT_ASSERT(CheckConnectionTypes(ConnectionTypes({
        "DqCnMap", "DqCnMerge"}, ctx), ctx) == false);
}

} // Y_UNIT_TEST_SUITE

} // namespace NYql::NDq {

