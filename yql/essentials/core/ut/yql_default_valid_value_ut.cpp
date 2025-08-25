#include "yql_default_valid_value.h"
#include "yql_expr_type_annotation.h"
#include "yql_type_annotation.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/services/yql_transform_pipeline.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

namespace {

TExprNode::TPtr DeduceTypes(TExprNode::TPtr expr, TExprContext& exprCtx, TTypeAnnotationContext& typesCtx) {
    UNIT_ASSERT(InstantAnnotateTypes(expr, exprCtx, /*wholeProgram=*/false, typesCtx));
    return expr;
}

void TestTypeValidValue(const TOptionalExprType* type, TExprContext& ctx, bool isTypeSupported = true) {
    TTypeAnnotationContext typesCtx;
    TPositionHandle posStub;
    UNIT_ASSERT_EQUAL(IsValidValueSupported(type), isTypeSupported);
    if (isTypeSupported) {
        auto value = MakeValidValue(type, posStub, ctx);
        auto actualType = DeduceTypes(value, ctx, typesCtx)->GetTypeAnn();
        if (!actualType->Equals(*type->GetItemType())) {
            Cerr << GetTypeDiff(*actualType, *type->GetItemType()) << Endl;
        }
        UNIT_ASSERT(actualType->Equals(*type->GetItemType()));
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TYqlDefaulValidValueTest) {

Y_UNIT_TEST(Ui8) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint8));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(I8) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Int8));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Ui16) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint16));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(I16) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Int16));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(I32) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Int32));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(I64) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Int64));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Ui32) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint32));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Ui64) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint64));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Float) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Float));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Double) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Double));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Bool) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Bool));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(String) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::String));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Utf8) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Utf8));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Json) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Json));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Uuid) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uuid));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Yson) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Yson));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Date) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Date));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Datetime) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Datetime));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Timestamp) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Timestamp));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Interval) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Interval));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(TzDate) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::TzDate));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(TzDatetime) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::TzDatetime));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(TzTimestamp) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::TzTimestamp));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Date32) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Date32));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Datetime64) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Datetime64));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Timestamp64) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Timestamp64));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Interval64) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Interval64));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Decimal) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprParamsType>(EDataSlot::Decimal, "22", "9"));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(JsonDocument) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::JsonDocument));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(TzDate32) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::TzDate32));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(TzDatetime64) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::TzDatetime64));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(TzTimestamp64) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::TzTimestamp64));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(DyNumber) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::DyNumber));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Optional) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint32)));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Tuple) {
    TExprContext ctx;
    TVector<const TTypeAnnotationNode*> items = {
        ctx.MakeType<TDataExprType>(EDataSlot::Uint32),
        ctx.MakeType<TDataExprType>(EDataSlot::Int16)};
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TTupleExprType>(items));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(TupleEmpty) {
    TExprContext ctx;
    TVector<const TTypeAnnotationNode*> items;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TTupleExprType>(items));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Struct) {
    TExprContext ctx;
    TVector<const TItemExprType*> items = {
        ctx.MakeType<TItemExprType>("a", ctx.MakeType<TDataExprType>(EDataSlot::Uint32))};
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TStructExprType>(items));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(StructEmpty) {
    TExprContext ctx;
    TVector<const TItemExprType*> items;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TStructExprType>(items));
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Void) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TVoidExprType>());
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(EmptyDict) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TEmptyDictExprType>());
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(EmptyList) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TEmptyListExprType>());
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Null) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TNullExprType>());
    TestTypeValidValue(type, ctx);
}

Y_UNIT_TEST(Pg) {
    TExprContext ctx;
    auto typeId = NPg::LookupType("int4").TypeId;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TPgExprType>(typeId));
    TestTypeValidValue(type, ctx);
}

// Tests for unsupported types (should return false from IsValidValueSupported)
Y_UNIT_TEST(UnsupportedUnit) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TUnitExprType>());
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(UnsupportedMulti) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TMultiExprType>(TTypeAnnotationNode::TListType{}));
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(UnsupportedList) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TListExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint32)));
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(UnsupportedStream) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TStreamExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint32)));
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(UnsupportedFlow) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TFlowExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint32)));
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(UnsupportedWorld) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TWorldExprType>());
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(UnsupportedResource) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TResourceExprType>("TestResource"));
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(UnsupportedTypeExpr) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TTypeExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint32)));
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(UnsupportedDict) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDictExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint32), ctx.MakeType<TDataExprType>(EDataSlot::String)));
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(UnsupportedGeneric) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TGenericExprType>());
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(UnsupportedError) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TErrorExprType>(TIssue{}));
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(UnsupportedVariant) {
    TExprContext ctx;
    TVector<const TTypeAnnotationNode*> items = {
        ctx.MakeType<TDataExprType>(EDataSlot::Uint32),
        ctx.MakeType<TDataExprType>(EDataSlot::String)};
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TVariantExprType>(ctx.MakeType<TTupleExprType>(items)));
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(UnsupportedBlock) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TBlockExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint32)));
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(UnsupportedScalar) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TScalarExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint32)));
    TestTypeValidValue(type, ctx, false);
}

// Tests for complex types with unsupported children
Y_UNIT_TEST(TupleWithUnsupportedChild) {
    TExprContext ctx;
    TVector<const TTypeAnnotationNode*> items = {
        ctx.MakeType<TDataExprType>(EDataSlot::Uint32),                             // supported
        ctx.MakeType<TListExprType>(ctx.MakeType<TDataExprType>(EDataSlot::String)) // unsupported
    };
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TTupleExprType>(items));
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(StructWithUnsupportedChild) {
    TExprContext ctx;
    TVector<const TItemExprType*> items = {
        ctx.MakeType<TItemExprType>("supported", ctx.MakeType<TDataExprType>(EDataSlot::Uint32)),
        ctx.MakeType<TItemExprType>("unsupported", ctx.MakeType<TListExprType>(ctx.MakeType<TDataExprType>(EDataSlot::String)))};
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TStructExprType>(items));
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(TaggedWithUnsupportedBase) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TTaggedExprType>(ctx.MakeType<TListExprType>(ctx.MakeType<TDataExprType>(EDataSlot::String)), "TestTag"));
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(TaggedWithSupportedBase) {
    TExprContext ctx;
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TTaggedExprType>(ctx.MakeType<TDataExprType>(EDataSlot::String), "TestTag"));
    TestTypeValidValue(type, ctx, true);
}

Y_UNIT_TEST(NestedTupleWithUnsupported) {
    TExprContext ctx;
    TVector<const TTypeAnnotationNode*> innerItems = {
        ctx.MakeType<TDataExprType>(EDataSlot::Uint32),
        ctx.MakeType<TWorldExprType>() // unsupported
    };
    TVector<const TTypeAnnotationNode*> outerItems = {
        ctx.MakeType<TDataExprType>(EDataSlot::String),
        ctx.MakeType<TTupleExprType>(innerItems)};
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TTupleExprType>(outerItems));
    TestTypeValidValue(type, ctx, false);
}

Y_UNIT_TEST(NestedStructWithUnsupported) {
    TExprContext ctx;
    TVector<const TItemExprType*> innerItems = {
        ctx.MakeType<TItemExprType>("good", ctx.MakeType<TDataExprType>(EDataSlot::Uint32)),
        ctx.MakeType<TItemExprType>("bad", ctx.MakeType<TResourceExprType>("TestResource")) // unsupported
    };
    TVector<const TItemExprType*> outerItems = {
        ctx.MakeType<TItemExprType>("outer", ctx.MakeType<TDataExprType>(EDataSlot::String)),
        ctx.MakeType<TItemExprType>("inner", ctx.MakeType<TStructExprType>(innerItems))};
    auto* type = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TStructExprType>(outerItems));
    TestTypeValidValue(type, ctx, false);
}

} // Y_UNIT_TEST_SUITE(TYqlDefaulValidValueTest)

} // namespace NYql
