#include <yql/essentials/core/yql_window_frame_settings_pg.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>

#include <library/cpp/testing/unittest/registar.h>

#include <variant>

namespace NYql {

namespace {

struct TPgTypeConst {
    TStringBuf TypeName;
    TStringBuf Value;
};

constexpr TPgTypeConst Int2{.TypeName = "int2", .Value = "1"};
constexpr TPgTypeConst Int4{.TypeName = "int4", .Value = "1"};
constexpr TPgTypeConst Int8{.TypeName = "int8", .Value = "10"};
constexpr TPgTypeConst Float4{.TypeName = "float4", .Value = "1.0"};
constexpr TPgTypeConst Float8{.TypeName = "float8", .Value = "0.5"};
constexpr TPgTypeConst Numeric{.TypeName = "numeric", .Value = "1.0"};
constexpr TPgTypeConst Text{.TypeName = "text", .Value = "abc"};
constexpr TPgTypeConst Timestamp{.TypeName = "timestamp", .Value = "2024-01-01 00:00:00"};
constexpr TPgTypeConst Interval{.TypeName = "interval", .Value = "1 day"};

struct TExpectedTypes {
    TString ColumnType;
    TString OffsetType;
};

struct TExpectFailure {};

using TExpectedResult = std::variant<TExpectedTypes, TExpectFailure>;

struct TInRangeTestCase {
    TPgTypeConst Column;
    TPgTypeConst Offset;
    TExpectedResult Expected;
};

TExprNode::TPtr MakePgConstNode(TExprContext& ctx, const TPgTypeConst& typeConst) {
    // clang-format off
    return ctx.Builder(TPositionHandle())
        .Callable("PgConst")
            .Atom(0, typeConst.Value)
            .Callable(1, "PgType")
                .Atom(0, typeConst.TypeName)
            .Seal()
        .Seal()
        .Build();
    // clang-format on
}

TExprNode::TPtr AnnotateNode(TExprNode::TPtr node, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    UNIT_ASSERT(InstantAnnotateTypes(node, ctx, false, typesCtx));
    return node;
}

ui32 GetPgTypeId(const TExprNode::TPtr& node) {
    UNIT_ASSERT(node->GetTypeAnn());
    UNIT_ASSERT(node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg);
    return node->GetTypeAnn()->Cast<TPgExprType>()->GetId();
}

} // namespace

Y_UNIT_TEST_SUITE(TLookupInRangeCastsTests) {

Y_UNIT_TEST(TestInRangeCasts) {
    const TVector<TInRangeTestCase> testCases = {
        // Exact matches
        {.Column = Int4, .Offset = Int4, .Expected = TExpectedTypes{.ColumnType = "int4", .OffsetType = "int4"}},
        {.Column = Int4, .Offset = Int8, .Expected = TExpectedTypes{.ColumnType = "int4", .OffsetType = "int8"}},
        {.Column = Int4, .Offset = Int2, .Expected = TExpectedTypes{.ColumnType = "int4", .OffsetType = "int2"}},
        {.Column = Int2, .Offset = Int8, .Expected = TExpectedTypes{.ColumnType = "int2", .OffsetType = "int8"}},
        {.Column = Int2, .Offset = Int4, .Expected = TExpectedTypes{.ColumnType = "int2", .OffsetType = "int4"}},
        {.Column = Int2, .Offset = Int2, .Expected = TExpectedTypes{.ColumnType = "int2", .OffsetType = "int2"}},
        {.Column = Float8, .Offset = Float8, .Expected = TExpectedTypes{.ColumnType = "float8", .OffsetType = "float8"}},
        {.Column = Float4, .Offset = Float8, .Expected = TExpectedTypes{.ColumnType = "float4", .OffsetType = "float8"}},
        {.Column = Timestamp, .Offset = Interval, .Expected = TExpectedTypes{.ColumnType = "timestamp", .OffsetType = "interval"}},
        {.Column = Interval, .Offset = Interval, .Expected = TExpectedTypes{.ColumnType = "interval", .OffsetType = "interval"}},
        {.Column = Numeric, .Offset = Numeric, .Expected = TExpectedTypes{.ColumnType = "numeric", .OffsetType = "numeric"}},
        // With casts
        {.Column = Float4, .Offset = Int4, .Expected = TExpectedTypes{.ColumnType = "float4", .OffsetType = "float8"}},
        {.Column = Float8, .Offset = Int4, .Expected = TExpectedTypes{.ColumnType = "float8", .OffsetType = "float8"}},
        {.Column = Numeric, .Offset = Int4, .Expected = TExpectedTypes{.ColumnType = "numeric", .OffsetType = "numeric"}},
        {.Column = Numeric, .Offset = Float8, .Expected = TExpectedTypes{.ColumnType = "float8", .OffsetType = "float8"}},
        // Failures
        {.Column = Text, .Offset = Text, .Expected = TExpectFailure{}},
        {.Column = Interval, .Offset = Timestamp, .Expected = TExpectFailure{}},
        {.Column = Interval, .Offset = Int4, .Expected = TExpectFailure{}},
        {.Column = Timestamp, .Offset = Int4, .Expected = TExpectFailure{}},
        {.Column = Int4, .Offset = Timestamp, .Expected = TExpectFailure{}},
    };

    for (const auto& tc : testCases) {
        TExprContext ctx;
        TTypeAnnotationContext typesCtx;

        const auto columnTypeId = NPg::LookupType(TString(tc.Column.TypeName)).TypeId;
        const auto offsetTypeId = NPg::LookupType(TString(tc.Offset.TypeName)).TypeId;
        const auto* columnType = ctx.MakeType<TPgExprType>(columnTypeId);
        const auto* offsetType = ctx.MakeType<TPgExprType>(offsetTypeId);

        auto result = LookupInRangeCasts(columnType, offsetType, TPositionHandle(), ctx);

        const bool expectSuccess = std::holds_alternative<TExpectedTypes>(tc.Expected);
        UNIT_ASSERT_VALUES_EQUAL_C(result.has_value(), expectSuccess,
                                   "Failed for (" << tc.Column.TypeName << ", " << tc.Offset.TypeName << ")"
                                                  << (result.has_value() ? "" : ": " + result.error()));

        if (expectSuccess) {
            const auto& expected = std::get<TExpectedTypes>(tc.Expected);

            auto columnNode = MakePgConstNode(ctx, tc.Column);
            auto offsetNode = MakePgConstNode(ctx, tc.Offset);

            TExprNode::TPtr columnCast = result->ColumnCast ? (*result->ColumnCast)(columnNode, ctx) : columnNode;
            TExprNode::TPtr offsetCast = result->OffsetCast ? (*result->OffsetCast)(offsetNode, ctx) : offsetNode;

            columnCast = AnnotateNode(columnCast, ctx, typesCtx);
            offsetCast = AnnotateNode(offsetCast, ctx, typesCtx);

            UNIT_ASSERT_VALUES_EQUAL_C(GetPgTypeId(columnCast), NPg::LookupType(expected.ColumnType).TypeId,
                                       "Column type mismatch for (" << tc.Column.TypeName << ", " << tc.Offset.TypeName << ")");
            UNIT_ASSERT_VALUES_EQUAL_C(GetPgTypeId(offsetCast), NPg::LookupType(expected.OffsetType).TypeId,
                                       "Offset type mismatch for (" << tc.Column.TypeName << ", " << tc.Offset.TypeName << ")");
        }
    }
}

} // Y_UNIT_TEST_SUITE(TLookupInRangeCastsTests)

} // namespace NYql
