#include <ydb/library/yql/providers/function/provider/service_expression.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDqFunction {
namespace {

struct TFixture {
    TExprContext Ctx;
    TPositionHandle Pos = Ctx.AppendPosition(TPosition());
    TDqFunctionGatewayFactory Factory;

    TFixture() {
        Factory.Register("Profiles", [](const auto&, const auto&) -> IDqFunctionGateway::TPtr {
            UNIT_FAIL("Signature lookup must not resolve credentials or make network calls");
            return {};
        });
        Factory.RegisterServiceFunction("Profiles", "Lookup", { { { "id", "Uint64" } }, { { "name", "Utf8" } }, true, 64 });
    }

    TExprNode::TPtr Value(TStringBuf type, TStringBuf value) {
        return Ctx.NewCallable(Pos, type, { Ctx.NewAtom(Pos, value) });
    }

    TExprNode::TPtr Options(TExprNode::TPtr connection, TExprNode::TPtr batch = {}) {
        TExprNode::TListType fields;
        if (connection) {
            fields.push_back(Ctx.NewList(Pos, { Ctx.NewAtom(Pos, "connection"), connection }));
        }
        if (batch) {
            fields.push_back(Ctx.NewList(Pos, { Ctx.NewAtom(Pos, "batch_size"), batch }));
        }
        return Ctx.NewCallable(Pos, "AsStruct", std::move(fields));
    }

    TExprNode::TPtr Call(TExprNode::TPtr options, TStringBuf name = "Profiles.Lookup") {
        return Ctx.NewCallable(Pos, "Apply", { Ctx.NewCallable(Pos, "Udf", { Ctx.NewAtom(Pos, name) }), Value("Uint64", "42"), options });
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TServiceExpressionContract) {
    Y_UNIT_TEST(PreservesOrdinaryUdfAndUsesManifestSignature) {
        TFixture f;
        auto options = f.Options(f.Value("String", "/Root/profiles"));
        auto ordinary = f.Call(options, "String.Length");
        UNIT_ASSERT(RewriteServiceExpression(ordinary, f.Factory, f.Ctx) == ordinary);
        auto result = RewriteServiceExpression(f.Call(options), f.Factory, f.Ctx);
        UNIT_ASSERT(result && result->IsCallable("WasmServiceCall"));
        UNIT_ASSERT(result->Head().IsCallable("SqlExternalFunction"));
        UNIT_ASSERT_VALUES_EQUAL(result->Child(1)->Head().Head().Content(), "id");
        UNIT_ASSERT_VALUES_EQUAL(result->Head().Child(2)->ChildrenSize(), 3);
        auto second = RewriteServiceExpression(f.Call(options), f.Factory, f.Ctx);
        UNIT_ASSERT(result->Tail().Content() != second->Tail().Content());
    }

    Y_UNIT_TEST(RejectsDynamicOrInvalidOptionsBeforeExecution) {
        for (const TString batch : { "0", "-1", "65", "2147483648", "not-a-number" }) {
            TFixture f;
            UNIT_ASSERT(
                !RewriteServiceExpression(f.Call(f.Options(f.Value("String", "/Root/profiles"), f.Value("Int32", batch))), f.Factory, f.Ctx));
        }
        TFixture f;
        UNIT_ASSERT(!RewriteServiceExpression(f.Call(f.Options(f.Ctx.NewArgument(f.Pos, "connection"))), f.Factory, f.Ctx));
        UNIT_ASSERT(!RewriteServiceExpression(f.Call(f.Options(f.Value("String", ""))), f.Factory, f.Ctx));
        UNIT_ASSERT(!RewriteServiceExpression(f.Call(f.Options({})), f.Factory, f.Ctx));
        UNIT_ASSERT(!RewriteServiceExpression(f.Call(f.Ctx.NewArgument(f.Pos, "options")), f.Factory, f.Ctx));
    }

    Y_UNIT_TEST(ExplicitBatchSizeAndUnsupportedBatchMethod) {
        TFixture f;
        auto options = f.Options(f.Value("String", "/Root/profiles"), f.Value("Int32", "64"));
        auto result = RewriteServiceExpression(f.Call(options), f.Factory, f.Ctx);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Head().Child(2)->ChildrenSize(), 4);
        f.Factory.RegisterServiceFunction("Profiles", "Single", { { { "id", "Uint64" } }, { { "name", "Utf8" } }, false, 1 });
        UNIT_ASSERT(!RewriteServiceExpression(f.Call(options, "Profiles.Single"), f.Factory, f.Ctx));
    }
}   // Y_UNIT_TEST_SUITE(TServiceExpressionContract)
}   // namespace NYql::NDqFunction
