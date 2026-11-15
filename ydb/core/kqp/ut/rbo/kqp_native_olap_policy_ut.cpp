#include "kqp_rbo_test_helpers.h"
#include <ydb/core/kqp/opt/rbo/frontend_lowering.h>
#include <ydb/core/kqp/opt/rbo/frontend/predicate.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>
#include <ydb/core/kqp/opt/physical/predicate_collector.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy_olap_filter.h>
#include <yql/essentials/core/peephole_opt/yql_opt_peephole_physical.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <library/cpp/testing/unittest/registar.h>
#include <array>

namespace NKikimr::NKqp {

namespace {

// Supply the real KQP closure annotator and the provider's default constraint
// transformer, without catalog I/O or a query executor. Common peepholes need
// both provider hooks even though this fixture contains only scalar expressions.
class TLegacyPredicateProvider final : public NYql::TDataProviderBase {
public:
    explicit TLegacyPredicateProvider(NTests::TIdTestContext& f)
        : Annotation(NOpt::CreateKqpTypeAnnotationTransformer("ut", f.Tables, f.Config))
        , Constraints(NYql::CreateDefCallableConstraintTransformer()) {}

    TStringBuf GetName() const override { return "kikimr"; }
    bool CanParse(const NYql::TExprNode& node) override {
        return NYql::NNodes::TKqpPredicateClosure::Match(&node);
    }
    NYql::IGraphTransformer& GetTypeAnnotationTransformer(bool) override { return *Annotation; }
    NYql::IGraphTransformer& GetConstraintTransformer(bool, bool) override { return *Constraints; }

private:
    THolder<NYql::TVisitorTransformerBase> Annotation;
    TAutoPtr<NYql::IGraphTransformer> Constraints;
};

// The pre-native rule coalesced filter truth and ran these peepholes before
// collection. This is an independent test oracle, never a production adapter.
NYql::NNodes::TCoLambda NormalizeLegacyPredicate(const NYql::TExprNode::TPtr& predicate,
    const NYql::TExprNode::TPtr& row, NTests::TIdTestContext& f, NYql::TTypeAnnotationContext& types) {
    using namespace NYql;
    using namespace NYql::NNodes;
    auto& ctx = f.ExprCtx;
    const auto pos = predicate->Pos();
    auto argument = ctx.NewArgument(pos, "normalized_row");
    argument->SetTypeAnn(row->GetTypeAnn());
    argument->SetState(TExprNode::EState::ConstrComplete);
    auto body = ctx.NewCallable(pos, "Coalesce", {ctx.ReplaceNode(TExprNode::TPtr(predicate), *row, argument),
        ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, "false")})});
    auto closure = ctx.NewCallable(pos, TKqpPredicateClosure::CallableName(), {
        ctx.NewLambda(pos, ctx.NewArguments(pos, {argument}), std::move(body)),
        ExpandType(pos, *ctx.MakeType<TTupleExprType>(TVector<const TTypeAnnotationNode*>{row->GetTypeAnn()}), ctx)});
    TExprNode::TPtr normalized;
    bool nonDeterministic = false;
    UNIT_ASSERT_VALUES_EQUAL_C(PeepHoleOptimizeNode(closure, normalized, ctx, types, nullptr, nonDeterministic),
        IGraphTransformer::TStatus::Ok, ctx.IssueManager.GetIssues().ToString());
    return TKqpPredicateClosure(normalized).Lambda();
}

} // namespace

Y_UNIT_TEST_SUITE(KqpNativeOlapPolicy) {
    Y_UNIT_TEST(RegexAdmissionPreservesExistingConfiguration) {
        using namespace NFrontend;
        using namespace NYql;
        for (const bool nullable : {false, true})
        for (const auto op : {EOpcode::RegexMatch, EOpcode::RegexGrep, EOpcode::Like, EOpcode::ILike})
        for (const bool parameter : {false, true}) for (const bool negate : {false, true}) {
            NTests::TIdTestContext f;
            auto& arena = *f.Props.NativeExpressions;
            auto& ctx = f.ExprCtx;
            const auto id = f.Id("s");
            const TType string{ETypeKind::String, nullable};
            const auto pattern = parameter ? arena.Parameter("pattern", {ETypeKind::String})
                : arena.Literal({ETypeKind::String}, std::string{"a.*"});
            const bool fullMatch = op != EOpcode::RegexGrep;
            auto root = op == EOpcode::Like || op == EOpcode::ILike
                ? arena.Like(arena.Column(id, string), pattern, {}, {}, op == EOpcode::Like)
                : arena.Regex(arena.Column(id, string), pattern, fullMatch);
            if (negate) root = arena.Unary(EOpcode::Not, root);

            // Independently build the typed legacy runtime call. The policy
            // oracle consumes its public UDF signature and configuration,
            // never the native emitter's result or eligibility decision.
            const auto atom = [&](TStringBuf text) {
                auto node = ctx.NewAtom(f.Pos, text); node->SetTypeAnn(ctx.MakeType<TUnitExprType>()); return node;
            };
            const auto typed = [&](TStringBuf name, NYql::TExprNode::TListType args, const TTypeAnnotationNode* type) {
                auto node = ctx.NewCallable(f.Pos, name, std::move(args)); node->SetTypeAnn(type); return node;
            };
            const auto syntax = [&](const TTypeAnnotationNode* type) { return LowerExecutionType(type, ctx, f.Pos); };
            const auto* textType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String);
            const auto* boolType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
            const auto* optionalText = ctx.MakeType<TOptionalExprType>(textType);
            const auto* fieldType = nullable ? static_cast<const TTypeAnnotationNode*>(optionalText) : textType;
            const auto* rowType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{ctx.MakeType<TItemExprType>("s", fieldType),
                ctx.MakeType<TItemExprType>("n", ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int32))});
            auto row = ctx.NewArgument(f.Pos, "row"); row->SetTypeAnn(rowType);
            auto member = typed("Member", {row, atom("s")}, fieldType);
            if (!nullable) member = typed("Just", {member}, optionalText);
            auto legacyPattern = parameter ? typed("Parameter", {atom("pattern"), syntax(textType)}, textType)
                : typed("String", {atom("a.*")}, textType);
            const bool like = op == EOpcode::Like || op == EOpcode::ILike;
            if (like) {
                // SQL LIKE carries PatternFromLike in the regex run config;
                // plain RE2 has a separate configuration gate.
                const auto* voidType = ctx.MakeType<TVoidExprType>();
                const auto* converterSignature = ctx.MakeType<TCallableExprType>(textType,
                    TVector<TCallableExprType::TArgumentInfo>{{textType, {}, 0}, {optionalText, {}, 0}}, 1, TStringBuf{});
                auto converterSettings = ctx.NewList(f.Pos, {});
                converterSettings->SetTypeAnn(ctx.MakeType<TTupleExprType>(TVector<const TTypeAnnotationNode*>{}));
                const auto converter = typed("Udf", {atom("Re2.PatternFromLike"), typed("Void", {}, voidType),
                    syntax(voidType), atom(""), syntax(converterSignature), syntax(voidType), atom(""), converterSettings}, converterSignature);
                legacyPattern = typed("Apply", {converter, legacyPattern, typed("Nothing", {syntax(optionalText)}, optionalText)}, textType);
            }
            TVector<const TItemExprType*> fields;
            for (const TStringBuf name : {"CaseSensitive", "DotNl", "Literal", "LogErrors", "LongestMatch", "MaxMem",
                    "NeverCapture", "NeverNl", "OneLine", "PerlClasses", "PosixSyntax", "Utf8", "WordBoundary"})
                fields.push_back(ctx.MakeType<TItemExprType>(name, name == "MaxMem"
                    ? ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64) : boolType));
            const auto* optionsStruct = ctx.MakeType<TStructExprType>(fields);
            const auto* optionsType = ctx.MakeType<TOptionalExprType>(optionsStruct);
            auto options = typed("Nothing", {syntax(optionsType)}, optionsType);
            if (like) {
                NYql::TExprNode::TListType values;
                for (const auto* field : fields) {
                    const auto name = field->GetName();
                    const auto value = name == "MaxMem"
                        ? typed("Uint64", {atom("8388608")}, field->GetItemType())
                        : typed("Bool", {atom((name == "CaseSensitive" && op == EOpcode::Like) || name == "LogErrors" || name == "Utf8"
                            ? "true" : "false")}, boolType);
                    auto entry = ctx.NewList(f.Pos, {atom(name), value});
                    entry->SetTypeAnn(ctx.MakeType<TTupleExprType>(TVector<const TTypeAnnotationNode*>{ctx.MakeType<TUnitExprType>(), field->GetItemType()}));
                    values.push_back(entry);
                }
                options = typed("Just", {typed("AsStruct", std::move(values), optionsStruct)}, optionsType);
            }
            auto config = ctx.NewList(f.Pos, {legacyPattern, options});
            config->SetTypeAnn(ctx.MakeType<TTupleExprType>(TVector<const TTypeAnnotationNode*>{textType, optionsType}));
            const auto* signature = ctx.MakeType<TCallableExprType>(boolType,
                TVector<TCallableExprType::TArgumentInfo>{{optionalText, {}, 0}}, 0, TStringBuf{});
            auto settings = ctx.NewList(f.Pos, {});
            settings->SetTypeAnn(ctx.MakeType<TTupleExprType>(TVector<const TTypeAnnotationNode*>{}));
            const auto udf = typed("Udf", {atom(fullMatch ? "Re2.Match" : "Re2.Grep"), config,
                syntax(ctx.MakeType<TVoidExprType>()), atom(""), syntax(signature), syntax(config->GetTypeAnn()), atom(""), settings}, signature);
            auto legacy = typed("Apply", {udf, member}, boolType);
            if (negate) legacy = typed("Not", {legacy}, boolType);
            for (const bool apply : {false, true}) for (const bool regexp : {false, true}) {
                NOpt::TOLAPPredicateNode collected;
                NOpt::CollectPredicates(NNodes::TExprBase(legacy), collected, row.Get(), rowType, {apply, false, false, regexp});
                const bool expected = apply ? collected.CanBePushedApply : collected.CanBePushed;
                UNIT_ASSERT_VALUES_EQUAL(expected, apply && (like || regexp));
                UNIT_ASSERT_VALUES_EQUAL(CanPushNativeOlapPredicate(arena, root, apply, regexp), expected);
                f.Config->SetEnableOlapScalarApply(apply);
                f.Config->SetEnableOlapPushdownRegexp(regexp);
                auto read = f.Read({id}); read->StorageType = NYql::EStorageType::ColumnStorage;
                const auto filter = MakeIntrusive<TOpFilter>(read, f.Pos, TExpression::FromNative(f.Props.NativeExpressions, root, &f.Props));
                const auto result = TPushOlapFilterRule{}.SimpleMatchAndApply(filter, f.RboCtx, f.Props);
                UNIT_ASSERT_VALUES_EQUAL(result->Kind == EOperator::Source && bool(CastOperator<TOpRead>(*result).OlapProgram), expected);
                if (expected) {
                    const auto emitted = LowerNativeOlapPredicate(arena, root, ctx, f.Pos);
                    UNIT_ASSERT_VALUES_EQUAL(FindNodes(emitted, [](const auto& node) { return node->IsCallable("KqpOlapApply"); }).size(), 1);
                }
                for (const bool regexFirst : {false, true}) {
                    const auto numberId = f.Id("n");
                    const auto comparison = arena.Binary(EOpcode::Greater,
                        arena.Column(numberId, {ETypeKind::Int32}), arena.Literal({ETypeKind::Int32}, std::int64_t{0}));
                    const auto* integer = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
                    const auto oldComparison = typed(">", {
                        typed("Member", {row, atom("n")}, integer), typed("Int32", {atom("0")}, integer)}, boolType);
                    const auto oldOrdered = typed("And", regexFirst ? NYql::TExprNode::TListType{legacy, oldComparison}
                        : NYql::TExprNode::TListType{oldComparison, legacy}, boolType);
                    NOpt::TOLAPPredicateNode tree;
                    tree.ExprNode = oldOrdered;
                    NOpt::CollectPredicates(NNodes::TExprBase(oldOrdered), tree, row.Get(), rowType, {apply, false, false, regexp});
                    auto [oldPushed, oldRemaining] = NOpt::SplitForPartialPushdown(tree, false);
                    if (apply) {
                        TVector<NOpt::TOLAPPredicateNode> retained;
                        for (const auto& residual : oldRemaining) {
                            // These required Boolean leaves are already in the
                            // normalized form; retry each independently as the
                            // existing rule does after its first strictness barrier.
                            auto [more, rest] = NOpt::SplitForPartialPushdown(residual, true);
                            oldPushed.insert(oldPushed.end(), more.begin(), more.end());
                            retained.insert(retained.end(), rest.begin(), rest.end());
                        }
                        oldRemaining = std::move(retained);
                    }
                    const auto columns = [](const auto& predicates) {
                        THashSet<TString> names;
                        for (const auto& predicate : predicates)
                            for (const auto& member : FindNodes(predicate.ExprNode,
                                [](const auto& node) { return node->IsCallable("Member"); }))
                                names.insert(TString(member->Tail().Content()));
                        return names;
                    };
                    const auto pushedNames = columns(oldPushed), remainingNames = columns(oldRemaining);
                    const auto ordered = arena.Binary(EOpcode::And, regexFirst ? root : comparison, regexFirst ? comparison : root);
                    auto orderedRead = f.Read({id, numberId}); orderedRead->StorageType = NYql::EStorageType::ColumnStorage;
                    const auto orderedFilter = MakeIntrusive<TOpFilter>(orderedRead, f.Pos,
                        TExpression::FromNative(f.Props.NativeExpressions, ordered, &f.Props));
                    const auto nodes = ctx.NodeAllocationCounter, typeNodes = ctx.TypeNodes.size();
                    const auto actual = TPushOlapFilterRule{}.SimpleMatchAndApply(orderedFilter, f.RboCtx, f.Props);
                    UNIT_ASSERT_VALUES_EQUAL(ctx.NodeAllocationCounter, nodes);
                    UNIT_ASSERT_VALUES_EQUAL(ctx.TypeNodes.size(), typeNodes);
                    const auto source = actual->Kind == EOperator::Filter ? actual->GetChildren().front() : actual;
                    const auto& actualRead = CastOperator<TOpRead>(*source);
                    UNIT_ASSERT_VALUES_EQUAL(bool(actualRead.OlapProgram), !oldPushed.empty());
                    UNIT_ASSERT_VALUES_EQUAL(actual->Kind == EOperator::Filter, !oldRemaining.empty());
                    for (const auto& [column, name] : {std::pair{id, TString{"s"}}, std::pair{numberId, TString{"n"}}}) {
                        const bool pushedColumn = actualRead.OlapProgram && actualRead.OlapProgram.Native().Predicate &&
                            actualRead.OlapProgram.Native().Predicate->GetRawInputIUs().Contains(column);
                        const bool remainingColumn = actual->Kind == EOperator::Filter &&
                            CastOperator<TOpFilter>(*actual).GetFilterExpression().GetRawInputIUs().Contains(column);
                        UNIT_ASSERT_VALUES_EQUAL_C(pushedColumn, pushedNames.contains(name),
                            TStringBuilder() << "regexFirst=" << regexFirst << " apply=" << apply << " regexp=" << regexp << " column=" << name);
                        UNIT_ASSERT_VALUES_EQUAL(remainingColumn, remainingNames.contains(name));
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(PredicateFormsMatchExistingNormalizationAndCollector) {
        using namespace NFrontend;
        using namespace NYql;
        using K = ETypeKind;
        using O = EOpcode;
        struct TPair { TExprId Native; NYql::TExprNode::TPtr Legacy; };
        TString mismatches;
        unsigned tested = 0;
        for (const bool nullable : {false, true}) {
            NTests::TIdTestContext f;
            auto& arena = *f.Props.NativeExpressions;
            auto& ctx = f.ExprCtx;
            const auto types = MakeIntrusive<TTypeAnnotationContext>();
            types->AddDataSink("kikimr", MakeIntrusive<TLegacyPredicateProvider>(f));
            const TPositionHandle pos;
            const TType integer{K::Int32, nullable}, boolean{K::Bool, nullable}, string{K::String, nullable};
            const auto* rowType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
                ctx.MakeType<TItemExprType>("x", ExportNativeType(integer, ctx)),
                ctx.MakeType<TItemExprType>("y", ExportNativeType(integer, ctx)),
                ctx.MakeType<TItemExprType>("b", ExportNativeType(boolean, ctx)),
                ctx.MakeType<TItemExprType>("s", ExportNativeType(string, ctx))});
            const auto row = ctx.NewArgument(pos, "row"); row->SetTypeAnn(rowType);
            const auto pair = [&](TExprId id, TStringBuf callable, NYql::TExprNode::TListType args) {
                auto legacy = ctx.NewCallable(pos, callable, std::move(args));
                return TPair{id, std::move(legacy)};
            };
            const auto column = [&](unsigned id, TType type, TStringBuf name) {
                return pair(arena.Column(id, type), "Member", {row, ctx.NewAtom(pos, name)});
            };
            const auto xId = f.Id("x"), yId = f.Id("y"), bId = f.Id("b"), sId = f.Id("s");
            const auto x = column(xId, integer, "x"), y = column(yId, integer, "y");
            const auto b = column(bId, boolean, "b"), s = column(sId, string, "s");
            const auto one = pair(arena.Literal({K::Int32}, std::int64_t{1}), "Int32", {ctx.NewAtom(pos, "1")});
            const auto yes = pair(arena.Literal({K::Bool}, true), "Bool", {ctx.NewAtom(pos, "true")});
            const auto no = pair(arena.Literal({K::Bool}, false), "Bool", {ctx.NewAtom(pos, "false")});
            const auto text = pair(arena.Literal({K::String}, std::string{"a"}), "String", {ctx.NewAtom(pos, "a")});
            const auto binary = [&](O op, TStringBuf name, const TPair& left, const TPair& right) {
                return pair(arena.Binary(op, left.Native, right.Native), name, {left.Legacy, right.Legacy});
            };
            const auto unary = [&](O op, TStringBuf name, const TPair& value) {
                return pair(arena.Unary(op, value.Native), name, {value.Legacy});
            };
            const auto coalesce = [&](const TPair& left, const TPair& right) {
                const TExprId args[]{left.Native, right.Native};
                return pair(arena.Coalesce(args), "Coalesce", {left.Legacy, right.Legacy});
            };
            const auto condition = [&](const TPair& test, const TPair& left, const TPair& right) {
                const auto truth = arena[test.Native].Type.OptionalDepth
                    ? ctx.NewCallable(pos, "Coalesce", {test.Legacy, no.Legacy}) : test.Legacy;
                return pair(arena.If(test.Native, left.Native, right.Native), "If", {truth, left.Legacy, right.Legacy});
            };
            const auto compare = binary(O::Less, "<", x, y);
            const auto absent = pair(arena.Literal({K::Bool, true}, {}), "Nothing", {
                ExpandType(pos, *ExportNativeType({K::Bool, true}, ctx), ctx)});
            const auto sum = binary(O::Add, "+", x, y);
            const auto contains = binary(O::Contains, "StringContains", s, text);
            const auto exists = unary(O::IsNotNull, "Exists", x);
            struct TCase { TStringBuf Name; TPair Value; };
            const TVector<TCase> cases{
                {"boolean column", b}, {"comparison", compare},
                {"not column", unary(O::Not, "Not", b)},
                {"not comparison", unary(O::Not, "Not", compare)},
                {"column and comparison", binary(O::And, "And", b, compare)},
                {"column or comparison", binary(O::Or, "Or", b, compare)},
                {"comparison and exists", binary(O::And, "And", compare, exists)},
                {"comparison or exists", binary(O::Or, "Or", compare, exists)},
                {"comparison and true", binary(O::And, "And", compare, yes)},
                {"comparison or false", binary(O::Or, "Or", compare, no)},
                {"comparison or null", binary(O::Or, "Or", compare, absent)},
                {"null or comparison", binary(O::Or, "Or", absent, compare)},
                {"column or null", binary(O::Or, "Or", b, absent)},
                {"null or column", binary(O::Or, "Or", absent, b)},
                {"coalesce column", coalesce(b, no)},
                {"coalesce comparison", coalesce(compare, no)},
                {"if column", condition(b, yes, no)},
                {"if comparison", condition(compare, yes, no)},
                {"if predicates", condition(compare, exists, unary(O::Not, "Not", compare))},
                {"just column", unary(O::OptionalWrap, "Just", b)},
                {"just comparison", unary(O::OptionalWrap, "Just", compare)},
                {"column exists", exists},
                {"computed exists", unary(O::IsNotNull, "Exists", sum)},
                {"computed null", unary(O::Not, "Not", unary(O::IsNotNull, "Exists", sum))},
                {"addition comparison", binary(O::Less, "<", sum, one)},
                {"same-column addition", binary(O::Less, "<", binary(O::Add, "+", x, x), one)},
                {"negation comparison", binary(O::Less, "<", unary(O::Negate, "Minus", x), one)},
                {"contains", contains}, {"not contains", unary(O::Not, "Not", contains)},
                {"coalesce contains", coalesce(contains, yes)},
            };
            for (const auto& [name, value] : cases) for (const bool apply : {false, true}) {
                // Pre-PR RBO rejects Bool?? filters before the OLAP rule.
                if (arena[value.Native].Type.OptionalDepth > 1) continue;
                const auto normalized = NormalizeLegacyPredicate(value.Legacy, row, f, *types);
                NOpt::TOLAPPredicateNode collected;
                NOpt::CollectPredicates(normalized.Body(), collected, normalized.Args().Arg(0).Raw(), rowType, {apply, false});
                const bool expected = apply ? collected.CanBePushedApply : collected.CanBePushed;
                f.Config->SetEnableOlapScalarApply(apply);
                auto read = f.Read({xId, yId, bId, sId});
                read->StorageType = NYql::EStorageType::ColumnStorage;
                const TExprId roots[]{PrepareFilterPredicate(arena, value.Native)};
                const auto root = arena.FoldConstants(roots).front();
                const auto filter = MakeIntrusive<TOpFilter>(read, f.Pos,
                    TExpression::FromNative(f.Props.NativeExpressions, root, &f.Props));
                TPushOlapFilterRule rule;
                const auto result = rule.SimpleMatchAndApply(filter, f.RboCtx, f.Props);
                ++tested;
                const bool actual = result->Kind == EOperator::Source && CastOperator<TOpRead>(*result).OlapProgram;
                if (actual != expected) mismatches += TStringBuilder() << name << " nullable=" << nullable
                    << " scalarApply=" << apply << " native=" << actual << " legacy=" << expected
                    << " normalized=" << KqpExprToPrettyString(normalized.Body(), ctx) << '\n';
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(tested, 116);
        UNIT_ASSERT_C(mismatches.empty(), mismatches);
    }

    Y_UNIT_TEST(StringCaseOperandAdmissionMatchesLegacyOptionalMapping) {
        using namespace NFrontend;
        using namespace NYql;
        for (const bool nullable : {false, true}) for (const bool upper : {false, true}) {
            NTests::TIdTestContext f;
            auto& arena = *f.Props.NativeExpressions;
            auto& ctx = f.ExprCtx;
            const auto id = f.Id("s");
            const auto* text = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String);
            const auto* optionalText = ctx.MakeType<TOptionalExprType>(text);
            const auto* boolean = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
            const auto* optionalBool = ctx.MakeType<TOptionalExprType>(boolean);
            const auto* voidType = ctx.MakeType<TVoidExprType>();
            const TTypeAnnotationNode* fieldType = nullable ? static_cast<const TTypeAnnotationNode*>(optionalText) : text;
            const auto* rowType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{ctx.MakeType<TItemExprType>("s", fieldType)});
            auto row = ctx.NewArgument(f.Pos, "row"); row->SetTypeAnn(rowType);
            const auto typed = [&](TStringBuf name, NYql::TExprNode::TListType args, const TTypeAnnotationNode* type) {
                auto node = ctx.NewCallable(f.Pos, name, std::move(args)); node->SetTypeAnn(type); return node;
            };
            const auto atom = [&](TStringBuf name) { return ctx.NewAtom(f.Pos, name); };
            const auto syntax = [&](const TTypeAnnotationNode* type) { return LowerExecutionType(type, ctx, f.Pos); };
            const auto member = typed("Member", {row, atom("s")}, fieldType);
            const auto* signature = ctx.MakeType<TCallableExprType>(text,
                TVector<TCallableExprType::TArgumentInfo>{{text, {}, NUdf::ICallablePayload::TArgumentFlags::AutoMap}}, 0, TStringBuf{});
            const auto settings = ctx.NewList(f.Pos, {ctx.NewList(f.Pos, {atom("strict")})});
            const auto udf = typed("Udf", {atom(upper ? "String.AsciiToUpper" : "String.AsciiToLower"),
                typed("Void", {}, voidType), syntax(voidType), atom(""), syntax(signature), syntax(voidType), atom(""), settings}, signature);
            auto argument = ctx.NewArgument(f.Pos, "value"); argument->SetTypeAnn(text);
            auto mapped = typed("Apply", {udf, nullable ? argument : member}, text);
            if (nullable) {
                auto present = typed("Just", {mapped}, optionalText);
                mapped = typed("IfPresent", {member,
                    ctx.NewLambda(f.Pos, ctx.NewArguments(f.Pos, {argument}), std::move(present)),
                    typed("Nothing", {syntax(optionalText)}, optionalText)}, optionalText);
            }
            const auto literal = typed("String", {atom("abc")}, text);
            const auto comparison = typed("==", {mapped, literal}, nullable ? static_cast<const TTypeAnnotationNode*>(optionalBool) : boolean);
            const auto predicate = nullable ? typed("Coalesce", {comparison, typed("Bool", {atom("false")}, boolean)}, boolean) : comparison;
            const auto native = arena.Binary(EOpcode::Equal,
                arena.Call(upper ? "String::AsciiToUpper" : "String::AsciiToLower",
                    std::array{arena.Column(id, {ETypeKind::String, nullable})}),
                arena.Literal({ETypeKind::String}, std::string{"abc"}));
            for (const bool apply : {false, true}) {
                NOpt::TOLAPPredicateNode collected;
                NOpt::CollectPredicates(NNodes::TExprBase(predicate), collected, row.Get(), rowType, {apply, false});
                const bool expected = apply ? collected.CanBePushedApply : collected.CanBePushed;
                UNIT_ASSERT_VALUES_EQUAL(expected, apply && nullable);
                const auto prepared = PrepareFilterPredicate(arena, native);
                UNIT_ASSERT_VALUES_EQUAL(CanPushNativeOlapPredicate(arena, prepared, apply), expected);
                f.Config->SetEnableOlapScalarApply(apply);
                auto read = f.Read({id}); read->StorageType = NYql::EStorageType::ColumnStorage;
                const auto filter = MakeIntrusive<TOpFilter>(read, f.Pos,
                    TExpression::FromNative(f.Props.NativeExpressions, prepared, &f.Props));
                const auto nodes = ctx.NodeAllocationCounter, typeNodes = ctx.TypeNodes.size();
                const auto result = TPushOlapFilterRule{}.SimpleMatchAndApply(filter, f.RboCtx, f.Props);
                UNIT_ASSERT_VALUES_EQUAL(ctx.NodeAllocationCounter, nodes);
                UNIT_ASSERT_VALUES_EQUAL(ctx.TypeNodes.size(), typeNodes);
                UNIT_ASSERT_VALUES_EQUAL(result->Kind == EOperator::Source && bool(CastOperator<TOpRead>(*result).OlapProgram), expected);
            }
        }
    }

    Y_UNIT_TEST(PartialPushdownMatchesExistingCheckedPredicatePolicy) {
        using namespace NFrontend;
        using namespace NYql;
        using K = ETypeKind;
        using O = EOpcode;
        struct TPair { TExprId Native; NYql::TExprNode::TPtr Legacy; };
        TString mismatches;
        unsigned tested = 0;
        for (const bool nullable : {false, true}) {
            NTests::TIdTestContext f;
            auto& arena = *f.Props.NativeExpressions;
            auto& ctx = f.ExprCtx;
            const auto types = MakeIntrusive<TTypeAnnotationContext>();
            types->AddDataSink("kikimr", MakeIntrusive<TLegacyPredicateProvider>(f));
            const TPositionHandle pos;
            const TType integer{K::Int32, nullable}, boolean{K::Bool};
            const auto xId = f.Id("x"), yId = f.Id("y"), bId = f.Id("b"), nbId = f.Id("nb");
            const auto* rowType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
                ctx.MakeType<TItemExprType>("x", ExportNativeType(integer, ctx)),
                ctx.MakeType<TItemExprType>("y", ExportNativeType(integer, ctx)),
                ctx.MakeType<TItemExprType>("b", ExportNativeType(boolean, ctx)),
                ctx.MakeType<TItemExprType>("nb", ExportNativeType(boolean.Optional(), ctx))});
            const auto row = ctx.NewArgument(pos, "row"); row->SetTypeAnn(rowType);
            const auto pair = [&](TExprId id, TStringBuf callable, NYql::TExprNode::TListType args) {
                return TPair{id, ctx.NewCallable(pos, callable, std::move(args))};
            };
            const auto column = [&](unsigned id, TType type, TStringBuf name) {
                return pair(arena.Column(id, type), "Member", {row, ctx.NewAtom(pos, name)});
            };
            const auto x = column(xId, integer, "x"), y = column(yId, integer, "y");
            const auto b = column(bId, boolean, "b"), nb = column(nbId, boolean.Optional(), "nb");
            const auto one = pair(arena.Literal({K::Int32}, std::int64_t{1}), "Int32", {ctx.NewAtom(pos, "1")});
            const auto no = pair(arena.Literal(boolean, false), "Bool", {ctx.NewAtom(pos, "false")});
            const auto message = ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "checked row")});
            const auto binary = [&](O op, TStringBuf name, const TPair& left, const TPair& right) {
                return pair(arena.Binary(op, left.Native, right.Native), name, {left.Legacy, right.Legacy});
            };
            const auto conjunction = [&](const TPair& left, const TPair& right) {
                return binary(O::And, "And", left, right);
            };
            const auto coalesce = [&](const TPair& value) {
                const TExprId args[]{value.Native, no.Native};
                return pair(arena.Coalesce(args), "Coalesce", {value.Legacy, no.Legacy});
            };
            const auto checked = pair(arena.Ensure(b.Native, b.Native, "checked row"), "Ensure", {b.Legacy, b.Legacy, message});
            const auto unwrapped = pair(arena.Unary(O::Unwrap, nb.Native), "Unwrap", {nb.Legacy});
            const auto compareX = binary(O::Greater, ">", x, one), compareY = binary(O::Less, "<", y, one);
            const auto checkedValue = pair(arena.Ensure(x.Native, b.Native, "checked row"), "Ensure", {x.Legacy, b.Legacy, message});
            const auto condition = [&](const TPair& test, const TPair& yes, const TPair& otherwise) {
                return pair(arena.If(test.Native, yes.Native, otherwise.Native), "If", {
                    coalesce(test).Legacy, yes.Legacy, otherwise.Legacy});
            };
            const auto guarded = condition(compareX, checked, no);
            const auto two = pair(arena.Literal({K::Int32}, std::int64_t{2}), "Int32", {ctx.NewAtom(pos, "2")});
            const auto memberMatch = binary(O::Or, "Or", binary(O::Equal, "==", x, one), binary(O::Equal, "==", x, two));
            // Feed the old rule its actual SqlIn input. A hand-expanded IF
            // has equivalent values but a different admission contract.
            const TPair membership{arena.InResult(x.Native, memberMatch.Native, false),
                ctx.NewCallable(pos, "SqlIn", {ctx.NewList(pos, {one.Legacy, two.Legacy}), x.Legacy, ctx.NewList(pos, {})})};
            const auto negatedMembership = pair(arena.Unary(O::Not, membership.Native), "Not", {membership.Legacy});

            struct TCase { TStringBuf Name; TPair Value; };
            const TVector<TCase> cases{
                {"comparison then ensure", conjunction(compareX, checked)},
                {"ensure then comparison", conjunction(checked, compareX)},
                {"coalesced then ensure", conjunction(coalesce(compareX), checked)},
                {"ensure then coalesced", conjunction(checked, coalesce(compareX))},
                {"comparison then unwrap", conjunction(compareX, unwrapped)},
                {"unwrap then comparison", conjunction(unwrapped, compareX)},
                {"checked comparison", binary(O::Greater, ">", checkedValue, one)},
                {"guarded ensure", guarded},
                {"guarded comparison", condition(compareX, compareY, no)},
                {"inverse guarded comparison", condition(compareX, no, compareY)},
                {"checked conditional branches", condition(compareX, checked, compareY)},
                {"ensure or comparison", binary(O::Or, "Or", checked, compareX)},
                {"comparison or ensure", binary(O::Or, "Or", compareX, checked)},
                {"comparison ensure comparison", conjunction(conjunction(compareX, checked), compareY)},
                {"ensure two comparisons", conjunction(checked, conjunction(compareX, compareY))},
                {"two comparisons ensure", conjunction(conjunction(compareX, compareY), checked)},
                {"strict residual then comparison", conjunction(b, compareX)},
                {"membership", membership},
                {"negated membership", negatedMembership},
                {"membership then comparison", conjunction(membership, compareY)},
                {"comparison then membership", conjunction(compareY, membership)},
                {"membership as Boolean operand", binary(O::Equal, "==", membership, b)},
            };
            for (const auto& [name, value] : cases) for (const bool apply : {false, true}) {
                const auto normalized = NormalizeLegacyPredicate(value.Legacy, row, f, *types);
                NOpt::TOLAPPredicateNode collected;
                collected.ExprNode = normalized.Body().Ptr();
                NOpt::CollectPredicates(normalized.Body(), collected, normalized.Args().Arg(0).Raw(), rowType, {apply, false});
                auto [pushed, remaining] = NOpt::SplitForPartialPushdown(collected, false);
                if (apply) {
                    TVector<NOpt::TOLAPPredicateNode> afterApply;
                    for (const auto& residual : remaining) {
                        const auto rewritten = NormalizeLegacyPredicate(residual.ExprNode, normalized.Args().Arg(0).Ptr(), f, *types);
                        NOpt::TOLAPPredicateNode candidate;
                        candidate.ExprNode = rewritten.Body().Ptr();
                        NOpt::CollectPredicates(rewritten.Body(), candidate, rewritten.Args().Arg(0).Raw(), rowType, {true, false});
                        auto [more, rest] = NOpt::SplitForPartialPushdown(candidate, true);
                        pushed.insert(pushed.end(), more.begin(), more.end());
                        if (!rest.empty()) afterApply.push_back(residual);
                    }
                    remaining = std::move(afterApply);
                }
                const auto dependencies = [&](const TVector<NOpt::TOLAPPredicateNode>& predicates) {
                    THashSet<TString> result;
                    for (const auto& predicate : predicates)
                        for (const auto& member : FindNodes(predicate.ExprNode, [](const auto& node) { return node->IsCallable("Member"); }))
                            result.insert(TString(member->Tail().Content()));
                    return result;
                };
                const auto pushedNames = dependencies(pushed), residualNames = dependencies(remaining);
                f.Config->SetEnableOlapScalarApply(apply);
                auto read = f.Read({xId, yId, bId, nbId});
                read->StorageType = NYql::EStorageType::ColumnStorage;
                const TExprId roots[]{PrepareFilterPredicate(arena, value.Native)};
                const auto root = arena.FoldConstants(roots).front();
                const auto filter = MakeIntrusive<TOpFilter>(read, f.Pos,
                    TExpression::FromNative(f.Props.NativeExpressions, root, &f.Props));
                TPushOlapFilterRule rule;
                const auto nodes = ctx.NodeAllocationCounter, typeNodes = ctx.TypeNodes.size();
                const auto result = rule.SimpleMatchAndApply(filter, f.RboCtx, f.Props);
                UNIT_ASSERT_VALUES_EQUAL(ctx.NodeAllocationCounter, nodes);
                UNIT_ASSERT_VALUES_EQUAL(ctx.TypeNodes.size(), typeNodes);
                const auto source = result->Kind == EOperator::Filter ? result->GetChildren().front() : result;
                UNIT_ASSERT(source->Kind == EOperator::Source);
                const auto& nativeRead = CastOperator<TOpRead>(*source);
                bool same = bool(nativeRead.OlapProgram) == !pushed.empty();
                same &= (result->Kind == EOperator::Filter) == !remaining.empty();
                for (const auto& [id, columnName] : {std::pair{xId, TString{"x"}}, {yId, TString{"y"}}, {bId, TString{"b"}}, {nbId, TString{"nb"}}}) {
                    const bool pushedColumn = nativeRead.OlapProgram && nativeRead.OlapProgram.Native().Predicate &&
                        nativeRead.OlapProgram.Native().Predicate->GetRawInputIUs().Contains(id);
                    const bool residualColumn = result->Kind == EOperator::Filter &&
                        CastOperator<TOpFilter>(*result).GetFilterExpression().GetRawInputIUs().Contains(id);
                    same &= pushedColumn == pushedNames.contains(columnName);
                    same &= residualColumn == residualNames.contains(columnName);
                }
                if (!same) mismatches += TStringBuilder() << name << " nullable=" << nullable << " scalarApply=" << apply
                    << " expectedPush=" << !pushed.empty() << " actualPush=" << bool(nativeRead.OlapProgram)
                    << " normalized=" << KqpExprToPrettyString(normalized.Body(), ctx) << '\n';
                ++tested;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(tested, 88);
        UNIT_ASSERT_C(mismatches.empty(), mismatches);
    }

}
} // namespace NKikimr::NKqp
