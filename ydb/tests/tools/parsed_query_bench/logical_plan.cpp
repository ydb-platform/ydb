#include "logical_plan.h"

#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <util/generic/scope.h>

namespace NKikimr::NKqp::NParsedQueryBench {
namespace {

using P = Ydb::Query::NativeQuery;
using L = Ydb::Query::LogicalPlan;

constexpr size_t MaxNodes = 1024;
constexpr size_t MaxOperatorDepth = 256;
constexpr size_t MaxExpressionDepth = 64;
constexpr size_t MaxInformationUnits = 16 * 1024;
constexpr size_t MaxExpressions = 64 * 1024;
constexpr size_t MaxSpelling = 1024;

template <class TMessage>
void CheckFields(const TMessage& message) {
    Y_ENSURE(message.GetReflection()->GetUnknownFields(message).empty(), "Unknown LogicalPlan field");
}

TString CopyString(TStringBuf value) {
    Y_ENSURE(value.size() <= MaxSpelling, "LogicalPlan spelling limit exceeded");
    return TString(value);
}

std::optional<EDataSlot> LiteralSlot(TStringBuf name) {
    if (name == "Bool") { return EDataSlot::Bool; }
    if (name == "Int32") { return EDataSlot::Int32; }
    if (name == "Int64") { return EDataSlot::Int64; }
    if (name == "Uint32") { return EDataSlot::Uint32; }
    if (name == "Uint64") { return EDataSlot::Uint64; }
    if (name == "String") { return EDataSlot::String; }
    return std::nullopt;
}

bool SupportedScalar(TStringBuf name) {
    static const THashSet<TStringBuf> Calls = {
        "==", "!=", "<", ">=", "And", "Not", "Exists", "Coalesce", "If", "SqlIn", "Just", "Unwrap", "Nothing", "Null",
        "Convert", "SafeCast", "StrictCast",
    };
    return LiteralSlot(name).has_value() || Calls.contains(name);
}

bool ValidLiteral(EDataSlot slot, TStringBuf value) {
    if (slot == EDataSlot::String) {
        return value.size() <= MaxSpelling;
    }
    if (value.empty() || value.size() > 21) {
        return false;
    }
    i32 signed32;
    i64 signed64;
    ui32 unsigned32;
    ui64 unsigned64;
    switch (slot) {
        case EDataSlot::Bool: return value == "true" || value == "false";
        case EDataSlot::Int32: return TryFromString(value, signed32);
        case EDataSlot::Int64: return TryFromString(value, signed64);
        case EDataSlot::Uint32: return TryFromString(value, unsigned32);
        case EDataSlot::Uint64: return TryFromString(value, unsigned64);
        default: return false;
    }
}

const TTypeAnnotationNode* DataType(TExprContext& ctx, TStringBuf slotName, TStringBuf precision = {}, TStringBuf scale = {}) {
    const auto slot = NUdf::FindDataSlot(slotName);
    Y_ENSURE(slot, "Unsupported LogicalPlan data type: " << slotName);
    if (*slot == EDataSlot::Decimal) {
        ui32 p, s;
        Y_ENSURE(TryFromString(precision, p) && TryFromString(scale, s) && p > 0 && p <= 35 && s <= p,
            "Invalid LogicalPlan Decimal type");
        return ctx.MakeType<TDataExprParamsType>(*slot, precision, scale);
    }
    Y_ENSURE(precision.empty() && scale.empty() && NUdf::GetDataTypeInfo(*slot).ParamsCount == 0,
        "Unsupported LogicalPlan type parameters");
    return ctx.MakeType<TDataExprType>(*slot);
}

TString ReadJoinKind(P::Join::Kind kind) {
    switch (kind) {
        case P::Join::INNER: return "Inner";
        case P::Join::LEFT: return "Left";
        case P::Join::LEFT_ONLY: return "LeftOnly";
        case P::Join::CROSS: return "Cross";
        default: ythrow yexception() << "Unsupported LogicalPlan JOIN kind: " << static_cast<int>(kind);
    }
}

const TTypeAnnotationNode& OptionalItemType(const TTypeAnnotationNode& type) {
    return type.GetKind() == ETypeAnnotationKind::Optional ? *type.Cast<TOptionalExprType>()->GetItemType() : type;
}

bool IsSupportedJoinKeyType(const TTypeAnnotationNode& left, const TTypeAnnotationNode& right) {
    const auto& key = OptionalItemType(left);
    return key.GetKind() == ETypeAnnotationKind::Data && key.IsEquatable()
        && IsSameAnnotation(key, OptionalItemType(right));
}

using TUnitTypes = THashMap<TInfoUnitId, const TTypeAnnotationNode*>;
using TParameterTypes = THashMap<TString, const TTypeAnnotationNode*>;

void NullExtend(TExprContext& ctx, TUnitTypes& types, const TUnorderedIUs& units) {
    for (const auto id : units) {
        auto& type = types.at(id);
        if (!type->IsOptionalOrNull()) {
            type = ctx.MakeType<TOptionalExprType>(type);
        }
    }
}

bool IsCountInput(const TTypeAnnotationNode& type, bool distinct) {
    const auto& item = OptionalItemType(type);
    return item.GetKind() == ETypeAnnotationKind::Data && (!distinct || (item.IsHashable() && item.IsEquatable()));
}

bool SameWireType(const TTypeAnnotationNode& left, const TTypeAnnotationNode& right) {
    return IsSameAnnotation(left, right)
        || (left.GetHash() == right.GetHash() && FormatType(&left) == FormatType(&right));
}

void CheckExpressionType(const TExprNode& node, const TTypeAnnotationNode& expected) {
    const auto* actual = node.GetTypeAnn().Get();
    Y_ENSURE(!actual || SameWireType(*actual, expected), "LogicalPlan type does not match expression");
}

const TTypeAnnotationNode* AnnotateScalarLambda(const TExprNode& lambda, TExprContext& source, TExprContext& ctx,
    TTypeAnnotationContextPtr& annotationContext, const TUnitTypes& unitTypes, IOperator& input)
{
    const auto& row = lambda.Head().Head();
    TVector<const TItemExprType*> fields;
    THashSet<TInfoUnitId> bound;
    VisitExpr(lambda.Tail(), [&](const TExprNode& node) {
        if (node.IsCallable("Member")) {
            Y_ENSURE(&node.Head() == &row, "Out-of-scope LogicalPlan lambda argument");
            const auto id = GetMemberId(node);
            Y_ENSURE(unitTypes.contains(id) && input.GetOutputIUs().Contains(id), "Unavailable LogicalPlan input IU");
            const auto* type = unitTypes.at(id);
            CheckExpressionType(node, *type);
            if (bound.insert(id).second) {
                fields.push_back(ctx.MakeType<TItemExprType>(TString(node.Tail().Content()), type));
            }
        }
        return true;
    });

    // Reannotate a copy so stock type checking cannot rewrite the plan itself.
    TNodeOnNodeOwnedMap copies;
    auto typed = ctx.DeepCopy(lambda, source, copies, &source != &ctx, false);
    typed->Head().SetTypeAnn(ctx.MakeType<TUnitExprType>());
    typed->Head().Head().SetTypeAnn(ctx.MakeType<TStructExprType>(fields));
    const auto repeats = ctx.RepeatTransformCounter;
    const auto steps = ctx.Step;
    Y_DEFER { ctx.RepeatTransformCounter = repeats; ctx.Step = steps; };
    ctx.RepeatTransformCounter = 0;
    if (!annotationContext) {
        annotationContext = MakeIntrusive<TTypeAnnotationContext>();
    }
    auto transformer = CreateTypeAnnotationTransformer(
        CreateExtCallableTypeAnnotationTransformer(*annotationContext, true), *annotationContext);
    Y_ENSURE(InstantTransform(*transformer, typed, ctx) == IGraphTransformer::TStatus::Ok,
        "LogicalPlan scalar type annotation failed");
    const auto* type = typed->Tail().GetTypeAnn().Get();
    Y_ENSURE(type, "LogicalPlan lambda result has no type");
    CheckExpressionType(lambda.Tail(), *type);
    for (const auto& [original, copy] : copies) {
        if (const auto* actual = copy->GetTypeAnn().Get()) {
            CheckExpressionType(*original, *actual);
        }
    }
    return type;
}

struct TExpressionEntry {
    const P::Expression* Wire = nullptr;
    TExprNode::TPtr Node;
    size_t Depth = 0;
    bool Used = false;
};

struct TOperatorEntry {
    TIntrusivePtr<IOperator> Node;
    size_t Depth;
};

class TPlanReader {
public:
    TPlanReader(TExprContext& ctx, const TVector<const TTypeAnnotationNode*>& types, const TVector<const P::Function*>& functions,
        const TParameterTypes& parameters, THashSet<TString>& usedParameters, const TCatalogLookup& lookup)
        : Ctx(ctx)
        , Types(types)
        , Functions(functions)
        , Parameters(parameters)
        , UsedParameters(usedParameters)
        , Lookup(lookup)
    {}

    TIntrusivePtr<TOpRoot> Read(const L::Root& root, ui32 position, TVector<const TTypeAnnotationNode*>& resultTypes) {
        const auto& plan = root.graph();
        CheckFields(plan);
        Y_ENSURE(plan.subqueries().empty() && plan.replicate_hubs_size() <= plan.operators_size()
            && static_cast<size_t>(plan.information_units_size()) <= MaxInformationUnits
            && static_cast<size_t>(plan.expressions_size()) <= MaxExpressions
            && static_cast<size_t>(plan.operators_size()) <= MaxNodes && !plan.operators().empty(),
            "Unsupported LogicalPlan plan size or features");
        TVector<TString> columnNames(plan.information_units_size());
        for (const auto& op : plan.operators()) {
            if (op.has_read()) {
                for (const auto& binding : op.read().columns()) {
                    Y_ENSURE(binding.iu_id() && binding.iu_id() <= columnNames.size(), "Invalid scan IU");
                    columnNames[binding.iu_id() - 1] = CopyString(binding.column());
                }
            }
        }
        for (const auto& iu : plan.information_units()) {
            CheckFields(iu);
            Y_ENSURE(iu.id() == DefinedTypes.size() + 1 && iu.defined_type_id() && iu.defined_type_id() <= Types.size(),
                "Invalid LogicalPlan IU definition");
            DefinedTypes.push_back(Types[iu.defined_type_id() - 1]);
            const auto& name = columnNames[iu.id() - 1];
            Props.InfoUnitRegistry.Add(TInfoUnit("", name ? name : ToString(iu.id())));
        }
        for (const auto& expression : plan.expressions()) {
            ReadExpression(expression);
        }
        ReplicateHubs.reserve(plan.replicate_hubs_size());
        for (const auto& hub : plan.replicate_hubs()) {
            CheckFields(hub);
            Y_ENSURE(hub.id() == ReplicateHubs.size() + 1 && !hub.has_location(), "Invalid LogicalPlan Replicate hub");
            ReplicateHubs.push_back(&hub);
        }
        Operators.reserve(plan.operators_size());
        for (const auto& op : plan.operators()) {
            Y_ENSURE(op.id() == Operators.size() + 1, "Invalid LogicalPlan operator order");
            size_t depth = 1;
            auto decoded = ReadOperator(op, depth);
            Y_ENSURE(depth <= MaxOperatorDepth, "LogicalPlan operator nesting limit exceeded");
            decoded->InitializeOutputIUs();
            Operators.push_back({std::move(decoded), depth});
        }
        auto input = TakeOperator(plan.input_operator_id()).Node;
        Y_ENSURE(std::ranges::none_of(Operators, [](const TOperatorEntry& op) { return bool(op.Node); })
            && std::ranges::none_of(ReplicateHubs, [](const auto* hub) { return hub != nullptr; })
            && UnitTypes.size() == Props.InfoUnitRegistry.Size(), "Unused LogicalPlan operator, Replicate hub or IU");
        for (const auto& expression : Expressions) {
            Y_ENSURE(expression.Used, "Unused LogicalPlan expression");
        }

        TOrderedIUs<TString> columns;
        for (const auto& column : root.columns()) {
            CheckFields(column);
            const auto id = AvailableUnit(column.iu_id(), *input);
            columns.Append(id, CopyString(column.name()));
            resultTypes.push_back(UnitTypes.at(id));
        }
        TVector<TString> queryColumns;
        for (const auto& column : root.columns()) {
            queryColumns.emplace_back(column.name());
        }
        auto result = MakeIntrusive<TOpRoot>(std::move(input), TPositionHandle(), std::move(columns), std::move(queryColumns));
        result->PlanProps = std::move(Props);
        result->PlanProps.ResultIndex = position;
        result->PlanProps.UnorderedResult = !root.ordered_rows();
        for (const auto& entry : *result) {
            entry.Current->BindExpressionPlanProps(&result->PlanProps);
        }
        return result;
    }

private:
    TExpressionEntry& Reference(ui32 id, P::Expression::NodeCase kind) {
        Y_ENSURE(id > 0 && id <= Expressions.size(), "Invalid or forward LogicalPlan expression reference");
        auto& entry = Expressions[id - 1];
        Y_ENSURE(entry.Wire->node_case() == kind, "Wrong LogicalPlan expression node kind");
        entry.Used = true;
        return entry;
    }

    TExpressionEntry& ScalarReference(ui32 id) {
        Y_ENSURE(id > 0 && id <= Expressions.size(), "Invalid LogicalPlan scalar reference");
        const auto& wire = *Expressions[id - 1].Wire;
        Y_ENSURE(wire.has_atom() || wire.has_list() || wire.has_call() || wire.has_column()
            || wire.has_type_literal() || wire.has_parameter(), "Invalid LogicalPlan scalar kind");
        return Reference(id, wire.node_case());
    }

    void ReadExpression(const P::Expression& expression) {
        CheckFields(expression);
        Y_ENSURE(expression.id() == Expressions.size() + 1 && !expression.has_location(),
            "Invalid LogicalPlan expression order or location");
        TPositionHandle pos;
        TExprNode::TPtr node;
        size_t depth = 1;
        const auto takeScalar = [this, &depth](ui32 id) {
            auto& entry = ScalarReference(id);
            depth = std::max(depth, 1 + entry.Depth);
            return entry.Node;
        };
        switch (expression.node_case()) {
            case P::Expression::kAtom: {
                const auto& atom = expression.atom();
                CheckFields(atom);
                Y_ENSURE((atom.flags() & ~TNodeFlags::FlagsMask) == 0, "Unsupported LogicalPlan atom flags");
                node = Ctx.NewAtom(pos, CopyString(atom.content()), atom.flags());
                break;
            }
            case P::Expression::kArgument: {
                CheckFields(expression.argument());
                node = Ctx.NewArgument(pos, CopyString(expression.argument().debug_name()));
                break;
            }
            case P::Expression::kArguments: {
                const auto& args = expression.arguments();
                CheckFields(args);
                Y_ENSURE(args.arguments_size() == 1 && OwnedArguments.insert(args.arguments(0)).second,
                    "Invalid LogicalPlan argument ownership");
                node = Ctx.NewArguments(pos, {Reference(args.arguments(0), P::Expression::kArgument).Node});
                break;
            }
            case P::Expression::kLambda: {
                const auto& lambda = expression.lambda();
                CheckFields(lambda);
                Y_ENSURE(lambda.body_size() == 1 && OwnedArgumentLists.insert(lambda.arguments_expr_id()).second,
                    "Invalid LogicalPlan lambda ownership");
                auto args = Reference(lambda.arguments_expr_id(), P::Expression::kArguments).Node;
                auto bodyNode = takeScalar(lambda.body(0));
                node = Ctx.NewLambda(pos, std::move(args), std::move(bodyNode));
                break;
            }
            case P::Expression::kCall: {
                const auto& call = expression.call();
                CheckFields(call);
                Y_ENSURE(call.function_id() > 0 && call.function_id() <= Functions.size() && call.arguments_size() <= 256,
                    "Invalid LogicalPlan scalar callable");
                const auto& function = *Functions[call.function_id() - 1];
                Y_ENSURE(function.kind() == P::Function::YQL_CALLABLE, "Unsupported LogicalPlan scalar function kind");
                const auto& name = function.name();
                if (const auto slot = LiteralSlot(name)) {
                    Y_ENSURE(call.arguments_size() == 1, "Invalid LogicalPlan literal arity");
                    auto atom = Reference(call.arguments(0), P::Expression::kAtom).Node;
                    Y_ENSURE(ValidLiteral(*slot, atom->Content()), "Invalid LogicalPlan literal value");
                }
                TExprNode::TListType args;
                for (const auto id : call.arguments()) {
                    args.push_back(takeScalar(id));
                }
                node = Ctx.NewCallable(pos, name, std::move(args));
                break;
            }
            case P::Expression::kList: {
                const auto& list = expression.list();
                CheckFields(list);
                Y_ENSURE(list.items_size() <= 256, "LogicalPlan expression list limit exceeded");
                TExprNode::TListType items;
                for (const auto id : list.items()) {
                    items.push_back(takeScalar(id));
                }
                node = Ctx.NewList(pos, std::move(items));
                break;
            }
            case P::Expression::kTypeLiteral: {
                const auto id = expression.type_literal();
                Y_ENSURE(id > 0 && id <= Types.size() && expression.has_type_id(), "Invalid LogicalPlan type literal");
                node = ExpandType(pos, *Types[id - 1], Ctx);
                break;
            }
            case P::Expression::kParameter: {
                const auto name = CopyString(expression.parameter());
                const auto* type = Parameters.FindPtr(name);
                Y_ENSURE(type, "Unknown LogicalPlan parameter");
                UsedParameters.insert(name);
                auto expandedType = ExpandType(pos, **type, Ctx);
                expandedType->SetTypeAnn(Ctx.MakeType<TTypeExprType>(*type));
                node = Ctx.NewCallable(pos, "Parameter", {Ctx.NewAtom(pos, name), std::move(expandedType)});
                break;
            }
            case P::Expression::kColumn: {
                const auto& column = expression.column();
                CheckFields(column);
                Y_ENSURE(column.iu_id() > 0 && column.iu_id() <= Props.InfoUnitRegistry.Size(),
                    "Invalid LogicalPlan column IU");
                auto argument = Reference(column.row_argument_expr_id(), P::Expression::kArgument).Node;
                node = Ctx.NewCallable(pos, "Member", {std::move(argument), Ctx.NewAtom(pos, column.iu_id() - 1)});
                break;
            }
            default:
                ythrow yexception() << "Unsupported LogicalPlan expression kind";
        }
        if (expression.has_type_id()) {
            Y_ENSURE(expression.type_id() > 0 && expression.type_id() <= Types.size(), "Invalid LogicalPlan expression annotation");
            const auto* type = Types[expression.type_id() - 1];
            Y_ENSURE((expression.has_atom() && type->GetKind() == ETypeAnnotationKind::Unit)
                || expression.has_column() || expression.has_call() || expression.has_list()
                || expression.has_parameter() || expression.has_type_literal(), "Invalid LogicalPlan expression type");
            if (expression.has_type_literal()) {
                Y_ENSURE(type->GetKind() == ETypeAnnotationKind::Type
                    && IsSameAnnotation(*type->Cast<TTypeExprType>()->GetType(), *Types[expression.type_literal() - 1]),
                    "Invalid LogicalPlan type literal annotation");
            }
            if (expression.has_parameter()) {
                Y_ENSURE(IsSameAnnotation(*type, *Parameters.at(expression.parameter())), "Invalid LogicalPlan parameter annotation");
            }
            node->SetTypeAnn(type);
        }
        Y_ENSURE(depth <= MaxExpressionDepth, "LogicalPlan expression nesting limit exceeded");
        Expressions.push_back({&expression, std::move(node), depth, false});
    }

    TInfoUnitId AvailableUnit(ui32 wireId, IOperator& input) const {
        Y_ENSURE(wireId > 0 && UnitTypes.contains(wireId - 1)
            && input.GetOutputIUs().Contains(wireId - 1), "Unavailable LogicalPlan input IU");
        return wireId - 1;
    }

    const TTypeAnnotationNode* LambdaType(const TExpressionEntry& lambda, IOperator& input) {
        const auto& body = Expressions[lambda.Wire->lambda().body(0) - 1];
        const auto& row = lambda.Node->Head().Head();
        const auto columnType = [&](const TExprNode& member) {
            Y_ENSURE(&member.Head() == &row, "Out-of-scope LogicalPlan lambda argument");
            const auto* type = UnitTypes.at(AvailableUnit(GetMemberId(member) + 1, input));
            CheckExpressionType(member, *type);
            return type;
        };
        const auto& node = *body.Node;
        if (node.IsCallable("Member")) {
            return columnType(node);
        }
        if (node.IsCallable("Parameter")) {
            return Parameters.at(TString(node.Head().Content()));
        }
        Y_ENSURE(node.IsCallable(), "Unsupported LogicalPlan lambda result");
        if (const auto slot = LiteralSlot(node.Content())) {
            const auto* type = Ctx.MakeType<TDataExprType>(*slot);
            CheckExpressionType(node, *type);
            return type;
        }

        return AnnotateScalarLambda(*lambda.Node, Ctx, Ctx, AnnotationContext, UnitTypes, input);
    }

    void Define(ui32 wireId, const TTypeAnnotationNode* type) {
        Y_ENSURE(wireId && wireId <= DefinedTypes.size() && IsSameAnnotation(*DefinedTypes[wireId - 1], *type)
            && UnitTypes.emplace(wireId - 1, type).second, "Invalid LogicalPlan defined IU type or duplicate definition");
    }

    TIntrusivePtr<IOperator> ReadTable(const L::Read& read, TPositionHandle pos) {
        CheckFields(read);
        const auto metadata = Lookup(CopyString(read.table_path()));
        TUnorderedIUs outputs;
        THashSet<TString> names;
        for (const auto& column : read.columns()) {
            CheckFields(column);
            const auto* binding = metadata->Columns.FindPtr(column.column());
            Y_ENSURE(binding && names.insert(column.column()).second, "Unknown or duplicate scan column: " << column.column());
            const TTypeAnnotationNode* type;
            if (binding->TypeInfo.GetTypeId() == NScheme::NTypeIds::Decimal) {
                const auto decimal = binding->TypeInfo.GetDecimalType();
                type = Ctx.MakeType<TDataExprParamsType>(EDataSlot::Decimal, ToString(decimal.GetPrecision()), ToString(decimal.GetScale()));
            } else {
                type = DataType(Ctx, binding->Type);
            }
            Define(column.iu_id(), binding->NotNull ? type : Ctx.MakeType<TOptionalExprType>(type));
            outputs.Add(column.iu_id() - 1);
        }
        auto table = NYql::NNodes::Build<NYql::NNodes::TKqpTable>(Ctx, pos)
            .Path().Build(metadata->Name)
            .PathId().Build(metadata->PathId.ToString())
            .SysView().Build("")
            .Version().Build(ToString(metadata->SchemaVersion))
            .Done().Ptr();
        const auto storage = metadata->StoreType == EStoreType::Column ? NYql::EStorageType::ColumnStorage : NYql::EStorageType::RowStorage;
        return MakeIntrusive<TOpRead>("", std::move(outputs), storage, std::move(table), nullptr, nullptr,
            std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, pos);
    }

    TOperatorEntry TakeOperator(ui32 id) {
        Y_ENSURE(id > 0 && id <= Operators.size() && Operators[id - 1].Node,
            "Invalid, forward or reused LogicalPlan operator reference");
        return std::move(Operators[id - 1]);
    }

    TIntrusivePtr<IOperator> ReadOperator(const L::Operator& op, size_t& depth) {
        CheckFields(op);
        TPositionHandle pos;
        switch (op.node_case()) {
            case L::Operator::kRows: {
                const auto& source = op.rows();
                CheckFields(source);
                Y_ENSURE(!source.has_rows_expr_id() && source.columns().empty(),
                    "Unsupported LogicalPlan empty source");
                return MakeIntrusive<TOpEmptySource>(pos);
            }
            case L::Operator::kRead:
                return ReadTable(op.read(), pos);
            case L::Operator::kMap: {
                const auto& map = op.map();
                CheckFields(map);
                Y_ENSURE(!map.definitions().empty() && static_cast<size_t>(map.definitions_size()) <= Props.InfoUnitRegistry.Size(),
                    "Unsupported LogicalPlan Map");
                auto [input, childDepth] = TakeOperator(map.input());
                depth = childDepth + 1;
                TMapIUs definitions;
                for (const auto& definition : map.definitions()) {
                    CheckFields(definition);
                    const auto id = definition.output_iu_id();
                    Y_ENSURE(id > 0 && id <= Props.InfoUnitRegistry.Size(), "Invalid LogicalPlan map output IU");
                    const auto& lambda = Reference(definition.lambda_expr_id(), P::Expression::kLambda);
                    const auto* type = LambdaType(lambda, *input);
                    Define(id, type);
                    definitions.Add(id - 1, TExpression(lambda.Node, &Ctx));
                }
                return MakeIntrusive<TOpMap>(std::move(input), pos, std::move(definitions));
            }
            case L::Operator::kFilter: {
                const auto& filter = op.filter();
                CheckFields(filter);
                auto [input, childDepth] = TakeOperator(filter.input());
                depth = childDepth + 1;
                const auto& lambda = Reference(filter.predicate_lambda_expr_id(), P::Expression::kLambda);
                const auto& item = OptionalItemType(*LambdaType(lambda, *input));
                Y_ENSURE(item.GetKind() == ETypeAnnotationKind::Data && item.Cast<TDataExprType>()->GetSlot() == EDataSlot::Bool,
                    "Non-boolean LogicalPlan predicate");
                return MakeIntrusive<TOpFilter>(std::move(input), pos, TExpression(lambda.Node, &Ctx));
            }
            case L::Operator::kAggregate: {
                const auto& aggregate = op.aggregate();
                CheckFields(aggregate);
                Y_ENSURE(aggregate.key_iu_ids().empty() && !aggregate.distinct_all() && !aggregate.aggregations().empty()
                    && static_cast<size_t>(aggregate.aggregations_size()) <= Props.InfoUnitRegistry.Size(),
                    "LogicalPlan supports logical COUNT without grouping keys");
                auto [input, childDepth] = TakeOperator(aggregate.input());
                depth = childDepth + 1;
                TAggregationIUs traits;
                const auto* type = Ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
                for (const auto& item : aggregate.aggregations()) {
                    CheckFields(item);
                    const auto id = item.output_iu_id();
                    const auto function = item.function_id();
                    Y_ENSURE(id > 0 && id <= Props.InfoUnitRegistry.Size() && function > 0 && function <= Functions.size()
                        && Functions[function - 1]->kind() == P::Function::AGGREGATE && !item.unwrap(),
                        "Unsupported LogicalPlan aggregate function or output IU");
                    const auto inputId = AvailableUnit(item.input_iu_id(), *input);
                    Y_ENSURE(IsCountInput(*UnitTypes.at(inputId), item.distinct()), "Unsupported LogicalPlan COUNT input");
                    Define(id, type);
                    traits.Add(id - 1, TOpAggregationTraits{inputId, "count", item.distinct()});
                }
                return MakeIntrusive<TOpAggregate>(std::move(input), std::move(traits), TOrderedIUs<>{},
                    EOpPhase::Undefined, false, pos);
            }
            case L::Operator::kJoin: {
                const auto& join = op.join();
                CheckFields(join);
                auto kind = ReadJoinKind(join.kind());
                Y_ENSURE(join.filter_lambda_expr_ids().empty() && static_cast<size_t>(join.keys_size()) <= MaxNodes
                    && ((join.kind() == P::Join::CROSS) == join.keys().empty()),
                    "LogicalPlan JOIN requires ordinary equi-keys or a CROSS join");
                auto [left, leftDepth] = TakeOperator(join.left());
                auto [right, rightDepth] = TakeOperator(join.right());
                depth = std::max(leftDepth, rightDepth) + 1;
                Y_ENSURE(!left->GetOutputIUs().HasAny(right->GetOutputIUs()), "Overlapping LogicalPlan JOIN input bindings");
                TJoinIUs keys;
                for (const auto& key : join.keys()) {
                    CheckFields(key);
                    const auto leftId = AvailableUnit(key.left_iu_id(), *left);
                    const auto rightId = AvailableUnit(key.right_iu_id(), *right);
                    Y_ENSURE(!key.equal_nulls() && IsSupportedJoinKeyType(*UnitTypes.at(leftId), *UnitTypes.at(rightId)),
                        "LogicalPlan JOIN keys require ordinary equality and matching comparable data item types");
                    Y_ENSURE(keys.Add(leftId, rightId), "Duplicate LogicalPlan JOIN key");
                }
                if (join.kind() == P::Join::LEFT) {
                    NullExtend(Ctx, UnitTypes, right->GetOutputIUs());
                }
                return MakeIntrusive<TOpJoin>(std::move(left), std::move(right), pos, std::move(kind), std::move(keys));
            }
            case L::Operator::kReplicatePort: {
                const auto& port = op.replicate_port();
                CheckFields(port);
                const auto id = port.hub_id();
                Y_ENSURE(port.port_index() == 0 && port.columns().empty() && id > 0 && id <= ReplicateHubs.size()
                    && ReplicateHubs[id - 1], "LogicalPlan supports a single primary port per Replicate hub");
                const auto* hub = ReplicateHubs[id - 1];
                ReplicateHubs[id - 1] = nullptr;
                auto [input, childDepth] = TakeOperator(hub->input_operator_id());
                depth = childDepth + 1;
                return TReplicate::Create(std::move(input), pos, Props.InfoUnitRegistry)->AddOutput();
            }
            default:
                ythrow yexception() << "Unsupported LogicalPlan operator kind";
        }
    }

    TExprContext& Ctx;
    const TVector<const TTypeAnnotationNode*>& Types;
    const TVector<const P::Function*>& Functions;
    const TParameterTypes& Parameters;
    THashSet<TString>& UsedParameters;
    const TCatalogLookup& Lookup;
    TVector<const TTypeAnnotationNode*> DefinedTypes;
    TPlanProps Props;
    TVector<TExpressionEntry> Expressions;
    TVector<TOperatorEntry> Operators;
    TVector<const P::ReplicateHub*> ReplicateHubs;
    THashSet<ui32> OwnedArguments;
    THashSet<ui32> OwnedArgumentLists;
    TUnitTypes UnitTypes;
    TTypeAnnotationContextPtr AnnotationContext;
};

} // namespace

TImportedPlan ImportLogicalPlan(const L& query, TExprContext& ctx, const TCatalogLookup& lookup,
    const TParameterMap& parameters)
{
    CheckFields(query);
    Y_ENSURE(query.version() == 1 && query.required_features().empty(), "Unsupported LogicalPlan version or features");
    Y_ENSURE(query.roots_size() && static_cast<size_t>(query.roots_size()) <= MaxNodes, "Invalid LogicalPlan root count");
    TVector<const TTypeAnnotationNode*> types;
    TVector<const P::Function*> functions;
    TVector<size_t> typeDepths;
    Y_ENSURE(static_cast<size_t>(query.types_size()) <= MaxNodes && query.functions_size() <= 64,
        "Unsupported LogicalPlan function or type registry size");
    for (const auto& type : query.types()) {
        CheckFields(type);
        Y_ENSURE(type.id() == types.size() + 1, "Invalid LogicalPlan type ID");
        size_t depth = 1;
        const auto item = [&](ui32 id) {
            Y_ENSURE(id > 0 && id <= types.size(), "Invalid or forward LogicalPlan type reference");
            depth = std::max(depth, 1 + typeDepths[id - 1]);
            Y_ENSURE(depth <= MaxExpressionDepth, "LogicalPlan type nesting limit exceeded");
            return types[id - 1];
        };
        if (type.has_unit()) {
            CheckFields(type.unit());
            types.push_back(ctx.MakeType<TUnitExprType>());
        } else if (type.has_null_type()) {
            CheckFields(type.null_type());
            types.push_back(ctx.MakeType<TNullExprType>());
        } else if (type.has_optional_item()) {
            types.push_back(ctx.MakeType<TOptionalExprType>(item(type.optional_item())));
        } else if (type.has_list_item()) {
            types.push_back(ctx.MakeType<TListExprType>(item(type.list_item())));
        } else if (type.has_type_of()) {
            types.push_back(ctx.MakeType<TTypeExprType>(item(type.type_of())));
        } else if (type.has_tuple()) {
            CheckFields(type.tuple());
            Y_ENSURE(type.tuple().type_ids_size() <= 256, "LogicalPlan tuple size limit exceeded");
            TTypeAnnotationNode::TListType items;
            for (const auto id : type.tuple().type_ids()) {
                items.push_back(item(id));
            }
            types.push_back(ctx.MakeType<TTupleExprType>(items));
        } else {
            Y_ENSURE(type.has_data(), "Unsupported LogicalPlan type");
            const auto& data = type.data();
            CheckFields(data);
            Y_ENSURE(data.parameters_size() == (data.slot() == "Decimal" ? 2 : 0), "Unsupported LogicalPlan data type parameters");
            types.push_back(data.parameters_size() ? DataType(ctx, data.slot(), data.parameters(0), data.parameters(1))
                : DataType(ctx, data.slot()));
        }
        typeDepths.push_back(depth);
    }
    for (const auto& function : query.functions()) {
        CheckFields(function);
        const bool supported = (function.kind() == P::Function::YQL_CALLABLE && SupportedScalar(function.name()))
            || (function.kind() == P::Function::AGGREGATE && function.name() == "count");
        Y_ENSURE(function.id() == functions.size() + 1 && supported
            && function.module().empty() && function.implementation_version().empty() && !function.has_signature_type_id(),
            "Unsupported LogicalPlan function");
        Y_ENSURE(std::ranges::none_of(functions, [&](const auto* known) {
            return known->kind() == function.kind() && known->name() == function.name();
        }), "Duplicate LogicalPlan function");
        functions.push_back(&function);
    }

    TParameterTypes boundParameters;
    for (const auto& parameter : query.parameters()) {
        CheckFields(parameter);
        const auto name = CopyString(parameter.name());
        const auto id = parameter.type_id();
        Y_ENSURE(name.StartsWith('$') && id && id <= types.size(), "Invalid LogicalPlan parameter declaration");
        auto it = parameters.find(name);
        if (it == parameters.end()) {
            it = parameters.find(name.substr(1));
        }
        Y_ENSURE(it != parameters.end() && it->second.has_type(), "Missing LogicalPlan parameter: " << name);
        const auto* type = ParseTypeFromYdbType(it->second.type(), ctx);
        Y_ENSURE(type && IsSameAnnotation(*type, *types[id - 1]) && boundParameters.emplace(name, type).second,
            "LogicalPlan parameter type differs from request");
    }
    THashSet<TString> usedParameters;
    TImportedPlan output;
    for (const auto& root : query.roots()) {
        CheckFields(root);
        const auto position = static_cast<ui32>(output.Roots.size());
        Y_ENSURE(root.has_result_index() && root.result_index() == position && root.id() == position + 1
            && root.has_graph() && root.columns_size() && !root.has_returning_limit_expr_id()
            && root.has_statement_index() && (!position || query.roots(position - 1).statement_index() < root.statement_index()),
            "LogicalPlan expects read-only result roots in statement order");
        Y_ENSURE(!root.ordered_rows(), "Ordered LogicalPlan results require a supported ordering operator");
        output.Roots.push_back(TPlanReader(ctx, types, functions, boundParameters, usedParameters, lookup)
            .Read(root, position, output.ColumnTypes.emplace_back()));
    }
    Y_ENSURE(usedParameters.size() == boundParameters.size(), "Unused LogicalPlan parameter declaration");
    return output;
}

} // namespace NKikimr::NKqp::NParsedQueryBench
