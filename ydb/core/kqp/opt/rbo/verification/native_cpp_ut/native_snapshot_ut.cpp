#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_test_helpers.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <ydb/core/kqp/opt/rbo/verification/semantic_snapshot.h>
#include <ydb/core/kqp/opt/rbo/verification/read_range_diagnostics_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/scheme_types/scheme_decimal_type.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/time_provider.h>

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/shellcommand.h>

#include <limits>

namespace NKikimr::NKqp {
namespace {

using namespace NYql;

struct TFixture {
    explicit TFixture(TString groupType = "String", bool groupNotNull = false, TString otherType = "Int32",
        ui8 pricePrecision = 15)
        : Functions(NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry()))
        , Config(MakeIntrusive<TKikimrConfiguration>())
        , Query(MakeIntrusive<TKikimrQueryContext>(
              Functions.Get(), CreateDefaultTimeProvider(), CreateDefaultRandomProvider()))
        , Tables(MakeIntrusive<TKikimrTablesData>())
        , Request(MakeIntrusive<TUserRequestContext>())
        , Optimize("ut", Config, Query, Tables, Request)
        , Rbo(Optimize, Expr, Types, TypeAnnotation, *Functions)
    {
        auto& table = Tables->GetOrAddTable("ut", "/Root", "/Root/NativeSnapshot");
        table.Metadata = MakeIntrusive<TKikimrTableMetadata>("ut", "/Root/NativeSnapshot");
        table.Metadata->DoesExist = true;
        table.Metadata->PathId = TKikimrPathId(1, 1);
        table.Metadata->SchemaVersion = 1;
        table.Metadata->Kind = EKikimrTableKind::Datashard;
        table.Metadata->KeyColumnNames = {"k"};
        table.Metadata->Columns.emplace("k", TKikimrColumnMetadata("k", 1, "Int32", true));
        table.Metadata->Columns.emplace("x", TKikimrColumnMetadata("x", 2, otherType, true));
        table.Metadata->Columns.emplace("g", TKikimrColumnMetadata("g", 3, groupType, groupNotNull));
        TKikimrColumnMetadata price("price", 4, "Decimal", false);
        price.TypeInfo = NScheme::TTypeInfo(NScheme::TDecimalType(pricePrecision, 2));
        table.Metadata->Columns.emplace("price", std::move(price));
        table.Metadata->ColumnOrder = {"k", "x", "g", "price"};
        UNIT_ASSERT(table.Load(Expr));
        Table = &table;
        Key = Registry.Add(TInfoUnit("same", "k"));
        Other = Registry.Add(TInfoUnit("same", "x"));
        OtherType = Expr.MakeType<TDataExprType>(*NUdf::FindDataSlot(otherType));
        Group = Registry.Add(TInfoUnit("same", "g"));
        Price = Registry.Add(TInfoUnit("same", "price"));
    }

    TIntrusivePtr<TOpRead> Read() {
        auto read = MakeIntrusive<TOpRead>(
            "same", TUnorderedIUs{Key, Other}, NYql::EStorageType::RowStorage,
            NOpt::BuildTableMeta(*Table, TPositionHandle(), Expr).Ptr(),
            nullptr, nullptr, std::nullopt, std::nullopt, ESortDir::None,
            TPhysicalOpProps{}, TPositionHandle());
        const auto* int32 = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
        SetType(*read, {{Key, int32}, {Other, OtherType}});
        return read;
    }

    void SetType(IOperator& op, const TVector<std::pair<TInfoUnitId, const TTypeAnnotationNode*>>& columns) {
        TVector<const TItemExprType*> items;
        for (const auto& [id, type] : columns) {
            items.push_back(Expr.MakeType<TItemExprType>(ToString(id), type));
        }
        op.Type = Expr.MakeType<TListExprType>(Expr.MakeType<TStructExprType>(std::move(items)));
    }

    TSemanticSnapshotExportResult Export(TOpRoot& root) {
        root.PlanProps.InfoUnitRegistry = Registry;
        return ExportSemanticSnapshotV1(root, Rbo);
    }

    TIntrusivePtr<TOpWindow> Window(const TString& function, bool wholePartition, const TString& partitionType = "String") {
        const auto* decimal = Expr.MakeType<TOptionalExprType>(
            Expr.MakeType<TDataExprParamsType>(NUdf::EDataSlot::Decimal, "35", "2"));
        // Leave accumulator headroom at the two-row proof bound. Full-width
        // Decimal sources intentionally fail the model's overflow admission.
        const auto* priceType = Expr.MakeType<TOptionalExprType>(
            Expr.MakeType<TDataExprParamsType>(NUdf::EDataSlot::Decimal, "15", "2"));
        const auto* groupType = Expr.MakeType<TOptionalExprType>(
            Expr.MakeType<TDataExprType>(*NUdf::FindDataSlot(partitionType)));
        auto read = MakeIntrusive<TOpRead>(
            "same", TUnorderedIUs{Key, Group, Price}, NYql::EStorageType::RowStorage,
            NOpt::BuildTableMeta(*Table, TPositionHandle(), Expr).Ptr(),
            nullptr, nullptr, std::nullopt, std::nullopt, ESortDir::None,
            TPhysicalOpProps{}, TPositionHandle());
        SetType(*read, {{Key, Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32)},
            {Group, groupType}, {Price, priceType}});
        Sum = Registry.Add(TInfoUnit("", "total"));
        WindowResult = Registry.Add(TInfoUnit("", "total"));
        auto aggregate = MakeIntrusive<TOpAggregate>(read,
            TAggregationIUs{{Sum, {Price, "sum", false, false}}},
            TOrderedIUs<>{Group, Key}, EOpPhase::Undefined, false, TPositionHandle());
        SetType(*aggregate, {{Key, Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32)},
            {Group, groupType}, {Sum, decimal}});
        TOpWindowFrame frame;
        frame.EndKind = wholePartition ? EWindowFrameBound::UnboundedFollowing : EWindowFrameBound::CurrentRow;
        auto window = MakeIntrusive<TOpWindow>(aggregate, TPositionHandle(),
            TWindowIUs{{WindowResult, {function, EWindowFuncKind::Aggregate, TOrderedIUs<>{Sum}}}},
            TOrderedIUs<>{Group}, TSortIUs{}, frame);
        SetType(*window, {{Key, Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32)},
            {Group, groupType}, {Sum, decimal}, {WindowResult, decimal}});
        return window;
    }

    TIntrusivePtr<TOpMap> StringConcat(bool copyColumn, TInfoUnitId& result,
        TString textType = "String", bool twoMembers = false, bool mixedLiteral = false, bool olap = false) {
        const auto pos = TPositionHandle();
        const auto* int32 = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
        const auto* string = Expr.MakeType<TDataExprType>(*NUdf::FindDataSlot(textType));
        const auto* resultType = mixedLiteral ? Expr.MakeType<TDataExprType>(NUdf::EDataSlot::String) : string;
        const auto* optionalString = Expr.MakeType<TOptionalExprType>(string);
        auto read = Read();
        read->GetColumns() = TUnorderedIUs{Key, Group};
        if (olap) {
            Table->Metadata->Kind = EKikimrTableKind::Olap;
            read->StorageType = NYql::EStorageType::ColumnStorage;
        }
        if (twoMembers) {
            read->GetColumns().Add(Other);
        }
        TVector<std::pair<TInfoUnitId, const TTypeAnnotationNode*>> types{{Key, int32}, {Group, optionalString}};
        if (twoMembers) { types.emplace_back(Other, string); }
        SetType(*read, types);
        TIntrusivePtr<IOperator> input = read;
        auto source = Group;
        if (copyColumn) {
            source = Registry.Add(TInfoUnit("", "copy"));
            auto access = MakeColumnAccess(Group, pos, &Expr, &ExpressionProps);
            access.GetExpressionBody()->SetTypeAnn(optionalString);
            auto copy = MakeIntrusive<TOpMap>(input, pos, TMapIUs{{source, TMapElement(access)}});
            types.emplace_back(source, optionalString);
            SetType(*copy, types);
            input = copy;
        }
        auto row = Expr.NewArgument(pos, "row");
        row->SetTypeAnn(input->Type->Cast<TListExprType>()->GetItemType());
        auto member = Expr.NewCallable(pos, "Member", {row, Expr.NewAtom(pos, ToString(source))});
        member->SetTypeAnn(optionalString);
        const auto literal = [&](TStringBuf value) {
            auto node = Expr.NewCallable(pos, textType, {Expr.NewAtom(pos, value)});
            node->SetTypeAnn(string);
            return node;
        };
        auto coalesce = Expr.NewCallable(pos, "Coalesce", {member, literal("")});
        coalesce->SetTypeAnn(string);
        auto separator = literal(", ");
        if (mixedLiteral) {
            separator = Expr.NewCallable(pos, "String", {Expr.NewAtom(pos, ", ")});
            separator->SetTypeAnn(resultType);
        }
        auto concat = Expr.NewCallable(pos, "Concat", {coalesce, separator});
        concat->SetTypeAnn(resultType);
        if (twoMembers) {
            auto other = Expr.NewCallable(pos, "Member", {row, Expr.NewAtom(pos, ToString(Other))});
            other->SetTypeAnn(string);
            concat = Expr.NewCallable(pos, "Concat", {concat, other});
            concat->SetTypeAnn(resultType);
        }
        auto lambda = Expr.NewLambda(pos, Expr.NewArguments(pos, {row}), std::move(concat));
        result = Registry.Add(TInfoUnit("", "answer"));
        auto map = MakeIntrusive<TOpMap>(input, pos, TMapIUs{{result,
            TMapElement(TExpression(lambda, &Expr, &ExpressionProps))}});
        types.emplace_back(result, resultType);
        SetType(*map, types);
        return map;
    }

    void StageSerial(TOpRoot& root) {
        TVector<IOperator*> compute;
        auto* input = root.GetInput().Get();
        while (input->GetKind() != EOperator::Source) {
            UNIT_ASSERT_VALUES_EQUAL(input->GetChildCount(), 1);
            compute.push_back(input);
            input = input->GetChild(0).Get();
        }
        UNIT_ASSERT(!compute.empty());
        auto& graph = root.PlanProps.StageGraph;
        const auto source = graph.AddSourceStage(static_cast<TOpRead*>(input)->StorageType);
        const auto sink = graph.AddStage();
        input->Props.StageId = source;
        for (auto* op : compute) {
            op->Props.StageId = sink;
        }
        graph.Connect(source, sink, MakeIntrusive<TUnionAllConnection>(graph.GetOutputIndex(source)));
    }

    TSemanticSnapshotExportResult CorrelatedExists(
        TInfoUnitId captured, bool wrongType = false, bool withResidual = false)
    {
        const auto pos = TPositionHandle();
        const auto* int32 = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
        const auto* boolean = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
        const auto innerKey = Registry.Add(TInfoUnit("inner", "k"));
        const auto local = Registry.Add(TInfoUnit("", "capture"));
        const auto binding = Registry.Add(TInfoUnit("", "exists"));
        auto inner = MakeIntrusive<TOpRead>("inner", TUnorderedIUs{innerKey},
            NYql::EStorageType::RowStorage,
            NOpt::BuildTableMeta(*Table, pos, Expr).Ptr(), nullptr, nullptr,
            std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, pos);
        SetType(*inner, {{innerKey, int32}});
        auto capture = MakeIntrusive<TOpAddDependencies>(inner, pos,
            TDependencyIUs{{local, {captured, wrongType ? boolean : int32}}});
        SetType(*capture, {{innerKey, int32}, {local, int32}});
        auto row = Expr.NewArgument(pos, "row");
        row->SetTypeAnn(capture->Type->Cast<TListExprType>()->GetItemType());
        auto innerMember = Expr.NewCallable(pos, "Member", {row, Expr.NewAtom(pos, ToString(innerKey))});
        auto localMember = Expr.NewCallable(pos, "Member", {row, Expr.NewAtom(pos, ToString(local))});
        innerMember->SetTypeAnn(int32);
        localMember->SetTypeAnn(int32);
        auto equals = Expr.NewCallable(pos, "==", {innerMember, localMember});
        equals->SetTypeAnn(boolean);
        const auto typedComparison = equals;
        if (withResidual) {
            auto residual = Expr.NewCallable(pos, "Bool", {Expr.NewAtom(pos, "true")});
            residual->SetTypeAnn(boolean);
            equals = Expr.NewCallable(pos, "And", {equals, residual});
            equals->SetTypeAnn(boolean);
        }
        auto predicate = Expr.NewLambda(pos, Expr.NewArguments(pos, {row}), std::move(equals));
        auto filtered = MakeIntrusive<TOpFilter>(capture, pos,
            TExpression(predicate, &Expr, &ExpressionProps));
        SetType(*filtered, {{innerKey, int32}, {local, int32}});
        auto outer = Read();
        auto call = MakeColumnAccess(binding, pos, &Expr, &ExpressionProps);
        call.GetExpressionBody()->SetTypeAnn(boolean);
        auto consumer = MakeIntrusive<TOpFilter>(outer, pos, call);
        SetType(*consumer, {{Key, int32}, {Other, int32}});
        TOpRoot root(consumer, pos, {{Key, "key"}});
        root.PlanProps.Subplans.Add(binding, filtered, ESubplanType::EXISTS);
        root.PlanProps.Subplans.RefreshDependencies(binding);
        filtered->BindExpressionPlanProps(&root.PlanProps);
        consumer->BindExpressionPlanProps(&root.PlanProps);
        auto exported = Export(root);
        UNIT_ASSERT_VALUES_EQUAL(localMember->Child(1)->Content(), ToString(local));
        UNIT_ASSERT(typedComparison->GetTypeAnn() == boolean);
        UNIT_ASSERT(localMember->GetTypeAnn() == int32);
        return exported;
    }

    TSemanticSnapshotExportResult CorrelatedScalarCount(
        TInfoUnitId captured, bool passiveMap = false, bool wrongType = false,
        bool aggregateCapture = false, bool pushedToOlap = false)
    {
        const auto pos = TPositionHandle();
        const auto* int32 = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
        const auto* uint64 = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64);
        const auto* optionalUint64 = Expr.MakeType<TOptionalExprType>(uint64);
        const auto* boolean = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
        const auto innerKey = Registry.Add(TInfoUnit("inner", "k"));
        const auto local = Registry.Add(TInfoUnit("", "capture"));
        const auto count = Registry.Add(TInfoUnit("", "count"));
        const auto binding = Registry.Add(TInfoUnit("", "scalar"));
        const auto result = Registry.Add(TInfoUnit("", "result"));
        auto inner = MakeIntrusive<TOpRead>("inner", TUnorderedIUs{innerKey},
            NYql::EStorageType::RowStorage,
            NOpt::BuildTableMeta(*Table, pos, Expr).Ptr(), nullptr, nullptr,
            std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, pos);
        SetType(*inner, {{innerKey, int32}});
        auto capture = MakeIntrusive<TOpAddDependencies>(inner, pos,
            TDependencyIUs{{local, {captured, wrongType ? boolean : int32}}});
        SetType(*capture, {{innerKey, int32}, {local, int32}});
        auto row = Expr.NewArgument(pos, "row");
        row->SetTypeAnn(capture->Type->Cast<TListExprType>()->GetItemType());
        auto innerMember = Expr.NewCallable(pos, "Member", {row, Expr.NewAtom(pos, ToString(innerKey))});
        auto localMember = Expr.NewCallable(pos, "Member", {row, Expr.NewAtom(pos, ToString(local))});
        innerMember->SetTypeAnn(int32);
        localMember->SetTypeAnn(int32);
        auto equals = Expr.NewCallable(pos, "==", {innerMember, localMember});
        equals->SetTypeAnn(boolean);
        auto predicate = Expr.NewLambda(pos, Expr.NewArguments(pos, {row}), std::move(equals));
        auto filtered = MakeIntrusive<TOpFilter>(capture, pos,
            TExpression(predicate, &Expr, &ExpressionProps));
        SetType(*filtered, {{innerKey, int32}, {local, int32}});
        TIntrusivePtr<IOperator> aggregateInput = filtered;
        auto argument = aggregateCapture ? local : innerKey;
        if (passiveMap) {
            const auto copy = Registry.Add(TInfoUnit("", "copy"));
            auto access = MakeColumnAccess(argument, pos, &Expr, &ExpressionProps);
            access.GetExpressionBody()->SetTypeAnn(int32);
            auto map = MakeIntrusive<TOpMap>(filtered, pos, TMapIUs{{copy, TMapElement(access)}});
            SetType(*map, {{innerKey, int32}, {local, int32}, {copy, int32}});
            argument = copy;
            aggregateInput = map;
        }
        auto aggregate = MakeIntrusive<TOpAggregate>(aggregateInput,
            TAggregationIUs{{count, {argument, "count", false, false}}},
            TOrderedIUs<>{}, EOpPhase::Undefined, false, pos);
        if (pushedToOlap) {
            aggregate->SetPushedToOlap();
        }
        SetType(*aggregate, {{count, uint64}});
        auto outer = Read();
        auto call = MakeColumnAccess(binding, pos, &Expr, &ExpressionProps);
        call.GetExpressionBody()->SetTypeAnn(optionalUint64);
        auto consumer = MakeIntrusive<TOpMap>(outer, pos, TMapIUs{{result, TMapElement(call)}});
        SetType(*consumer, {{Key, int32}, {Other, int32}, {result, optionalUint64}});
        TOpRoot root(consumer, pos, {{Key, "key"}, {result, "count"}});
        root.PlanProps.Subplans.Add(binding, aggregate, ESubplanType::EXPR, {}, count);
        root.PlanProps.Subplans.RefreshDependencies(binding);
        aggregate->BindExpressionPlanProps(&root.PlanProps);
        consumer->BindExpressionPlanProps(&root.PlanProps);
        auto exported = Export(root);
        UNIT_ASSERT_VALUES_EQUAL(localMember->Child(1)->Content(), ToString(local));
        UNIT_ASSERT(localMember->GetTypeAnn() == int32);
        UNIT_ASSERT_VALUES_EQUAL(capture->GetDependencies().At(local).Outer, captured);
        return exported;
    }

    TSemanticSnapshotExportResult NullableKeyJoin(const TString& scalarType, const TString& kind,
                                                  bool equalNulls, bool staged) {
        const auto pos = TPositionHandle();
        const auto* type = Expr.MakeType<TOptionalExprType>(
            Expr.MakeType<TDataExprType>(*NUdf::FindDataSlot(scalarType)));
        const auto rightGroup = Registry.Add(TInfoUnit("right", "g"));
        const auto makeRead = [&](TString alias, TInfoUnitId id) {
            auto read = MakeIntrusive<TOpRead>(std::move(alias), TUnorderedIUs{id}, NYql::EStorageType::RowStorage,
                NOpt::BuildTableMeta(*Table, pos, Expr).Ptr(), nullptr, nullptr,
                std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, pos);
            SetType(*read, {{id, type}});
            return read;
        };
        auto left = makeRead("same", Group);
        auto right = makeRead("right", rightGroup);
        TJoinIUs keys;
        keys.Add({Group, rightGroup, equalNulls});
        auto join = MakeIntrusive<TOpJoin>(left, right, pos, kind, keys);
        TOrderedIUs<TString> output;
        TVector<std::pair<TInfoUnitId, const TTypeAnnotationNode*>> types;
        if (kind != "RightSemi" && kind != "RightOnly") {
            output.Append(Group, "left");
            types.emplace_back(Group, type);
        }
        if (kind != "LeftSemi" && kind != "LeftOnly") {
            output.Append(rightGroup, "right");
            types.emplace_back(rightGroup, type);
        }
        SetType(*join, types);
        TOpRoot root(join, pos, std::move(output));
        if (staged) {
            auto& graph = root.PlanProps.StageGraph;
            const auto leftStage = graph.AddSourceStage(NYql::EStorageType::RowStorage);
            const auto rightStage = graph.AddSourceStage(NYql::EStorageType::RowStorage);
            const auto joinStage = graph.AddStage();
            left->Props.StageId = leftStage;
            right->Props.StageId = rightStage;
            join->Props.StageId = joinStage;
            root.ComputeParents();
            graph.Connect(leftStage, joinStage, MakeIntrusive<TUnionAllConnection>(graph.GetOutputIndex(leftStage)));
            graph.Connect(rightStage, joinStage, MakeIntrusive<TUnionAllConnection>(graph.GetOutputIndex(rightStage)));
        }
        return Export(root);
    }

    TSemanticSnapshotExportResult SharedReadJoin(
        bool shared, bool staged, bool changeJoinKey = false, bool wrongType = false,
        bool wrongPortIndex = false, bool sparsePorts = false, bool computeProducer = false)
    {
        const auto pos = TPositionHandle();
        const auto* int32 = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
        auto read = Read();
        TIntrusivePtr<IOperator> producer = read;
        if (computeProducer) {
            UNIT_ASSERT(shared);
            auto predicate = MakeConstant("Bool", "true", pos, &Expr);
            predicate.GetExpressionBody()->SetTypeAnn(Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Bool));
            producer = MakeIntrusive<TOpFilter>(read, pos, predicate);
            producer->Type = read->Type;
        }
        auto hub = TReplicate::Create(producer, pos, Registry);
        auto primary = hub->AddOutput();
        // A removed intermediate port leaves stable local ordinals with a gap;
        // physical output indices are compacted over the remaining live ports.
        auto unused = sparsePorts ? hub->AddOutput() : TIntrusivePtr<TOpReplicate>{};
        auto secondary = hub->AddOutput();
        const auto rightKey = secondary->GetRebindings().At(Key);
        const auto rightOther = secondary->GetRebindings().At(Other);
        SetType(*secondary, {{rightKey, int32}, {rightOther, wrongType
            ? Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Bool) : int32}});
        TIntrusivePtr<IOperator> left = primary;
        TIntrusivePtr<IOperator> right = secondary;
        if (!shared) {
            left = read;
            right = MakeIntrusive<TOpRead>("same", TUnorderedIUs{rightKey, rightOther},
                NYql::EStorageType::RowStorage,
                NOpt::BuildTableMeta(*Table, pos, Expr).Ptr(), nullptr, nullptr,
                std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, pos);
            SetType(*right, {{rightKey, int32}, {rightOther, int32}});
        }
        TJoinIUs keys;
        keys.Add(Key, changeJoinKey ? rightOther : rightKey);
        auto join = MakeIntrusive<TOpJoin>(left, right, pos, "Inner", keys);
        SetType(*join, {{Key, int32}, {Other, int32}, {rightKey, int32}, {rightOther, int32}});
        TOpRoot root(join, pos, {{Key, "key"}, {Other, "left"}, {rightOther, "right"}});
        if (staged) {
            auto& graph = root.PlanProps.StageGraph;
            const auto sourceStage = graph.AddSourceStage(NYql::EStorageType::RowStorage);
            const auto leftStage = computeProducer ? graph.AddStage() : sourceStage;
            const auto rightStage = shared ? leftStage : graph.AddSourceStage(NYql::EStorageType::RowStorage);
            const auto joinedStage = graph.AddStage();
            read->Props.StageId = sourceStage;
            producer->Props.StageId = leftStage;
            if (computeProducer) {
                graph.Connect(sourceStage, leftStage, MakeIntrusive<TUnionAllConnection>(graph.GetOutputIndex(sourceStage)));
            }
            left->Props.StageId = leftStage;
            right->Props.StageId = rightStage;
            join->Props.StageId = joinedStage;
            root.ComputeParents();
            const ui32 leftOutput = shared ? *GetReplicateOutputIndex(*primary) : graph.GetOutputIndex(leftStage);
            const ui32 rightOutput = shared ? *GetReplicateOutputIndex(*secondary) : graph.GetOutputIndex(rightStage);
            graph.Connect(leftStage, joinedStage, MakeIntrusive<TUnionAllConnection>(wrongPortIndex ? rightOutput : leftOutput));
            graph.Connect(rightStage, joinedStage, MakeIntrusive<TUnionAllConnection>(rightOutput));
        }
        return Export(root);
    }

    TSemanticSnapshotExportResult GuardedKeyCount(
        bool guarded, bool staged, bool uniqueKey = true, TString threshold = "1")
    {
        const auto pos = TPositionHandle();
        const auto* int32 = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
        const auto* uint64 = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64);
        const auto* boolean = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
        const auto* string = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::String);
        if (!uniqueKey) {
            Table->Metadata->KeyColumnNames.clear();
        }
        auto read = Read();
        const auto count = Registry.Add(TInfoUnit("", "count"));
        const auto checked = Registry.Add(TInfoUnit("", "checked"));
        auto aggregate = MakeIntrusive<TOpAggregate>(read,
            TAggregationIUs{{count, {Other, "count", false, false}}},
            TOrderedIUs<>{Key}, EOpPhase::Undefined, false, pos);
        SetType(*aggregate, {{Key, int32}, {count, uint64}});
        TIntrusivePtr<IOperator> input = aggregate;
        auto result = count;
        if (guarded) {
            auto row = Expr.NewArgument(pos, "row");
            row->SetTypeAnn(aggregate->Type->Cast<TListExprType>()->GetItemType());
            auto value = Expr.NewCallable(pos, "Member", {row, Expr.NewAtom(pos, ToString(count))});
            value->SetTypeAnn(uint64);
            auto one = Expr.NewCallable(pos, "Uint64", {Expr.NewAtom(pos, threshold)});
            one->SetTypeAnn(uint64);
            auto predicate = Expr.NewCallable(pos, "<=", {value, one});
            predicate->SetTypeAnn(boolean);
            auto message = Expr.NewCallable(pos, "String", {Expr.NewAtom(pos, "Scalar subquery returned more than one row")});
            message->SetTypeAnn(string);
            auto ensure = Expr.NewCallable(pos, "Ensure", {value, predicate, message});
            ensure->SetTypeAnn(uint64);
            auto lambda = Expr.NewLambda(pos, Expr.NewArguments(pos, {row}), std::move(ensure));
            auto map = MakeIntrusive<TOpMap>(aggregate, pos, TMapIUs{{checked,
                TMapElement(TExpression(lambda, &Expr, &ExpressionProps))}});
            SetType(*map, {{Key, int32}, {count, uint64}, {checked, uint64}});
            input = map;
            result = checked;
        }
        TOpRoot root(input, pos, {{Key, "key"}, {result, "count"}});
        if (staged) {
            StageSerial(root);
        }
        return Export(root);
    }

    TSemanticSnapshotExportResult KeyOnlyAggregate(bool staged, bool extraKey = false, bool distinctAll = false,
        bool pushedToOlap = false) {
        const auto pos = TPositionHandle();
        const auto* int32 = Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
        const auto* groupType = Expr.MakeType<TOptionalExprType>(
            Expr.MakeType<TDataExprType>(NUdf::EDataSlot::String));
        auto read = Read();
        read->GetColumns() = TUnorderedIUs{Key, Group};
        SetType(*read, {{Key, int32}, {Group, groupType}});
        TOrderedIUs<> keys{Group};
        TVector<std::pair<TInfoUnitId, const TTypeAnnotationNode*>> columns{{Group, groupType}};
        if (extraKey) {
            keys.Append(Key);
            columns.emplace_back(Key, int32);
        }
        auto aggregate = MakeIntrusive<TOpAggregate>(read, TAggregationIUs{}, keys,
            staged ? EOpPhase::Intermediate : EOpPhase::Undefined, distinctAll, pos);
        if (pushedToOlap) {
            aggregate->SetPushedToOlap();
        }
        SetType(*aggregate, columns);
        TIntrusivePtr<IOperator> result = aggregate;
        if (staged) {
            auto final = MakeIntrusive<TOpAggregate>(aggregate, TAggregationIUs{}, keys,
                EOpPhase::Final, distinctAll, pos);
            SetType(*final, columns);
            result = final;
        }
        TOpRoot root(result, pos, {{Group, "group"}});
        if (staged) {
            StageSerial(root);
        }
        return Export(root);
    }

    TExprContext Expr;
    TTypeAnnotationContext Types;
    TNullTransformer TypeAnnotation;
    TIntrusivePtr<NMiniKQL::IFunctionRegistry> Functions;
    TKikimrConfiguration::TPtr Config;
    TIntrusivePtr<TKikimrQueryContext> Query;
    TIntrusivePtr<TKikimrTablesData> Tables;
    TIntrusivePtr<TUserRequestContext> Request;
    NOpt::TKqpOptimizeContext Optimize;
    TRBOContext Rbo;
    TInfoUnitRegistry Registry;
    TPlanProps ExpressionProps;
    TKikimrTableDescription* Table = nullptr;
    TInfoUnitId Key;
    TInfoUnitId Other;
    const TTypeAnnotationNode* OtherType = nullptr;
    TInfoUnitId Group;
    TInfoUnitId Price;
    TInfoUnitId Sum = TUnorderedIUs::InvalidBit;
    TInfoUnitId WindowResult = TUnorderedIUs::InvalidBit;
};

NJson::TJsonValue Parse(const TSemanticSnapshotExportResult& exported) {
    UNIT_ASSERT_C(exported.IsSupported(), exported.UnsupportedReason);
    NJson::TJsonValue result;
    UNIT_ASSERT(NJson::ReadJsonTree(exported.Json, &result, true));
    return result;
}

TString Verify(const TString& before, const TString& after, bool diagnosticPrefix = false) {
    auto directory = TTempDir::NewTempDir(GetOutputPath().GetPath());
    directory.DoNotRemove();
    Cerr << "Verification artifacts: " << directory.Path().GetPath() << Endl;
    const auto beforePath = directory.Path() / "before.json";
    const auto afterPath = directory.Path() / "after.json";
    TFileOutput(beforePath.GetPath()).Write(before);
    TFileOutput(afterPath.GetPath()).Write(after);
    TShellCommand command(BinaryPath("ydb/core/kqp/opt/rbo/verification/bin/kqp_rbo_verify"));
    command << beforePath.GetPath() << afterPath.GetPath()
        << "--rows" << "2" << "--timeout-ms" << "10000"
        << "--emit-smt" << (directory.Path() / "problem.smt2").GetPath()
        << "--solver" << BinaryPath("contrib/tools/z3/z3");
    if (diagnosticPrefix) {
        command << "--diagnostic-transformation-prefix";
    }
    command.Run();
    TFileOutput((directory.Path() / "verdict.json").GetPath()).Write(command.GetOutput());
    TFileOutput((directory.Path() / "verifier.stderr").GetPath()).Write(command.GetError());
    NJson::TJsonValue verdict;
    UNIT_ASSERT_C(NJson::ReadJsonTree(command.GetOutput(), &verdict, false),
        command.GetError() << command.GetOutput());
    if (diagnosticPrefix) {
        UNIT_ASSERT_VALUES_EQUAL(verdict["comparison_scope"].GetStringSafe(), "OPTIMIZER_TRANSFORMATION_PREFIX");
    }
    const TString status = verdict["status"].GetStringSafe();
    UNIT_ASSERT(command.GetExitCode().Defined());
    const int expectedExit = status == "VERIFIED_BOUNDED" ? 0 : status == "UNKNOWN" ? 2 : 1;
    UNIT_ASSERT_VALUES_EQUAL_C(command.GetExitCode().GetRef(), expectedExit,
        command.GetError() << command.GetOutput());
    return status;
}


class TNativeCaptureSink final : public IRBOSemanticSnapshotSink {
public:
    void OnSemanticSnapshot(TRBOSemanticSnapshotBoundaryResultV1 result) override {
        Results.push_back(std::move(result));
    }
    TVector<TRBOSemanticSnapshotBoundaryResultV1> Results;
};

TExpression NativeScalarFromExpr(TFixture& fixture, bool predicate) {
    const auto pos = TPositionHandle();
    const auto* int32 = fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const auto* resultType = predicate
        ? fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Bool) : int32;
    auto row = fixture.Expr.NewArgument(pos, "source_row");
    row->SetTypeAnn(fixture.Expr.MakeType<TStructExprType>(TVector<const TItemExprType*>{
        fixture.Expr.MakeType<TItemExprType>("k", int32)}));
    auto member = fixture.Expr.NewCallable(pos, "Member", {row, fixture.Expr.NewAtom(pos, "k")});
    member->SetTypeAnn(int32);
    auto one = fixture.Expr.NewCallable(pos, "Int32", {fixture.Expr.NewAtom(pos, "1")});
    one->SetTypeAnn(int32);
    auto body = fixture.Expr.NewCallable(pos, predicate ? "==" : "+", {member, one});
    body->SetTypeAnn(resultType);
    auto lambda = fixture.Expr.NewLambda(pos, fixture.Expr.NewArguments(pos, {row}), std::move(body));
    lambda->SetTypeAnn(resultType);
    auto expression = TExpression::FromExpr(std::move(lambda), {{"k", fixture.Key}},
        fixture.Expr, fixture.ExpressionProps);
    // Exercise main's real conversion, which creates fresh numeric members and
    // deliberately invalidates annotations while rebinding the row argument.
    UNIT_ASSERT(!expression.GetExpressionBody()->GetTypeAnn());
    UNIT_ASSERT(!expression.GetLambda()->GetTypeAnn());
    UNIT_ASSERT_VALUES_EQUAL(expression.GetExpressionBody()->Head().Tail().Content(), ToString(fixture.Key));
    return expression;
}

void CheckNativeInitialScalarTypes(bool predicate) {
    TFixture fixture;
    auto expression = NativeScalarFromExpr(fixture, predicate);
    auto read = fixture.Read();
    TIntrusivePtr<IOperator> input;
    TInfoUnitId result = fixture.Key;
    if (predicate) {
        input = MakeIntrusive<TOpFilter>(read, TPositionHandle(), expression);
    } else {
        result = fixture.Registry.Add(TInfoUnit("", "incremented"));
        input = MakeIntrusive<TOpMap>(read, TPositionHandle(), TMapIUs{{result, TMapElement(expression)}});
    }
    UNIT_ASSERT(!input->Type);
    TOpRoot root(input, TPositionHandle(), {{result, "answer"}});
    root.PlanProps.InfoUnitRegistry = fixture.Registry;
    auto annotation = CreateTypeAnnotationTransformer(
        CreateExtCallableTypeAnnotationTransformer(fixture.Types), fixture.Types);
    TRBOContext rbo(fixture.Optimize, fixture.Expr, fixture.Types, *annotation, *fixture.Functions);
    TNativeCaptureSink sink;
    TSemanticSnapshotPairCaptureV1 capture(&sink);
    capture.CaptureInitial(root, rbo);
    UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 1);
    UNIT_ASSERT_C(sink.Results.front().IsSupported(), sink.Results.front().UnsupportedReason);
    UNIT_ASSERT(input->Type);
    UNIT_ASSERT(input->GetExpressions().front().get().GetExpressionBody()->GetTypeAnn());
    fixture.StageSerial(root);
    capture.CaptureFinal(root, rbo);
    UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
    UNIT_ASSERT_C(sink.Results.back().IsSupported(), sink.Results.back().UnsupportedReason);
    UNIT_ASSERT_VALUES_EQUAL(Verify(sink.Results.front().Json, sink.Results.back().Json), "VERIFIED_BOUNDED");
}

TString CheckNativeTextMembership(const TString& sourceType, const TString& itemType,
    bool nullable, bool negate, bool changeLastItem = false, bool dictionary = false, bool strictCast = false,
    bool safeCast = false)
{
    TFixture beforeFixture(sourceType, !nullable);
    TFixture afterFixture(sourceType, !nullable);
    const auto build = [&](TFixture& fixture, bool rewrite, bool change) {
        const auto pos = TPositionHandle();
        auto read = fixture.Read();
        read->GetColumns() = TUnorderedIUs{fixture.Key, fixture.Group};
        const auto slot = sourceType == "String" ? NUdf::EDataSlot::String : NUdf::EDataSlot::Utf8;
        const TTypeAnnotationNode* textType = fixture.Expr.MakeType<TDataExprType>(slot);
        if (nullable) {
            textType = fixture.Expr.MakeType<TOptionalExprType>(textType);
        }
        fixture.SetType(*read, {{fixture.Key, fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32)},
            {fixture.Group, textType}});
        auto row = fixture.Expr.NewArgument(pos, "row");
        auto member = fixture.Expr.NewCallable(pos, "Member", {row, fixture.Expr.NewAtom(pos, "g")});
        TExprNode::TListType items;
        for (const TString& value : TVector<TString>{"red", "Ж", change ? "changed" : ""}) {
            items.push_back(fixture.Expr.NewCallable(pos, itemType, {fixture.Expr.NewAtom(pos, value)}));
        }
        TExprNode::TPtr body;
        if (rewrite && dictionary) {
            UNIT_ASSERT(!nullable);
            UNIT_ASSERT(sourceType == itemType || (sourceType == "Utf8" && itemType == "String"));
            auto key = fixture.Expr.NewArgument(pos, "key");
            auto payload = fixture.Expr.NewArgument(pos, "payload");
            const auto* itemAnnotation = fixture.Expr.MakeType<TDataExprType>(
                itemType == "String" ? NUdf::EDataSlot::String : NUdf::EDataSlot::Utf8);
            const auto* listType = fixture.Expr.MakeType<TListExprType>(itemAnnotation);
            auto descriptor = ExpandType(pos, *listType, fixture.Expr);
            items.insert(items.begin(), std::move(descriptor));
            auto dict = fixture.Expr.NewCallable(pos, "ToDict", {
                fixture.Expr.NewCallable(pos, "List", std::move(items)),
                fixture.Expr.NewLambda(pos, fixture.Expr.NewArguments(pos, {key}), TExprNode::TPtr(key)),
                fixture.Expr.NewLambda(pos, fixture.Expr.NewArguments(pos, {payload}),
                    fixture.Expr.NewCallable(pos, "Void", {})),
                fixture.Expr.NewList(pos, {fixture.Expr.NewAtom(pos, "One"), fixture.Expr.NewAtom(pos, "Auto")})});
            auto lookup = member;
            if (sourceType != itemType) {
                lookup = strictCast || safeCast
                    ? fixture.Expr.NewCallable(pos, safeCast ? "SafeCast" : "StrictCast",
                        {member, ExpandType(pos, *itemAnnotation, fixture.Expr)})
                    : fixture.Expr.NewCallable(pos, "ToString", {member});
            }
            body = fixture.Expr.NewCallable(pos, "Contains", {dict, lookup});
        } else if (rewrite) {
            TExprNode::TListType comparisons;
            for (const auto& item : items) {
                comparisons.push_back(fixture.Expr.NewCallable(pos, "==", {member, item}));
            }
            body = fixture.Expr.NewCallable(pos, "Or", std::move(comparisons));
        } else {
            body = fixture.Expr.NewCallable(pos, "SqlIn", {
                fixture.Expr.NewList(pos, std::move(items)), member,
                fixture.Expr.NewList(pos, {fixture.Expr.NewList(pos, {fixture.Expr.NewAtom(pos, "ansi")})})});
        }
        if (negate) {
            body = fixture.Expr.NewCallable(pos, "Not", {body});
        }
        auto lambda = fixture.Expr.NewLambda(pos, fixture.Expr.NewArguments(pos, {row}), std::move(body));
        auto expression = TExpression::FromExpr(std::move(lambda), {{"g", fixture.Group}},
            fixture.Expr, fixture.ExpressionProps);
        const auto result = fixture.Registry.Add(TInfoUnit("", "membership"));
        auto map = MakeIntrusive<TOpMap>(read, pos, TMapIUs{{result, TMapElement(expression)}});
        return std::pair{map, result};
    };
    auto [before, beforeResult] = build(beforeFixture, false, false);
    auto [after, afterResult] = build(afterFixture, true, changeLastItem);
    TOpRoot beforeRoot(before, TPositionHandle(), {{beforeFixture.Key, "key"}, {beforeResult, "membership"}});
    TOpRoot afterRoot(after, TPositionHandle(), {{afterFixture.Key, "key"}, {afterResult, "membership"}});
    beforeRoot.PlanProps.InfoUnitRegistry = beforeFixture.Registry;
    afterRoot.PlanProps.InfoUnitRegistry = afterFixture.Registry;
    auto beforeAnnotation = CreateTypeAnnotationTransformer(
        CreateExtCallableTypeAnnotationTransformer(beforeFixture.Types), beforeFixture.Types);
    auto afterAnnotation = CreateTypeAnnotationTransformer(
        CreateExtCallableTypeAnnotationTransformer(afterFixture.Types), afterFixture.Types);
    TRBOContext beforeRbo(beforeFixture.Optimize, beforeFixture.Expr, beforeFixture.Types,
        *beforeAnnotation, *beforeFixture.Functions);
    TRBOContext afterRbo(afterFixture.Optimize, afterFixture.Expr, afterFixture.Types,
        *afterAnnotation, *afterFixture.Functions);
    TNativeCaptureSink sink;
    TSemanticSnapshotPairCaptureV1 capture(&sink);
    capture.CaptureInitial(beforeRoot, beforeRbo);
    UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 1);
    UNIT_ASSERT_C(sink.Results.front().IsSupported(), sink.Results.front().UnsupportedReason);
    afterRoot.RecomputeOutputIUsSubtree();
    UNIT_ASSERT(afterRoot.ComputeTypes(afterRbo) == IGraphTransformer::TStatus::Ok);
    afterFixture.StageSerial(afterRoot);
    capture.CaptureFinal(afterRoot, afterRbo);
    UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
    UNIT_ASSERT_C(sink.Results.back().IsSupported(), sink.Results.back().UnsupportedReason);
    return Verify(sink.Results.front().Json, sink.Results.back().Json);
}

void CheckNativeSortNullPlacement(bool merge) {
    for (bool ascending : {false, true}) {
        // This is the optional-value comparator used by WideSort and DQ Merge.
        const bool nullsFirst = NMiniKQL::CompareValues(NUdf::EDataSlot::Int32,
            ascending, true, NUdf::TUnboxedValuePod(), NUdf::TUnboxedValuePod(i32(1))) < 0;
        UNIT_ASSERT_VALUES_EQUAL(nullsFirst, ascending);
        for (bool sourceNullsFirst : {false, true}) {
            TFixture fixture;
            auto read = fixture.Read();
            read->GetColumns() = TUnorderedIUs{fixture.Key, fixture.Group};
            fixture.SetType(*read, {
                {fixture.Key, fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32)},
                {fixture.Group, fixture.Expr.MakeType<TOptionalExprType>(
                    fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::String))}});
            const TSortIUs keys{{fixture.Group, {ascending, sourceNullsFirst}}};
            auto sort = MakeIntrusive<TOpSort>(read, TPositionHandle(), keys);
            sort->Type = read->Type;
            TIntrusivePtr<IOperator> input = sort;
            if (merge) {
                input = MakeIntrusive<TOpLimit>(sort, TPositionHandle(),
                    MakeConstant("Uint64", "2", TPositionHandle(), &fixture.Expr), EOpPhase::Undefined);
                input->Type = read->Type;
            }
            TOpRoot root(input, TPositionHandle(), {{fixture.Group, "value"}, {fixture.Key, "key"}});
            const auto before = fixture.Export(root);
            auto initial = Parse(before);
            size_t sorts = 0;
            for (const auto& node : initial["plan"]["nodes"].GetArraySafe()) {
                if (node["op"].GetStringSafe() == "sort") {
                    ++sorts;
                    UNIT_ASSERT_VALUES_EQUAL(node["order"][0]["nulls_first"].GetBooleanSafe(), nullsFirst);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(sorts, 1);
            fixture.StageSerial(root);
            if (merge) {
                auto& graph = root.PlanProps.StageGraph;
                const auto producer = *sort->Props.StageId;
                const auto consumer = graph.AddStage();
                input->Props.StageId = consumer;
                graph.Connect(producer, consumer, MakeIntrusive<TMergeConnection>(keys, graph.GetOutputIndex(producer)));
            }
            const auto after = fixture.Export(root);
            auto final = Parse(after);
            if (merge) {
                size_t merges = 0;
                for (const auto& edge : final["stage_graph"]["edges"].GetArraySafe()) {
                    if (edge["kind"].GetStringSafe() == "merge") {
                        ++merges;
                        UNIT_ASSERT_VALUES_EQUAL(edge["order"][0]["nulls_first"].GetBooleanSafe(), nullsFirst);
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL(merges, 1);
            }
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
            if (!merge) {
                for (auto& node : final["plan"]["nodes"].GetArraySafe()) {
                    if (node["op"].GetStringSafe() == "sort") {
                        node["order"][0]["nulls_first"] = !ascending;
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, NJson::WriteJson(final, false)), "COUNTEREXAMPLE");
            }
        }
    }
}

TSemanticSnapshotExportResult NativeDateCast(
    const TString& type, bool nullable, bool lowered, bool missingZero, bool staged,
    bool malformedDescriptor = false, bool coalesceZero = false, bool foldedZero = false,
    std::optional<i64> shiftMicros = std::nullopt, bool materializeCast = false, bool subtract = false)
{
    TFixture fixture(type, !nullable);
    const auto pos = TPositionHandle();
    const auto* bytes = fixture.Expr.MakeType<TDataExprType>(*NUdf::FindDataSlot(type));
    const TTypeAnnotationNode* sourceType = nullable
        ? static_cast<const TTypeAnnotationNode*>(fixture.Expr.MakeType<TOptionalExprType>(bytes)) : bytes;
    const auto* int32 = fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const auto* date = fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Date);
    const auto* optionalDate = fixture.Expr.MakeType<TOptionalExprType>(date);
    auto read = fixture.Read();
    read->GetColumns() = TUnorderedIUs{fixture.Key, fixture.Group};
    fixture.SetType(*read, {{fixture.Key, int32}, {fixture.Group, sourceType}});
    auto row = fixture.Expr.NewArgument(pos, "row");
    row->SetTypeAnn(read->Type->Cast<TListExprType>()->GetItemType());
    auto member = fixture.Expr.NewCallable(pos, "Member", {row, fixture.Expr.NewAtom(pos, ToString(fixture.Group))});
    member->SetTypeAnn(sourceType);
    auto itemDescriptor = fixture.Expr.NewCallable(pos, "DataType", {
        fixture.Expr.NewAtom(pos, malformedDescriptor ? "Utf8" : "Date")});
    itemDescriptor->SetTypeAnn(fixture.Expr.MakeType<TTypeExprType>(date));
    auto descriptor = fixture.Expr.NewCallable(pos, "OptionalType", {itemDescriptor});
    descriptor->SetTypeAnn(fixture.Expr.MakeType<TTypeExprType>(optionalDate));
    const auto cast = [&](TExprNode::TPtr input) {
        auto result = fixture.Expr.NewCallable(pos, "SafeCast", {input, descriptor});
        result->SetTypeAnn(optionalDate);
        return result;
    };
    auto body = cast(member);
    if (lowered) {
        UNIT_ASSERT(nullable);
        auto argument = fixture.Expr.NewArgument(pos, "bytes");
        argument->SetTypeAnn(bytes);
        auto missing = fixture.Expr.NewCallable(pos, "Nothing", {descriptor});
        if (missingZero) {
            auto zero = fixture.Expr.NewCallable(pos, "Date", {fixture.Expr.NewAtom(pos, "0")});
            zero->SetTypeAnn(date);
            missing = fixture.Expr.NewCallable(pos, "Just", {zero});
        }
        missing->SetTypeAnn(optionalDate);
        body = fixture.Expr.NewCallable(pos, "IfPresent", {member,
            fixture.Expr.NewLambda(pos, fixture.Expr.NewArguments(pos, {argument}), cast(argument)), missing});
        body->SetTypeAnn(optionalDate);
    }
    if (coalesceZero) {
        auto zero = fixture.Expr.NewCallable(pos, foldedZero ? "Date" : "Int32", {fixture.Expr.NewAtom(pos, "0")});
        zero->SetTypeAnn(foldedZero ? date : int32);
        auto fallback = foldedZero ? fixture.Expr.NewCallable(pos, "Just", {zero}) : cast(zero);
        fallback->SetTypeAnn(optionalDate);
        body = fixture.Expr.NewCallable(pos, "Coalesce", {body, fallback});
        body->SetTypeAnn(optionalDate);
    }
    TIntrusivePtr<IOperator> input = read;
    std::optional<TInfoUnitId> castColumn;
    if (shiftMicros) {
        if (materializeCast) {
            const auto castResult = fixture.Registry.Add(TInfoUnit("", "parsed"));
            auto castExpression = fixture.Expr.NewLambda(pos, fixture.Expr.NewArguments(pos, {row}), std::move(body));
            auto castMap = MakeIntrusive<TOpMap>(input, pos,
                TMapIUs{{castResult, TMapElement(TExpression(castExpression, &fixture.Expr, &fixture.ExpressionProps))}});
            fixture.SetType(*castMap, {{fixture.Key, int32}, {fixture.Group, sourceType}, {castResult, optionalDate}});
            input = castMap;
            castColumn = castResult;
            row = fixture.Expr.NewArgument(pos, "parsed_row");
            row->SetTypeAnn(castMap->Type->Cast<TListExprType>()->GetItemType());
            body = fixture.Expr.NewCallable(pos, "Member", {row, fixture.Expr.NewAtom(pos, ToString(castResult))});
            body->SetTypeAnn(optionalDate);
        }
        const auto* intervalType = fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Interval);
        auto interval = fixture.Expr.NewCallable(pos, "Interval", {fixture.Expr.NewAtom(pos, ToString(*shiftMicros))});
        interval->SetTypeAnn(intervalType);
        auto optionalInterval = fixture.Expr.NewCallable(pos, "Just", {interval});
        optionalInterval->SetTypeAnn(fixture.Expr.MakeType<TOptionalExprType>(intervalType));
        body = fixture.Expr.NewCallable(pos, subtract ? "-" : "+", {body, optionalInterval});
        body->SetTypeAnn(optionalDate);
    }
    auto expression = fixture.Expr.NewLambda(pos, fixture.Expr.NewArguments(pos, {row}), std::move(body));
    const auto result = fixture.Registry.Add(TInfoUnit("", "date"));
    auto map = MakeIntrusive<TOpMap>(input, pos,
        TMapIUs{{result, TMapElement(TExpression(expression, &fixture.Expr, &fixture.ExpressionProps))}});
    TVector<std::pair<TInfoUnitId, const TTypeAnnotationNode*>> columns{
        {fixture.Key, int32}, {fixture.Group, sourceType}, {result, optionalDate}};
    if (castColumn) {
        columns.emplace_back(*castColumn, optionalDate);
    }
    fixture.SetType(*map, columns);
    TOpRoot root(map, pos, {{fixture.Key, "key"}, {result, "date"}});
    if (staged) {
        fixture.StageSerial(root);
    }
    return fixture.Export(root);
}

TSemanticSnapshotExportResult NativeOlapDatePredicate(
    const TString& textType, bool nullable, const TString& comparison,
    bool justConstant, bool pushed, ui16 days = 11334, TStringBuf mutation = "")
{
    TFixture fixture(textType, !nullable);
    auto& ctx = fixture.Expr;
    const auto pos = TPositionHandle();
    const auto* text = ctx.MakeType<TDataExprType>(*NUdf::FindDataSlot(textType));
    const TTypeAnnotationNode* sourceType = nullable
        ? static_cast<const TTypeAnnotationNode*>(ctx.MakeType<TOptionalExprType>(text)) : text;
    const auto* int32 = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const auto* date = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Date);
    const auto* optionalDate = ctx.MakeType<TOptionalExprType>(date);
    const auto* boolean = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
    const auto* optionalBool = ctx.MakeType<TOptionalExprType>(boolean);
    auto read = fixture.Read();
    fixture.Table->Metadata->Kind = EKikimrTableKind::Olap;
    read->StorageType = NYql::EStorageType::ColumnStorage;
    read->GetColumns() = {fixture.Key, fixture.Group};
    fixture.SetType(*read, {{fixture.Key, int32}, {fixture.Group, sourceType}});
    auto row = ctx.NewArgument(pos, "row");
    row->SetTypeAnn(read->Type->Cast<TListExprType>()->GetItemType());
    auto member = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, ToString(fixture.Group))});
    member->SetTypeAnn(sourceType);
    auto target = ExpandType(pos, *optionalDate, ctx);
    if (mutation == "wrong_target") {
        target = ExpandType(pos, *ctx.MakeType<TOptionalExprType>(text), ctx);
    }
    // ExpandType builds syntax; these fixtures bypass the type annotator.
    // Preserve the exact annotations the real pushdown leaves on descriptors.
    const auto* targetItem = mutation == "wrong_target" ? text : date;
    target->Child(0)->SetTypeAnn(ctx.MakeType<TTypeExprType>(targetItem));
    target->SetTypeAnn(ctx.MakeType<TTypeExprType>(ctx.MakeType<TOptionalExprType>(targetItem)));
    auto cast = ctx.NewCallable(pos, "SafeCast", {member, target});
    cast->SetTypeAnn(optionalDate);
    auto constant = ctx.NewCallable(pos, "Date", {ctx.NewAtom(pos, ToString(days))});
    constant->SetTypeAnn(mutation == "wrong_literal_type" ? int32 : date);
    if (justConstant) {
        constant = ctx.NewCallable(pos, "Just", {constant});
        constant->SetTypeAnn(mutation == "wrong_just_type" ? optionalBool : optionalDate);
    }
    auto predicate = ctx.NewCallable(pos, comparison, {cast, constant});
    predicate->SetTypeAnn(optionalBool);
    auto fallback = ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, mutation == "true_fallback" ? "true" : "false")});
    fallback->SetTypeAnn(boolean);
    auto body = ctx.NewCallable(pos, "Coalesce", {predicate, fallback});
    body->SetTypeAnn(boolean);
    TIntrusivePtr<IOperator> input = read;
    if (pushed) {
        auto argument = ctx.NewArgument(pos, "members_g");
        auto bound = mutation == "wrong_binding" ? ctx.NewArgument(pos, "unbound") : argument;
        TNodeOnNodeOwnedMap replacements{{member.Get(), bound}};
        auto pushedBody = ctx.ReplaceNodes(std::move(body), replacements);
        if (mutation == "typed_cast") {
            pushedBody->Child(0)->Child(0)->SetTypeAnn(optionalDate);
        }
        auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {argument}), std::move(pushedBody));
        if (mutation == "extra_lambda_arg") {
            lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {argument, ctx.NewArgument(pos, "extra")}),
                lambda->ChildPtr(1));
        }
        auto column = ctx.NewCallable(pos, "KqpOlapApplyColumnArg", {
            ExpandType(pos, *row->GetTypeAnn(), ctx), ctx.NewAtom(pos, ToString(fixture.Group))});
        auto apply = ctx.NewCallable(pos, "KqpOlapApply", {
            lambda, ctx.NewList(pos, {column}), ctx.NewAtom(pos, "")});
        auto flow = ctx.NewArgument(pos, "flow");
        auto filter = ctx.NewCallable(pos, "KqpOlapFilter", {flow, apply});
        read->OlapFilterLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {flow}), std::move(filter));
    } else {
        auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(body));
        auto filter = MakeIntrusive<TOpFilter>(read, pos, TExpression(lambda, &ctx, &fixture.ExpressionProps));
        filter->Type = read->Type;
        input = filter;
    }
    TOpRoot root(input, pos, {{fixture.Key, "key"}, {fixture.Group, "value"}});
    if (pushed) {
        read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(read->StorageType);
    }
    return fixture.Export(root);
}


TSemanticSnapshotExportResult NativeNarrowDecimalZero(bool folded, bool staged, ui8 precision,
    bool changeJust = false, bool malformedDescriptor = false)
{
    TFixture fixture("String", false, "Int32", precision);
    auto& ctx = fixture.Expr;
    const auto pos = TPositionHandle();
    const auto* int32 = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const auto* decimal = ctx.MakeType<TDataExprParamsType>(
        NUdf::EDataSlot::Decimal, ToString(precision), "2");
    const auto* optional = ctx.MakeType<TOptionalExprType>(decimal);
    auto read = fixture.Read();
    read->GetColumns() = {fixture.Key, fixture.Price};
    fixture.SetType(*read, {{fixture.Key, int32}, {fixture.Price, optional}});
    auto row = ctx.NewArgument(pos, "row");
    row->SetTypeAnn(read->Type->Cast<TListExprType>()->GetItemType());
    auto member = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, ToString(fixture.Price))});
    member->SetTypeAnn(optional);
    auto sourceZero = ctx.NewCallable(pos, "Int32", {ctx.NewAtom(pos, "0")});
    sourceZero->SetTypeAnn(int32);
    auto descriptor = ExpandType(pos, *(malformedDescriptor
        ? ctx.MakeType<TDataExprParamsType>(NUdf::EDataSlot::Decimal, ToString(precision + 1), "2")
        : decimal), ctx);
    descriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(decimal));
    auto zero = ctx.NewCallable(pos, "SafeCast", {sourceZero, descriptor});
    if (folded) {
        zero = ctx.NewCallable(pos, "Decimal", {
            ctx.NewAtom(pos, "0"), ctx.NewAtom(pos, ToString(precision)), ctx.NewAtom(pos, "2")});
    }
    zero->SetTypeAnn(decimal);
    auto justValue = zero;
    if (changeJust) {
        justValue = ctx.NewCallable(pos, "Decimal", {
            ctx.NewAtom(pos, "1"), ctx.NewAtom(pos, ToString(precision)), ctx.NewAtom(pos, "2")});
        justValue->SetTypeAnn(decimal);
    }
    auto just = ctx.NewCallable(pos, "Just", {justValue});
    just->SetTypeAnn(optional);
    auto coalesce = ctx.NewCallable(pos, "Coalesce", {member, zero});
    coalesce->SetTypeAnn(decimal);
    const auto zeroOutput = fixture.Registry.Add(TInfoUnit("", "zero"));
    const auto filledOutput = fixture.Registry.Add(TInfoUnit("", "filled"));
    auto justLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(just));
    auto coalesceLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(coalesce));
    auto map = MakeIntrusive<TOpMap>(read, pos, TMapIUs{
        {zeroOutput, TMapElement(TExpression(justLambda, &ctx, &fixture.ExpressionProps))},
        {filledOutput, TMapElement(TExpression(coalesceLambda, &ctx, &fixture.ExpressionProps))}});
    fixture.SetType(*map, {{fixture.Key, int32}, {fixture.Price, optional},
        {zeroOutput, optional}, {filledOutput, decimal}});
    TOpRoot root(map, pos, {{fixture.Key, "key"}, {zeroOutput, "zero"}, {filledOutput, "filled"}});
    if (staged) {
        fixture.StageSerial(root);
    }
    return fixture.Export(root);
}


template <typename T>
void CheckStableScalarPacking(NUdf::EDataSlot slot, TVector<T> values) {
    NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    NMiniKQL::TTypeEnvironment environment(alloc);
    NMiniKQL::TMemoryUsageInfo memory("StablePickle contract");
    NMiniKQL::THolderFactory holders(alloc.Ref(), memory);
    auto* type = NMiniKQL::TDataType::Create(NUdf::GetDataTypeInfo(slot).TypeId, environment);
    auto* optionalType = NMiniKQL::TOptionalType::Create(type, environment);
    NMiniKQL::TValuePacker required(true, type), optional(true, optionalType);
    const TString nullBytes(optional.Pack(NUdf::TUnboxedValuePod()));
    UNIT_ASSERT(!optional.Unpack(nullBytes, holders));
    TVector<TString> encodings;
    for (size_t index = 0; index < values.size(); ++index) {
        const T value = values[index];
        const TString bytes(required.Pack(NUdf::TUnboxedValuePod(value)));
        const TString present(optional.Pack(NUdf::TUnboxedValuePod(value)));
        UNIT_ASSERT_VALUES_EQUAL(bytes, present);
        UNIT_ASSERT(bytes != nullBytes);
        UNIT_ASSERT_VALUES_EQUAL(required.Unpack(bytes, holders).template Get<T>(), value);
        UNIT_ASSERT_VALUES_EQUAL(optional.Unpack(present, holders).template Get<T>(), value);
        for (size_t previous = 0; previous < index; ++previous) {
            UNIT_ASSERT_VALUES_EQUAL(bytes == encodings[previous], value == values[previous]);
        }
        encodings.push_back(bytes);
    }
}

void CheckStableTextPacking(NUdf::EDataSlot slot) {
    NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    NMiniKQL::TTypeEnvironment environment(alloc);
    NMiniKQL::TMemoryUsageInfo memory("StablePickle text contract");
    NMiniKQL::THolderFactory holders(alloc.Ref(), memory);
    auto* type = NMiniKQL::TDataType::Create(NUdf::GetDataTypeInfo(slot).TypeId, environment);
    auto* optionalType = NMiniKQL::TOptionalType::Create(type, environment);
    NMiniKQL::TValuePacker required(true, type), optional(true, optionalType);
    const TString nullBytes(optional.Pack(NUdf::TUnboxedValuePod()));
    UNIT_ASSERT(!optional.Unpack(nullBytes, holders));
    TVector<TString> values{"", "a", "b", TString("a\0b", 3), TString("\0", 1),
                           TString("\xc3\xa9"), TString("\xe6\xbc\xa2"), TString("\xf0\x9f\x98\x80")};
    for (size_t length : {7, 8, 15, 16, 127, 128, 255, 256, 16383, 16384}) {
        values.push_back(TString(length, 'x'));
    }
    if (slot == NUdf::EDataSlot::String) {
        values.push_back(TString("\xff\x80\0\xfe", 4));
    }
    TVector<TString> encodings;
    for (size_t index = 0; index < values.size(); ++index) {
        const NUdf::TUnboxedValue value(NMiniKQL::MakeString(values[index]));
        const TString bytes(required.Pack(value)), present(optional.Pack(value));
        UNIT_ASSERT_VALUES_EQUAL(bytes, present);
        UNIT_ASSERT(bytes != nullBytes);
        const auto restoredRequired = required.Unpack(bytes, holders);
        const auto restoredOptional = optional.Unpack(present, holders);
        UNIT_ASSERT(restoredOptional);
        UNIT_ASSERT_VALUES_EQUAL(TString(restoredRequired.AsStringRef()), values[index]);
        UNIT_ASSERT_VALUES_EQUAL(TString(restoredOptional.AsStringRef()), values[index]);
        for (size_t previous = 0; previous < index; ++previous) {
            UNIT_ASSERT_VALUES_EQUAL(bytes == encodings[previous], values[index] == values[previous]);
        }
        encodings.push_back(bytes);
    }
}

TSemanticSnapshotExportResult NativeStablePickle(
    const TString& type, bool nullable, bool staged, bool constant = false,
    bool malformedResult = false)
{
    TFixture fixture(type, !nullable);
    const auto pos = TPositionHandle();
    const auto* item = fixture.Expr.MakeType<TDataExprType>(*NUdf::FindDataSlot(type));
    const TTypeAnnotationNode* sourceType = nullable
        ? static_cast<const TTypeAnnotationNode*>(fixture.Expr.MakeType<TOptionalExprType>(item)) : item;
    const auto* int32 = fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const auto* string = fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::String);
    auto read = fixture.Read();
    read->GetColumns() = TUnorderedIUs{fixture.Key, fixture.Group};
    fixture.SetType(*read, {{fixture.Key, int32}, {fixture.Group, sourceType}});
    auto row = fixture.Expr.NewArgument(pos, "row");
    row->SetTypeAnn(read->Type->Cast<TListExprType>()->GetItemType());
    auto argument = fixture.Expr.NewCallable(pos, "Member", {row, fixture.Expr.NewAtom(pos, ToString(fixture.Group))});
    argument->SetTypeAnn(sourceType);
    if (constant) {
        argument = fixture.Expr.NewCallable(pos, type, {fixture.Expr.NewAtom(pos, type == "Bool" ? "false" : "0")});
        argument->SetTypeAnn(item);
    }
    auto body = fixture.Expr.NewCallable(pos, "StablePickle", {argument});
    body->SetTypeAnn(malformedResult ? sourceType : string);
    auto expression = fixture.Expr.NewLambda(pos, fixture.Expr.NewArguments(pos, {row}), std::move(body));
    const auto result = fixture.Registry.Add(TInfoUnit("", "pickle"));
    auto map = MakeIntrusive<TOpMap>(read, pos,
        TMapIUs{{result, TMapElement(TExpression(expression, &fixture.Expr, &fixture.ExpressionProps))}});
    fixture.SetType(*map, {{fixture.Key, int32}, {fixture.Group, sourceType}, {result, string}});
    TOpRoot root(map, pos, {{fixture.Key, "key"}, {result, "pickle"}});
    if (staged) {
        fixture.StageSerial(root);
    }
    return fixture.Export(root);
}


enum class ETimestamp64LiteralMetadataMutation {
    None,
    RootResult,
    RootPosition,
    RootSemilatticeEffects,
    RootGeneralEffects,
    RootUnordered,
    AtomResult,
    AtomPosition,
    AtomSemilatticeEffects,
    AtomGeneralEffects,
};

TSemanticSnapshotExportResult NativeTimestamp64Expression(
    TStringBuf text, TStringBuf operation = "literal", bool nullable = true,
    TString sourceType = "Timestamp64", TString literalType = "Timestamp64",
    TString annotationType = "Timestamp64", bool nullableLiteral = false,
    ETimestamp64LiteralMetadataMutation metadataMutation = ETimestamp64LiteralMetadataMutation::None)
{
    TFixture fixture(sourceType, !nullable);
    const auto pos = TPositionHandle();
    const auto* int32 = fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const auto* boolean = fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
    const auto* sourceItem = fixture.Expr.MakeType<TDataExprType>(*NUdf::FindDataSlot(sourceType));
    const TTypeAnnotationNode* source = nullable
        ? static_cast<const TTypeAnnotationNode*>(fixture.Expr.MakeType<TOptionalExprType>(sourceItem))
        : sourceItem;
    const auto* literalItem = fixture.Expr.MakeType<TDataExprType>(*NUdf::FindDataSlot(annotationType));
    const TTypeAnnotationNode* literalAnnotation = nullableLiteral
        ? static_cast<const TTypeAnnotationNode*>(fixture.Expr.MakeType<TOptionalExprType>(literalItem))
        : literalItem;
    auto read = fixture.Read();
    read->GetColumns() = TUnorderedIUs{fixture.Key, fixture.Group};
    fixture.SetType(*read, {{fixture.Key, int32}, {fixture.Group, source}});
    auto row = fixture.Expr.NewArgument(pos, "row");
    row->SetTypeAnn(read->Type->Cast<TListExprType>()->GetItemType());
    auto member = fixture.Expr.NewCallable(pos, "Member", {row, fixture.Expr.NewAtom(pos, ToString(fixture.Group))});
    member->SetTypeAnn(source);
    auto literal = fixture.Expr.NewCallable(pos, literalType, {fixture.Expr.NewAtom(pos, text)});
    literal->SetTypeAnn(literalAnnotation);
    switch (metadataMutation) {
        case ETimestamp64LiteralMetadataMutation::None:
            break;
        case ETimestamp64LiteralMetadataMutation::RootResult:
            literal->SetResult(fixture.Expr.NewAtom(pos, "executed"));
            break;
        case ETimestamp64LiteralMetadataMutation::RootPosition:
            literal->SetPosAware();
            break;
        case ETimestamp64LiteralMetadataMutation::RootSemilatticeEffects:
            literal->SetSideEffects(ESideEffects::SemilatticeRT);
            break;
        case ETimestamp64LiteralMetadataMutation::RootGeneralEffects:
            literal->SetSideEffects(ESideEffects::General);
            break;
        case ETimestamp64LiteralMetadataMutation::RootUnordered:
            literal->SetUnorderedChildren();
            break;
        case ETimestamp64LiteralMetadataMutation::AtomResult:
            literal->Child(0)->SetResult(fixture.Expr.NewAtom(pos, "executed"));
            break;
        case ETimestamp64LiteralMetadataMutation::AtomPosition:
            literal->Child(0)->SetPosAware();
            break;
        case ETimestamp64LiteralMetadataMutation::AtomSemilatticeEffects:
            literal->Child(0)->SetSideEffects(ESideEffects::SemilatticeRT);
            break;
        case ETimestamp64LiteralMetadataMutation::AtomGeneralEffects:
            literal->Child(0)->SetSideEffects(ESideEffects::General);
            break;
    }
    auto body = literal;
    const TTypeAnnotationNode* resultType = literalAnnotation;
    if (operation == "<" || operation == "<=" || operation == "==") {
        resultType = nullable || nullableLiteral
            ? static_cast<const TTypeAnnotationNode*>(fixture.Expr.MakeType<TOptionalExprType>(boolean))
            : boolean;
        body = fixture.Expr.NewCallable(pos, operation, {member, literal});
        body->SetTypeAnn(resultType);
    } else if (operation == "+") {
        resultType = source;
        body = fixture.Expr.NewCallable(pos, operation, {member, literal});
        body->SetTypeAnn(resultType);
    } else if (operation == "SafeCast" || operation == "Convert") {
        resultType = nullable || operation == "SafeCast"
            ? static_cast<const TTypeAnnotationNode*>(fixture.Expr.MakeType<TOptionalExprType>(literalItem))
            : literalItem;
        auto descriptor = ExpandType(pos, *resultType, fixture.Expr);
        body = fixture.Expr.NewCallable(pos, operation, {member, descriptor});
        body->SetTypeAnn(resultType);
    }
    auto expression = fixture.Expr.NewLambda(pos, fixture.Expr.NewArguments(pos, {row}), std::move(body));
    const auto result = fixture.Registry.Add(TInfoUnit("", "value"));
    auto map = MakeIntrusive<TOpMap>(read, pos,
        TMapIUs{{result, TMapElement(TExpression(expression, &fixture.Expr, &fixture.ExpressionProps))}});
    fixture.SetType(*map, {{fixture.Key, int32}, {fixture.Group, source}, {result, resultType}});
    TOpRoot root(map, pos, {{fixture.Key, "key"}, {result, "value"}});
    return fixture.Export(root);
}


TSemanticSnapshotExportResult NativeTimestamp64Extrema(const TString& function, bool nullable, bool split) {
    TFixture f("Timestamp64", !nullable);
    const auto pos = TPositionHandle();
    auto read = f.Read();
    read->GetColumns() = TUnorderedIUs{f.Key, f.Group};
    const auto* item = f.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Timestamp64);
    const TTypeAnnotationNode* valueType = nullable
        ? static_cast<const TTypeAnnotationNode*>(f.Expr.MakeType<TOptionalExprType>(item)) : item;
    f.SetType(*read, {{f.Key, f.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32)}, {f.Group, valueType}});
    const auto result = f.Registry.Add(TInfoUnit("", "extreme"));
    TIntrusivePtr<IOperator> plan;
    if (split) {
        const auto state = f.Registry.Add(TInfoUnit("", "state"));
        auto partial = MakeIntrusive<TOpAggregate>(read,
            TAggregationIUs{{state, {f.Group, function, false, false}}},
            TOrderedIUs<>{}, EOpPhase::Intermediate, false, pos);
        plan = MakeIntrusive<TOpAggregate>(partial,
            TAggregationIUs{{result, {state, function, false, false}}},
            TOrderedIUs<>{}, EOpPhase::Final, false, pos);
    } else {
        plan = MakeIntrusive<TOpAggregate>(read,
            TAggregationIUs{{result, {f.Group, function, false, false}}},
            TOrderedIUs<>{}, EOpPhase::Undefined, false, pos);
    }
    TOpRoot root(plan, pos, {{result, "extreme"}});
    root.PlanProps.InfoUnitRegistry = f.Registry;
    for (const auto& entry : root) {
        entry.Current->BindExpressionPlanProps(&root.PlanProps);
    }
    UNIT_ASSERT(root.ComputeTypes(f.Rbo) == IGraphTransformer::TStatus::Ok);
    if (split) {
        f.StageSerial(root);
    }
    return f.Export(root);
}

TSemanticSnapshotExportResult NativeAliasedDecimalWindow(
    const TString& function, size_t aliasCount, bool staged)
{
    TFixture f;
    const auto pos = TPositionHandle();
    auto original = f.Window(function, true);
    TIntrusivePtr<IOperator> input = original->GetInput();
    auto argument = f.Sum;
    auto partition = f.Group;
    for (size_t index = 0; index < aliasCount; ++index) {
        const auto sumCopy = f.Registry.Add(TInfoUnit("", "sum_alias"));
        const auto keyCopy = f.Registry.Add(TInfoUnit("", "key_alias"));
        input = MakeIntrusive<TOpMap>(input, pos, TMapIUs{
            {sumCopy, TMapElement(MakeColumnAccess(argument, pos, &f.Expr, &f.ExpressionProps))},
            {keyCopy, TMapElement(MakeColumnAccess(partition, pos, &f.Expr, &f.ExpressionProps))},
        });
        argument = sumCopy;
        partition = keyCopy;
    }
    auto window = MakeIntrusive<TOpWindow>(input, pos,
        TWindowIUs{{f.WindowResult, {function, EWindowFuncKind::Aggregate, TOrderedIUs<>{argument}}}},
        TOrderedIUs<>{partition}, TSortIUs{}, original->GetFrame());
    TOpRoot plan(window, pos, {{f.WindowResult, "answer"}});
    plan.PlanProps.InfoUnitRegistry = f.Registry;
    for (const auto& entry : plan) { entry.Current->BindExpressionPlanProps(&plan.PlanProps); }
    auto annotation = CreateTypeAnnotationTransformer(
        CreateExtCallableTypeAnnotationTransformer(f.Types), f.Types);
    TRBOContext rbo(f.Optimize, f.Expr, f.Types, *annotation, *f.Functions);
    UNIT_ASSERT(plan.ComputeTypes(rbo) == IGraphTransformer::TStatus::Ok);
    if (staged) { f.StageSerial(plan); }
    return ExportSemanticSnapshotV1(plan, rbo);
}

TSemanticSnapshotExportResult NativeRank(
    const TString& type, bool nullable, bool partitioned, bool staged,
    bool ascending = true, bool secondKey = false, bool ordered = true,
    const TString& function = "rank", bool wholeFrame = false)
{
    TFixture f(type, !nullable);
    const auto pos = TPositionHandle();
    auto read = f.Read();
    read->GetColumns() = TUnorderedIUs{f.Key, f.Other, f.Group};
    const auto* scalar = f.Expr.MakeType<TDataExprType>(*NUdf::FindDataSlot(type));
    const TTypeAnnotationNode* groupType = nullable
        ? static_cast<const TTypeAnnotationNode*>(f.Expr.MakeType<TOptionalExprType>(scalar)) : scalar;
    const auto* int32 = f.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    f.SetType(*read, {{f.Key, int32}, {f.Other, int32}, {f.Group, groupType}});
    const auto rank = f.Registry.Add(TInfoUnit("", "rank"));
    TSortIUs order;
    if (ordered) { order.Append(f.Group, TSortOrder{ascending, ascending}); }
    if (secondKey) { order.Append(f.Key, TSortOrder{true, true}); }
    TOpWindowFrame frame;
    if (wholeFrame) { frame.EndKind = EWindowFrameBound::UnboundedFollowing; }
    auto window = MakeIntrusive<TOpWindow>(read, pos,
        TWindowIUs{{rank, {function, EWindowFuncKind::Native, {}}}},
        partitioned ? TOrderedIUs<>{f.Other} : TOrderedIUs<>{}, std::move(order), frame);
    TOpRoot root(window, pos, {{f.Key, "key"}, {rank, "rank"}});
    root.PlanProps.InfoUnitRegistry = f.Registry;
    for (const auto& entry : root) { entry.Current->BindExpressionPlanProps(&root.PlanProps); }
    UNIT_ASSERT(root.ComputeTypes(f.Rbo) == IGraphTransformer::TStatus::Ok);
    if (staged) { f.StageSerial(root); }
    return f.Export(root);
}

TSemanticSnapshotExportResult NativeIntegralAverage(
    const TString& scalarType, bool nullable, bool grouped, bool split, bool staged)
{
    TFixture f(scalarType, !nullable);
    const auto pos = TPositionHandle();
    auto read = f.Read();
    read->GetColumns() = TUnorderedIUs{f.Key, f.Group};
    const auto* itemType = f.Expr.MakeType<TDataExprType>(*NUdf::FindDataSlot(scalarType));
    const TTypeAnnotationNode* valueType = nullable
        ? static_cast<const TTypeAnnotationNode*>(f.Expr.MakeType<TOptionalExprType>(itemType)) : itemType;
    f.SetType(*read, {{f.Key, f.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32)}, {f.Group, valueType}});
    const auto result = f.Registry.Add(TInfoUnit("", "average"));
    const TOrderedIUs<> keys = grouped ? TOrderedIUs<>{f.Key} : TOrderedIUs<>{};
    TIntrusivePtr<IOperator> plan;
    if (split) {
        const auto state = f.Registry.Add(TInfoUnit("", "average_state"));
        auto partial = MakeIntrusive<TOpAggregate>(read,
            TAggregationIUs{{state, {f.Group, "avg", false, false}}}, keys, EOpPhase::Intermediate, false, pos);
        plan = MakeIntrusive<TOpAggregate>(partial,
            TAggregationIUs{{result, {state, "avg", false, false}}}, keys, EOpPhase::Final, false, pos);
    } else {
        plan = MakeIntrusive<TOpAggregate>(read,
            TAggregationIUs{{result, {f.Group, "avg", false, false}}}, keys, EOpPhase::Undefined, false, pos);
    }
    TOrderedIUs<TString> columns;
    if (grouped) { columns.Append(f.Key, "key"); }
    columns.Append(result, "average");
    TOpRoot root(plan, pos, std::move(columns));
    root.PlanProps.InfoUnitRegistry = f.Registry;
    for (const auto& entry : root) {
        entry.Current->BindExpressionPlanProps(&root.PlanProps);
    }
    UNIT_ASSERT(root.ComputeTypes(f.Rbo) == IGraphTransformer::TStatus::Ok);
    if (staged) {
        f.StageSerial(root);
    }
    return f.Export(root);
}

std::pair<TSemanticSnapshotExportResult, TSemanticSnapshotExportResult> NativeGroupingSets(
    const TString& function, bool nullable, bool duplicateSets, bool singleSet = false,
    bool wrongOutputNullability = false, bool stagedTemplate = false, bool stagedExpanded = false,
    bool nullableGroupingKey = false, TString groupingType = "Int32", bool countAll = false)
{
    TFixture fixture("Int32", !nullable, groupingType);
    const auto pos = TPositionHandle();
    auto read = fixture.Read();
    read->GetColumns().Add(fixture.Group);
    const auto* int32 = fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const TTypeAnnotationNode* valueType = nullable
        ? static_cast<const TTypeAnnotationNode*>(fixture.Expr.MakeType<TOptionalExprType>(int32)) : int32;
    TVector<std::pair<TInfoUnitId, const TTypeAnnotationNode*>> columns{
        {fixture.Key, int32}, {fixture.Other, fixture.OtherType}, {fixture.Group, valueType}};
    if (function == "avg") {
        read->GetColumns().Add(fixture.Price);
        columns.emplace_back(fixture.Price, fixture.Expr.MakeType<TOptionalExprType>(
            fixture.Expr.MakeType<TDataExprParamsType>(NUdf::EDataSlot::Decimal, "15", "2")));
    }
    fixture.SetType(*read, columns);
    auto measure = function == "avg" ? fixture.Price : fixture.Group;
    TIntrusivePtr<IOperator> aggregateInput = read;
    if (countAll) {
        UNIT_ASSERT_VALUES_EQUAL(function, "count");
        measure = fixture.Registry.Add(TInfoUnit("", "unit"));
        auto row = fixture.Expr.NewArgument(pos, "row");
        auto lambda = fixture.Expr.NewLambda(pos, fixture.Expr.NewArguments(pos, {row}),
            fixture.Expr.NewCallable(pos, "Void", {}));
        aggregateInput = MakeIntrusive<TOpMap>(read, pos, TMapIUs{{measure,
            TMapElement(TExpression(lambda, &fixture.Expr, &fixture.ExpressionProps))}});
    }
    const auto value = fixture.Registry.Add(TInfoUnit("", "value"));
    const auto outputKey = fixture.Registry.Add(TInfoUnit("", "group"));
    const auto outputValue = fixture.Registry.Add(TInfoUnit("", "value"));
    const auto indicator = fixture.Registry.Add(TInfoUnit("", "indicator"));
    const auto groupingKey = nullableGroupingKey ? fixture.Group : fixture.Other;
    auto aggregate = MakeIntrusive<TOpAggregate>(aggregateInput,
        TAggregationIUs{{value, {measure, function, false, false}}},
        TOrderedIUs<>{groupingKey}, EOpPhase::Undefined, false, pos);
    TVector<TUnorderedIUs> sets{{groupingKey}};
    if (!singleSet) {
        sets.push_back({});
    }
    if (duplicateSets) {
        sets.push_back({});
        sets.push_back({groupingKey});
    }
    auto grouping = MakeIntrusive<TOpGroupingSets>(aggregate, std::move(sets),
        TMappedIUs<TInfoUnitId>{{outputKey, groupingKey}, {outputValue, value}}, pos,
        TMappedIUs<TInfoUnitId>{{indicator, groupingKey}});
    TOpRoot root(grouping, pos, {{outputKey, "group"}, {outputValue, "value"}, {indicator, "indicator"}});
    root.PlanProps.InfoUnitRegistry = fixture.Registry;
    auto annotation = CreateTypeAnnotationTransformer(
        CreateExtCallableTypeAnnotationTransformer(fixture.Types), fixture.Types);
    TRBOContext rbo(fixture.Optimize, fixture.Expr, fixture.Types, *annotation, *fixture.Functions);
    for (const auto& entry : root) {
        entry.Current->BindExpressionPlanProps(&root.PlanProps);
    }
    root.RecomputeOutputIUsSubtree();
    UNIT_ASSERT(root.ComputeTypes(rbo) == IGraphTransformer::TStatus::Ok);
    if (wrongOutputNullability) {
        fixture.SetType(*grouping, {{outputKey, int32},
            {outputValue, grouping->GetIUType(outputValue, fixture.Expr)},
            {indicator, fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64)}});
    }
    if (stagedTemplate) {
        fixture.StageSerial(root);
    }
    auto initial = ExportSemanticSnapshotV1(root, rbo);
    if (wrongOutputNullability || stagedTemplate) {
        return {std::move(initial), {}};
    }
    UNIT_ASSERT_C(initial.IsSupported(), "GroupingSets template: " << initial.UnsupportedReason);
    Parse(initial);
    TExpandGroupingSetsRule rule;
    root.SetInput(rule.SimpleMatchAndApply(grouping, rbo, root.PlanProps));
    root.RecomputeOutputIUsSubtree();
    root.ComputeParents();
    UNIT_ASSERT(root.ComputeTypes(rbo) == IGraphTransformer::TStatus::Ok);
    if (stagedExpanded) {
        if (singleSet) {
            fixture.StageSerial(root);
        } else {
            auto& graph = root.PlanProps.StageGraph;
            const auto source = graph.AddSourceStage(NYql::EStorageType::RowStorage);
            read->Props.StageId = source;
            const auto producer = countAll ? graph.AddStage() : source;
            if (countAll) {
                aggregateInput->Props.StageId = producer;
                graph.Connect(source, producer, MakeIntrusive<TUnionAllConnection>(graph.GetOutputIndex(source)));
            }
            auto& unionAll = static_cast<TOpUnionAll&>(*root.GetInput());
            const auto sink = graph.AddStage();
            unionAll.Props.StageId = sink;
            for (auto* branch : unionAll.GetChildren()) {
                const auto stage = graph.AddStage();
                auto* current = branch;
                while (current->GetKind() != EOperator::Replicate) {
                    UNIT_ASSERT_VALUES_EQUAL(current->GetChildCount(), 1);
                    current->Props.StageId = stage;
                    current = current->GetChild(0).Get();
                }
                auto& port = static_cast<TOpReplicate&>(*current);
                port.Props.StageId = producer;
                graph.Connect(producer, stage, MakeIntrusive<TUnionAllConnection>(*GetReplicateOutputIndex(port)));
                graph.Connect(stage, sink, MakeIntrusive<TUnionAllConnection>(graph.GetOutputIndex(stage)));
            }
        }
    }
    auto expanded = ExportSemanticSnapshotV1(root, rbo);
    UNIT_ASSERT_C(expanded.IsSupported(), "Expanded GroupingSets: " << expanded.UnsupportedReason);
    Parse(expanded);
    return {std::move(initial), std::move(expanded)};
}

TSemanticSnapshotExportResult NativeCompiledLike(
    bool utf8, bool nullable, bool staged, bool changedPattern = false, bool wrongMemberType = false,
    TStringBuf junction = {}, bool pushed = false, TStringBuf mutation = {}, bool logicalPushed = false)
{
    TFixture fixture(utf8 ? "Utf8" : "String", !nullable);
    auto& ctx = fixture.Expr;
    const auto pos = TPositionHandle();
    const auto* boolean = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
    const auto* optionalBool = ctx.MakeType<TOptionalExprType>(boolean);
    const auto* string = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String);
    const auto* optionalString = ctx.MakeType<TOptionalExprType>(string);
    const auto* uint64 = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64);
    const auto* optionalUint64 = ctx.MakeType<TOptionalExprType>(uint64);
    const auto* voidType = ctx.MakeType<TVoidExprType>();
    const auto typed = [&](TStringBuf name, TExprNode::TListType children, const TTypeAnnotationNode* type) {
        auto node = ctx.NewCallable(pos, name, std::move(children));
        node->SetTypeAnn(type);
        return node;
    };
    const auto literal = [&](TStringBuf type, TStringBuf value, const TTypeAnnotationNode* annotation) {
        return typed(type, {ctx.NewAtom(pos, value)}, annotation);
    };
    const auto callable = [&](const TTypeAnnotationNode* result,
                              TVector<TCallableExprType::TArgumentInfo> arguments, size_t optional = 0) {
        return ctx.MakeType<TCallableExprType>(result, std::move(arguments), optional, TStringBuf());
    };
    const auto argument = [](const TTypeAnnotationNode* type, TStringBuf name = TStringBuf()) {
        TCallableExprType::TArgumentInfo result;
        result.Type = type;
        result.Name = name;
        result.Flags = 0;
        return result;
    };
    const auto descriptor = [&](const TTypeAnnotationNode* type) {
        auto node = ExpandType(pos, *type, ctx);
        auto annotation = CreateTypeAnnotationTransformer(
            CreateExtCallableTypeAnnotationTransformer(fixture.Types), fixture.Types);
        UNIT_ASSERT(SyncTransform(*annotation, node, ctx) == IGraphTransformer::TStatus::Ok);
        return node;
    };
    const auto udf = [&](TStringBuf name, const TCallableExprType* type,
                         TExprNode::TPtr config, const TTypeAnnotationNode* configType, bool strict = false) {
        TExprNode::TListType settings;
        if (strict) {
            settings.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, "strict")}));
        }
        return typed("Udf", {ctx.NewAtom(pos, name), config, descriptor(voidType),
            ctx.NewAtom(pos, ""), descriptor(type), descriptor(configType),
            ctx.NewAtom(pos, ""), ctx.NewList(pos, std::move(settings))}, type);
    };
    TVector<const TItemExprType*> optionItems;
    for (const TStringBuf name : {"CaseSensitive", "DotNl", "Literal", "LogErrors", "LongestMatch", "MaxMem",
                                 "NeverCapture", "NeverNl", "OneLine", "PerlClasses", "PosixSyntax", "Utf8", "WordBoundary"}) {
        optionItems.push_back(ctx.MakeType<TItemExprType>(name, name == "MaxMem" ? uint64 : boolean));
    }
    const auto* optionsType = ctx.MakeType<TStructExprType>(optionItems);
    const auto* optionalOptions = ctx.MakeType<TOptionalExprType>(optionsType);
    TVector<TCallableExprType::TArgumentInfo> optionArguments;
    for (const TStringBuf name : {"Utf8", "PosixSyntax", "LongestMatch", "LogErrors", "MaxMem", "Literal",
                                 "NeverNl", "DotNl", "NeverCapture", "CaseSensitive", "PerlClasses", "WordBoundary", "OneLine"}) {
        optionArguments.push_back(argument(name == "MaxMem" ? optionalUint64 : optionalBool, name));
    }
    const auto* optionsCallable = callable(optionsType, optionArguments, optionArguments.size());
    auto optionsUdf = udf("Re2.Options", optionsCallable, typed("Void", {}, voidType), voidType, true);
    auto positional = ctx.NewList(pos, {});
    positional->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{}));
    auto field = ctx.NewList(pos, {ctx.NewAtom(pos, "CaseSensitive"),
        typed("Just", {literal("Bool", "true", boolean)}, optionalBool)});
    const auto* namedType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
        ctx.MakeType<TItemExprType>("CaseSensitive", optionalBool)});
    auto options = typed("NamedApply", {optionsUdf, positional, typed("AsStruct", {field}, namedType)}, optionsType);
    const auto* patternCallable = callable(string, {argument(string), argument(optionalString)}, 1);
    const auto makeMatch = [&](TStringBuf patternText) {
        auto pattern = typed("Apply", {udf("Re2.PatternFromLike", patternCallable, typed("Void", {}, voidType), voidType),
            literal("String", patternText, string)}, string);
        const auto* configType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{string, optionalOptions});
        auto config = ctx.NewList(pos, {pattern, typed("Just", {options}, optionalOptions)});
        config->SetTypeAnn(configType);
        const auto* matchCallable = callable(boolean, {argument(optionalString)});
        return typed("AssumeStrict", {udf("Re2.Match", matchCallable, config, configType)}, matchCallable);
    };
    auto match = makeMatch(changedPattern ? "%a%c%" : "%a%b%");

    auto read = fixture.Read();
    read->GetColumns() = {fixture.Key, fixture.Group};
    const auto* text = ctx.MakeType<TDataExprType>(utf8 ? NUdf::EDataSlot::Utf8 : NUdf::EDataSlot::String);
    const auto* optionalText = ctx.MakeType<TOptionalExprType>(text);
    const TTypeAnnotationNode* sourceType = nullable ? static_cast<const TTypeAnnotationNode*>(optionalText) : text;
    const auto* int32 = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    fixture.SetType(*read, {{fixture.Key, int32}, {fixture.Group, sourceType}});
    auto row = ctx.NewArgument(pos, "row");
    row->SetTypeAnn(read->Type->Cast<TListExprType>()->GetItemType());
    auto member = typed("Member", {row, ctx.NewAtom(pos, ToString(fixture.Group))}, sourceType);
    auto bytes = utf8 ? typed("ToString", {member}, nullable
        ? static_cast<const TTypeAnnotationNode*>(optionalString) : string) : member;
    auto input = nullable ? bytes : typed("Just", {bytes}, optionalString);
    if (wrongMemberType) {
        member->SetTypeAnn(nullable ? static_cast<const TTypeAnnotationNode*>(text) : optionalText);
    }
    if (!junction.empty()) {
        fixture.Table->Metadata->Kind = EKikimrTableKind::Olap;
        read->StorageType = NYql::EStorageType::ColumnStorage;
        auto first = typed("Apply", {match, input}, boolean);
        auto second = typed("Apply", {makeMatch("%x%y%"), input}, boolean);
        const TStringBuf operation = junction == "NotOr" ? "Or" : junction;
        auto body = typed(operation, {first, second}, boolean);
        if (junction == "NotOr") {
            body = typed("Not", {body}, boolean);
        }
        TIntrusivePtr<IOperator> plan = read;
        if (pushed) {
            auto argument = ctx.NewArgument(pos, "members_g");
            auto bound = mutation == "wrong_binding" ? ctx.NewArgument(pos, "unbound") : argument;
            TNodeOnNodeOwnedMap replacements{{member.Get(), bound}};
            body = ctx.ReplaceNodes(std::move(body), replacements);
            if (mutation == "typed_body") {
                body->SetTypeAnn(boolean);
            } else if (mutation == "empty_junction") {
                body = ctx.NewCallable(pos, operation, {});
            } else if (mutation == "arbitrary_body") {
                body = ctx.NewCallable(pos, "SomeOtherPredicate", {bound});
            }
            auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {argument}), std::move(body));
            auto column = ctx.NewCallable(pos, "KqpOlapApplyColumnArg", {
                ExpandType(pos, *row->GetTypeAnn(), ctx), ctx.NewAtom(pos, ToString(fixture.Group))});
            auto apply = ctx.NewCallable(pos, "KqpOlapApply", {
                lambda, ctx.NewList(pos, {column}), ctx.NewAtom(pos, "")});
            auto flow = ctx.NewArgument(pos, "flow");
            auto filter = ctx.NewCallable(pos, "KqpOlapFilter", {flow, apply});
            read->OlapFilterLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {flow}), std::move(filter));
        } else {
            auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(body));
            auto filter = MakeIntrusive<TOpFilter>(read, pos, TExpression(lambda, &ctx, &fixture.ExpressionProps));
            filter->Type = read->Type;
            plan = filter;
        }
        TOpRoot root(plan, pos, {{fixture.Key, "key"}, {fixture.Group, "value"}});
        if (pushed && !logicalPushed) {
            read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(read->StorageType);
        }
        return fixture.Export(root);
    }
    auto expression = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), typed("Apply", {match, input}, boolean));
    const auto output = fixture.Registry.Add(TInfoUnit("", "matches"));
    auto map = MakeIntrusive<TOpMap>(read, pos,
        TMapIUs{{output, TMapElement(TExpression(expression, &ctx, &fixture.ExpressionProps))}});
    fixture.SetType(*map, {{fixture.Key, int32}, {fixture.Group, sourceType}, {output, boolean}});
    TOpRoot root(map, pos, {{fixture.Key, "key"}, {output, "matches"}});
    if (staged) {
        fixture.StageSerial(root);
    }
    return fixture.Export(root);
}

TSemanticSnapshotExportResult NativeScalarParameter(
    const TString& type, bool nullable, bool staged, const TString& name = "$p",
    bool mismatchedDescriptor = false, bool divide = false, TString itemDescriptorMutation = {})
{
    TFixture fixture;
    auto& ctx = fixture.Expr;
    const auto pos = TPositionHandle();
    const auto* int32 = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const TTypeAnnotationNode* scalar = type == "Decimal"
        ? static_cast<const TTypeAnnotationNode*>(ctx.MakeType<TDataExprParamsType>(
            NUdf::EDataSlot::Decimal, "15", "2"))
        : ctx.MakeType<TDataExprType>(*NUdf::FindDataSlot(type));
    const TTypeAnnotationNode* valueType = nullable ? ctx.MakeType<TOptionalExprType>(scalar) : scalar;
    TExprNode::TListType descriptorArguments{ctx.NewAtom(pos, type)};
    if (type == "Decimal") {
        descriptorArguments.push_back(ctx.NewAtom(pos, "15"));
        descriptorArguments.push_back(ctx.NewAtom(pos, "2"));
    }
    auto descriptor = ctx.NewCallable(pos, "DataType", std::move(descriptorArguments));
    descriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(scalar));
    if (!itemDescriptorMutation.empty()) {
        UNIT_ASSERT(nullable);
        if (itemDescriptorMutation == "missing") {
            descriptor->SetTypeAnn(nullptr);
        } else if (itemDescriptorMutation == "wrong_kind") {
            descriptor->SetTypeAnn(scalar);
        } else if (itemDescriptorMutation == "wrong_type") {
            descriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Bool)));
        } else if (itemDescriptorMutation == "nested_optional") {
            descriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(ctx.MakeType<TOptionalExprType>(scalar)));
        } else {
            UNIT_FAIL("Unknown parameter descriptor mutation");
        }
    }
    if (nullable) {
        descriptor = ctx.NewCallable(pos, "OptionalType", {descriptor});
        descriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(valueType));
    }
    if (mismatchedDescriptor) {
        descriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Bool)));
    }
    auto body = ctx.NewCallable(pos, "Parameter", {ctx.NewAtom(pos, name), descriptor});
    body->SetTypeAnn(valueType);
    if (divide) {
        UNIT_ASSERT_VALUES_EQUAL(type, "Int32");
        auto one = ctx.NewCallable(pos, "Int32", {ctx.NewAtom(pos, "1")});
        one->SetTypeAnn(int32);
        body = ctx.NewCallable(pos, "/", {body, one});
        valueType = ctx.MakeType<TOptionalExprType>(int32);
        body->SetTypeAnn(valueType);
    }
    auto read = fixture.Read();
    auto row = ctx.NewArgument(pos, "row");
    row->SetTypeAnn(read->Type->Cast<TListExprType>()->GetItemType());
    auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(body));
    lambda->SetTypeAnn(valueType);
    const auto result = fixture.Registry.Add(TInfoUnit("", "parameter_value"));
    auto map = MakeIntrusive<TOpMap>(read, pos,
        TMapIUs{{result, TMapElement(TExpression(lambda, &ctx, &fixture.ExpressionProps))}});
    fixture.SetType(*map, {{fixture.Key, int32}, {fixture.Other, int32}, {result, valueType}});
    TOpRoot root(map, pos, {{fixture.Key, "key"}, {result, "value"}});
    if (staged) {
        fixture.StageSerial(root);
    }
    return fixture.Export(root);
}

TSemanticSnapshotExportResult NativeStringListParameter(
    bool nullableItems, bool nullableLookup, bool ansi, bool staged,
    const TString& mutation = {}, const TString& name = "$list",
    int unsafeTarget = -1, int unsafeKind = 0)
{
    TFixture fixture("String", !nullableLookup);
    auto& ctx = fixture.Expr;
    const auto pos = TPositionHandle();
    const auto* string = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String);
    const auto* boolean = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
    const auto* int32 = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const TTypeAnnotationNode* itemType = mutation == "utf8"
        ? ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Utf8) : string;
    auto dataDescriptor = ctx.NewCallable(pos, "DataType", {
        ctx.NewAtom(pos, mutation == "utf8" ? "Utf8" : "String")});
    dataDescriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(itemType));
    auto itemDescriptor = dataDescriptor;
    if (nullableItems) {
        itemType = ctx.MakeType<TOptionalExprType>(itemType);
        itemDescriptor = ctx.NewCallable(pos, "OptionalType", {dataDescriptor});
        itemDescriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(itemType));
    }
    if (mutation == "nested_optional") {
        itemType = ctx.MakeType<TOptionalExprType>(itemType);
        itemDescriptor = ctx.NewCallable(pos, "OptionalType", {itemDescriptor});
        itemDescriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(itemType));
    }
    const TTypeAnnotationNode* listType = ctx.MakeType<TListExprType>(itemType);
    auto listDescriptor = ctx.NewCallable(pos, "ListType", {itemDescriptor});
    listDescriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(listType));
    if (mutation == "outer_optional") {
        listType = ctx.MakeType<TOptionalExprType>(listType);
        listDescriptor = ctx.NewCallable(pos, "OptionalType", {listDescriptor});
        listDescriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(listType));
    }
    auto parameter = ctx.NewCallable(pos, "Parameter", {ctx.NewAtom(pos, name), listDescriptor});
    parameter->SetTypeAnn(listType);
    if (mutation == "outer_descriptor") {
        listDescriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(boolean));
    } else if (mutation == "item_descriptor") {
        itemDescriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(boolean));
    } else if (mutation == "data_descriptor") {
        dataDescriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(boolean));
    } else if (mutation == "missing_inner_annotation") {
        dataDescriptor->SetTypeAnn(nullptr);
    }
    auto read = fixture.Read();
    read->GetColumns() = {fixture.Key, fixture.Group};
    const TTypeAnnotationNode* lookupType = nullableLookup
        ? static_cast<const TTypeAnnotationNode*>(ctx.MakeType<TOptionalExprType>(string)) : string;
    fixture.SetType(*read, {{fixture.Key, int32}, {fixture.Group, lookupType}});
    auto row = ctx.NewArgument(pos, "row");
    row->SetTypeAnn(read->Type->Cast<TListExprType>()->GetItemType());
    auto lookup = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, ToString(fixture.Group))});
    lookup->SetTypeAnn(lookupType);
    TExprNode::TListType options;
    for (const TString& flag : TVector<TString>{ansi ? "ansi" : "warnNoAnsi", "isCompact"}) {
        options.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, flag)}));
    }
    if (mutation == "duplicate_option") {
        options.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, ansi ? "ansi" : "warnNoAnsi")}));
    } else if (mutation == "tableSource" || mutation == "nullsProcessed" || mutation == "future_option") {
        options.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, mutation)}));
    } else if (mutation == "option_shape") {
        options.push_back(ctx.NewList(pos, {}));
    }
    auto optionTuple = ctx.NewList(pos, std::move(options));
    const bool resultNullable = nullableLookup || (ansi && nullableItems);
    const TTypeAnnotationNode* resultType = (resultNullable != (mutation == "result_nullability"))
        ? static_cast<const TTypeAnnotationNode*>(ctx.MakeType<TOptionalExprType>(boolean)) : boolean;
    auto collection = parameter;
    if (mutation == "transformed_list") {
        collection = ctx.NewCallable(pos, "Reverse", {parameter});
        collection->SetTypeAnn(listType);
    }
    auto body = ctx.NewCallable(pos, "SqlIn", {collection, lookup, optionTuple});
    body->SetTypeAnn(resultType);
    if (mutation == "projection") {
        body = parameter;
        resultType = listType;
    } else if (mutation == "length") {
        body = ctx.NewCallable(pos, "Length", {parameter});
        resultType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64);
        body->SetTypeAnn(resultType);
    }
    TVector<TExprNode*> safetyNodes{body.Get(), parameter.Get(), parameter->Child(0),
        listDescriptor.Get(), itemDescriptor.Get(), dataDescriptor.Get(), dataDescriptor->Child(0),
        optionTuple.Get(), optionTuple->Child(0), optionTuple->Child(0)->Child(0),
        lookup.Get(), lookup->Child(1)};
    if (unsafeTarget >= 0) {
        UNIT_ASSERT(static_cast<size_t>(unsafeTarget) < safetyNodes.size());
        auto* target = safetyNodes[unsafeTarget];
        switch (unsafeKind) {
            case 0: target->SetSideEffects(ESideEffects::General); break;
            case 1: target->SetSideEffects(ESideEffects::SemilatticeRT); break;
            case 2: target->SetResult(ctx.NewAtom(pos, "executed")); break;
            case 3: target->SetPosAware(); break;
            default: UNIT_FAIL("Unknown safety mutation");
        }
    }
    auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(body));
    lambda->SetTypeAnn(resultType);
    const auto output = fixture.Registry.Add(TInfoUnit("", "membership"));
    auto map = MakeIntrusive<TOpMap>(read, pos,
        TMapIUs{{output, TMapElement(TExpression(lambda, &ctx, &fixture.ExpressionProps))}});
    fixture.SetType(*map, {{fixture.Key, int32}, {fixture.Group, lookupType}, {output, resultType}});
    TOpRoot root(map, pos, {{fixture.Key, "key"}, {output, "membership"}});
    if (staged) {
        fixture.StageSerial(root);
    }
    return fixture.Export(root);
}

TSemanticSnapshotExportResult NativeIntegralCoalesce(
    const TString& type, bool coalesce, bool staged, bool changedFallback = false, bool wrongFallbackType = false, const TString& fallbackSource = {})
{
    TFixture fixture(type);
    auto& ctx = fixture.Expr;
    const auto pos = TPositionHandle();
    const auto* scalar = ctx.MakeType<TDataExprType>(*NUdf::FindDataSlot(type));
    const auto* optional = ctx.MakeType<TOptionalExprType>(scalar);
    const auto* int32 = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    auto read = fixture.Read();
    read->GetColumns() = {fixture.Key, fixture.Group};
    fixture.SetType(*read, {{fixture.Key, int32}, {fixture.Group, optional}});
    auto row = ctx.NewArgument(pos, "row");
    row->SetTypeAnn(read->Type->Cast<TListExprType>()->GetItemType());
    auto member = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, ToString(fixture.Group))});
    member->SetTypeAnn(optional);
    auto fallback = ctx.NewCallable(pos, type, {ctx.NewAtom(pos, changedFallback ? "1" : "0")});
    fallback->SetTypeAnn(wrongFallbackType ? ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Bool) : scalar);
    if (!fallbackSource.empty()) {
        auto literal = ctx.NewCallable(pos, fallbackSource, {ctx.NewAtom(pos, changedFallback ? "1" : "0")});
        literal->SetTypeAnn(ctx.MakeType<TDataExprType>(*NUdf::FindDataSlot(fallbackSource)));
        auto descriptor = ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, type)});
        descriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(scalar));
        fallback = ctx.NewCallable(pos, "SafeCast", {literal, descriptor});
        fallback->SetTypeAnn(scalar);
    }
    TExprNode::TPtr body;
    if (coalesce) {
        body = ctx.NewCallable(pos, "Coalesce", {member, fallback});
    } else {
        auto value = ctx.NewArgument(pos, "present");
        value->SetTypeAnn(scalar);
        auto handler = ctx.NewLambda(pos, ctx.NewArguments(pos, {value}), TExprNode::TPtr(value));
        handler->SetTypeAnn(scalar);
        body = ctx.NewCallable(pos, "IfPresent", {member, handler, fallback});
    }
    body->SetTypeAnn(scalar);
    auto expression = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(body));
    const auto output = fixture.Registry.Add(TInfoUnit("", "coalesced"));
    auto map = MakeIntrusive<TOpMap>(read, pos,
        TMapIUs{{output, TMapElement(TExpression(expression, &ctx, &fixture.ExpressionProps))}});
    fixture.SetType(*map, {{fixture.Key, int32}, {fixture.Group, optional}, {output, scalar}});
    TOpRoot root(map, pos, {{fixture.Key, "key"}, {output, "value"}});
    if (staged) {
        fixture.StageSerial(root);
    }
    return fixture.Export(root);
}

TSemanticSnapshotExportResult NativeExactLiteralJust(
    const TString& type, const TString& value, bool cast = false, bool staged = false,
    TStringBuf mutation = {}, ui32 metadataDepth = 0)
{
    TFixture fixture;
    auto& ctx = fixture.Expr;
    const auto pos = TPositionHandle();
    const auto* scalar = ctx.MakeType<TDataExprType>(*NUdf::FindDataSlot(type));
    const auto* optional = ctx.MakeType<TOptionalExprType>(scalar);
    const auto* int32 = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const auto* boolean = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
    const TTypeAnnotationNode* resultType = optional;
    auto read = fixture.Read();
    auto row = ctx.NewArgument(pos, "row");
    row->SetTypeAnn(read->Type->Cast<TListExprType>()->GetItemType());
    auto literal = ctx.NewCallable(pos, type, {ctx.NewAtom(pos, value)});
    literal->SetTypeAnn(scalar);
    TExprNode::TPtr body;
    if (cast) {
        // Match the actual SQL CAST(1 AS Uint64) initial snapshot, including
        // the annotation on both the OptionalType and its DataType child.
        UNIT_ASSERT_VALUES_EQUAL(type, "Uint64");
        literal = ctx.NewCallable(pos, "Int32", {ctx.NewAtom(pos, value)});
        literal->SetTypeAnn(int32);
        auto descriptor = ExpandType(pos, *optional, ctx);
        descriptor->Child(0)->SetTypeAnn(ctx.MakeType<TTypeExprType>(scalar));
        descriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(optional));
        body = ctx.NewCallable(pos, "SafeCast", {literal, descriptor});
    } else {
        if (mutation == "literal_annotation") {
            literal->SetTypeAnn(boolean);
        } else if (mutation == "literal_optional") {
            literal->SetTypeAnn(optional);
        } else if (mutation == "literal_arity") {
            literal = ctx.NewCallable(pos, type, {ctx.NewAtom(pos, value), ctx.NewAtom(pos, value)});
            literal->SetTypeAnn(scalar);
        }
        body = ctx.NewCallable(pos, "Just", {literal});
        if (mutation == "result_annotation") {
            resultType = ctx.MakeType<TOptionalExprType>(boolean);
        } else if (mutation == "result_required") {
            resultType = scalar;
        } else if (mutation == "nested_optional") {
            body->SetTypeAnn(optional);
            body = ctx.NewCallable(pos, "Just", {body});
            resultType = ctx.MakeType<TOptionalExprType>(optional);
        }
    }
    body->SetTypeAnn(resultType);
    // Exercise safety checks independently at Just, literal, and atom depth.
    auto* target = body.Get();
    for (ui32 depth = 0; depth < metadataDepth; ++depth) {
        target = target->Child(0);
    }
    if (mutation == "executed") {
        target->SetResult(ctx.NewAtom(pos, "executed"));
    } else if (mutation == "position") {
        target->SetPosAware();
    } else if (mutation == "semilattice") {
        target->SetSideEffects(ESideEffects::SemilatticeRT);
    } else if (mutation == "general") {
        target->SetSideEffects(ESideEffects::General);
    } else if (mutation == "unordered") {
        target->SetUnorderedChildren();
    }
    auto expression = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(body));
    const auto output = fixture.Registry.Add(TInfoUnit("", "constant"));
    auto map = MakeIntrusive<TOpMap>(read, pos,
        TMapIUs{{output, TMapElement(TExpression(expression, &ctx, &fixture.ExpressionProps))}});
    fixture.SetType(*map, {{fixture.Key, int32}, {fixture.Other, int32}, {output, resultType}});
    TOpRoot root(map, pos, {{fixture.Key, "key"}, {output, "value"}});
    if (staged) {
        fixture.StageSerial(root);
    }
    return fixture.Export(root);
}

TSemanticSnapshotExportResult NativeBooleanCoalesce(bool coalesce, bool staged, bool fallbackTrue = false, bool wrongType = false) {
    TFixture fixture("Bool");
    auto& ctx = fixture.Expr;
    const auto pos = TPositionHandle();
    const auto* boolean = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
    const auto* optional = ctx.MakeType<TOptionalExprType>(boolean);
    const auto* int32 = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    auto read = fixture.Read();
    read->GetColumns() = {fixture.Key, fixture.Group};
    fixture.SetType(*read, {{fixture.Key, int32}, {fixture.Group, optional}});
    auto row = ctx.NewArgument(pos, "row");
    row->SetTypeAnn(read->Type->Cast<TListExprType>()->GetItemType());
    auto member = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, ToString(fixture.Group))});
    member->SetTypeAnn(optional);
    auto fallback = ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, fallbackTrue ? "true" : "false")});
    fallback->SetTypeAnn(wrongType ? int32 : boolean);
    TExprNode::TPtr body;
    if (coalesce) {
        body = ctx.NewCallable(pos, "Coalesce", {member, fallback});
    } else {
        auto value = ctx.NewArgument(pos, "present");
        value->SetTypeAnn(boolean);
        auto handler = ctx.NewLambda(pos, ctx.NewArguments(pos, {value}), TExprNode::TPtr(value));
        handler->SetTypeAnn(boolean);
        body = ctx.NewCallable(pos, "IfPresent", {member, handler, fallback});
    }
    body->SetTypeAnn(boolean);
    auto expression = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(body));
    const auto output = fixture.Registry.Add(TInfoUnit("", "coalesced"));
    auto map = MakeIntrusive<TOpMap>(read, pos,
        TMapIUs{{output, TMapElement(TExpression(expression, &ctx, &fixture.ExpressionProps))}});
    fixture.SetType(*map, {{fixture.Key, int32}, {fixture.Group, optional}, {output, boolean}});
    TOpRoot plan(map, pos, {{fixture.Key, "key"}, {output, "value"}});
    if (staged) {
        fixture.StageSerial(plan);
    }
    return fixture.Export(plan);
}

struct TNativeRowRangeOptions {
    TString KeyType = "Int32";
    std::optional<TString> Lower;
    std::optional<TString> Upper;
    bool LowerInclusive = true;
    bool UpperInclusive = true;
    bool Parameters = false;
    bool PruneKey = false;
    bool Residual = false;
    bool AnnotatedWrappers = true;
    TString Mutation;
};

TSemanticSnapshotExportResult NativeRowLiteralRange(
    const TNativeRowRangeOptions& options, bool pushed, bool staged)
{
    TFixture fixture;
    auto& ctx = fixture.Expr;
    const auto pos = TPositionHandle();
    const auto* keyType = ctx.MakeType<TDataExprType>(*NUdf::FindDataSlot(options.KeyType));
    const auto* int32 = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const auto* uint64 = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64);
    const auto* boolean = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
    const bool nullableKey = options.Mutation == "nullable_key";
    fixture.Table->Metadata->Columns["k"] = TKikimrColumnMetadata("k", 1, options.KeyType, !nullableKey);
    if (options.Mutation == "composite_key") {
        fixture.Table->Metadata->KeyColumnNames = {"k", "x"};
    }
    if (options.Mutation == "system_view") {
        fixture.Table->Metadata->SysView = "unaudited_test_view";
    }
    UNIT_ASSERT(fixture.Table->Load(ctx));
    const TTypeAnnotationNode* emittedKeyType = nullableKey
        ? static_cast<const TTypeAnnotationNode*>(ctx.MakeType<TOptionalExprType>(keyType)) : keyType;
    auto read = fixture.Read();
    fixture.SetType(*read, {{fixture.Key, emittedKeyType}, {fixture.Other, int32}});

    const auto literal = [&](TStringBuf type, const TString& text, const TTypeAnnotationNode* annotation) {
        auto value = ctx.NewCallable(pos, type, {ctx.NewAtom(pos, text)});
        value->SetTypeAnn(annotation);
        return value;
    };
    const auto boundValue = [&](const TString& text) {
        TExprNode::TPtr value;
        if (options.Parameters) {
            auto descriptor = ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, options.KeyType)});
            descriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(keyType));
            const TTypeAnnotationNode* parameterType = keyType;
            if (options.Mutation == "optional_parameter") {
                parameterType = ctx.MakeType<TOptionalExprType>(keyType);
                descriptor = ctx.NewCallable(pos, "OptionalType", {descriptor});
                descriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(parameterType));
            }
            value = ctx.NewCallable(pos, "Parameter", {ctx.NewAtom(pos, text), descriptor});
            value->SetTypeAnn(parameterType);
        } else {
            value = literal(options.KeyType, text, keyType);
        }
        if (options.Mutation == "missing_annotation") {
            value->SetTypeAnn(nullptr);
        } else if (options.Mutation == "wrong_annotation") {
            value->SetTypeAnn(boolean);
        } else if (options.Mutation == "computed_bound") {
            value = ctx.NewCallable(pos, "+", {value, literal(options.KeyType, "0", keyType)});
            value->SetTypeAnn(keyType);
        } else if (options.Mutation == "null_bound") {
            auto descriptor = ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, options.KeyType)});
            descriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(keyType));
            const auto* optional = ctx.MakeType<TOptionalExprType>(keyType);
            descriptor = ctx.NewCallable(pos, "OptionalType", {descriptor});
            descriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(optional));
            value = ctx.NewCallable(pos, "Nothing", {descriptor});
            value->SetTypeAnn(optional);
        } else if (options.Mutation == "unsafe_bound") {
            value->SetSideEffects(ESideEffects::General);
        }
        return value;
    };
    const auto lower = options.Lower ? boundValue(*options.Lower) : TExprNode::TPtr();
    const auto upper = options.Upper ? boundValue(*options.Upper) : TExprNode::TPtr();
    if (pushed) {
        const auto boundary = [&](bool inclusive, const TExprNode::TPtr& value) {
            TExprNode::TListType children;
            TTypeAnnotationNode::TListType itemTypes;
            if (value) {
                children.push_back(value);
                itemTypes.push_back(value->GetTypeAnn() ? value->GetTypeAnn().Get() : keyType);
            }
            if (options.Mutation == "boundary_arity") {
                children.push_back(literal(options.KeyType, "7", keyType));
                itemTypes.push_back(keyType);
            }
            auto result = ctx.NewCallable(pos,
                options.Mutation == "boundary_tag" ? "WrongKeyBound" : inclusive ? "KqlKeyInc" : "KqlKeyExc",
                std::move(children));
            if (options.AnnotatedWrappers) {
                result->SetTypeAnn(ctx.MakeType<TTupleExprType>(itemTypes));
            }
            if (options.Mutation == "boundary_annotation") {
                result->SetTypeAnn(boolean);
            }
            return result;
        };
        auto from = boundary(options.LowerInclusive, lower);
        auto to = boundary(options.UpperInclusive, upper);
        auto range = ctx.NewList(pos, {from, to});
        if (options.AnnotatedWrappers) {
            range->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
                from->GetTypeAnn().Get(), to->GetTypeAnn().Get()}));
        }
        if (options.Mutation == "outer_annotation") {
            range->SetTypeAnn(boolean);
        } else if (options.Mutation == "outer_shape") {
            range = ctx.NewCallable(pos, "KqlKeyRange", {from, to});
        } else if (options.Mutation == "unordered_bounds") {
            range->SetUnorderedChildren();
        }

        // Deliberately disagree with the literal interval: row physical
        // conversion selects LiteralRange, never this otherwise valid point.
        auto descriptor = ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, options.KeyType)});
        descriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(keyType));
        auto point = ctx.NewCallable(pos, "RangeFor", {
            ctx.NewAtom(pos, "==="), literal("Int32", "999", int32), descriptor});
        auto rangeUnion = ctx.NewCallable(pos, "RangeUnion", {point});
        auto multiply = ctx.NewCallable(pos, "RangeMultiply", {
            literal("Uint64", "10000", uint64), rangeUnion});
        read->RangeInfo = TOpRead::TRangeInfo{
            .ComputeNode = ctx.NewCallable(pos, "RangeFinalize", {multiply}),
            .KeyColumns = options.Mutation == "composite_key" ? TVector<TString>{"k", "x"} :
                TVector<TString>{options.Mutation == "wrong_key" ? "x" : "k"},
            .UsedPrefixLen = 1,
            .ExpectedMaxRanges = options.Mutation == "range_count" ? 2 : 1,
            .LiteralRange = options.Mutation == "missing_literal" ? TExprNode::TPtr() : range,
        };
        if (options.PruneKey) {
            read->GetColumns() = {fixture.Other};
            fixture.SetType(*read, {{fixture.Other, int32}});
        }
    }

    auto row = ctx.NewArgument(pos, "row");
    row->SetTypeAnn(read->Type->Cast<TListExprType>()->GetItemType());
    const auto member = [&](TInfoUnitId id, const TTypeAnnotationNode* type) {
        auto result = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, ToString(id))});
        result->SetTypeAnn(type);
        return result;
    };
    TExprNode::TListType predicates;
    if (!pushed) {
        if (lower) {
            auto comparison = ctx.NewCallable(pos, options.LowerInclusive ? ">=" : ">", {member(fixture.Key, keyType), lower});
            comparison->SetTypeAnn(boolean);
            predicates.push_back(comparison);
        }
        if (upper) {
            auto comparison = ctx.NewCallable(pos, options.UpperInclusive ? "<=" : "<", {member(fixture.Key, keyType), upper});
            comparison->SetTypeAnn(boolean);
            predicates.push_back(comparison);
        }
    }
    if (options.Residual) {
        auto comparison = ctx.NewCallable(pos, ">", {member(fixture.Other, int32), literal("Int32", "0", int32)});
        comparison->SetTypeAnn(boolean);
        predicates.push_back(comparison);
    }
    auto predicate = predicates.empty() ? literal("Bool", "true", boolean) :
        predicates.size() == 1 ? predicates.front() : ctx.NewCallable(pos, "And", std::move(predicates));
    predicate->SetTypeAnn(boolean);
    auto expression = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(predicate));
    auto filter = MakeIntrusive<TOpFilter>(read, pos,
        TExpression(expression, &ctx, &fixture.ExpressionProps));
    filter->Type = read->Type;
    TVector<std::pair<TInfoUnitId, TString>> outputs{{fixture.Other, "value"}};
    if (!options.PruneKey) {
        outputs.insert(outputs.begin(), {fixture.Key, "key"});
    }
    TOpRoot root(filter, pos, TOrderedIUs<TString>(outputs.begin(), outputs.end()));
    if (staged) {
        fixture.StageSerial(root);
    }
    return fixture.Export(root);
}

TSemanticSnapshotExportResult NativeInt32ReadPoint(bool pushed, bool staged, i32 pointValue,
    bool wrongDescriptor = false, bool pruneKey = false)
{
    TFixture fixture;
    const auto pos = TPositionHandle();
    auto& ctx = fixture.Expr;
    const auto* int32 = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const auto* uint64 = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64);
    const auto* boolean = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
    auto read = fixture.Read();
    read->StorageType = NYql::EStorageType::ColumnStorage;
    auto value = ctx.NewCallable(pos, "Int32", {ctx.NewAtom(pos, ToString(pointValue))});
    value->SetTypeAnn(int32);
    TIntrusivePtr<IOperator> input = read;
    if (pushed) {
        auto descriptor = ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, wrongDescriptor ? "Int64" : "Int32")});
        descriptor->SetTypeAnn(ctx.MakeType<TTypeExprType>(wrongDescriptor
            ? ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Int64) : int32));
        auto point = ctx.NewCallable(pos, "RangeFor", {ctx.NewAtom(pos, "==="), value, descriptor});
        auto ranges = ctx.NewCallable(pos, "RangeUnion", {point});
        auto cap = ctx.NewCallable(pos, "Uint64", {ctx.NewAtom(pos, "10000")});
        cap->SetTypeAnn(uint64);
        auto multiply = ctx.NewCallable(pos, "RangeMultiply", {cap, ranges});
        read->RangeInfo = TOpRead::TRangeInfo{
            .ComputeNode = ctx.NewCallable(pos, "RangeFinalize", {multiply}),
            .KeyColumns = {"k"},
            .UsedPrefixLen = 1,
            .ExpectedMaxRanges = 1,
        };
    } else {
        auto row = ctx.NewArgument(pos, "row");
        row->SetTypeAnn(read->Type->Cast<TListExprType>()->GetItemType());
        auto member = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, ToString(fixture.Key))});
        member->SetTypeAnn(int32);
        auto comparison = ctx.NewCallable(pos, "==", {member, value});
        comparison->SetTypeAnn(boolean);
        auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(comparison));
        auto filter = MakeIntrusive<TOpFilter>(read, pos,
            TExpression(lambda, &ctx, &fixture.ExpressionProps));
        filter->Type = read->Type;
        input = filter;
    }
    if (pushed && pruneKey) {
        read->GetColumns() = {fixture.Other};
        fixture.SetType(*read, {{fixture.Other, int32}});
    }
    TVector<std::pair<TInfoUnitId, TString>> resultColumns{{fixture.Other, "value"}};
    if (!pruneKey) {
        resultColumns.insert(resultColumns.begin(), {fixture.Key, "key"});
    }
    TOpRoot root(input, pos, TOrderedIUs<TString>(resultColumns.begin(), resultColumns.end()));
    if (staged) {
        if (pushed) {
            read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(read->StorageType);
        } else {
            fixture.StageSerial(root);
        }
    }
    return fixture.Export(root);
}

TSemanticSnapshotExportResult NativeDistinctAliases(bool nullable, bool aliases,
    bool reverseTraits = false, bool reverseKeys = false, bool staged = false,
    bool swapValues = false, bool duplicateInput = false)
{
    TFixture fixture("Int32", !nullable);
    const auto pos = TPositionHandle();
    const auto* int32 = fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const TTypeAnnotationNode* groupType = nullable
        ? static_cast<const TTypeAnnotationNode*>(fixture.Expr.MakeType<TOptionalExprType>(int32)) : int32;
    auto read = fixture.Read();
    read->GetColumns().Add(fixture.Group);
    fixture.SetType(*read, {{fixture.Key, int32}, {fixture.Other, int32}, {fixture.Group, groupType}});
    const auto early = fixture.Registry.Add(TInfoUnit("", reverseTraits ? "second" : "first"));
    const auto late = fixture.Registry.Add(TInfoUnit("", reverseTraits ? "first" : "second"));
    const auto first = reverseTraits ? late : early;
    const auto second = reverseTraits ? early : late;
    TAggregationIUs traits;
    if (aliases) {
        const auto firstSource = duplicateInput ? fixture.Group : fixture.Other;
        if (reverseTraits) {
            traits.Add(second, TOpAggregationTraits{fixture.Group, "distinct", false, false});
            traits.Add(first, TOpAggregationTraits{firstSource, "distinct", false, false});
        } else {
            traits.Add(first, TOpAggregationTraits{firstSource, "distinct", false, false});
            traits.Add(second, TOpAggregationTraits{fixture.Group, "distinct", false, false});
        }
    }
    const auto keys = reverseKeys ? TOrderedIUs<>{fixture.Group, fixture.Other}
                                 : TOrderedIUs<>{fixture.Other, fixture.Group};
    auto aggregate = MakeIntrusive<TOpAggregate>(read, std::move(traits), keys,
        EOpPhase::Undefined, aliases, pos);
    const auto firstOutput = aliases ? first : fixture.Other;
    const auto secondOutput = aliases ? second : fixture.Group;
    fixture.SetType(*aggregate, {{firstOutput, int32}, {secondOutput, groupType}});
    TOpRoot root(aggregate, pos, {{swapValues ? secondOutput : firstOutput, "first"},
                                {swapValues ? firstOutput : secondOutput, "second"}});
    if (staged) {
        fixture.StageSerial(root);
    }
    return fixture.Export(root);
}

TSemanticSnapshotExportResult NativeRepeatedDistinctAliases(bool nullable, bool aliases,
    bool duplicateKeys = false, bool reverseKeys = false, bool staged = false,
    TStringBuf mutation = {})
{
    TFixture fixture("Int32", !nullable);
    const auto pos = TPositionHandle();
    const auto* int32 = fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const TTypeAnnotationNode* groupType = nullable
        ? static_cast<const TTypeAnnotationNode*>(fixture.Expr.MakeType<TOptionalExprType>(int32)) : int32;
    auto read = fixture.Read();
    read->GetColumns().Add(fixture.Group);
    fixture.SetType(*read, {{fixture.Key, int32}, {fixture.Other, int32}, {fixture.Group, groupType}});
    // Allocate output IDs in a different order from the root projection.
    const auto third = fixture.Registry.Add(TInfoUnit("", "third"));
    const auto first = fixture.Registry.Add(TInfoUnit("", "first"));
    const auto second = fixture.Registry.Add(TInfoUnit("", "second"));
    TAggregationIUs traits;
    if (aliases) {
        traits.Add(first, TOpAggregationTraits{fixture.Group, "distinct", false, false});
        traits.Add(second, TOpAggregationTraits{
            mutation == "missing_key" ? fixture.Group : fixture.Other, "distinct", false, false});
        const auto thirdSource = mutation == "foreign_key" ? fixture.Key
            : mutation == "wrong_binding" ? fixture.Other : fixture.Group;
        traits.Add(third, TOpAggregationTraits{thirdSource,
            mutation == "wrong_function" ? "min" : "distinct", mutation == "distinct_flag", false});
    }
    auto keys = reverseKeys ? TOrderedIUs<>{fixture.Group, fixture.Other}
                            : TOrderedIUs<>{fixture.Other, fixture.Group};
    if (duplicateKeys) {
        keys.Append(fixture.Group);
    }
    auto aggregate = MakeIntrusive<TOpAggregate>(read, std::move(traits), keys,
        EOpPhase::Undefined, aliases, pos);
    const auto firstOutput = aliases ? first : fixture.Group;
    const auto secondOutput = aliases ? second : fixture.Other;
    const auto thirdOutput = aliases ? third : fixture.Group;
    if (aliases) {
        const TTypeAnnotationNode* thirdType = mutation == "wrong_type"
            ? fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64)
            : mutation == "wrong_nullability" ? int32 : groupType;
        TVector<std::pair<TInfoUnitId, const TTypeAnnotationNode*>> columns{
            {first, groupType}, {second, int32}, {third, thirdType}};
        if (mutation == "missing_column") {
            columns.pop_back();
        } else if (mutation == "extra_column") {
            columns.emplace_back(fixture.Key, int32);
        }
        fixture.SetType(*aggregate, columns);
    } else {
        fixture.SetType(*aggregate, {{fixture.Other, int32}, {fixture.Group, groupType}});
    }
    if (mutation == "remove_distinct") {
        TOpRoot root(read, pos, {{fixture.Group, "first"}, {fixture.Other, "second"}, {fixture.Group, "third"}});
        if (staged) {
            read->Props.StageId = root.PlanProps.StageGraph.AddSourceStage(read->StorageType);
        }
        return fixture.Export(root);
    }
    TOpRoot root(aggregate, pos, {{firstOutput, "first"}, {secondOutput, "second"}, {thirdOutput, "third"}});
    if (staged) {
        fixture.StageSerial(root);
    }
    return fixture.Export(root);
}


std::pair<TSemanticSnapshotExportResult, TSemanticSnapshotExportResult> NativeNullableCountDistinct(
    bool grouped, bool nullable = true, bool removeDistinct = false)
{
    TFixture fixture("Int32", !nullable);
    const auto pos = TPositionHandle();
    auto read = fixture.Read();
    read->GetColumns().Add(fixture.Group);
    const auto* int32 = fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const TTypeAnnotationNode* valueType = nullable
        ? static_cast<const TTypeAnnotationNode*>(fixture.Expr.MakeType<TOptionalExprType>(int32)) : int32;
    fixture.SetType(*read, {{fixture.Key, int32}, {fixture.Other, int32}, {fixture.Group, valueType}});
    const auto count = fixture.Registry.Add(TInfoUnit("", "count"));
    auto aggregate = MakeIntrusive<TOpAggregate>(read,
        TAggregationIUs{{count, {fixture.Group, "count", true, false}}},
        grouped ? TOrderedIUs<>{fixture.Other} : TOrderedIUs<>{}, EOpPhase::Undefined, false, pos);
    TOpRoot root(aggregate, pos, {{count, "count"}});
    root.PlanProps.InfoUnitRegistry = fixture.Registry;
    auto annotation = CreateTypeAnnotationTransformer(
        CreateExtCallableTypeAnnotationTransformer(fixture.Types), fixture.Types);
    TRBOContext rbo(fixture.Optimize, fixture.Expr, fixture.Types, *annotation, *fixture.Functions);
    root.RecomputeOutputIUsSubtree();
    UNIT_ASSERT(root.ComputeTypes(rbo) == IGraphTransformer::TStatus::Ok);
    auto initial = ExportSemanticSnapshotV1(root, rbo);
    Parse(initial);
    if (removeDistinct) {
        aggregate->SetAggregationTraits(TAggregationIUs{{count, {fixture.Group, "count", false, false}}});
    } else {
        TExpandDistinctAggregationRule rule;
        root.SetInput(rule.SimpleMatchAndApply(aggregate, rbo, root.PlanProps));
    }
    root.RecomputeOutputIUsSubtree();
    root.ComputeParents();
    UNIT_ASSERT(root.ComputeTypes(rbo) == IGraphTransformer::TStatus::Ok);
    fixture.StageSerial(root);
    auto expanded = ExportSemanticSnapshotV1(root, rbo);
    Parse(expanded);
    return {std::move(initial), std::move(expanded)};
}

std::pair<TSemanticSnapshotExportResult, TSemanticSnapshotExportResult> NativeDecimalAverageDistinct(
    bool extraDistinct, bool corruptCount = false, bool staged = true, ui32 malformedFinal = 0)
{
    TFixture fixture;
    const auto pos = TPositionHandle();
    auto read = fixture.Read();
    read->GetColumns().Add(fixture.Price);
    const auto* int32 = fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
    const auto* priceType = fixture.Expr.MakeType<TOptionalExprType>(
        fixture.Expr.MakeType<TDataExprParamsType>(NUdf::EDataSlot::Decimal, "15", "2"));
    fixture.SetType(*read, {{fixture.Key, int32}, {fixture.Other, int32}, {fixture.Price, priceType}});
    const auto average = fixture.Registry.Add(TInfoUnit("", "average"));
    const auto count = fixture.Registry.Add(TInfoUnit("", "count"));
    const auto distinct = fixture.Registry.Add(TInfoUnit("", "distinct_count"));
    const auto extra = fixture.Registry.Add(TInfoUnit("", "extra_distinct_count"));
    TAggregationIUs traits{{average, {fixture.Price, "avg", false, false}},
        {count, {fixture.Price, "count", false, false}}, {distinct, {fixture.Other, "count", true, false}}};
    if (extraDistinct) {
        traits.Add(extra, TOpAggregationTraits{fixture.Key, "count", true, false});
    }
    auto aggregate = MakeIntrusive<TOpAggregate>(read, std::move(traits), TOrderedIUs<>{},
        EOpPhase::Undefined, false, pos);
    TOrderedIUs<TString> outputs{{average, "average"}, {count, "count"}, {distinct, "distinct_count"}};
    if (extraDistinct) {
        outputs.Append(extra, "extra_distinct_count");
    }
    TOpRoot root(aggregate, pos, std::move(outputs));
    root.PlanProps.InfoUnitRegistry = fixture.Registry;
    auto annotation = CreateTypeAnnotationTransformer(
        CreateExtCallableTypeAnnotationTransformer(fixture.Types), fixture.Types);
    TRBOContext rbo(fixture.Optimize, fixture.Expr, fixture.Types, *annotation, *fixture.Functions);
    root.RecomputeOutputIUsSubtree();
    root.ComputeParents();
    UNIT_ASSERT(root.ComputeTypes(rbo) == IGraphTransformer::TStatus::Ok);
    auto initial = ExportSemanticSnapshotV1(root, rbo);
    Parse(initial);
    TExpandDistinctAggregationRule expand;
    root.SetInput(expand.SimpleMatchAndApply(aggregate, rbo, root.PlanProps));
    auto finalAggregate = CastOperator<TOpAggregate>(root.GetInput());
    UNIT_ASSERT_VALUES_EQUAL(finalAggregate->GetInput()->GetChildCount(), extraDistinct ? 3 : 2);
    if (corruptCount) {
        auto finalTraits = finalAggregate->GetAggregationTraits();
        auto corrupted = finalTraits.At(distinct);
        corrupted.Input = finalTraits.At(count).Input;
        finalTraits.Replace(distinct, std::move(corrupted));
        finalAggregate->SetAggregationTraits(std::move(finalTraits));
    }
    TVector<std::unique_ptr<IRule>> rules;
    rules.emplace_back(std::make_unique<TExpandReplicateRule>());
    TRuleBasedStage("Expand shared average inputs", std::move(rules)).RunStage(root, rbo);
    TGlobalInliningStage("Inline average input aliases").RunStage(root, rbo);
    root.RecomputeOutputIUsSubtree();
    root.ComputeParents();
    UNIT_ASSERT(root.ComputeTypes(rbo) == IGraphTransformer::TStatus::Ok);
    if (staged) {
        TAssignStagesStage().RunStage(root, rbo);
        TPropagateHashFuncStage().RunStage(root, rbo);
    }
    if (malformedFinal) {
        const TTypeAnnotationNode* bad = nullptr;
        if (malformedFinal == 1) {
            bad = fixture.Expr.MakeType<TDataExprParamsType>(NUdf::EDataSlot::Decimal, "35", "2");
        } else {
            const auto* sumType = fixture.Expr.MakeType<TDataExprParamsType>(
                NUdf::EDataSlot::Decimal, malformedFinal == 2 ? "34" : "35", "2");
            const auto* countType = fixture.Expr.MakeType<TDataExprType>(
                malformedFinal == 3 ? NUdf::EDataSlot::Int64 : NUdf::EDataSlot::Uint64);
            bad = fixture.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{sumType, countType});
        }
        bad = fixture.Expr.MakeType<TOptionalExprType>(bad);
        TVector<const TItemExprType*> fields;
        for (const auto* item : finalAggregate->Type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()->GetItems()) {
            fields.push_back(item->GetName() == ToString(average)
                ? fixture.Expr.MakeType<TItemExprType>(item->GetName(), bad) : item);
        }
        finalAggregate->Type = fixture.Expr.MakeType<TListExprType>(fixture.Expr.MakeType<TStructExprType>(fields));
    }
    auto final = ExportSemanticSnapshotV1(root, rbo);
    if (!malformedFinal) { Parse(final); }
    return {std::move(initial), std::move(final)};
}

TSemanticSnapshotExportResult StagedRead(ui32 firstStage) {
    TFixture fixture;
    auto read = fixture.Read();
    TOpRoot root(read, TPositionHandle(), {{fixture.Key, "key"}});
    auto& graph = root.PlanProps.StageGraph;
    graph.StageCounter = firstStage;
    read->Props.StageId = graph.AddSourceStage(NYql::EStorageType::RowStorage);
    auto exported = fixture.Export(root);
    UNIT_ASSERT_VALUES_EQUAL(graph.StageIds.front(), firstStage);
    UNIT_ASSERT_VALUES_EQUAL(*read->Props.StageId, firstStage);
    return exported;
}

// Same schema family as compilation corpus 1487: nullable Bool index key,
// required Utf8 base key, and nullable String fetched payload.
std::pair<TSemanticSnapshotExportResult, TSemanticSnapshotExportResult> NativeSyncIndexLookup(
    bool staged, TStringBuf mutation = {}, bool uniqueIndex = false)
{
    TFixture f("Bool", false, "String");
    const auto pos = TPositionHandle();
    auto& ctx = f.Expr;
    auto& meta = *f.Table->Metadata;
    meta.Columns.at("k") = TKikimrColumnMetadata("k", 1, "Utf8", true);
    meta.Columns.at("x") = TKikimrColumnMetadata("x", 2, "String", false);
    auto& index = f.Tables->GetOrAddTable("ut", "/Root", "/Root/NativeSnapshot/by_flag/indexImplTable");
    index.Metadata = MakeIntrusive<TKikimrTableMetadata>("ut", "/Root/NativeSnapshot/by_flag/indexImplTable");
    auto& impl = *index.Metadata;
    impl.DoesExist = true;
    impl.PathId = TKikimrPathId(1, 2);
    impl.SchemaVersion = 1;
    impl.Kind = EKikimrTableKind::Datashard;
    impl.KeyColumnNames = {"g", "k"};
    impl.ColumnOrder = {"g", "k"};
    impl.Columns.emplace("g", TKikimrColumnMetadata("g", 1, "Bool", false));
    impl.Columns.emplace("k", TKikimrColumnMetadata("k", 2, "Utf8", true));
    if (mutation == "index_type") { impl.Columns.at("g").Type = "String"; }
    if (mutation == "index_nullable") { impl.Columns.at("g").NotNull = true; }
    if (mutation == "index_pk_order") { impl.KeyColumnNames = {"k", "g"}; }
    if (mutation == "index_missing_column") { impl.Columns.erase("k"); }
    meta.Indexes.emplace_back("by_flag", TVector<TString>{"g"}, TVector<TString>{},
        mutation == "async" ? TIndexDescription::EType::GlobalAsync :
        uniqueIndex ? TIndexDescription::EType::GlobalSyncUnique : TIndexDescription::EType::GlobalSync,
        mutation == "not_ready" ? TIndexDescription::EIndexState::NotReady : TIndexDescription::EIndexState::Ready,
        1, 2, 1, TIndexDescription::TSpecializedIndexDescription{});
    meta.ImplTables.push_back(index.Metadata);
    if (mutation == "extra_unique") {
        auto unique = MakeIntrusive<TKikimrTableMetadata>("ut", "/Root/NativeSnapshot/unique_payload/indexImplTable");
        unique->DoesExist = true;
        unique->Kind = EKikimrTableKind::Datashard;
        unique->PathId = TKikimrPathId(1, 4);
        unique->SchemaVersion = 1;
        unique->KeyColumnNames = {"x", "k"};
        unique->ColumnOrder = unique->KeyColumnNames;
        for (const auto& name : unique->ColumnOrder) { unique->Columns.emplace(name, meta.Columns.at(name)); }
        meta.Indexes.emplace_back("unique_payload", TVector<TString>{"x"}, TVector<TString>{},
            TIndexDescription::EType::GlobalSyncUnique, TIndexDescription::EIndexState::Ready,
            1, 3, 1, TIndexDescription::TSpecializedIndexDescription{});
        meta.ImplTables.push_back(unique);
    }

    const auto* boolean = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
    const auto* optionalBool = ctx.MakeType<TOptionalExprType>(boolean);
    const auto* utf8 = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Utf8);
    const auto* payload = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String));
    auto baseTable = NOpt::BuildTableMeta(*f.Table, pos, ctx).Ptr();
    auto base = MakeIntrusive<TOpRead>("same", TUnorderedIUs{f.Key, f.Group, f.Other},
        NYql::EStorageType::RowStorage, baseTable, nullptr, nullptr, std::nullopt, std::nullopt,
        ESortDir::None, TPhysicalOpProps{}, pos);
    f.SetType(*base, {{f.Key, utf8}, {f.Group, optionalBool}, {f.Other, payload}});
    auto row = ctx.NewArgument(pos, "row");
    row->SetTypeAnn(base->Type->Cast<TListExprType>()->GetItemType());
    auto flag = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, ToString(f.Group))});
    flag->SetTypeAnn(optionalBool);
    auto no = ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, "false")});
    no->SetTypeAnn(boolean);
    auto predicate = ctx.NewCallable(pos, "Coalesce", {flag, no});
    predicate->SetTypeAnn(boolean);
    auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(predicate));
    auto filter = MakeIntrusive<TOpFilter>(base, pos, TExpression(lambda, &ctx, &f.ExpressionProps));
    filter->Type = base->Type;
    TOpRoot beforeRoot(filter, pos, {{f.Other, "value"}});
    beforeRoot.PlanProps.InfoUnitRegistry = f.Registry;
    const auto catalog = CaptureSemanticSnapshotCatalogV1(beforeRoot, f.Rbo);
    UNIT_ASSERT_C(catalog.IsSupported(), catalog.UnsupportedReason);
    const bool excludedIndex = mutation == "index_type" || mutation == "index_nullable" ||
        mutation == "index_pk_order" || mutation == "index_missing_column" ||
        mutation == "async" || mutation == "not_ready";
    // Unsupported index metadata must not destroy an otherwise valid initial
    // base-table snapshot. Final then fails because the index is absent.
    UNIT_ASSERT_VALUES_EQUAL(catalog.Catalog.Tables.size(), excludedIndex ? 1 : mutation == "extra_unique" ? 3 : 2);
    auto before = ExportSemanticSnapshotV1(beforeRoot, f.Rbo, catalog.Catalog);

    const auto probeKey = f.Registry.Add(TInfoUnit("index", "k"));
    const auto probeFlag = f.Registry.Add(TInfoUnit("index", "g"));
    const auto prefixValue = mutation == "changed_prefix" ? "false" : "true";
    auto lowerValue = ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, prefixValue)});
    lowerValue->SetTypeAnn(mutation == "nullable_boundary" ? static_cast<const TTypeAnnotationNode*>(optionalBool) : boolean);
    auto upperValue = ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, mutation == "different_upper" ? "false" : prefixValue)});
    upperValue->SetTypeAnn(boolean);
    if (mutation == "null_boundary") {
        lowerValue = ctx.NewCallable(pos, "Nothing", {ExpandType(pos, *optionalBool, ctx)});
        lowerValue->SetTypeAnn(optionalBool);
    }
    if (mutation == "diagnostic_long_reason") {
        lowerValue = ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, TString(70 * 1024, 'x'))});
        lowerValue->SetTypeAnn(boolean);
    }
    auto range = ctx.NewList(pos, {
        ctx.NewCallable(pos, mutation == "exclusive_boundary" ? "KqlKeyExc" : "KqlKeyInc", {lowerValue}),
        ctx.NewCallable(pos, "KqlKeyInc", {upperValue})});
    auto read = MakeIntrusive<TOpRead>("index", TUnorderedIUs{probeKey, probeFlag}, NYql::EStorageType::RowStorage,
        NOpt::BuildTableMeta(index, pos, ctx).Ptr(), nullptr, nullptr,
        TOpRead::TRangeInfo{.ComputeNode = range, .KeyColumns = {"g", "k"},
            .UsedPrefixLen = 1, .PointPrefixLen = 1, .ExpectedMaxRanges = 1, .LiteralRange = range},
        std::nullopt, ESortDir::None, TPhysicalOpProps{}, pos);
    f.SetType(*read, {{probeKey, utf8}, {probeFlag, optionalBool}});
    if (mutation == "range_key_order") { read->RangeInfo->KeyColumns = {"k", "g"}; }
    if (mutation == "range_prefix_len") { read->RangeInfo->UsedPrefixLen = 2; }
    if (mutation == "diagnostic_compute_only") { read->RangeInfo->LiteralRange.Reset(); }
    TOpTableLookup::TLookupKeys keys;
    if (mutation != "missing_key") { keys.Append(mutation == "wrong_key" ? probeFlag : probeKey, "k"); }
    auto lookup = MakeIntrusive<TOpTableLookup>(read, pos, baseTable, TUnorderedIUs{f.Other}, std::move(keys));
    f.SetType(*lookup, {{f.Other, payload}});
    if (mutation == "allow_null_setting") { lookup->AllowNullKeys = true; }
    if (mutation == "input_lookup") { lookup->KeysFromInputLookup = true; }
    if (mutation == "join") { lookup->Strategy = ELookupStrategy::LookupJoinRows; }
    if (mutation == "prefix") { lookup->Prefix.emplace(); }
    if (mutation == "fetched_type") { f.SetType(*lookup, {{f.Other, utf8}}); }
    TOpRoot afterRoot(lookup, pos, {{f.Other, "value"}});
    afterRoot.PlanProps.InfoUnitRegistry = f.Registry;
    if (staged) {
        auto& graph = afterRoot.PlanProps.StageGraph;
        const auto source = graph.AddSourceStage(NYql::EStorageType::RowStorage);
        const auto sink = graph.AddStage();
        read->Props.StageId = source;
        lookup->Props.StageId = sink;
        auto fetched = ctx.NewList(pos, mutation == "stream_columns" ? TExprNode::TListType{} :
            TExprNode::TListType{ctx.NewAtom(pos, "x")});
        auto settings = ctx.NewList(pos, {ctx.NewList(pos, {ctx.NewAtom(pos, "Strategy"),
            ctx.NewAtom(pos, mutation == "stream_strategy" ? "LookupUniqueRows" : "LookupRows")})});
        const auto* inputRow = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
            ctx.MakeType<TItemExprType>("k", mutation == "stream_type" ? boolean : utf8)});
        auto inputType = ExpandType(pos, *ctx.MakeType<TListExprType>(inputRow), ctx);
        if (mutation == "stream_annotation") {
            inputType->SetTypeAnn(ctx.MakeType<TTypeExprType>(boolean));
        }
        if (mutation == "stream_map") {
            graph.Connect(source, sink, MakeIntrusive<TMapConnection>(graph.GetOutputIndex(source)));
        } else {
            graph.Connect(source, sink, MakeIntrusive<TStreamLookupConnection>(graph.GetOutputIndex(source),
                mutation == "stream_table" ? read->TableCallable : baseTable, fetched, inputType, settings));
        }
    }
    return {std::move(before), ExportSemanticSnapshotV1(afterRoot, f.Rbo, catalog.Catalog)};
}

// Only k/x are projected, while hidden g/price carry an unselected UNIQUE law.
TSemanticSnapshotCatalogCaptureResult NativeUniqueConstraintCatalog(TStringBuf mutation = {}) {
    TFixture f;
    auto& metadata = *f.Table->Metadata;
    TVector<TString> keys = {"g", "price"};
    if (mutation == "overlap_key") { keys = {"g", "k", "price"}; }
    if (mutation == "composite_pk") { metadata.KeyColumnNames = {"x", "k"}; }
    if (mutation == "empty_keys") { keys.clear(); }
    if (mutation == "duplicate_key") { keys = {"g", "g"}; }
    if (mutation == "unknown_key") { keys = {"missing"}; }
    if (mutation == "unsupported_type") { metadata.Columns.at("g").Type = "Double"; }
    if (mutation == "base_build") { metadata.Columns.at("g").IsBuildInProgress = true; }
    if (mutation == "base_not_null_transition") { metadata.Columns.at("g").SetNotNullInProgress = true; }
    if (mutation == "base_kind") { metadata.Kind = EKikimrTableKind::Olap; }
    if (mutation == "base_missing") { metadata.DoesExist = false; }
    if (mutation == "base_no_pk") { metadata.KeyColumnNames.clear(); }
    if (mutation == "direct_index_impl") { metadata.IsIndexImplTable = true; }
    auto impl = MakeIntrusive<TKikimrTableMetadata>("ut", "/Root/NativeSnapshot/unique_hidden/indexImplTable");
    impl->DoesExist = true;
    impl->Kind = EKikimrTableKind::Datashard;
    impl->PathId = TKikimrPathId(1, 3);
    impl->SchemaVersion = 1;
    impl->KeyColumnNames = {"g", "price", "k"};
    if (mutation == "overlap_key") { impl->KeyColumnNames = {"g", "k", "price"}; }
    if (mutation == "composite_pk") { impl->KeyColumnNames = {"g", "price", "x", "k"}; }
    impl->ColumnOrder = impl->KeyColumnNames;
    for (const auto& name : impl->ColumnOrder) { impl->Columns.emplace(name, metadata.Columns.at(name)); }
    if (mutation == "impl_order") { impl->KeyColumnNames = {"price", "g", "k"}; }
    if (mutation == "impl_type") { impl->Columns.at("g").Type = "Utf8"; }
    if (mutation == "impl_nullable") { impl->Columns.at("g").NotNull = true; }
    if (mutation == "impl_build") { impl->Columns.at("g").IsBuildInProgress = true; }
    if (mutation == "impl_not_null_transition") { impl->Columns.at("g").SetNotNullInProgress = true; }
    if (mutation == "impl_path") { impl->Name = "/Root/NativeSnapshot/wrong/indexImplTable"; }
    if (mutation == "impl_cluster") { impl->Cluster = "other"; }
    if (mutation == "impl_truncated_pk") { impl->KeyColumnNames = {"g", "price"}; }
    if (mutation == "impl_extra_column") { impl->Columns.emplace("x", metadata.Columns.at("x")); }
    if (mutation == "impl_kind") { impl->Kind = EKikimrTableKind::Olap; }
    if (mutation == "impl_missing_column") { impl->Columns.erase("price"); }
    if (mutation == "impl_chain") { impl->Next = MakeIntrusive<TKikimrTableMetadata>("ut", "/Root/extra"); }
    auto state = mutation == "not_ready" ? TIndexDescription::EIndexState::NotReady :
        mutation == "write_only" ? TIndexDescription::EIndexState::WriteOnly :
        mutation == "invalid_state" ? TIndexDescription::EIndexState::Invalid : TIndexDescription::EIndexState::Ready;
    auto kind = mutation == "unknown_kind" ? static_cast<TIndexDescription::EType>(999) :
        TIndexDescription::EType::GlobalSyncUnique;
    TIndexDescription::TSpecializedIndexDescription special;
    if (mutation == "specialized") { special = TIndexDescription::TLocalBloomFilterDescription{}; }
    metadata.Indexes.emplace_back("unique_hidden", keys, TVector<TString>{}, kind, state, 1, 2, 1, special);
    if (mutation != "missing_impl") { metadata.ImplTables.push_back(impl); }
    if (mutation == "duplicate_name") {
        metadata.Indexes.emplace_back("unique_hidden", keys, TVector<TString>{}, kind, state, 1, 4, 1, special);
        metadata.ImplTables.push_back(impl);
    }
    if (mutation == "second_unique") {
        auto second = MakeIntrusive<TKikimrTableMetadata>("ut", "/Root/NativeSnapshot/second/indexImplTable");
        second->DoesExist = true;
        second->Kind = EKikimrTableKind::Datashard;
        second->PathId = TKikimrPathId(1, 5);
        second->SchemaVersion = 1;
        second->KeyColumnNames = {"price", "k"};
        second->ColumnOrder = second->KeyColumnNames;
        for (const auto& name : second->ColumnOrder) { second->Columns.emplace(name, metadata.Columns.at(name)); }
        metadata.Indexes.emplace_back("second", TVector<TString>{"price"}, TVector<TString>{},
            TIndexDescription::EType::GlobalSyncUnique, TIndexDescription::EIndexState::Ready,
            1, 4, 1, TIndexDescription::TSpecializedIndexDescription{});
        metadata.ImplTables.push_back(second);
    }
    auto read = f.Read();
    TOpRoot root(read, TPositionHandle(), {{f.Key, "key"}});
    root.PlanProps.InfoUnitRegistry = f.Registry;
    return CaptureSemanticSnapshotCatalogV1(root, f.Rbo);
}

} // namespace

Y_UNIT_TEST_SUITE(TNativeSemanticSnapshot) {
    Y_UNIT_TEST(NativeCatalogCapturesHiddenUnselectedUniqueConstraintsAndPhysicalKey) {
        for (const TStringBuf mutation : {TStringBuf{}, TStringBuf("second_unique")}) {
            const auto captured = NativeUniqueConstraintCatalog(mutation);
            UNIT_ASSERT_C(captured.IsSupported(), captured.UnsupportedReason);
            UNIT_ASSERT_VALUES_EQUAL(captured.Catalog.Tables.size(), mutation.empty() ? 2 : 3);
            const auto& table = captured.Catalog.Tables.front();
            UNIT_ASSERT(table.PrimaryKey);
            UNIT_ASSERT_VALUES_EQUAL(*table.PrimaryKey, TVector<TString>({"k"}));
            UNIT_ASSERT_VALUES_EQUAL(table.UniqueKeys.size(), mutation.empty() ? 2 : 3);
            UNIT_ASSERT_VALUES_EQUAL(table.UniqueKeys[0].Columns, TVector<TString>({"k"}));
            UNIT_ASSERT(!table.UniqueKeys[0].NullsDistinct);
            UNIT_ASSERT_VALUES_EQUAL(table.UniqueKeys[1].Columns, TVector<TString>({"g", "price"}));
            UNIT_ASSERT(table.UniqueKeys[1].NullsDistinct);
            if (!mutation.empty()) {
                UNIT_ASSERT_VALUES_EQUAL(table.UniqueKeys[2].Columns, TVector<TString>({"price"}));
                UNIT_ASSERT(table.UniqueKeys[2].NullsDistinct);
            }
            bool hiddenString = false;
            bool hiddenDecimal = false;
            for (const auto& column : table.Columns) {
                hiddenString |= column.Name == "g" && column.Type == "String" && column.Nullable;
                hiddenDecimal |= column.Name == "price" && column.Type == "Decimal(15,2)" && column.Nullable;
            }
            UNIT_ASSERT(hiddenString && hiddenDecimal);
            for (size_t i = 1; i < captured.Catalog.Tables.size(); ++i) {
                const auto& derived = captured.Catalog.Tables[i];
                UNIT_ASSERT(derived.DerivedFrom);
                UNIT_ASSERT_VALUES_EQUAL(*derived.DerivedFrom, table.Name);
                UNIT_ASSERT(derived.PrimaryKey);
                const TVector<TString> expected = i == 1
                    ? TVector<TString>{"g", "price", "k"} : TVector<TString>{"price", "k"};
                UNIT_ASSERT_VALUES_EQUAL(*derived.PrimaryKey, expected);
                UNIT_ASSERT_VALUES_EQUAL(derived.UniqueKeys.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(derived.UniqueKeys.front().Columns, expected);
                UNIT_ASSERT(!derived.UniqueKeys.front().NullsDistinct);
            }
        }
    }

    Y_UNIT_TEST(NativeCatalogRejectsUnauditedEnforcedUniqueMetadata) {
        UNIT_ASSERT(NativeUniqueConstraintCatalog().IsSupported());
        for (const TStringBuf mutation : {"empty_keys", "duplicate_key", "unknown_key", "unsupported_type",
                "base_build", "base_not_null_transition", "base_kind", "base_missing", "base_no_pk",
                "impl_order", "impl_type", "impl_nullable", "impl_build", "impl_not_null_transition",
                "impl_path", "impl_cluster", "impl_truncated_pk", "impl_extra_column",
                "impl_kind", "impl_missing_column", "impl_chain", "not_ready", "write_only",
                "invalid_state", "unknown_kind", "specialized", "missing_impl", "duplicate_name",
                "direct_index_impl"})
        {
            const auto captured = NativeUniqueConstraintCatalog(mutation);
            UNIT_ASSERT_C(!captured.IsSupported(), mutation);
        }
    }

    Y_UNIT_TEST(NativeUniqueDerivedKeyKeepsBaseKeyOrderAndAvoidsDuplicateSuffix) {
        for (const TStringBuf mutation : {"overlap_key", "composite_pk"}) {
            const auto captured = NativeUniqueConstraintCatalog(mutation);
            UNIT_ASSERT_C(captured.IsSupported(), captured.UnsupportedReason);
            UNIT_ASSERT_VALUES_EQUAL(captured.Catalog.Tables.size(), 2);
            const auto& base = captured.Catalog.Tables[0];
            const auto& derived = captured.Catalog.Tables[1];
            const bool overlap = mutation == "overlap_key";
            const TVector<TString> baseKey = overlap
                ? TVector<TString>{"k"} : TVector<TString>{"x", "k"};
            const TVector<TString> businessKey = overlap
                ? TVector<TString>{"g", "k", "price"} : TVector<TString>{"g", "price"};
            const TVector<TString> physicalKey = overlap
                ? TVector<TString>{"g", "k", "price"} : TVector<TString>{"g", "price", "x", "k"};
            UNIT_ASSERT(base.PrimaryKey);
            UNIT_ASSERT_VALUES_EQUAL(*base.PrimaryKey, baseKey);
            UNIT_ASSERT_VALUES_EQUAL(base.UniqueKeys.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(base.UniqueKeys[0].Columns, baseKey);
            UNIT_ASSERT(!base.UniqueKeys[0].NullsDistinct);
            UNIT_ASSERT_VALUES_EQUAL(base.UniqueKeys[1].Columns, businessKey);
            UNIT_ASSERT(base.UniqueKeys[1].NullsDistinct);
            UNIT_ASSERT(derived.DerivedFrom);
            UNIT_ASSERT_VALUES_EQUAL(*derived.DerivedFrom, base.Name);
            UNIT_ASSERT(derived.PrimaryKey);
            UNIT_ASSERT_VALUES_EQUAL(*derived.PrimaryKey, physicalKey);
            UNIT_ASSERT_VALUES_EQUAL(derived.UniqueKeys.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(derived.UniqueKeys[0].Columns, physicalKey);
            UNIT_ASSERT(!derived.UniqueKeys[0].NullsDistinct);
            UNIT_ASSERT_VALUES_EQUAL(derived.Columns.size(), physicalKey.size());
        }
    }

    Y_UNIT_TEST(NativeCatalogPhysicalPrimaryKeyHasIndependentValidatedIdentity) {
        TFixture f;
        auto read = f.Read();
        TOpRoot root(read, TPositionHandle(), {{f.Key, "key"}});
        root.PlanProps.InfoUnitRegistry = f.Registry;
        const auto captured = CaptureSemanticSnapshotCatalogV1(root, f.Rbo);
        UNIT_ASSERT_C(captured.IsSupported(), captured.UnsupportedReason);
        const auto valid = ExportSemanticSnapshotV1(root, f.Rbo, captured.Catalog);
        Parse(valid);
        UNIT_ASSERT_STRING_CONTAINS(valid.Json, "primary_key");
        for (const auto& badKey : TVector<TVector<TString>>{{}, {"missing"}, {"k", "k"}, {"x"}}) {
            auto changed = captured.Catalog;
            changed.Tables.front().PrimaryKey = badKey;
            const auto exported = ExportSemanticSnapshotV1(root, f.Rbo, changed);
            UNIT_ASSERT(!exported.IsSupported());
        }
        auto changed = captured.Catalog;
        changed.Tables.front().UniqueKeys.front().NullsDistinct = true;
        UNIT_ASSERT(!ExportSemanticSnapshotV1(root, f.Rbo, changed).IsSupported());
    }

    Y_UNIT_TEST(NativeOrdinaryLookupKeepsAllUnselectedUniqueConstraints) {
        for (const bool staged : {false, true}) {
            const auto [before, after] = NativeSyncIndexLookup(staged, "extra_unique");
            const auto initial = Parse(before);
            const auto final = Parse(after);
            UNIT_ASSERT(after.Json.Contains("point_lookup"));
            UNIT_ASSERT(initial["schema"] == final["schema"]);
            const auto& tables = initial["schema"]["tables"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(tables.size(), 3);
            const auto& keys = tables[0]["unique_keys"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(keys.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(keys[1]["columns"].GetArraySafe().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(keys[1]["columns"].GetArraySafe()[0].GetStringSafe(), "x");
            UNIT_ASSERT(keys[1]["nulls_distinct"].GetBooleanSafe());
            UNIT_ASSERT_VALUES_EQUAL(tables[0]["primary_key"].GetArraySafe()[0].GetStringSafe(), "k");
            UNIT_ASSERT_VALUES_EQUAL(tables[2]["derived_from"].GetStringSafe(), tables[0]["name"].GetStringSafe());
            const auto& physicalKey = tables[2]["primary_key"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(physicalKey.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(physicalKey[0].GetStringSafe(), "x");
            UNIT_ASSERT_VALUES_EQUAL(physicalKey[1].GetStringSafe(), "k");
        }
    }

    Y_UNIT_TEST(NativeSynchronousIndexLookupPreservesBaseRowsAndNullFlags) {
        for (const bool staged : {false, true}) {
            const auto [before, after] = NativeSyncIndexLookup(staged);
            Parse(before); Parse(after);
            UNIT_ASSERT(after.Json.Contains("point_lookup"));
            UNIT_ASSERT(before.Json.Contains("derived_from"));
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json, !staged), "VERIFIED_BOUNDED");
            const auto [sameBefore, changed] = NativeSyncIndexLookup(staged, "changed_prefix");
            Parse(changed);
            UNIT_ASSERT_VALUES_EQUAL(Verify(sameBefore.Json, changed.Json, !staged), "COUNTEREXAMPLE");
        }
    }

    Y_UNIT_TEST(NativeSelectedUniqueIndexKeepsBusinessAndPhysicalKeysSeparate) {
        for (const bool staged : {false, true}) {
            const auto [before, after] = NativeSyncIndexLookup(staged, {}, true);
            const auto initial = Parse(before);
            const auto final = Parse(after);
            UNIT_ASSERT(initial["schema"] == final["schema"]);
            const auto& tables = initial["schema"]["tables"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(tables.size(), 2);
            const auto& base = tables[0];
            const auto& derived = tables[1];
            const auto& baseKey = base["primary_key"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(baseKey.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(baseKey[0].GetStringSafe(), "k");
            const auto& baseLaws = base["unique_keys"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(baseLaws.size(), 2);
            UNIT_ASSERT(baseLaws[0]["columns"] == base["primary_key"]);
            UNIT_ASSERT(!baseLaws[0]["nulls_distinct"].GetBooleanSafe());
            UNIT_ASSERT_VALUES_EQUAL(baseLaws[1]["columns"].GetArraySafe().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(baseLaws[1]["columns"].GetArraySafe()[0].GetStringSafe(), "g");
            UNIT_ASSERT(baseLaws[1]["nulls_distinct"].GetBooleanSafe());
            UNIT_ASSERT_VALUES_EQUAL(derived["derived_from"].GetStringSafe(), base["name"].GetStringSafe());
            const auto& physicalKey = derived["primary_key"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(physicalKey.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(physicalKey[0].GetStringSafe(), "g");
            UNIT_ASSERT_VALUES_EQUAL(physicalKey[1].GetStringSafe(), "k");
            const auto& derivedLaws = derived["unique_keys"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(derivedLaws.size(), 1);
            UNIT_ASSERT(derivedLaws[0]["columns"] == derived["primary_key"]);
            UNIT_ASSERT(!derivedLaws[0]["nulls_distinct"].GetBooleanSafe());
            bool nullableFlag = false;
            for (const auto& column : derived["columns"].GetArraySafe()) {
                nullableFlag |= column["name"].GetStringSafe() == "g" &&
                    column["type"].GetStringSafe() == "Bool" && column["nullable"].GetBooleanSafe();
            }
            UNIT_ASSERT(nullableFlag);
            size_t lookups = 0;
            for (const auto& node : final["plan"]["nodes"].GetArraySafe()) {
                if (node["op"].GetStringSafe() != "point_lookup") { continue; }
                ++lookups;
                UNIT_ASSERT_VALUES_EQUAL(node["table"].GetStringSafe(), base["name"].GetStringSafe());
                UNIT_ASSERT_VALUES_EQUAL(node["keys"].GetArraySafe().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(node["keys"].GetArraySafe()[0]["source"].GetStringSafe(), "k");
            }
            UNIT_ASSERT_VALUES_EQUAL(lookups, 1);
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json, !staged), "VERIFIED_BOUNDED");
            const auto [sameBefore, changed] = NativeSyncIndexLookup(staged, "changed_prefix", true);
            Parse(changed);
            UNIT_ASSERT_VALUES_EQUAL(Verify(sameBefore.Json, changed.Json, !staged), "COUNTEREXAMPLE");
        }
    }

    Y_UNIT_TEST(NativeSelectedUniqueLookupKeepsExistingRangeAndLookupRestrictions) {
        for (const TStringBuf mutation : {"nullable_boundary", "null_boundary", "different_upper",
                "exclusive_boundary", "range_key_order", "range_prefix_len", "missing_key", "wrong_key",
                "allow_null_setting", "input_lookup", "join", "prefix", "fetched_type",
                "stream_columns", "stream_strategy", "stream_type", "stream_map", "stream_table", "stream_annotation"})
        {
            const auto [before, after] = NativeSyncIndexLookup(true, mutation, true);
            Parse(before);
            UNIT_ASSERT_C(!after.IsSupported(), mutation << ": " << after.Json);
        }
    }

    Y_UNIT_TEST(NativeSynchronousIndexLookupRejectsUnauditedMetadataAndLookupSettings) {
        for (const TStringBuf mutation : {"index_type", "index_nullable", "index_pk_order", "index_missing_column",
                "async", "not_ready", "nullable_boundary", "null_boundary", "different_upper",
                "exclusive_boundary", "range_key_order", "range_prefix_len", "missing_key", "wrong_key",
                "allow_null_setting", "input_lookup", "join", "prefix", "fetched_type",
                "stream_columns", "stream_strategy", "stream_type", "stream_map", "stream_table", "stream_annotation"})
        {
            const auto [before, after] = NativeSyncIndexLookup(true, mutation);
            UNIT_ASSERT_C(!after.IsSupported(), mutation << ": " << after.Json);
        }
    }



    Y_UNIT_TEST(NativeDerivedRangeRejectionKeepsOriginalFailureAndComputedEvidence) {
        for (const bool staged : {false, true}) {
            const auto [before, after] = NativeSyncIndexLookup(staged, "diagnostic_compute_only");
            UNIT_ASSERT_C(before.IsSupported(), before.UnsupportedReason);
            UNIT_ASSERT(!after.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(after.UnsupportedReason,
                "Derived index range requires one exact literal Bool prefix of its physical key ; derived_index_range_v1{");
            UNIT_ASSERT_STRING_CONTAINS(after.UnsupportedReason, ";selected=ComputeNode;");
            UNIT_ASSERT_STRING_CONTAINS(after.UnsupportedReason, ";LiteralRange_root=absent;");
            UNIT_ASSERT_STRING_CONTAINS(after.UnsupportedReason, "KqlKeyInc");
            // The catalog identity exceeds the fragment limit; the small AST is complete.
            UNIT_ASSERT_STRING_CONTAINS(after.UnsupportedReason, "END derived_index_range_v1 truncated=1 flags=2");
            const auto [validBefore, validAfter] = NativeSyncIndexLookup(staged);
            UNIT_ASSERT(validBefore.IsSupported());
            UNIT_ASSERT_C(validAfter.IsSupported(), validAfter.UnsupportedReason);
            UNIT_ASSERT(!validAfter.Json.Contains("derived_index_range_v1"));
        }
        // The inherited failure can fill yexception's 64KiB buffer. Keep that
        // original failure rather than append an incomplete diagnostic silently.
        const auto [longBefore, longFailure] = NativeSyncIndexLookup(false, "diagnostic_long_reason");
        UNIT_ASSERT(longBefore.IsSupported());
        UNIT_ASSERT(!longFailure.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(longFailure.UnsupportedReason, "Invalid Bool literal xxxxxxxxx");
        UNIT_ASSERT(longFailure.UnsupportedReason.size() > 64 * 1024 - 512);
        UNIT_ASSERT(!longFailure.UnsupportedReason.Contains("derived_index_range_v1"));
    }

    Y_UNIT_TEST(NativeDerivedRangeDiagnosticKeepsTypesIdentityAndRepresentation) {
        TFixture f;
        auto read = f.Read();
        const auto pos = TPositionHandle();
        const auto* decimal = f.Expr.MakeType<TDataExprParamsType>(NUdf::EDataSlot::Decimal, "15", "2");
        const auto* optional = f.Expr.MakeType<TOptionalExprType>(decimal);
        const auto* nested = f.Expr.MakeType<TOptionalExprType>(optional);
        auto parameter = f.Expr.NewCallable(pos, "Parameter", {f.Expr.NewAtom(pos, "$threshold")});
        parameter->SetTypeAnn(optional);
        auto nothing = f.Expr.NewCallable(pos, "Nothing", {ExpandType(pos, *optional, f.Expr)});
        nothing->SetTypeAnn(optional);
        auto just = f.Expr.NewCallable(pos, "Just", {nothing});
        just->SetTypeAnn(nested);
        auto rangeFor = f.Expr.NewCallable(pos, "RangeFor", {f.Expr.NewAtom(pos, "=="), parameter, just});
        auto compute = f.Expr.NewCallable(pos, "RangeFinalize", {
            f.Expr.NewCallable(pos, "RangeMultiply", {f.Expr.NewAtom(pos, "10000"),
                f.Expr.NewCallable(pos, "RangeUnion", {rangeFor, rangeFor})})});
        auto literal = f.Expr.NewList(pos, {f.Expr.NewCallable(pos, "KqlKeyInc", {just}),
            f.Expr.NewCallable(pos, "KqlKeyExc", {parameter})});
        TOpRead::TRangeInfo range{.ComputeNode = compute, .KeyColumns = {"d", "k"},
            .UsedPrefixLen = 1, .PointPrefixLen = 0, .ExpectedMaxRanges = 2, .LiteralRange = literal};
        TSemanticSnapshotCatalogTableV1 table;
        table.Name = "index"; table.DerivedFrom = "base"; table.PrimaryKey = TVector<TString>{"d", "k"};
        table.Columns = {{"d", "Decimal(15,2)", true}, {"k", "Int32", false}};
        const auto diagnostic = NDetail::TDerivedIndexRangeDiagnostic::Render(*read, table, range);
        for (const TStringBuf expected : {"storage=RowStorage", "derived_from=\"base\"",
                "physical_key=count=2[\"d\":type=\"Decimal(15,2)\":nullable=1,\"k\":type=\"Int32\":nullable=0]",
                "used_prefix=1;point_prefix=0;expected_max_ranges=2;selected=LiteralRange",
                "LiteralRange_root=present", "ComputeNode_root=present", "RangeFinalize", "RangeMultiply",
                "10000", "RangeUnion", "RangeFor", "Parameter", "$threshold", "Nothing", "Just",
                "KqlKeyInc", "KqlKeyExc", "Optional(", "param1=\"15\",param2=\"2\"", "@n",
                "END derived_index_range_v1 truncated=0"})
        {
            UNIT_ASSERT_STRING_CONTAINS(diagnostic, expected);
        }
        range.LiteralRange.Reset(); range.ExpectedMaxRanges.Clear();
        const auto computed = NDetail::TDerivedIndexRangeDiagnostic::Render(*read, table, range);
        UNIT_ASSERT_STRING_CONTAINS(computed, "expected_max_ranges=absent;selected=ComputeNode");
        UNIT_ASSERT_STRING_CONTAINS(computed, "LiteralRange_root=absent");
    }

    Y_UNIT_TEST(NativeDerivedRangeDiagnosticBoundsDepthEdgesNodesAndText) {
        TFixture f;
        auto read = f.Read();
        TSemanticSnapshotCatalogTableV1 table;
        TOpRead::TRangeInfo range;
        const auto pos = TPositionHandle();
        const auto check = [&](TStringBuf marker) {
            const auto text = NDetail::TDerivedIndexRangeDiagnostic::Render(*read, table, range);
            UNIT_ASSERT_C(text.size() <= 16 * 1024, text.size());
            UNIT_ASSERT_STRING_CONTAINS(text, "END derived_index_range_v1 truncated=1");
            UNIT_ASSERT_STRING_CONTAINS(text, marker);
            for (const unsigned char byte : text) { UNIT_ASSERT(byte >= 0x20 && byte <= 0x7e); }
        };
        auto leaf = f.Expr.NewAtom(pos, "x");
        range.ComputeNode = leaf;
        for (size_t i = 0; i < 80; ++i) { range.ComputeNode = f.Expr.NewCallable(pos, "IfPresent", {range.ComputeNode}); }
        check("<depth-cut>");
        TExprNode::TListType children(600, leaf);
        range.ComputeNode = f.Expr.NewList(pos, std::move(children));
        check("<edges-cut>");
        children.clear();
        for (size_t i = 0; i < 240; ++i) { children.push_back(f.Expr.NewAtom(pos, ToString(i))); }
        range.ComputeNode = f.Expr.NewList(pos, std::move(children));
        check("<nodes-cut>");
        const TString bytes = TString("\0\xff\x80\n\"\\", 6) + TString(256, 'z');
        range.ComputeNode = f.Expr.NewAtom(pos, bytes);
        check("<fragment-cut:bytes=262>");
        const auto escaped = NDetail::TDerivedIndexRangeDiagnostic::Render(*read, table, range);
        UNIT_ASSERT_STRING_CONTAINS(escaped, "\\x00\\xff\\x80\\x0a\\x22\\x5c");
        children.clear();
        for (size_t i = 0; i < 240; ++i) { children.push_back(f.Expr.NewAtom(pos, TString(96, 'z') + ToString(i))); }
        range.ComputeNode = f.Expr.NewList(pos, std::move(children));
        check("END derived_index_range_v1");
        const auto capped = NDetail::TDerivedIndexRangeDiagnostic::Render(*read, table, range);
        UNIT_ASSERT_C(capped.size() >= 16 * 1024 - 512, capped.size());
        const TTypeAnnotationNode* type = f.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
        for (size_t i = 0; i < 80; ++i) { type = f.Expr.MakeType<TOptionalExprType>(type); }
        range.ComputeNode = f.Expr.NewArgument(pos, "deepType");
        range.ComputeNode->SetTypeAnn(type);
        check("<type-depth-cut>");
        const auto* boolean = f.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
        TTypeAnnotationNode::TListType types(600, boolean);
        range.ComputeNode = f.Expr.NewArgument(pos, "wideType");
        range.ComputeNode->SetTypeAnn(f.Expr.MakeType<TTupleExprType>(types));
        check("<type-edges-cut>");
        types.clear();
        for (size_t i = 0; i < 240; ++i) {
            types.push_back(f.Expr.MakeType<TItemExprType>(ToString(i), boolean));
        }
        range.ComputeNode = f.Expr.NewArgument(pos, "manyTypes");
        range.ComputeNode->SetTypeAnn(f.Expr.MakeType<TTupleExprType>(types));
        check("<types-cut>");
        table.PrimaryKey = TVector<TString>(40, "k");
        range.ComputeNode = leaf;
        check("<keys-cut>");
        // Escaping can expand metadata beyond the text cap. Presence/precedence
        // must already be visible before that variable-size section is clipped.
        const TString binaryName(96, '\xff');
        table.PrimaryKey = TVector<TString>(32, binaryName);
        table.Columns = {{binaryName, binaryName, true}};
        check("END derived_index_range_v1");
        const auto metadataCut = NDetail::TDerivedIndexRangeDiagnostic::Render(*read, table, range);
        UNIT_ASSERT_STRING_CONTAINS(metadataCut, ";selected=ComputeNode;");
        UNIT_ASSERT_STRING_CONTAINS(metadataCut, ";LiteralRange_root=absent;");
        UNIT_ASSERT_STRING_CONTAINS(metadataCut, "ComputeNode_root=present(Atom,\"x\",children=0)");
    }

    Y_UNIT_TEST(NativeStringListMembershipSharesInputsAcrossStages) {
        for (bool nullableItems : {false, true}) {
            for (bool nullableLookup : {false, true}) {
                for (bool ansi : {false, true}) {
                    const auto before = NativeStringListParameter(nullableItems, nullableLookup, ansi, false);
                    const auto after = NativeStringListParameter(nullableItems, nullableLookup, ansi, true);
                    const auto initial = Parse(before);
                    Parse(after);
                    size_t membershipExpressions = 0;
                    for (const auto& node : initial["plan"]["nodes"].GetArraySafe()) {
                        if (node["op"].GetStringSafe() != "project") {
                            continue;
                        }
                        for (const auto& column : node["columns"].GetArraySafe()) {
                            const auto& expression = column["expression"];
                            if (expression["kind"].GetStringSafe() == "in_parameter") {
                                ++membershipExpressions;
                                UNIT_ASSERT_VALUES_EQUAL(expression["parameter_type"].GetStringSafe(),
                                    nullableItems ? "List<Optional<String>>" : "List<String>");
                            }
                        }
                    }
                    UNIT_ASSERT_VALUES_EQUAL(membershipExpressions, 1);
                    UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
                }
            }
        }
    }

    Y_UNIT_TEST(NativeStringListMembershipRenameIsARealInputChange) {
        const auto before = NativeStringListParameter(false, false, true, false, {}, "$p");
        const auto after = NativeStringListParameter(false, false, true, true, {}, "$q");
        Parse(before);
        Parse(after);
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeStringListMembershipRejectsUnsupportedShapesAndTypes) {
        for (const TString& mutation : TVector<TString>{"utf8", "nested_optional", "outer_optional",
                "outer_descriptor", "item_descriptor", "data_descriptor", "missing_inner_annotation",
                "duplicate_option", "tableSource", "nullsProcessed", "future_option", "option_shape",
                "result_nullability", "transformed_list", "projection", "length"}) {
            const auto result = NativeStringListParameter(true, false, true, false, mutation);
            UNIT_ASSERT_C(!result.IsSupported(), mutation);
        }
        for (const TString& name : TVector<TString>{"", "$", "p", "%kqp%tx_result_binding_0_0"}) {
            const auto result = NativeStringListParameter(false, false, true, false, {}, name);
            UNIT_ASSERT_C(!result.IsSupported(), name);
        }
    }

    Y_UNIT_TEST(NativeStringListMembershipAuditsNestedSafetyMetadata) {
        // Root, collection, name, each type descriptor/atom, all option payloads,
        // lookup and its member-name atom must independently fail closed.
        for (int target = 0; target != 12; ++target) {
            for (int kind = 0; kind != 4; ++kind) {
                const auto result = NativeStringListParameter(true, true, true, false, {}, "$p", target, kind);
                UNIT_ASSERT_C(!result.IsSupported(), TStringBuilder() << "target=" << target << ", kind=" << kind);
            }
        }
    }

    Y_UNIT_TEST(NativeScalarParametersShareInputAcrossStaging) {
        for (const auto& type : TVector<TString>{"Bool", "Int32", "Uint64", "Date", "Timestamp", "Timestamp64", "String", "Utf8", "Decimal"}) {
            for (bool nullable : {false, true}) {
                const auto before = NativeScalarParameter(type, nullable, false);
                const auto after = NativeScalarParameter(type, nullable, true);
                Parse(before);
                Parse(after);
                UNIT_ASSERT_STRING_CONTAINS(before.Json, "parameter");
                UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
            }
        }
    }

    Y_UNIT_TEST(NativeScalarParameterRenameProducesCounterexample) {
        const auto before = NativeScalarParameter("Int32", false, false, "$p");
        const auto after = NativeScalarParameter("Int32", false, true, "$q");
        Parse(before);
        Parse(after);
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "COUNTEREXAMPLE");
        // Division is routed through the complete source-audit path as well as
        // the interpreted exporter. A parameter must be admitted in both.
        const auto dividedBefore = NativeScalarParameter("Int32", true, false, "$p", false, true);
        const auto dividedAfter = NativeScalarParameter("Int32", true, true, "$p", false, true);
        Parse(dividedBefore);
        Parse(dividedAfter);
        UNIT_ASSERT_VALUES_EQUAL(Verify(dividedBefore.Json, dividedAfter.Json), "VERIFIED_BOUNDED");
    }

    Y_UNIT_TEST(NativeScalarParameterRejectsContradictoryInnerOptionalDescriptor) {
        for (const TString& mutation : {TString("missing"), TString("wrong_kind"),
                                       TString("wrong_type"), TString("nested_optional")}) {
            for (const bool opaqueAudit : {false, true}) {
                const auto result = NativeScalarParameter("Int32", true, false,
                    "$p", false, opaqueAudit, mutation);
                UNIT_ASSERT(!result.IsSupported());
                UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "item descriptor annotation");
            }
        }
    }

    Y_UNIT_TEST(NativeScalarParameterRejectsBadNameAndDescriptorAnnotation) {
        const auto emptyName = NativeScalarParameter("Int32", false, false, "");
        UNIT_ASSERT(!emptyName.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(emptyName.UnsupportedReason, "Parameter");
        for (bool nullable : {false, true}) {
            const auto changed = NativeScalarParameter("Int32", nullable, false, "$p", true);
            UNIT_ASSERT(!changed.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(changed.UnsupportedReason, "annotations disagree");
        }
        for (const auto& name : TVector<TString>{"$", "p", "%kqp_physical_tx_param_binding_0", "%kqp%tx_result_binding_0_0"}) {
            const auto internal = NativeScalarParameter("Int32", false, false, name);
            UNIT_ASSERT(!internal.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(internal.UnsupportedReason, "external SQL Parameter");
        }
        const auto floating = NativeScalarParameter("Double", false, false);
        UNIT_ASSERT(!floating.IsSupported());
    }

    Y_UNIT_TEST(NativeFilterFusionIsVisibleToTransformationProofs) {
        TFixture f;
        const auto pos = TPositionHandle();
        auto read = f.Read();
        auto column = MakeColumnAccess(f.Key, pos, &f.Expr, &f.ExpressionProps);
        auto lower = MakeIntrusive<TOpFilter>(read, pos,
            MakeBinaryPredicate(">", column, MakeConstant("Int32", "0", pos, &f.Expr)));
        auto upper = MakeIntrusive<TOpFilter>(lower, pos,
            MakeBinaryPredicate("<", column, MakeConstant("Int32", "5", pos, &f.Expr)));
        TOpRoot root(upper, pos, {{f.Key, "key"}});
        root.PlanProps.InfoUnitRegistry = f.Registry;
        auto annotation = CreateTypeAnnotationTransformer(
            CreateExtCallableTypeAnnotationTransformer(f.Types), f.Types);
        TRBOContext rbo(f.Optimize, f.Expr, f.Types, *annotation, *f.Functions);
        TNativeCaptureSink sink;
        TSemanticSnapshotPairCaptureV1 capture(&sink);
        capture.CaptureInitial(root, rbo);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 1);
        UNIT_ASSERT_C(sink.Results.front().IsSupported(), sink.Results.front().UnsupportedReason);
        rbo.TransformationDebug.Reset(100);
        TVector<std::unique_ptr<IRule>> rules;
        rules.emplace_back(std::make_unique<TFuseFiltersRule>());
        TRuleBasedStage("Native filter fusion", std::move(rules)).RunStage(root, rbo);
        auto fused = CastOperator<TOpFilter>(root.GetInput());
        UNIT_ASSERT(fused->GetInput() == read);
        f.StageSerial(root);
        capture.CaptureFinal(root, rbo);
        UNIT_ASSERT_VALUES_EQUAL(sink.Results.size(), 2);
        UNIT_ASSERT_C(sink.Results.back().IsSupported(), sink.Results.back().UnsupportedReason);
        UNIT_ASSERT_VALUES_EQUAL(Verify(sink.Results.front().Json, sink.Results.back().Json), "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL_C(rbo.TransformationDebug.Events.size(), 1,
            "Filter fusion changed the native plan but was omitted from the transformation log");
        UNIT_ASSERT_VALUES_EQUAL(rbo.TransformationDebug.Events.front().Name, "Fuse filters");
    }

    Y_UNIT_TEST(NativeIntegralCoalescePreservesEmptyAndPresentValues) {
        for (const TString& type : {TString("Int8"), TString("Uint8"), TString("Int16"), TString("Uint16"),
                                   TString("Int32"), TString("Uint32"), TString("Int64"), TString("Uint64")}) {
            const auto before = NativeIntegralCoalesce(type, false, false);
            const auto after = NativeIntegralCoalesce(type, true, true);
            Parse(before);
            Parse(after);
            UNIT_ASSERT_STRING_CONTAINS(after.Json, "if_present");
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
            const auto changed = NativeIntegralCoalesce(type, true, true, true);
            Parse(changed);
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, changed.Json), "COUNTEREXAMPLE");
            UNIT_ASSERT(!NativeIntegralCoalesce(type, true, false, false, true).IsSupported());
        }
    }

    Y_UNIT_TEST(NativeOlapLikeJunctionsPreserveBooleanSemantics) {
        for (bool utf8 : {false, true}) {
            for (bool nullable : {false, true}) {
                for (const TStringBuf junction : {"And", "Or", "NotOr"}) {
                    const auto before = NativeCompiledLike(utf8, nullable, false, false, false, junction);
                    const auto after = NativeCompiledLike(utf8, nullable, false, false, false, junction, true);
                    const auto changed = NativeCompiledLike(utf8, nullable, false, true, false, junction, true);
                    Parse(before);
                    Parse(after);
                    Parse(changed);
                    UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
                    UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, changed.Json), "COUNTEREXAMPLE");
                }
            }
        }
        for (const TStringBuf mutation : {"wrong_binding", "typed_body", "empty_junction", "arbitrary_body"}) {
            UNIT_ASSERT(!NativeCompiledLike(false, true, false, false, false, "Or", true, mutation).IsSupported());
        }
    }

    Y_UNIT_TEST(NativeLogicalPushedLikePreservesRowPredicatesBeforeStages) {
        for (bool utf8 : {false, true}) {
            const auto before = NativeCompiledLike(utf8, true, false, false, false, "Or");
            const auto after = NativeCompiledLike(utf8, true, false, false, false, "Or", true, {}, true);
            const auto changed = NativeCompiledLike(utf8, true, false, true, false, "Or", true, {}, true);
            Parse(before);
            Parse(after);
            Parse(changed);
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json, true), "VERIFIED_BOUNDED");
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, changed.Json, true), "COUNTEREXAMPLE");
        }
    }

    Y_UNIT_TEST(NativeCompiledLikePreservesRequiredAndNullableText) {
        for (const bool utf8 : {false, true}) {
            for (const bool nullable : {false, true}) {
                const auto initial = NativeCompiledLike(utf8, nullable, false);
                const auto final = NativeCompiledLike(utf8, nullable, true);
                Parse(initial);
                Parse(final);
                UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, final.Json), "VERIFIED_BOUNDED");
                const auto changed = NativeCompiledLike(utf8, nullable, true, true);
                Parse(changed);
                UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, changed.Json), "COUNTEREXAMPLE");
                UNIT_ASSERT(!NativeCompiledLike(utf8, nullable, false, false, true).IsSupported());
            }
        }
    }

    Y_UNIT_TEST(NativeZeroWidthUnionPreservesCountAndBranchMultiplicity) {
        const auto capture = [](ui32 branches, bool zeroWidth, bool staged) {
            TFixture f("Int32");
            const auto pos = TPositionHandle();
            const auto payload = f.Registry.Add(TInfoUnit("", "payload"));
            const auto unit = f.Registry.Add(TInfoUnit("", "unit"));
            const auto count = f.Registry.Add(TInfoUnit("", "count"));
            TVector<TIntrusivePtr<IOperator>> inputs;
            TUnionInputRow sources;
            for (ui32 i = 0; i < branches; ++i) {
                inputs.push_back(f.Read());
                sources.Inputs.push_back(f.Key);
            }
            TUnionAllIUs columns(TUnionInputPolicy{branches});
            if (!zeroWidth) {
                columns.Add(payload, std::move(sources));
            }
            auto merge = MakeIntrusive<TOpUnionAll>(std::move(inputs), pos, std::move(columns));
            TIntrusivePtr<IOperator> input = merge;
            if (zeroWidth) {
                auto row = f.Expr.NewArgument(pos, "row");
                auto lambda = f.Expr.NewLambda(pos, f.Expr.NewArguments(pos, {row}),
                    f.Expr.NewCallable(pos, "Void", {}));
                input = MakeIntrusive<TOpMap>(merge, pos, TMapIUs{{unit,
                    TMapElement(TExpression(lambda, &f.Expr, &f.ExpressionProps))}});
            }
            auto aggregate = MakeIntrusive<TOpAggregate>(input,
                TAggregationIUs{{count, {zeroWidth ? unit : payload, "count", false, false}}},
                TOrderedIUs<>{}, EOpPhase::Undefined, false, pos);
            TOpRoot root(aggregate, pos, {{count, "count"}});
            root.PlanProps.InfoUnitRegistry = f.Registry;
            auto annotation = CreateTypeAnnotationTransformer(
                CreateExtCallableTypeAnnotationTransformer(f.Types), f.Types);
            TRBOContext rbo(f.Optimize, f.Expr, f.Types, *annotation, *f.Functions);
            for (const auto& entry : root) {
                entry.Current->BindExpressionPlanProps(&root.PlanProps);
            }
            root.RecomputeOutputIUsSubtree();
            UNIT_ASSERT(root.ComputeTypes(rbo) == IGraphTransformer::TStatus::Ok);
            root.ComputeParents();
            if (staged) {
                auto& graph = root.PlanProps.StageGraph;
                TVector<ui32> sources;
                for (auto* read : merge->GetChildren()) {
                    const auto source = graph.AddSourceStage(NYql::EStorageType::RowStorage);
                    read->Props.StageId = source;
                    sources.push_back(source);
                }
                const auto sink = graph.AddStage();
                merge->Props.StageId = sink;
                input->Props.StageId = sink;
                aggregate->Props.StageId = sink;
                for (const auto source : sources) {
                    graph.Connect(source, sink, MakeIntrusive<TUnionAllConnection>(graph.GetOutputIndex(source)));
                }
            }
            auto exported = ExportSemanticSnapshotV1(root, rbo);
            Parse(exported);
            return exported;
        };
        for (const bool staged : {false, true}) {
            for (const ui32 branches : {2, 3, 4}) {
                const auto before = capture(branches, false, false);
                const auto after = capture(branches, true, staged);
                UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json, !staged), "VERIFIED_BOUNDED");
            }
            const auto before = capture(3, false, false);
            const auto changed = capture(2, true, staged);
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, changed.Json, !staged), "COUNTEREXAMPLE");
        }
    }

    Y_UNIT_TEST(NativeGroupingSetsMatchesIndependentBagUnion) {
        for (const TString& function : {TString("count"), TString("sum"), TString("min"), TString("max")}) {
            for (const bool nullable : {false, true}) {
                for (const bool duplicateSets : {false, true}) {
                    const auto [initial, expanded] = NativeGroupingSets(function, nullable, duplicateSets);
                    UNIT_ASSERT_VALUES_EQUAL_C(Verify(initial.Json, expanded.Json, true), "VERIFIED_BOUNDED",
                        function << " nullable=" << nullable << " duplicateSets=" << duplicateSets);
                }
                const auto [initial, expanded] = NativeGroupingSets(function, nullable, false, true);
                UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, expanded.Json, true), "VERIFIED_BOUNDED");
            }
        }
    }

    Y_UNIT_TEST(NativeGroupingSetsCountAllPreservesUnitCarrierAndMultiplicity) {
        for (const bool nullableKey : {false, true}) {
            for (const bool duplicateSets : {false, true}) {
                for (const bool staged : {false, true}) {
                    const auto [initial, expanded] = NativeGroupingSets("count", true, duplicateSets,
                        false, false, false, staged, nullableKey, "Int32", true);
                    UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, expanded.Json, !staged), "VERIFIED_BOUNDED");
                }
            }
        }
        const auto [initial, expanded] = NativeGroupingSets("count", true, true,
            false, false, false, false, true, "Int32", true);
        auto changed = Parse(expanded);
        auto removed = NJson::TJsonValue(NJson::JSON_MAP);
        bool mutated = false;
        for (auto& node : changed["plan"]["nodes"].GetArraySafe()) {
            if (node["op"].GetStringSafe() != "union_all") {
                continue;
            }
            auto& last = node["inputs"].GetArraySafe().back();
            removed["id"] = "mutant_empty_count_branch";
            removed["op"] = "filter";
            removed["input"] = last["node"];
            removed["predicate"]["kind"] = "literal";
            removed["predicate"]["type"] = "Bool";
            removed["predicate"]["value"] = false;
            last["node"] = "mutant_empty_count_branch";
            mutated = true;
            break;
        }
        UNIT_ASSERT(mutated);
        changed["plan"]["nodes"].AppendValue(std::move(removed));
        UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, NJson::WriteJson(changed, false), true), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeGroupingSetsMatchesExpandedStages) {
        for (const TString& function : {TString("count"), TString("sum"), TString("min"), TString("max"), TString("avg")}) {
            const auto [initial, expanded] = NativeGroupingSets(function, true, true, false, false, false, true);
            UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, expanded.Json), "VERIFIED_BOUNDED");
        }
        const auto [initial, expanded] = NativeGroupingSets("sum", false, false, true, false, false, true);
        UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, expanded.Json), "VERIFIED_BOUNDED");
    }

    Y_UNIT_TEST(NativeGroupingSetsPreservesRequiredKeyValues) {
        for (const TString& type : {TString("Int8"), TString("Int16"), TString("Int32"), TString("Int64"),
                                   TString("Uint8"), TString("Uint16"), TString("Uint32"), TString("Uint64"),
                                   TString("Bool"), TString("String"), TString("Utf8"), TString("Date"),
                                   TString("Datetime"), TString("Timestamp")}) {
            const auto [initial, expanded] = NativeGroupingSets(
                "count", true, true, false, false, false, true, false, type);
            UNIT_ASSERT_VALUES_EQUAL_C(Verify(initial.Json, expanded.Json), "VERIFIED_BOUNDED", type);
        }
    }

    Y_UNIT_TEST(NativeGroupingSetsDistinguishesNullGroupsFromOmittedKeys) {
        for (const TString& function : {TString("count"), TString("sum")}) {
            const auto [initial, expanded] = NativeGroupingSets(function, true, true, false, false, false, true, true);
            UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, expanded.Json), "VERIFIED_BOUNDED");
        }
    }

    Y_UNIT_TEST(NativeGroupingSetsPreservesAuditedDecimalAverage) {
        for (const bool duplicateSets : {false, true}) {
            const auto [initial, expanded] = NativeGroupingSets("avg", true, duplicateSets);
            UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, expanded.Json, true), "VERIFIED_BOUNDED");
        }
    }

    Y_UNIT_TEST(NativeGroupingSetsRejectsInvalidNullabilityAndResidualPhysicalTemplates) {
        UNIT_ASSERT(!NativeGroupingSets("count", false, false, false, true).first.IsSupported());
        UNIT_ASSERT(!NativeGroupingSets("count", false, false, false, false, true).first.IsSupported());
    }

    Y_UNIT_TEST(NativeGroupingSetsDetectsLostBranchesAndWrongNullsOrIndicators) {
        const auto [initial, expanded] = NativeGroupingSets("sum", true, true);
        for (const TString& mutation : {TString("branch"), TString("null"), TString("indicator")}) {
            auto changed = Parse(expanded);
            bool mutated = false;
            auto removedBranch = NJson::TJsonValue(NJson::JSON_MAP);
            for (auto& node : changed["plan"]["nodes"].GetArraySafe()) {
                if (mutation == "branch" && node["op"].GetStringSafe() == "union_all") {
                    UNIT_ASSERT(node["inputs"].GetArraySafe().size() > 2);
                    auto& last = node["inputs"].GetArraySafe().back();
                    removedBranch["id"] = "mutant_empty_branch";
                    removedBranch["op"] = "filter";
                    removedBranch["input"] = last["node"];
                    removedBranch["predicate"]["kind"] = "literal";
                    removedBranch["predicate"]["type"] = "Bool";
                    removedBranch["predicate"]["value"] = false;
                    last["node"] = "mutant_empty_branch";
                    mutated = true;
                } else if (node["op"].GetStringSafe() == "project") {
                    for (auto& column : node["columns"].GetArraySafe()) {
                        auto& expression = column["expression"];
                        if (mutation == "null" && expression["kind"].GetStringSafe() == "null") {
                            const auto type = expression["type"].GetStringSafe();
                            expression = NJson::TJsonValue(NJson::JSON_MAP);
                            expression["kind"] = "if";
                            expression["condition"]["kind"] = "literal";
                            expression["condition"]["type"] = "Bool";
                            expression["condition"]["value"] = true;
                            expression["then"]["kind"] = "literal";
                            expression["then"]["type"] = type;
                            expression["then"]["value"] = i64(0);
                            expression["else"]["kind"] = "null";
                            expression["else"]["type"] = type;
                            expression["type"] = type;
                            expression["nullable"] = true;
                            mutated = true;
                            break;
                        }
                        if (mutation == "indicator" && expression["kind"].GetStringSafe() == "literal" &&
                            expression["type"].GetStringSafe() == "Uint64")
                        {
                            expression["value"] = ui64(expression["value"].GetUIntegerSafe() == 0 ? 1 : 0);
                            mutated = true;
                            break;
                        }
                    }
                }
                if (mutated) { break; }
            }
            UNIT_ASSERT(mutated);
            if (mutation == "branch") {
                changed["plan"]["nodes"].AppendValue(std::move(removedBranch));
            }
            UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, NJson::WriteJson(changed, false), true), "COUNTEREXAMPLE");
        }
    }

    Y_UNIT_TEST(NativeGroupingSetsDetectsChangedReducer) {
        const auto [initial, expanded] = NativeGroupingSets("min", true, true);
        auto changed = Parse(expanded);
        for (auto& node : changed["plan"]["nodes"].GetArraySafe()) {
            if (node["op"].GetStringSafe() == "aggregate") {
                node["aggregates"][0]["function"] = "max";
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, NJson::WriteJson(changed, false), true), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeDistinctAliasesUseBindingsInsteadOfTraitOrder) {
        for (const bool nullable : {false, true}) {
            const auto initial = NativeDistinctAliases(nullable, false);
            Parse(initial);
            for (const bool reverseTraits : {false, true}) {
                for (const bool reverseKeys : {false, true}) {
                    const auto final = NativeDistinctAliases(nullable, true, reverseTraits, reverseKeys, true);
                    Parse(final);
                    UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, final.Json), "VERIFIED_BOUNDED");
                }
            }
        }
    }

    Y_UNIT_TEST(NativeDistinctAliasesStillObserveOutputBindings) {
        const auto initial = NativeDistinctAliases(false, false);
        const auto changed = NativeDistinctAliases(false, true, true, true, true, true);
        Parse(initial);
        Parse(changed);
        UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, changed.Json), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeDistinctAliasesRejectUncoveredGroupingKeys) {
        UNIT_ASSERT(!NativeDistinctAliases(false, true, true, false, false, false, true).IsSupported());
    }

    Y_UNIT_TEST(NativeDistinctRepeatedAliasesPreserveAllResultColumns) {
        for (const bool nullable : {false, true}) {
            const auto initial = NativeRepeatedDistinctAliases(nullable, false);
            Parse(initial);
            for (const bool duplicateKeys : {false, true}) {
                for (const bool reverseKeys : {false, true}) {
                    const auto final = NativeRepeatedDistinctAliases(nullable, true,
                        duplicateKeys, reverseKeys, true);
                    auto snapshot = Parse(final);
                    for (const auto& node : snapshot["plan"]["nodes"].GetArraySafe()) {
                        if (node["op"].GetStringSafe() == "aggregate") {
                            UNIT_ASSERT_VALUES_EQUAL(node["keys"].GetArraySafe().size(), 2);
                            UNIT_ASSERT_VALUES_EQUAL(node["aggregates"].GetArraySafe().size(), 3);
                        }
                    }
                    UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, final.Json), "VERIFIED_BOUNDED");
                }
            }
        }
    }

    Y_UNIT_TEST(NativeDistinctRepeatedAliasesRejectSchemaAndBindingMutations) {
        for (const TStringBuf mutation : {"wrong_type", "wrong_nullability", "missing_column",
             "extra_column", "foreign_key", "missing_key", "wrong_function", "distinct_flag"})
        {
            UNIT_ASSERT_C(!NativeRepeatedDistinctAliases(true, true, true, false, false, mutation).IsSupported(), mutation);
        }
        // Ordinary Aggregate admission still rejects repeated keys.
        UNIT_ASSERT(!NativeRepeatedDistinctAliases(false, false, true).IsSupported());
    }

    Y_UNIT_TEST(NativeDistinctRepeatedAliasesObserveBindingAndMultiplicity) {
        const auto initial = NativeRepeatedDistinctAliases(false, false);
        Parse(initial);
        for (const TStringBuf mutation : {"wrong_binding", "remove_distinct"}) {
            const auto changed = NativeRepeatedDistinctAliases(false, true, true, false, true, mutation);
            Parse(changed);
            UNIT_ASSERT_VALUES_EQUAL_C(Verify(initial.Json, changed.Json), "COUNTEREXAMPLE", mutation);
        }
    }


    Y_UNIT_TEST(NativeDecimalAverageFollowsActualDistinctExpansionBindings) {
        for (const bool extraDistinct : {false, true}) {
            for (const bool corruptCount : {false, true}) {
                const auto [initial, final] = NativeDecimalAverageDistinct(extraDistinct, corruptCount);
                UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, final.Json),
                    corruptCount ? "COUNTEREXAMPLE" : "VERIFIED_BOUNDED");
            }
        }
    }

    Y_UNIT_TEST(NativeDecimalAverageDistinctPrefixesAreVerifiableBeforeStages) {
        for (const bool extraDistinct : {false, true}) {
            for (const bool corruptCount : {false, true}) {
                const auto [initial, prefix] = NativeDecimalAverageDistinct(extraDistinct, corruptCount, false);
                UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, prefix.Json, true),
                    corruptCount ? "COUNTEREXAMPLE" : "VERIFIED_BOUNDED");
            }
        }
    }

    Y_UNIT_TEST(NativeDecimalAverageCarrierRejectsInconsistentFinalAnnotations) {
        for (const bool staged : {false, true}) {
            for (ui32 malformed = 1; malformed <= 3; ++malformed) {
                const auto [initial, final] = NativeDecimalAverageDistinct(false, false, staged, malformed);
                UNIT_ASSERT(initial.IsSupported());
                UNIT_ASSERT(!final.IsSupported());
                UNIT_ASSERT_STRING_CONTAINS(final.UnsupportedReason,
                    "Decimal avg final annotation disagrees with the audited source and state");
            }
        }
    }

    Y_UNIT_TEST(NativeNullableCountDistinctMatchesProductionExpansion) {
        for (const bool grouped : {false, true}) {
            for (const bool nullable : {false, true}) {
                const auto [initial, final] = NativeNullableCountDistinct(grouped, nullable);
                UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, final.Json), "VERIFIED_BOUNDED");
            }
        }
    }

    Y_UNIT_TEST(NativeNullableCountDistinctDetectsLostDeduplication) {
        for (const bool grouped : {false, true}) {
            const auto [initial, changed] = NativeNullableCountDistinct(grouped, true, true);
            UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, changed.Json), "COUNTEREXAMPLE");
        }
    }

    Y_UNIT_TEST(NativeOuterJoinUniquenessDoesNotUseOnlyNullExtendedKeys) {
        for (const TString& kind : {TString("Inner"), TString("Left"), TString("Right"),
                                   TString("Full"), TString("Exclusion")}) {
            for (const bool joinsRightKey : {false, true}) {
                NTests::TIdTestContext f;
                const auto a = f.Id("a"), b = f.Id("b"), payload = f.Id("payload");
                auto left = f.Read({a}), right = f.Read({b, payload});
                left->Props.Metadata.emplace();
                right->Props.Metadata.emplace();
                left->Props.Metadata->ColumnsCount = 1;
                right->Props.Metadata->ColumnsCount = 2;
                left->Props.Metadata->KeyColumns = {a};
                right->Props.Metadata->KeyColumns = {b};
                auto join = MakeIntrusive<TOpJoin>(left, right, f.Pos, kind,
                    TJoinIUs{{a, joinsRightKey ? b : payload}});
                join->ComputeMetadata(f.RboCtx, f.Props);
                const TOrderedIUs<> expected = kind == "Full" || kind == "Exclusion" ? TOrderedIUs<>{}
                    : kind == "Right" ? TOrderedIUs<>{b}
                    : joinsRightKey ? TOrderedIUs<>{a}
                    : kind == "Left" ? TOrderedIUs<>{a, b} : TOrderedIUs<>{b};
                UNIT_ASSERT_C(join->Props.Metadata->KeyColumns == expected,
                    kind << " uses null-extended columns as its only unique key");
            }
        }
    }

    Y_UNIT_TEST(NativeFullJoinNullKeysDoNotPermitMultiplicityChangingElimination) {
        for (const bool equalNulls : {false, true}) {
            TFixture f("Int32");
            const auto pos = TPositionHandle();
            const auto* int32 = f.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32);
            const auto* optional = f.Expr.MakeType<TOptionalExprType>(int32);
            const auto rightKey = f.Registry.Add(TInfoUnit("right", "k"));
            const auto makeRead = [&](TString alias, TInfoUnitId id, const TTypeAnnotationNode* type) {
                auto read = MakeIntrusive<TOpRead>(std::move(alias), TUnorderedIUs{id}, NYql::EStorageType::RowStorage,
                    NOpt::BuildTableMeta(*f.Table, pos, f.Expr).Ptr(), nullptr, nullptr,
                    std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, pos);
                f.SetType(*read, {{id, type}});
                return read;
            };
            auto a = makeRead("same", f.Key, int32);
            auto b = makeRead("right", rightKey, int32);
            auto c = makeRead("same", f.Group, optional);
            auto predicate = MakeConstant("Bool", "false", pos, &f.Expr);
            predicate.GetExpressionBody()->SetTypeAnn(f.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Bool));
            auto empty = MakeIntrusive<TOpFilter>(a, pos, predicate);
            f.SetType(*empty, {{f.Key, int32}});
            auto full = MakeIntrusive<TOpJoin>(empty, b, pos, "Full", TJoinIUs{{f.Key, rightKey}});
            f.SetType(*full, {{f.Key, optional}, {rightKey, optional}});
            TJoinIUs keys;
            keys.Add({f.Group, f.Key, equalNulls});
            auto outer = MakeIntrusive<TOpJoin>(c, full, pos, "Left", keys);
            f.SetType(*outer, {{f.Group, optional}, {f.Key, optional}, {rightKey, optional}});
            TOpRoot root(outer, pos, {{f.Group, "group"}});
            root.PlanProps.InfoUnitRegistry = f.Registry;
            for (const auto& item : root) {
                item.Current->BindExpressionPlanProps(&root.PlanProps);
            }
            root.RecomputeOutputIUsSubtree();
            root.ComputeParents();
            a->ComputeMetadata(f.Rbo, root.PlanProps);
            b->ComputeMetadata(f.Rbo, root.PlanProps);
            c->ComputeMetadata(f.Rbo, root.PlanProps);
            empty->ComputeMetadata(f.Rbo, root.PlanProps);
            full->ComputeMetadata(f.Rbo, root.PlanProps);
            outer->ComputeMetadata(f.Rbo, root.PlanProps);
            ComputePlanLiveness(root);
            const auto before = f.Export(root);
            Parse(before);
            const bool eliminated = TEliminateLeftJoinRule().MatchAndApply(root.MutableChild(0), f.Rbo, root.PlanProps);
            root.ComputeParents();
            const auto after = f.Export(root);
            Parse(after);
            UNIT_ASSERT_VALUES_EQUAL_C(Verify(before.Json, after.Json, true), "VERIFIED_BOUNDED",
                "equalNulls=" << equalNulls << " eliminated=" << eliminated);
        }
    }

    Y_UNIT_TEST(NativeTableLookupRequiresTheCompletePrimaryKeyForUniqueness) {
        for (ui32 mask = 0; mask < 4; ++mask) {
            for (const bool joinMode : {false, true}) {
                NTests::TIdTestContext f;
                auto& table = f.Tables->GetOrAddTable("ut", "/Root", "/Root/table");
                table.Metadata = MakeIntrusive<TKikimrTableMetadata>("ut", "/Root/table");
                table.Metadata->DoesExist = true;
                table.Metadata->PathId = TKikimrPathId(1, 1);
                table.Metadata->SchemaVersion = 1;
                table.Metadata->Kind = EKikimrTableKind::Datashard;
                table.Metadata->KeyColumnNames = {"a", "b"};
                ui32 columnId = 0;
                for (const TString& name : {TString("a"), TString("b"), TString("payload")}) {
                    table.Metadata->Columns.emplace(name, TKikimrColumnMetadata(name, ++columnId, "Int32", true));
                    table.Metadata->ColumnOrder.push_back(name);
                }
                UNIT_ASSERT(table.Load(f.ExprCtx));
                const auto sourceA = f.Id("a"), sourceB = f.Id("b");
                const auto outputA = f.Id("a"), outputB = f.Id("b"), payload = f.Id("payload");
                auto source = f.Read({sourceA, sourceB});
                source->ComputeMetadata(f.RboCtx, f.Props);
                TUnorderedIUs columns{payload};
                if (mask & 1) { columns.Add(outputA); }
                if (mask & 2) { columns.Add(outputB); }
                TOpTableLookup::TLookupKeys keys;
                keys.Append(sourceA, "a");
                keys.Append(sourceB, "b");
                auto lookup = joinMode
                    ? MakeIntrusive<TOpTableLookup>(source, f.Pos, source->GetTable(), columns, keys, "Inner", std::nullopt)
                    : MakeIntrusive<TOpTableLookup>(source, f.Pos, source->GetTable(), columns, keys);
                lookup->ComputeMetadata(f.RboCtx, f.Props);
                const auto expected = !joinMode && mask == 3 ? TOrderedIUs<>{outputA, outputB} : TOrderedIUs<>{};
                UNIT_ASSERT_C(lookup->Props.Metadata->KeyColumns == expected,
                    "Lookup advertised a partial composite key as unique: mask=" << mask << " join=" << joinMode);
            }
        }
    }

    Y_UNIT_TEST(NativeGroupedAggregateRetainsKnownGroupingUniqueness) {
        for (const bool knownInputKey : {true, false}) {
            NTests::TIdTestContext f;
            const auto key = f.Id("key"), value = f.Id("value"), total = f.Id("total");
            auto read = f.Read({key, value});
            read->Props.Metadata.emplace();
            read->Props.Metadata->ColumnsCount = 2;
            if (knownInputKey) {
                read->Props.Metadata->KeyColumns = {key};
            }
            auto aggregate = MakeIntrusive<TOpAggregate>(read,
                TAggregationIUs{{total, {value, "sum", false, false}}},
                TOrderedIUs<>{key, value}, EOpPhase::Undefined, false, f.Pos);
            aggregate->ComputeMetadata(f.RboCtx, f.Props);
            const auto expected = knownInputKey ? TOrderedIUs<>{key} : TOrderedIUs<>{key, value};
            UNIT_ASSERT_C(aggregate->Props.Metadata->KeyColumns == expected,
                "Grouped aggregate lost uniqueness when its input key was unknown");
        }
    }

    Y_UNIT_TEST(NativeFullJoinDoesNotAdvertiseOneSidedShuffling) {
        for (const auto algo : {EJoinAlgoType::GraceJoin, EJoinAlgoType::MapJoin}) {
            for (const TString& kind : {TString("Inner"), TString("Left"), TString("Right"),
                                       TString("Full"), TString("Exclusion")}) {
                NTests::TIdTestContext f;
                f.Config->OptShuffleElimination = true;
                const auto leftKey = f.Id("left"), rightKey = f.Id("right"), count = f.Id("count");
                auto left = f.Read({leftKey}), right = f.Read({rightKey});
                left->Props.Metadata.emplace();
                right->Props.Metadata.emplace();
                left->Props.Metadata->ColumnsCount = right->Props.Metadata->ColumnsCount = 1;
                left->Props.Metadata->ShuffledByColumns = {leftKey};
                right->Props.Metadata->ShuffledByColumns = {rightKey};
                auto join = MakeIntrusive<TOpJoin>(left, right, f.Pos, kind, TJoinIUs{{leftKey, rightKey}});
                join->Props.JoinAlgo = algo;
                join->ComputeMetadata(f.RboCtx, f.Props);
                const bool bothSidesNullable = kind == "Full" || kind == "Exclusion";
                const auto expected = bothSidesNullable || (kind == "Right" && algo == EJoinAlgoType::MapJoin)
                    ? TOrderedIUs<>{} : TOrderedIUs<>{kind == "Right" ? rightKey : leftKey};
                UNIT_ASSERT_C(join->Props.Metadata->ShuffledByColumns == expected,
                    kind << " advertises a distribution that unmatched rows need not preserve");
                if (bothSidesNullable) {
                    auto aggregate = MakeIntrusive<TOpAggregate>(join,
                        TAggregationIUs{{count, {rightKey, "count", false, false}}},
                        TOrderedIUs<>{leftKey}, EOpPhase::Undefined, false, f.Pos);
                    UNIT_ASSERT(!CanEliminateAggregateShuffle(*aggregate, f.RboCtx));
                }
            }
        }
    }

    Y_UNIT_TEST(NativeStablePickleMatchesRuntimeScalarPacking) {
        CheckStableScalarPacking<bool>(NUdf::EDataSlot::Bool, {false, true});
        CheckStableScalarPacking<i8>(NUdf::EDataSlot::Int8, {-128, -1, 0, 1, 127});
        CheckStableScalarPacking<ui8>(NUdf::EDataSlot::Uint8, {0, 1, 127, 128, 255});
        CheckStableScalarPacking<i16>(NUdf::EDataSlot::Int16, {-32768, -1, 0, 1, 127, 128, 32767});
        CheckStableScalarPacking<ui16>(NUdf::EDataSlot::Uint16, {0, 1, 127, 128, 65535});
        CheckStableScalarPacking<i32>(NUdf::EDataSlot::Int32,
            {std::numeric_limits<i32>::min(), -1, 0, 1, 127, 128, 16383, 16384, std::numeric_limits<i32>::max()});
        CheckStableScalarPacking<ui32>(NUdf::EDataSlot::Uint32,
            {0, 1, 127, 128, 16383, 16384, std::numeric_limits<ui32>::max()});
        CheckStableScalarPacking<i64>(NUdf::EDataSlot::Int64,
            {std::numeric_limits<i64>::min(), -1, 0, 1, 127, 128, 16383, 16384, std::numeric_limits<i64>::max()});
        CheckStableScalarPacking<ui64>(NUdf::EDataSlot::Uint64,
            {0, 1, 127, 128, 16383, 16384, std::numeric_limits<ui64>::max()});
        CheckStableScalarPacking<ui16>(NUdf::EDataSlot::Date, {0, 1, 127, 128, 49672});
        CheckStableScalarPacking<ui32>(NUdf::EDataSlot::Datetime, {0, 1, 127, 128, 4291747199U});
        CheckStableScalarPacking<ui64>(NUdf::EDataSlot::Timestamp, {0, 1, 127, 128, 4291747199999999ULL});
    }

    Y_UNIT_TEST(NativeStablePickleMatchesRuntimeTextPacking) {
        CheckStableTextPacking(NUdf::EDataSlot::String);
        CheckStableTextPacking(NUdf::EDataSlot::Utf8);
    }

    Y_UNIT_TEST(NativeStablePicklePreservesNullableKeysAndDetectsChangedInputs) {
        for (const TString& type : {TString("Bool"), TString("Int32"), TString("Int64"), TString("Date"),
                                   TString("Datetime"), TString("Timestamp"), TString("String"), TString("Utf8")}) {
            for (const bool nullable : {false, true}) {
                const auto initial = NativeStablePickle(type, nullable, false);
                const auto final = NativeStablePickle(type, nullable, true);
                Parse(initial);
                Parse(final);
                UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, final.Json), "VERIFIED_BOUNDED");
                const auto changed = NativeStablePickle(type, nullable, true, true);
                Parse(changed);
                UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, changed.Json), "COUNTEREXAMPLE");
                if (type != "String" || nullable) {
                    UNIT_ASSERT(!NativeStablePickle(type, nullable, false, false, true).IsSupported());
                }
            }
        }
        for (const TString& type : {TString("Double")}) {
            UNIT_ASSERT(!NativeStablePickle(type, true, false).IsSupported());
        }
    }

    Y_UNIT_TEST(NativePruningKeepsGroupingSetsAggregateTemplate) {
        for (const bool keepCount : {true, false}) {
            TFixture fixture;
            const auto pos = TPositionHandle();
            const auto count = fixture.Registry.Add(TInfoUnit("", "count"));
            const auto output = fixture.Registry.Add(TInfoUnit("", "count_output"));
            auto aggregate = MakeIntrusive<TOpAggregate>(fixture.Read(),
                TAggregationIUs{{count, {fixture.Key, "count", false, false}}},
                TOrderedIUs<>{}, EOpPhase::Undefined, false, pos);
            auto grouping = MakeIntrusive<TOpGroupingSets>(aggregate,
                TVector<TUnorderedIUs>{{}, {}}, TMappedIUs<TInfoUnitId>{{output, count}}, pos);
            TOpRoot root(grouping, pos, keepCount
                ? TOrderedIUs<TString>{{output, "count"}} : TOrderedIUs<TString>{});
            root.PlanProps.InfoUnitRegistry = fixture.Registry;
            TGlobalPruningStage("early pruning").RunStage(root, fixture.Rbo);
            UNIT_ASSERT_C(grouping->GetInput()->Kind == EOperator::Aggregate,
                "GroupingSets lost its Aggregate definition during pruning");
            UNIT_ASSERT_VALUES_EQUAL(aggregate->GetAggregationTraits().Keys().Empty(), !keepCount);
            UNIT_ASSERT_VALUES_EQUAL(grouping->GetGroupingSets().size(), 2);
        }
    }

    Y_UNIT_TEST(NativePruningFoldsOrdinaryResultlessScalarAggregate) {
        TFixture fixture;
        const auto pos = TPositionHandle();
        auto aggregate = MakeIntrusive<TOpAggregate>(fixture.Read(),
            TAggregationIUs{}, TOrderedIUs<>{}, EOpPhase::Undefined, false, pos);
        TOpRoot root(aggregate, pos, TOrderedIUs<TString>{});
        root.PlanProps.InfoUnitRegistry = fixture.Registry;
        FinishLogicalRewrite(root, fixture.Expr);
        UNIT_ASSERT(root.GetInput()->Kind == EOperator::EmptySource);
    }

    Y_UNIT_TEST(NativeDateCoalesceZeroSurvivesConstantFolding) {
        for (const TString& type : {TString("String"), TString("Utf8")}) {
            for (const bool nullable : {false, true}) {
                const auto before = NativeDateCast(type, nullable, false, false, false, false, true, false);
                const auto after = NativeDateCast(type, nullable, false, false, true, false, true, true);
                Parse(before);
                Parse(after);
                UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
                const auto dropped = NativeDateCast(type, nullable, false, false, true);
                Parse(dropped);
                UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, dropped.Json), "COUNTEREXAMPLE");
                UNIT_ASSERT(!NativeDateCast(type, nullable, false, false, false, true, true).IsSupported());
            }
        }
    }

    Y_UNIT_TEST(NativeCompleteIntegerLiteralCastsSurviveConstantFolding) {
        for (const TString& source : {TString("Int8"), TString("Uint8"), TString("Int16"),
                                     TString("Uint16"), TString("Int32"), TString("Uint32")}) {
            const auto before = NativeIntegralCoalesce("Int64", false, false);
            const auto after = NativeIntegralCoalesce("Int64", true, true, false, false, source);
            const auto changed = NativeIntegralCoalesce("Int64", true, true, true, false, source);
            Parse(before);
            Parse(after);
            Parse(changed);
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, changed.Json), "COUNTEREXAMPLE");
        }
    }

    Y_UNIT_TEST(NativeLiteralJustPreservesOptionalConstantFolding) {
        const auto initial = NativeExactLiteralJust("Uint64", "1", true);
        const auto folded = NativeExactLiteralJust("Uint64", "1", false, true);
        const auto changed = NativeExactLiteralJust("Uint64", "2", false, true);
        Parse(initial);
        Parse(folded);
        Parse(changed);
        UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, folded.Json), "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, changed.Json), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeLiteralJustExportsExactValuesAndOptionalTypes) {
        const TVector<std::pair<TString, TString>> cases{
            {"Bool", "true"}, {"Bool", "false"},
            {"Int8", "-128"}, {"Int16", "-32768"}, {"Int32", "-2147483648"},
            {"Int64", "-9223372036854775808"},
            {"Uint8", "255"}, {"Uint16", "65535"}, {"Uint32", "4294967295"},
            {"Uint64", "18446744073709551615"},
            {"String", ""}, {"String", "literal"}, {"Utf8", "\xC3\xA9"},
            {"Date", ToString(NUdf::MAX_DATE - 1)},
            {"Datetime", ToString(NUdf::MAX_DATETIME - 1)},
            {"Timestamp", ToString(NUdf::MAX_TIMESTAMP - 1)},
            {"Timestamp64", ToString(NUdf::MIN_TIMESTAMP64)},
            {"Timestamp64", "-1"}, {"Timestamp64", ToString(NUdf::MAX_TIMESTAMP64)},
        };
        for (const auto& [type, value] : cases) {
            const auto json = Parse(NativeExactLiteralJust(type, value));
            ui32 found = 0;
            for (const auto& node : json["plan"]["nodes"].GetArraySafe()) {
                if (node["op"].GetStringSafe() != "project") {
                    continue;
                }
                for (const auto& column : node["columns"].GetArraySafe()) {
                    const auto& expression = column["expression"];
                    if (expression["kind"].GetStringSafe() == "column") {
                        continue;
                    }
                    UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), "if");
                    UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), type);
                    UNIT_ASSERT(expression["nullable"].GetBooleanSafe());
                    UNIT_ASSERT_VALUES_EQUAL(expression["condition"]["kind"].GetStringSafe(), "literal");
                    UNIT_ASSERT_VALUES_EQUAL(expression["condition"]["type"].GetStringSafe(), "Bool");
                    UNIT_ASSERT(expression["condition"]["value"].GetBooleanSafe());
                    UNIT_ASSERT_VALUES_EQUAL(expression["else"]["kind"].GetStringSafe(), "null");
                    UNIT_ASSERT_VALUES_EQUAL(expression["else"]["type"].GetStringSafe(), type);
                    const auto& present = expression["then"];
                    UNIT_ASSERT_VALUES_EQUAL(present["kind"].GetStringSafe(), "literal");
                    UNIT_ASSERT_VALUES_EQUAL(present["type"].GetStringSafe(), type);
                    if (type == "Bool") {
                        UNIT_ASSERT_VALUES_EQUAL(present["value"].GetBooleanSafe(), value == "true");
                    } else if (type == "String" || type == "Utf8") {
                        UNIT_ASSERT_VALUES_EQUAL(present["value"].GetStringSafe(), value);
                    } else if (type.StartsWith("Int") || type == "Timestamp64") {
                        UNIT_ASSERT_VALUES_EQUAL(present["value"].GetIntegerSafe(), FromString<i64>(value));
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(present["value"].GetUIntegerSafe(), FromString<ui64>(value));
                    }
                    ++found;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(found, 1);
        }
    }

    Y_UNIT_TEST(NativeLiteralJustRejectsInvalidAnnotationsValuesAndMetadata) {
        for (const TStringBuf mutation : {"literal_annotation", "literal_optional", "literal_arity",
                                          "result_annotation", "result_required", "nested_optional"}) {
            const auto result = NativeExactLiteralJust("Uint64", "1", false, false, mutation);
            UNIT_ASSERT_C(!result.IsSupported(), mutation);
        }
        const TVector<std::pair<TString, TString>> invalid{
            {"Bool", "1"}, {"Int8", "128"}, {"Uint8", "256"}, {"Uint64", "-1"},
            {"Uint64", "18446744073709551616"}, {"Int64", "-9223372036854775809"},
            {"Date", ToString(NUdf::MAX_DATE)},
            {"Datetime", ToString(NUdf::MAX_DATETIME)},
            {"Timestamp", ToString(NUdf::MAX_TIMESTAMP)},
            {"Timestamp64", ToString(NUdf::MIN_TIMESTAMP64 - 1)},
            {"Timestamp64", ToString(NUdf::MAX_TIMESTAMP64 + 1)},
            {"String", "\xFF"}, {"Utf8", "\xFF"},
        };
        for (const auto& [type, value] : invalid) {
            UNIT_ASSERT_C(!NativeExactLiteralJust(type, value).IsSupported(), type << ": " << value);
        }
        const TVector<std::pair<TStringBuf, TStringBuf>> unsafe{
            {"executed", "executed Result node"}, {"position", "position-aware node"},
            {"semilattice", "side-effecting or CSE-unsafe"},
            {"general", "side-effecting or CSE-unsafe"}, {"unordered", "unordered children"},
        };
        for (const ui32 depth : {0u, 1u, 2u}) {
            for (const auto& [mutation, reason] : unsafe) {
                if (depth == 2 && mutation == "unordered") {
                    continue; // Atom has no children to order.
                }
                const auto result = NativeExactLiteralJust("Uint64", "1", false, false, mutation, depth);
                UNIT_ASSERT_C(!result.IsSupported(), depth << ": " << mutation);
                UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, reason);
            }
        }
    }

    Y_UNIT_TEST(NativeDynamicDateCastPreservesNullAndRejectsContradictoryTypes) {
        for (const TString& type : {TString("String"), TString("Utf8")}) {
            for (bool nullable : {false, true}) {
                const auto initial = NativeDateCast(type, nullable, false, false, false);
                const auto final = NativeDateCast(type, nullable, nullable, false, true);
                Parse(initial);
                Parse(final);
                UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, final.Json), "VERIFIED_BOUNDED");
                UNIT_ASSERT(!NativeDateCast(type, nullable, false, false, false, true).IsSupported());
                if (nullable) {
                    const auto changed = NativeDateCast(type, true, true, true, true);
                    Parse(changed);
                    UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, changed.Json), "COUNTEREXAMPLE");
                }
            }
        }
    }

    Y_UNIT_TEST(NativePushedDateCastComparisonsPreserveNullsAndConstants) {
        for (const TString& textType : {TString("String"), TString("Utf8")}) {
            for (bool nullable : {false, true}) {
                for (const TString& comparison : {TString("=="), TString("!="), TString("<"),
                                                   TString("<="), TString(">"), TString(">=")}) {
                    for (bool justConstant : {false, true}) {
                        const auto before = NativeOlapDatePredicate(textType, nullable, comparison, justConstant, false);
                        const auto after = NativeOlapDatePredicate(textType, nullable, comparison, justConstant, true);
                        Parse(before);
                        Parse(after);
                        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
                    }
                    const auto before = NativeOlapDatePredicate(textType, nullable, comparison, true, false);
                    const auto changed = NativeOlapDatePredicate(textType, nullable, comparison, true, true, 11335);
                    Parse(changed);
                    UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, changed.Json), "COUNTEREXAMPLE");
                }
            }
        }
    }

    Y_UNIT_TEST(NativePushedDateCastComparisonsRejectMalformedApply) {
        for (const TStringBuf mutation : {"wrong_target", "wrong_literal_type", "wrong_just_type",
                                          "true_fallback", "wrong_binding", "typed_cast", "extra_lambda_arg"}) {
            const auto result = NativeOlapDatePredicate("Utf8", true, ">=", true, true, 11334, mutation);
            UNIT_ASSERT_C(!result.IsSupported(), mutation);
        }
    }


    Y_UNIT_TEST(NativeReadPointKeepsPrunedKeyPrivateToPredicate) {
        const auto before = NativeInt32ReadPoint(false, false, 42, false, true);
        Parse(before);
        for (bool staged : {false, true}) {
            const auto after = NativeInt32ReadPoint(true, staged, 42, false, true);
            const auto changed = NativeInt32ReadPoint(true, staged, 43, false, true);
            Parse(after);
            Parse(changed);
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json, !staged), "VERIFIED_BOUNDED");
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, changed.Json, !staged), "COUNTEREXAMPLE");
        }
    }

    Y_UNIT_TEST(NativeNarrowDecimalZeroWrappersSurviveConstantFolding) {
        for (const ui8 precision : {ui8(7), ui8(15), ui8(35)}) {
            const auto before = NativeNarrowDecimalZero(false, false, precision);
            Parse(before);
            for (bool staged : {false, true}) {
                const auto after = NativeNarrowDecimalZero(true, staged, precision);
                const auto changed = NativeNarrowDecimalZero(true, staged, precision, true);
                Parse(after);
                Parse(changed);
                UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json, !staged), "VERIFIED_BOUNDED");
                UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, changed.Json, !staged), "COUNTEREXAMPLE");
            }
        }
        UNIT_ASSERT(!NativeNarrowDecimalZero(false, false, 7, false, true).IsSupported());
    }


    Y_UNIT_TEST(NativeDateIntervalsAcceptReviewedStringCasts) {
        constexpr i64 day = 86'400'000'000LL;
        for (const TString& type : {TString("String"), TString("Utf8")}) {
            for (bool nullable : {false, true}) {
                for (bool subtract : {false, true}) {
                    const auto before = NativeDateCast(type, nullable, false, false, false,
                        false, false, false, 5 * day, false, subtract);
                    const auto after = NativeDateCast(type, nullable, false, false, true,
                        false, false, false, 5 * day, true, subtract);
                    const auto changed = NativeDateCast(type, nullable, false, false, true,
                        false, false, false, 6 * day, true, subtract);
                    Parse(before);
                    Parse(after);
                    Parse(changed);
                    UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
                    UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, changed.Json), "COUNTEREXAMPLE");
                }
                UNIT_ASSERT(!NativeDateCast(type, nullable, false, false, false,
                    true, false, false, day).IsSupported());
                UNIT_ASSERT(!NativeDateCast(type, nullable, false, false, false,
                    false, false, false, day + 1).IsSupported());
            }
        }
    }

    Y_UNIT_TEST(NativeTemporalColumnsPreserveUnitsAcrossStages) {
        for (const TString& type : {TString("Date"), TString("Datetime"), TString("Timestamp"), TString("Timestamp64")}) {
            TFixture fixture(type);
            auto read = fixture.Read();
            read->GetColumns() = TUnorderedIUs{fixture.Key, fixture.Group};
            fixture.SetType(*read, {
                {fixture.Key, fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32)},
                {fixture.Group, fixture.Expr.MakeType<TOptionalExprType>(
                    fixture.Expr.MakeType<TDataExprType>(*NUdf::FindDataSlot(type)))}});
            auto sorted = MakeIntrusive<TOpSort>(read, TPositionHandle(),
                TSortIUs{{fixture.Group, {true, true}}});
            sorted->Type = read->Type;
            TOpRoot root(sorted, TPositionHandle(), {{fixture.Key, "key"}, {fixture.Group, "time"}});
            const auto before = fixture.Export(root);
            Parse(before);
            fixture.StageSerial(root);
            const auto after = fixture.Export(root);
            Parse(after);
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
        }
    }


    Y_UNIT_TEST(NativeTimestamp64ExportsSignedLiteralBoundariesAndComparisons) {
        for (const i64 value : {NUdf::MIN_TIMESTAMP64, i64(-1), i64(0), NUdf::MAX_TIMESTAMP64}) {
            for (const bool nullable : {false, true}) {
                const auto exported = NativeTimestamp64Expression(ToString(value), "literal", nullable);
                auto json = Parse(exported);
                bool found = false;
                for (const auto& node : json["plan"]["nodes"].GetArraySafe()) {
                    if (node["op"].GetStringSafe() != "project") {
                        continue;
                    }
                    for (const auto& column : node["columns"].GetArraySafe()) {
                        const auto& expression = column["expression"];
                        if (expression["kind"].GetStringSafe() == "literal" &&
                            expression["type"].GetStringSafe() == "Timestamp64")
                        {
                            UNIT_ASSERT_VALUES_EQUAL(expression["value"].GetIntegerSafe(), value);
                            found = true;
                        }
                    }
                }
                UNIT_ASSERT(found);
                Parse(NativeTimestamp64Expression(ToString(value), "<", nullable));
                Parse(NativeTimestamp64Expression(ToString(value), "==", nullable));
            }
        }
    }

    Y_UNIT_TEST(NativeTimestamp64RejectsOutOfDomainAndMismatchedLiterals) {
        for (const TString& value : {ToString(NUdf::MIN_TIMESTAMP64 - 1),
                                    ToString(NUdf::MAX_TIMESTAMP64 + 1),
                                    TString("9223372036854775808"), TString("1970-01-01T00:00:00Z")}) {
            UNIT_ASSERT(!NativeTimestamp64Expression(value).IsSupported());
        }
        UNIT_ASSERT(!NativeTimestamp64Expression("0", "literal", true,
            "Timestamp64", "Timestamp64", "Timestamp").IsSupported());
        UNIT_ASSERT(!NativeTimestamp64Expression("0", "literal", true,
            "Timestamp64", "Timestamp64", "Timestamp64", true).IsSupported());
    }

    Y_UNIT_TEST(NativeTimestamp64RejectsUnsafeLiteralMetadata) {
        Parse(NativeTimestamp64Expression("0"));
        const TVector<std::pair<ETimestamp64LiteralMetadataMutation, TStringBuf>> cases{
            {ETimestamp64LiteralMetadataMutation::RootResult, "executed Result node"},
            {ETimestamp64LiteralMetadataMutation::RootPosition, "position-aware node"},
            {ETimestamp64LiteralMetadataMutation::RootSemilatticeEffects, "side-effecting or CSE-unsafe"},
            {ETimestamp64LiteralMetadataMutation::RootGeneralEffects, "side-effecting or CSE-unsafe"},
            {ETimestamp64LiteralMetadataMutation::RootUnordered, "unordered children"},
            {ETimestamp64LiteralMetadataMutation::AtomResult, "executed Result node"},
            {ETimestamp64LiteralMetadataMutation::AtomPosition, "position-aware node"},
            {ETimestamp64LiteralMetadataMutation::AtomSemilatticeEffects, "side-effecting or CSE-unsafe"},
            {ETimestamp64LiteralMetadataMutation::AtomGeneralEffects, "side-effecting or CSE-unsafe"},
        };
        for (const auto& [mutation, reason] : cases) {
            const auto result = NativeTimestamp64Expression("0", "literal", true,
                "Timestamp64", "Timestamp64", "Timestamp64", false, mutation);
            UNIT_ASSERT(!result.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, reason);
        }
    }

    Y_UNIT_TEST(NativeTimestamp64RejectsUnreviewedConversionsArithmeticAndPacking) {
        for (const TString& type : {TString("Int64"), TString("Timestamp"), TString("Date")}) {
            UNIT_ASSERT(!NativeTimestamp64Expression("0", "<", true,
                "Timestamp64", type, type).IsSupported());
            UNIT_ASSERT(!NativeTimestamp64Expression("0", "==", true,
                "Timestamp64", type, type).IsSupported());
            for (const TString& cast : {TString("SafeCast"), TString("Convert")}) {
                UNIT_ASSERT(!NativeTimestamp64Expression("0", cast, true,
                    "Timestamp64", type, type).IsSupported());
                UNIT_ASSERT(!NativeTimestamp64Expression("0", cast, true,
                    type, "Timestamp64", "Timestamp64").IsSupported());
            }
        }
        UNIT_ASSERT(!NativeTimestamp64Expression("1", "+").IsSupported());
        for (const bool nullable : {false, true}) {
            UNIT_ASSERT(!NativeStablePickle("Timestamp64", nullable, false).IsSupported());
        }
    }


    Y_UNIT_TEST(NativeTimestamp64ExtremaPreserveNullableAndEmptyPartitionResults) {
        for (const bool nullable : {false, true}) {
            for (const TString& function : {TString("min"), TString("max")}) {
                const auto before = NativeTimestamp64Extrema(function, nullable, false);
                const auto after = NativeTimestamp64Extrema(function, nullable, true);
                Parse(before);
                Parse(after);
                UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
            }
            const auto before = NativeTimestamp64Extrema("min", nullable, false);
            const auto changed = NativeTimestamp64Extrema("max", nullable, true);
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, changed.Json), "COUNTEREXAMPLE");
        }
    }

    Y_UNIT_TEST(NativeTimestamp64PrimaryKeyMetadataSurvivesProjectionAndStaging) {
        TFixture fixture("String", false, "Timestamp64");
        fixture.Table->Metadata->KeyColumnNames = {"x"};
        auto read = fixture.Read();
        const auto copy = fixture.Registry.Add(TInfoUnit("", "copy"));
        auto access = MakeColumnAccess(fixture.Other, TPositionHandle(), &fixture.Expr, &fixture.ExpressionProps);
        access.GetExpressionBody()->SetTypeAnn(fixture.OtherType);
        auto map = MakeIntrusive<TOpMap>(read, TPositionHandle(), TMapIUs{{copy, TMapElement(access)}});
        fixture.SetType(*map, {
            {fixture.Key, fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Int32)},
            {fixture.Other, fixture.OtherType}, {copy, fixture.OtherType}});
        TOpRoot root(map, TPositionHandle(), {{copy, "time"}});
        const auto before = fixture.Export(root);
        auto json = Parse(before);
        const auto& tables = json["schema"]["tables"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
        const auto& keys = tables[0]["unique_keys"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(keys[0]["columns"].GetArraySafe().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(keys[0]["columns"].GetArraySafe()[0].GetStringSafe(), "x");
        fixture.StageSerial(root);
        const auto after = fixture.Export(root);
        Parse(after);
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
    }

    Y_UNIT_TEST(NativeSortUsesPhysicalNullPlacement) {
        CheckNativeSortNullPlacement(false);
    }

    Y_UNIT_TEST(NativeMergeUsesPhysicalNullPlacement) {
        CheckNativeSortNullPlacement(true);
    }

    Y_UNIT_TEST(InitialBoundaryMaterializesNativeFilterTypes) {
        CheckNativeInitialScalarTypes(true);
    }

    Y_UNIT_TEST(InitialBoundaryMaterializesNativeComputedMapTypes) {
        CheckNativeInitialScalarTypes(false);
    }

    Y_UNIT_TEST(NativePushedOlapAggregateIsRejectedBeforeNormalization) {
        for (const bool staged : {false, true}) {
            TFixture controlFixture;
            const auto control = controlFixture.KeyOnlyAggregate(staged);
            UNIT_ASSERT_C(control.IsSupported(), control.UnsupportedReason);
            UNIT_ASSERT(!control.Json.empty());

            TFixture pushedFixture;
            const auto pushed = pushedFixture.KeyOnlyAggregate(staged, false, false, true);
            UNIT_ASSERT(!pushed.IsSupported());
            UNIT_ASSERT(pushed.Json.empty());
            UNIT_ASSERT_STRING_CONTAINS(pushed.UnsupportedReason, "Aggregate pushed to OLAP is not modeled");
        }
    }

    Y_UNIT_TEST(NativePushedOlapAggregateInScalarSubplanIsRejected) {
        TFixture controlFixture;
        const auto control = controlFixture.CorrelatedScalarCount(controlFixture.Key);
        UNIT_ASSERT_C(control.IsSupported(), control.UnsupportedReason);
        UNIT_ASSERT(!control.Json.empty());

        TFixture pushedFixture;
        const auto pushed = pushedFixture.CorrelatedScalarCount(pushedFixture.Key, false, false, false, true);
        UNIT_ASSERT(!pushed.IsSupported());
        UNIT_ASSERT(pushed.Json.empty());
        UNIT_ASSERT_STRING_CONTAINS(pushed.UnsupportedReason, "Aggregate pushed to OLAP is not modeled");
    }

    Y_UNIT_TEST(NativeKeyOnlyAggregatePreservesNullableGroups) {
        TFixture before, after;
        const auto initial = before.KeyOnlyAggregate(false);
        const auto final = after.KeyOnlyAggregate(true);
        Parse(initial);
        Parse(final);
        UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, final.Json), "VERIFIED_BOUNDED");
    }

    Y_UNIT_TEST(NativeKeyOnlyAggregateDetectsAnExtraGroupingKey) {
        TFixture before, after;
        const auto initial = before.KeyOnlyAggregate(false);
        const auto final = after.KeyOnlyAggregate(true, true);
        Parse(initial);
        Parse(final);
        UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, final.Json), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeKeyOnlyAggregateRejectsUnboundDistinctAliases) {
        TFixture fixture;
        const auto result = fixture.KeyOnlyAggregate(false, false, true);
        UNIT_ASSERT(!result.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(result.UnsupportedReason, "without traits");
    }

    Y_UNIT_TEST(NativeStaticTextMembershipMatchesItsEqualityExpansion) {
        for (const TString& sourceType : {TString("String"), TString("Utf8")}) {
            for (const TString& itemType : {TString("String"), TString("Utf8")}) {
                for (bool nullable : {false, true}) {
                    for (bool negate : {false, true}) {
                        UNIT_ASSERT_VALUES_EQUAL(CheckNativeTextMembership(sourceType, itemType, nullable, negate),
                            "VERIFIED_BOUNDED");
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(NativeStaticTextMembershipDetectsAChangedLiteral) {
        for (bool negate : {false, true}) {
            UNIT_ASSERT_VALUES_EQUAL(CheckNativeTextMembership("Utf8", "String", true, negate, true),
                "COUNTEREXAMPLE");
        }
    }

    Y_UNIT_TEST(NativeRequiredTextMembershipMatchesDictionaryLowering) {
        for (const TString& type : {TString("String"), TString("Utf8")}) {
            for (bool negate : {false, true}) {
                UNIT_ASSERT_VALUES_EQUAL(CheckNativeTextMembership(type, type, false, negate, false, true),
                    "VERIFIED_BOUNDED");
                UNIT_ASSERT_VALUES_EQUAL(CheckNativeTextMembership(type, type, false, negate, true, true),
                    "COUNTEREXAMPLE");
            }
        }
    }

    Y_UNIT_TEST(NativeBooleanCoalesceFalsePreservesTruthAndNullness) {
        const auto before = NativeBooleanCoalesce(true, false);
        Parse(before);
        UNIT_ASSERT_STRING_CONTAINS(before.Json, "if_present");
        for (bool staged : {false, true}) {
            const auto equivalent = NativeBooleanCoalesce(false, staged);
            const auto changed = NativeBooleanCoalesce(false, staged, true);
            Parse(equivalent);
            Parse(changed);
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, equivalent.Json, !staged), "VERIFIED_BOUNDED");
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, changed.Json, !staged), "COUNTEREXAMPLE");
        }
        UNIT_ASSERT(!NativeBooleanCoalesce(true, false, false, true).IsSupported());
    }

    Y_UNIT_TEST(NativeTextMembershipPreservesUtf8BytesThroughDictionaryCasts) {
        for (ui32 castKind : {0u, 1u, 2u}) {
            for (bool negate : {false, true}) {
                UNIT_ASSERT_VALUES_EQUAL(CheckNativeTextMembership("Utf8", "String", false, negate, false, true, castKind == 1, castKind == 2),
                    "VERIFIED_BOUNDED");
                UNIT_ASSERT_VALUES_EQUAL(CheckNativeTextMembership("Utf8", "String", false, negate, true, true, castKind == 1, castKind == 2),
                    "COUNTEREXAMPLE");
            }
        }
    }

    Y_UNIT_TEST(NativeRowLiteralRangesMatchLogicalIntervalPredicates) {
        TVector<TNativeRowRangeOptions> cases{
            {.Lower = TString("0"), .LowerInclusive = false},
            {.Upper = TString("42")},
            {.Upper = TString("42"), .UpperInclusive = false},
            {.Lower = TString("-2"), .Upper = TString("3")},
            {.Lower = TString("-2"), .Upper = TString("3"), .LowerInclusive = false, .UpperInclusive = false},
            {.Lower = TString("42"), .Upper = TString("42")},
            {.Lower = TString("42"), .Upper = TString("42"), .LowerInclusive = false},
            {.Lower = TString("43"), .Upper = TString("42")},
            {.Lower = TString("-2147483648"), .Upper = TString("2147483647")},
            {.Lower = TString("2147483647")},
            {.Upper = TString("-2147483648")},
            {},
            {.KeyType = "Int64", .Lower = TString("-9223372036854775808"), .Upper = TString("9223372036854775807")},
        };
        for (const auto& options : cases) {
            const auto before = NativeRowLiteralRange(options, false, false);
            const auto after = NativeRowLiteralRange(options, true, true);
            Parse(before);
            Parse(after);
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
        }
        auto unannotated = cases.front();
        unannotated.AnnotatedWrappers = false;
        const auto before = NativeRowLiteralRange(unannotated, false, false);
        const auto after = NativeRowLiteralRange(unannotated, true, true);
        Parse(before);
        Parse(after);
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
    }

    Y_UNIT_TEST(NativeRowLiteralBoundsAndInclusivityMutationsProduceCounterexamples) {
        const TNativeRowRangeOptions original{.Lower = TString("0"), .Upper = TString("2")};
        const auto before = NativeRowLiteralRange(original, false, false);
        Parse(before);
        for (ui32 mutation = 0; mutation < 4; ++mutation) {
            auto options = original;
            if (mutation == 0) { options.LowerInclusive = false; }
            if (mutation == 1) { options.UpperInclusive = false; }
            if (mutation == 2) { options.Lower = TString("-1"); }
            if (mutation == 3) { options.Upper = TString("3"); }
            const auto after = NativeRowLiteralRange(options, true, true);
            Parse(after);
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "COUNTEREXAMPLE");
        }
        const TNativeRowRangeOptions point{.Lower = TString("42"), .Upper = TString("42")};
        auto shifted = point;
        shifted.Lower = TString("43");
        shifted.Upper = TString("43");
        const auto pointBefore = NativeRowLiteralRange(point, false, false);
        const auto pointAfter = NativeRowLiteralRange(shifted, true, true);
        Parse(pointBefore);
        Parse(pointAfter);
        UNIT_ASSERT_VALUES_EQUAL(Verify(pointBefore.Json, pointAfter.Json), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeRowRangePrunedKeyAndResidualFilterRemainObservable) {
        const TNativeRowRangeOptions options{
            .Lower = TString("0"), .Upper = TString("10"), .PruneKey = true, .Residual = true};
        const auto before = NativeRowLiteralRange(options, false, false);
        const auto after = NativeRowLiteralRange(options, true, true);
        Parse(before);
        Parse(after);
        UNIT_ASSERT_STRING_CONTAINS(after.Json, "predicate_columns");
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
        auto droppedResidual = options;
        droppedResidual.Residual = false;
        const auto changed = NativeRowLiteralRange(droppedResidual, true, true);
        Parse(changed);
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, changed.Json), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeRowRangesRejectUnauditedKeysBoundsAndMetadata) {
        for (const auto& mutation : TVector<TString>{
            "nullable_key", "composite_key", "system_view", "null_bound", "computed_bound", "missing_annotation",
            "wrong_annotation", "unsafe_bound", "boundary_arity", "boundary_tag", "boundary_annotation",
            "outer_annotation", "outer_shape", "unordered_bounds", "wrong_key", "missing_literal", "range_count",
        }) {
            TNativeRowRangeOptions options{.Lower = TString("0"), .Upper = TString("42")};
            options.Mutation = mutation;
            const auto rejected = NativeRowLiteralRange(options, true, true);
            UNIT_ASSERT_C(!rejected.IsSupported(), mutation);
            UNIT_ASSERT(rejected.Json.empty());
        }
        for (bool lower : {false, true}) {
            TNativeRowRangeOptions options;
            options.LowerInclusive = !lower;
            options.UpperInclusive = lower;
            UNIT_ASSERT(!NativeRowLiteralRange(options, true, true).IsSupported());
        }
        const TNativeRowRangeOptions exclusivePoint{
            .Lower = TString("42"), .Upper = TString("42"), .LowerInclusive = false, .UpperInclusive = false};
        const auto rejected = NativeRowLiteralRange(exclusivePoint, true, true);
        UNIT_ASSERT(!rejected.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(rejected.UnsupportedReason, "identical exclusive bounds");
        for (const auto& value : TVector<TString>{"2147483648", "-2147483649"}) {
            TNativeRowRangeOptions options{.Lower = value};
            UNIT_ASSERT(!NativeRowLiteralRange(options, true, true).IsSupported());
        }
    }

    // Requires the scalar-parameter support patch and its annotation/replay companion.
    Y_UNIT_TEST(NativeRowRangeRequiredParametersShareLogicalInputs) {
        const TNativeRowRangeOptions options{
            .Lower = TString("$lower"), .Upper = TString("$upper"), .Parameters = true};
        const auto before = NativeRowLiteralRange(options, false, false);
        const auto after = NativeRowLiteralRange(options, true, true);
        Parse(before);
        Parse(after);
        UNIT_ASSERT_STRING_CONTAINS(after.Json, "parameter");
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
        auto renamed = options;
        renamed.Lower = TString("$other");
        const auto changed = NativeRowLiteralRange(renamed, true, true);
        Parse(changed);
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, changed.Json), "COUNTEREXAMPLE");
        auto optional = options;
        optional.Mutation = "optional_parameter";
        UNIT_ASSERT(!NativeRowLiteralRange(optional, true, true).IsSupported());
        auto internal = options;
        internal.Lower = TString("%kqp_physical_tx_param_binding_0");
        UNIT_ASSERT(!NativeRowLiteralRange(internal, true, true).IsSupported());
        auto exclusive = options;
        exclusive.Upper = exclusive.Lower;
        exclusive.LowerInclusive = exclusive.UpperInclusive = false;
        UNIT_ASSERT(!NativeRowLiteralRange(exclusive, true, true).IsSupported());
    }

    Y_UNIT_TEST(NativeReadPointRangesRespectInt32Keys) {
        for (i32 point : {std::numeric_limits<i32>::min(), 42, std::numeric_limits<i32>::max()}) {
            const auto before = NativeInt32ReadPoint(false, false, point);
            const auto after = NativeInt32ReadPoint(true, true, point);
            const auto changed = NativeInt32ReadPoint(true, true, point == 42 ? 43 : 42);
            Parse(before);
            Parse(after);
            Parse(changed);
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, changed.Json), "COUNTEREXAMPLE");
        }
        UNIT_ASSERT(!NativeInt32ReadPoint(true, false, 42, true).IsSupported());
    }

    Y_UNIT_TEST(NullSinkSkipsNativeTypeMaterialization) {
        TFixture fixture;
        auto expression = NativeScalarFromExpr(fixture, true);
        auto filter = MakeIntrusive<TOpFilter>(fixture.Read(), TPositionHandle(), expression);
        TOpRoot root(filter, TPositionHandle(), {{fixture.Key, "answer"}});
        root.PlanProps.InfoUnitRegistry = fixture.Registry;
        TSemanticSnapshotPairCaptureV1 capture(nullptr);
        capture.CaptureInitial(root, fixture.Rbo);
        UNIT_ASSERT(!filter->Type);
        UNIT_ASSERT(!filter->GetFilterExpression().GetExpressionBody()->GetTypeAnn());
    }

    Y_UNIT_TEST(ResultProjectionRetainsRepeatedBindingsAndLabels) {
        TFixture fixture;
        TOpRoot root(fixture.Read(), TPositionHandle(), {
            {fixture.Other, "duplicate"}, {fixture.Key, "duplicate"}, {fixture.Key, "last"}});
        const auto snapshot = Parse(fixture.Export(root));
        UNIT_ASSERT_VALUES_EQUAL(snapshot["plan"]["root"].GetStringSafe(), "result");
        const auto& projection = snapshot["plan"]["nodes"].GetArraySafe().back();
        const auto& columns = projection["columns"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(columns[0]["output"].GetStringSafe(), "result[0]:duplicate");
        UNIT_ASSERT_VALUES_EQUAL(columns[1]["output"].GetStringSafe(), "result[1]:duplicate");
        UNIT_ASSERT_VALUES_EQUAL(columns[2]["output"].GetStringSafe(), "result[2]:last");
        UNIT_ASSERT_VALUES_EQUAL(columns[0]["expression"]["column"].GetStringSafe(), ToString(fixture.Other));
        UNIT_ASSERT_VALUES_EQUAL(columns[1]["expression"]["column"].GetStringSafe(), ToString(fixture.Key));
        UNIT_ASSERT_VALUES_EQUAL(columns[2]["expression"]["column"].GetStringSafe(), ToString(fixture.Key));
    }

    Y_UNIT_TEST(StageNumberingIsLocalAndDoesNotMutateThePlan) {
        const auto zero = StagedRead(0);
        const auto offset = StagedRead(37);
        Parse(zero);
        const auto snapshot = Parse(offset);
        UNIT_ASSERT_VALUES_EQUAL(zero.Json, offset.Json);
        UNIT_ASSERT_VALUES_EQUAL(snapshot["stage_graph"]["root_stage"].GetStringSafe(), "s0");
    }

    Y_UNIT_TEST(UnknownStageReferencesAreRejected) {
        TFixture fixture;
        auto read = fixture.Read();
        TOpRoot root(read, TPositionHandle(), {{fixture.Key, "key"}});
        auto& graph = root.PlanProps.StageGraph;
        read->Props.StageId = graph.AddSourceStage(NYql::EStorageType::RowStorage);
        graph.StageInputs.emplace(900, TVector<ui32>{});
        const auto exported = fixture.Export(root);
        UNIT_ASSERT(!exported.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(exported.UnsupportedReason, "unknown stage");
    }

    Y_UNIT_TEST(RowSourceProofObservesEveryResultPosition) {
        TFixture fixture;
        auto read = fixture.Read();
        TOpRoot initial(read, TPositionHandle(), {{fixture.Key, "first"}, {fixture.Key, "second"}});
        const auto before = fixture.Export(initial);
        Parse(before);
        TOpRoot final(read, TPositionHandle(), {{fixture.Key, "first"}, {fixture.Key, "second"}});
        auto& graph = final.PlanProps.StageGraph;
        graph.StageCounter = 37;
        read->Props.StageId = graph.AddSourceStage(NYql::EStorageType::RowStorage);
        const auto after = fixture.Export(final);
        Parse(after);
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
        TOpRoot changed(read, TPositionHandle(), {{fixture.Key, "first"}, {fixture.Other, "second"}});
        changed.PlanProps.StageGraph = graph;
        const auto mutation = fixture.Export(changed);
        Parse(mutation);
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, mutation.Json), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeWholePartitionDecimalWindowsUseTheExistingModel) {
        for (const TString& function : {TString("sum"), TString("avg")}) {
            TFixture fixture;
            auto window = fixture.Window(function, true);
            TOpRoot root(window, TPositionHandle(), {{fixture.WindowResult, "answer"}});
            const auto exported = fixture.Export(root);
            const auto snapshot = Parse(exported);
            size_t matches = 0;
            for (const auto& node : snapshot["plan"]["nodes"].GetArraySafe()) {
                if (node["op"].GetStringSafe() != "project") {
                    continue;
                }
                for (const auto& column : node["columns"].GetArraySafe()) {
                    if (column["expression"]["kind"].GetStringSafe() == "window_" + function) {
                        ++matches;
                        UNIT_ASSERT_VALUES_EQUAL(column["expression"]["input"].GetStringSafe(), ToString(fixture.Sum));
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(matches, 1);
            fixture.StageSerial(root);
            const auto staged = fixture.Export(root);
            Parse(staged);
            UNIT_ASSERT_VALUES_EQUAL(Verify(exported.Json, staged.Json), "VERIFIED_BOUNDED");
        }
    }

    Y_UNIT_TEST(NativeWholePartitionWindowsAcceptUtf8PartitionKeys) {
        for (const TString& function : {TString("sum"), TString("avg")}) {
            TFixture fixture("Utf8");
            auto window = fixture.Window(function, true, "Utf8");
            TOpRoot root(window, TPositionHandle(), {{fixture.WindowResult, "answer"}});
            const auto before = fixture.Export(root);
            Parse(before);
            fixture.StageSerial(root);
            const auto after = fixture.Export(root);
            Parse(after);
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
        }
    }

    Y_UNIT_TEST(NativeWholePartitionWindowsFollowDirectColumnAliases) {
        for (const TString& function : {TString("sum"), TString("avg")}) {
            const auto direct = NativeAliasedDecimalWindow(function, 0, false);
            Parse(direct);
            for (const size_t aliases : {1, 8}) {
                const auto copied = NativeAliasedDecimalWindow(function, aliases, true);
                Parse(copied);
                UNIT_ASSERT_VALUES_EQUAL(Verify(direct.Json, copied.Json), "VERIFIED_BOUNDED");
            }
        }
        const auto sum = NativeAliasedDecimalWindow("sum", 8, false);
        const auto average = NativeAliasedDecimalWindow("avg", 8, true);
        Parse(sum);
        Parse(average);
        UNIT_ASSERT_VALUES_EQUAL(Verify(sum.Json, average.Json), "COUNTEREXAMPLE");
        const auto tooDeep = NativeAliasedDecimalWindow("sum", 9, false);
        UNIT_ASSERT(!tooDeep.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(tooDeep.UnsupportedReason, "exceeds eight");
    }

    Y_UNIT_TEST(NativeWindowProofDetectsSumChangedToAverage) {
        TFixture sumFixture;
        auto sum = sumFixture.Window("sum", true);
        TOpRoot before(sum, TPositionHandle(), {{sumFixture.WindowResult, "answer"}});
        const auto initial = sumFixture.Export(before);
        Parse(initial);
        TFixture averageFixture;
        auto average = averageFixture.Window("avg", true);
        TOpRoot after(average, TPositionHandle(), {{averageFixture.WindowResult, "answer"}});
        averageFixture.StageSerial(after);
        const auto changed = averageFixture.Export(after);
        Parse(changed);
        UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, changed.Json), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeColumnCopiesRetainStoredStringProvenance) {
        TFixture directFixture;
        TInfoUnitId directResult;
        auto direct = directFixture.StringConcat(false, directResult);
        TOpRoot before(direct, TPositionHandle(), {{directResult, "answer"}});
        const auto initial = directFixture.Export(before);
        Parse(initial);
        TFixture copyFixture;
        TInfoUnitId copiedResult;
        auto copied = copyFixture.StringConcat(true, copiedResult);
        TOpRoot after(copied, TPositionHandle(), {{copiedResult, "answer"}});
        copyFixture.StageSerial(after);
        const auto final = copyFixture.Export(after);
        Parse(final);
        UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, final.Json), "VERIFIED_BOUNDED");
    }

    Y_UNIT_TEST(NativeStoredUtf8ConcatPreservesValuesAndCheckedFailure) {
        for (const bool olap : {false, true}) {
            for (const bool mixedLiteral : {false, true}) {
                TFixture beforeFixture("Utf8", false, "Utf8");
                TInfoUnitId beforeResult;
                auto beforeMap = beforeFixture.StringConcat(false, beforeResult, "Utf8", true, mixedLiteral, olap);
                TOpRoot before(beforeMap, TPositionHandle(), {{beforeResult, "answer"}});
                const auto initial = beforeFixture.Export(before);
                const auto initialJson = Parse(initial);
                bool found = false;
                for (const auto& node : initialJson["plan"]["nodes"].GetArraySafe()) {
                    if (node["op"].GetStringSafe() != "project") { continue; }
                    for (const auto& column : node["columns"].GetArraySafe()) {
                        if (column["output"].GetStringSafe() != ToString(beforeResult)) { continue; }
                        const auto& expression = column["expression"];
                        UNIT_ASSERT_VALUES_EQUAL(expression["kind"].GetStringSafe(), olap ? "checked_concat" : "opaque");
                        UNIT_ASSERT_VALUES_EQUAL(expression["type"].GetStringSafe(), mixedLiteral ? "String" : "Utf8");
                        found = true;
                    }
                }
                UNIT_ASSERT(found);
                TFixture afterFixture("Utf8", false, "Utf8");
                TInfoUnitId afterResult;
                auto afterMap = afterFixture.StringConcat(true, afterResult, "Utf8", true, mixedLiteral, olap);
                TOpRoot after(afterMap, TPositionHandle(), {{afterResult, "answer"}});
                afterFixture.StageSerial(after);
                const auto final = afterFixture.Export(after);
                Parse(final);
                UNIT_ASSERT_VALUES_EQUAL(Verify(initial.Json, final.Json), "VERIFIED_BOUNDED");
            }
        }
    }

    Y_UNIT_TEST(NativeCheckedConcatCopiesRetainDemandAndRejectChangedUses) {
        const auto make = [](size_t depth, bool staged, TStringBuf mutation = "") {
            TFixture f("Utf8", false, "Utf8");
            const auto pos = TPositionHandle();
            TInfoUnitId original;
            TIntrusivePtr<IOperator> input = f.StringConcat(false, original, "Utf8", true, false, true);
            const auto* text = f.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Utf8);
            const auto* boolean = f.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
            auto output = original;
            for (size_t index = 0; index < depth; ++index) {
                // Reuse the retained original at depth two: every old alias
                // must remain audited after a copy is appended.
                const auto source = original;
                TMapIUs copies;
                TVector<std::pair<TInfoUnitId, const TTypeAnnotationNode*>> types;
                for (auto id : input->GetOutputIUs()) {
                    types.emplace_back(id, input->GetIUType(id, f.Expr));
                }
                auto access = MakeColumnAccess(source, pos, &f.Expr, &f.ExpressionProps);
                access.GetExpressionBody()->SetTypeAnn(mutation == "wrong_type" ? boolean : text);
                output = f.Registry.Add(TInfoUnit("", "copy"));
                copies.Add(output, TMapElement(access));
                types.emplace_back(output, text);
                if (mutation == "fanout") {
                    const auto extra = f.Registry.Add(TInfoUnit("", "extra"));
                    copies.Add(extra, TMapElement(access));
                    types.emplace_back(extra, text);
                }
                if (mutation == "transform_old" && index == 1) {
                    auto value = f.Expr.NewCallable(pos, "Exists", {access.GetExpressionBody()});
                    value->SetTypeAnn(boolean);
                    const auto extra = f.Registry.Add(TInfoUnit("", "changed"));
                    auto lambda = f.Expr.NewLambda(pos, access.GetLambda()->ChildPtr(0), std::move(value));
                    copies.Add(extra, TMapElement(TExpression(lambda, &f.Expr, &f.ExpressionProps)));
                    types.emplace_back(extra, boolean);
                }
                auto copy = MakeIntrusive<TOpMap>(input, pos, std::move(copies));
                f.SetType(*copy, types);
                input = copy;
            }
            TOpRoot root(input, pos, {{mutation == "drop" ? f.Key : output, "answer"}});
            if (staged) {
                f.StageSerial(root);
                if (mutation == "stage_edge") {
                    auto& graph = root.PlanProps.StageGraph;
                    const auto previous = *input->Props.StageId;
                    const auto final = graph.AddStage();
                    input->Props.StageId = final;
                    graph.Connect(previous, final,
                        MakeIntrusive<TUnionAllConnection>(graph.GetOutputIndex(previous)));
                }
            }
            return f.Export(root);
        };
        const auto direct = make(0, false);
        Parse(direct);
        for (const size_t depth : {1, 2}) {
            for (const bool staged : {false, true}) {
                const auto copied = make(depth, staged);
                Parse(copied);
                UNIT_ASSERT_VALUES_EQUAL(Verify(direct.Json, copied.Json, !staged), "VERIFIED_BOUNDED");
            }
        }
        for (const TStringBuf mutation : {"fanout", "transform_old", "drop", "wrong_type", "stage_edge"}) {
            const auto rejected = make(2, true, mutation);
            UNIT_ASSERT_C(!rejected.IsSupported(), mutation);
        }
    }

    Y_UNIT_TEST(NativeTextConcatRejectsWrongResultAndStoredTypes) {
        for (const bool wrongResult : {false, true}) {
            TFixture fixture("Utf8");
            TInfoUnitId result;
            auto map = fixture.StringConcat(false, result, wrongResult ? "Utf8" : "String");
            if (wrongResult) {
                map->GetMapElements().At(result).GetExpression().GetExpressionBody()->SetTypeAnn(
                    fixture.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::String));
            }
            TOpRoot root(map, TPositionHandle(), {{result, "answer"}});
            const auto exported = fixture.Export(root);
            UNIT_ASSERT_C(!exported.IsSupported(), exported.Json);
        }
    }

    Y_UNIT_TEST(NativeCorrelatedExistsUsesTheCallerBinding) {
        TFixture fixture;
        const auto exported = fixture.CorrelatedExists(fixture.Key);
        const auto snapshot = Parse(exported);
        const auto& subplan = snapshot["plan"]["subplans"].GetArraySafe().front();
        UNIT_ASSERT_VALUES_EQUAL(subplan["dependencies"][0].GetStringSafe(), ToString(fixture.Key));
        UNIT_ASSERT_VALUES_EQUAL(subplan["predicate"]["right"]["column"].GetStringSafe(), ToString(fixture.Key));
    }

    Y_UNIT_TEST(NativeCorrelatedExistsDetectsAChangedCallerBinding) {
        TFixture keyFixture;
        const auto before = keyFixture.CorrelatedExists(keyFixture.Key);
        Parse(before);
        TFixture otherFixture;
        const auto after = otherFixture.CorrelatedExists(otherFixture.Other);
        Parse(after);
        // Both boundaries are logical: explicitly exercise the diagnostic
        // prefix theorem, without relaxing the normal Final-stage requirement.
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, before.Json, true), "VERIFIED_BOUNDED");
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json, true), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeCorrelationAuditPreservesTypedConjuncts) {
        TFixture fixture;
        const auto plain = fixture.CorrelatedExists(fixture.Key);
        Parse(plain);
        TFixture residualFixture;
        const auto conjunction = residualFixture.CorrelatedExists(residualFixture.Key, false, true);
        Parse(conjunction);
        UNIT_ASSERT_VALUES_EQUAL(Verify(plain.Json, conjunction.Json, true), "VERIFIED_BOUNDED");
    }

    Y_UNIT_TEST(NativeCorrelatedExistsRejectsInconsistentCaptureTypes) {
        TFixture fixture;
        const auto exported = fixture.CorrelatedExists(fixture.Key, true);
        UNIT_ASSERT(!exported.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(exported.UnsupportedReason, "type");
    }

    Y_UNIT_TEST(NativeScalarCapturePreservesAPassiveMap) {
        TFixture directFixture;
        const auto before = directFixture.CorrelatedScalarCount(directFixture.Key);
        const auto snapshot = Parse(before);
        UNIT_ASSERT_VALUES_EQUAL(snapshot["plan"]["subplans"][0]["dependencies"][0].GetStringSafe(),
            ToString(directFixture.Key));
        TFixture mapFixture;
        const auto after = mapFixture.CorrelatedScalarCount(mapFixture.Key, true);
        Parse(after);
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json, true), "VERIFIED_BOUNDED");
    }

    Y_UNIT_TEST(NativeScalarCaptureDetectsAChangedCallerBinding) {
        TFixture keyFixture;
        const auto before = keyFixture.CorrelatedScalarCount(keyFixture.Key);
        Parse(before);
        TFixture otherFixture;
        const auto after = otherFixture.CorrelatedScalarCount(otherFixture.Other);
        Parse(after);
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json, true), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeScalarCaptureRejectsBadTypesAndOutOfScopeUses) {
        TFixture typeFixture;
        const auto badType = typeFixture.CorrelatedScalarCount(typeFixture.Key, false, true);
        UNIT_ASSERT(!badType.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(badType.UnsupportedReason, "type");
        for (bool throughMap : {false, true}) {
            TFixture useFixture;
            const auto badUse = useFixture.CorrelatedScalarCount(useFixture.Key, throughMap, false, true);
            UNIT_ASSERT(!badUse.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(badUse.UnsupportedReason, "outer dependency");
        }
    }

    Y_UNIT_TEST(NativeRankPreservesPartitionsNullsAndFramesDuringStaging) {
        for (const TString& type : {TString("Int32"), TString("Uint64"), TString("String"), TString("Date")}) {
            for (const bool nullable : {false, true}) {
                for (const bool partitioned : {false, true}) {
                    for (const bool ascending : {false, true}) {
                        const auto before = NativeRank(type, nullable, partitioned, false, ascending);
                        const auto after = NativeRank(type, nullable, partitioned, true, ascending,
                            false, true, "rank", true);
                        Parse(before);
                        Parse(after);
                        UNIT_ASSERT_VALUES_EQUAL_C(Verify(before.Json, after.Json), "VERIFIED_BOUNDED",
                            type << " nullable=" << nullable << " partitioned=" << partitioned << " asc=" << ascending);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(NativeRankDetectsChangedOrderAndPeerKeys) {
        const auto before = NativeRank("Int32", true, false, false);
        const auto reversed = NativeRank("Int32", true, false, true, false);
        const auto tieBroken = NativeRank("Int32", true, false, true, true, true);
        Parse(before);
        Parse(reversed);
        Parse(tieBroken);
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, reversed.Json), "COUNTEREXAMPLE");
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, tieBroken.Json), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeUnorderedRankIsOneAndOtherNativeFunctionsRemainUnsupported) {
        for (const bool partitioned : {false, true}) {
            const auto before = NativeRank("Int32", true, partitioned, false, true, false, false);
            const auto after = NativeRank("Int32", true, partitioned, true, true, false, false);
            Parse(before);
            Parse(after);
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
        }
        for (const TString& function : {TString("denserank"), TString("rownumber")}) {
            const auto result = NativeRank("Int32", true, false, false, true, false, true, function);
            UNIT_ASSERT(!result.IsSupported());
        }
    }

    Y_UNIT_TEST(NativeIntegerAveragePreservesRequiredAndOptionalStateTypes) {
        for (const TString& type : {TString("Int8"), TString("Int16"), TString("Int32"), TString("Int64"),
                                   TString("Uint8"), TString("Uint16"), TString("Uint32"), TString("Uint64")}) {
            for (const bool nullable : {false, true}) {
                for (const bool grouped : {false, true}) {
                    const auto before = NativeIntegralAverage(type, nullable, grouped, false, false);
                    const auto after = NativeIntegralAverage(type, nullable, grouped, true, true);
                    Parse(before);
                    Parse(after);
                    UNIT_ASSERT_VALUES_EQUAL_C(Verify(before.Json, after.Json), "VERIFIED_BOUNDED",
                        type << " nullable=" << nullable << " grouped=" << grouped);
                }
            }
        }
    }

    Y_UNIT_TEST(NativeNullEqualJoinKeysSurviveStagingAndDetectLostNullMatches) {
        for (const TString& type : {TString("Int32"), TString("Int64"), TString("String"), TString("Date")}) {
            TFixture initialFixture(type), finalFixture(type), changedFixture(type);
            const auto before = initialFixture.NullableKeyJoin(type, "Inner", true, false);
            const auto after = finalFixture.NullableKeyJoin(type, "Inner", true, true);
            const auto changed = changedFixture.NullableKeyJoin(type, "Inner", false, true);
            UNIT_ASSERT_STRING_CONTAINS(before.Json, "equal_nulls");
            UNIT_ASSERT_VALUES_EQUAL_C(Verify(before.Json, after.Json), "VERIFIED_BOUNDED", type);
            UNIT_ASSERT_VALUES_EQUAL_C(Verify(before.Json, changed.Json), "COUNTEREXAMPLE", type);
        }
        for (const TString& kind : {TString("Left"), TString("Full"), TString("LeftSemi"), TString("LeftOnly")}) {
            TFixture initialFixture("Int32"), finalFixture("Int32"), changedFixture("Int32");
            const auto before = initialFixture.NullableKeyJoin("Int32", kind, true, false);
            const auto after = finalFixture.NullableKeyJoin("Int32", kind, true, true);
            const auto changed = changedFixture.NullableKeyJoin("Int32", kind, false, true);
            UNIT_ASSERT_VALUES_EQUAL_C(Verify(before.Json, after.Json), "VERIFIED_BOUNDED", kind);
            UNIT_ASSERT_VALUES_EQUAL_C(Verify(before.Json, changed.Json), "COUNTEREXAMPLE", kind);
        }
    }

    Y_UNIT_TEST(NativeSharedReadPortsPreserveTheirProducer) {
        TFixture sharedFixture;
        const auto before = sharedFixture.SharedReadJoin(true, false);
        const auto snapshot = Parse(before);
        TVector<TString> portInputs;
        size_t scans = 0;
        for (const auto& node : snapshot["plan"]["nodes"].GetArraySafe()) {
            scans += node["op"].GetStringSafe() == "scan";
            if (node["op"].GetStringSafe() == "project" && node["id"].GetStringSafe() != "result") {
                portInputs.push_back(node["input"].GetStringSafe());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(scans, 1);
        UNIT_ASSERT_VALUES_EQUAL(portInputs.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(portInputs[0], portInputs[1]);
        TFixture copiedFixture;
        const auto after = copiedFixture.SharedReadJoin(false, true);
        Parse(after);
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
    }

    Y_UNIT_TEST(NativeSharedReadProofDetectsChangedJoinKeys) {
        TFixture sharedFixture;
        const auto before = sharedFixture.SharedReadJoin(true, false);
        Parse(before);
        TFixture changedFixture;
        const auto after = changedFixture.SharedReadJoin(false, true, true);
        Parse(after);
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeSharedReadRejectsBadTypesAndMismatchedStagePorts) {
        TFixture typedFixture;
        const auto badType = typedFixture.SharedReadJoin(true, false, false, true);
        UNIT_ASSERT(!badType.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(badType.UnsupportedReason, "column types");
        TFixture stagedFixture;
        const auto staged = stagedFixture.SharedReadJoin(true, true, false, false, true);
        UNIT_ASSERT(!staged.IsSupported());
        UNIT_ASSERT_STRING_CONTAINS(staged.UnsupportedReason, "output index disagrees");
    }

    Y_UNIT_TEST(NativeSharedStagePortsPreserveRowsAndCompactLiveIndices) {
        for (const auto [sparse, computeProducer] : TVector<std::pair<bool, bool>>{{false, false}, {true, false}, {false, true}}) {
            TFixture initialFixture;
            const auto before = initialFixture.SharedReadJoin(true, false);
            Parse(before);
            TFixture finalFixture;
            const auto after = finalFixture.SharedReadJoin(true, true, false, false, false, sparse, computeProducer);
            const auto snapshot = Parse(after);
            const auto& outputs = snapshot["stage_graph"]["stages"].GetArraySafe()[computeProducer ? 1 : 0]["outputs"].GetArraySafe();
            UNIT_ASSERT_VALUES_EQUAL(outputs.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(outputs[0]["index"].GetUIntegerSafe(), 0);
            UNIT_ASSERT_VALUES_EQUAL(outputs[1]["index"].GetUIntegerSafe(), 1);
            UNIT_ASSERT_VALUES_UNEQUAL(outputs[0]["node"].GetStringSafe(), outputs[1]["node"].GetStringSafe());
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "VERIFIED_BOUNDED");
        }
    }

    Y_UNIT_TEST(NativeSharedStagePortsDetectChangedJoinKeys) {
        TFixture initialFixture;
        const auto before = initialFixture.SharedReadJoin(true, false);
        Parse(before);
        TFixture changedFixture;
        const auto after = changedFixture.SharedReadJoin(true, true, true);
        Parse(after);
        UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), "COUNTEREXAMPLE");
    }

    Y_UNIT_TEST(NativeCardinalityGuardRequiresAProofOfTotality) {
        for (bool uniqueKey : {true, false}) {
            TFixture plainFixture;
            const auto before = plainFixture.GuardedKeyCount(false, false, uniqueKey);
            Parse(before);
            TFixture guardedFixture;
            const auto after = guardedFixture.GuardedKeyCount(true, true, uniqueKey);
            Parse(after);
            UNIT_ASSERT_VALUES_EQUAL(Verify(before.Json, after.Json), uniqueKey ? "VERIFIED_BOUNDED" : "UNKNOWN");
        }
    }

    Y_UNIT_TEST(NativeCardinalityGuardRejectsDifferentThresholds) {
        for (const auto& threshold : {TString("0"), TString("2")}) {
            TFixture fixture;
            const auto exported = fixture.GuardedKeyCount(true, false, true, threshold);
            UNIT_ASSERT(!exported.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(exported.UnsupportedReason, "cardinality Ensure");
        }
    }

    Y_UNIT_TEST(NativeWindowRejectsUnmodeledFramesAndFunctions) {
        for (const auto& [function, whole] : TVector<std::pair<TString, bool>>{{"sum", false}, {"count", true}}) {
            TFixture fixture;
            auto window = fixture.Window(function, whole);
            TOpRoot root(window, TPositionHandle(), {{fixture.WindowResult, "answer"}});
            const auto exported = fixture.Export(root);
            UNIT_ASSERT(!exported.IsSupported());
            UNIT_ASSERT_STRING_CONTAINS(exported.UnsupportedReason, "Native Window");
        }
    }
}

} // namespace NKikimr::NKqp
