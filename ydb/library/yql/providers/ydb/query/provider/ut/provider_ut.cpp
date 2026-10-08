#include <ydb/library/yql/providers/ydb/query/common/provider_names.h>
#include <ydb/library/yql/providers/ydb/query/provider/yql_ydb_provider_impl.h>
#include <ydb/library/yql/providers/ydb/query/expr_nodes/yql_ydb_expr_nodes.h>
#include <ydb/library/yql/providers/ydb/query/proto/source.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>
#include <yql/essentials/core/sql_types/block.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/json/json_value.h>

#include <tuple>

namespace NYql::NYdbQuery {
namespace {

using namespace NNodes;

struct TFixture {
    TExprContext Ctx;
    TIntrusivePtr<TTypeAnnotationContext> Types = MakeIntrusive<TTypeAnnotationContext>();
    NYdb::TDriver Driver{NYdb::TDriverConfig().SetNetworkThreadsNum(1).SetClientThreadsNum(1)};
    NYdb::TDriver TlsDriver{NYdb::TDriverConfig().SetNetworkThreadsNum(1).SetClientThreadsNum(1)};
    TState::TPtr State;

    explicit TFixture(TInstant deadline = TInstant::Max())
        : State(MakeIntrusive<TState>(Types.Get(),
            [cache = CreateYdbMetadataClientCache(Driver, TlsDriver)] { return cache; },
            CreateStructuredTokenCredentialsFactory(), deadline))
    {
        AddCluster(*State, "remote", {
            {"location", "localhost:2135"}, {"database_name", "/Remote/"},
            {"authMethod", "TOKEN"}, {"tokenReference", "secret-name"}, {"token", "private-token-value"},
            {"use_tls", "true"}
        });
        TTable table;
        table.ColumnTypes["key"].set_type_id(Ydb::Type::UINT64);
        table.ColumnTypes["value"].mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        table.RowType = Ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
            Ctx.MakeType<TItemExprType>("value", ParseColumnType(table.ColumnTypes.at("value"), Ctx)),
            Ctx.MakeType<TItemExprType>("key", ParseColumnType(table.ColumnTypes.at("key"), Ctx))});
        table.ColumnOrder = {"key", "value"};
        State->Tables.emplace(TState::TTableKey("remote", "items"), std::move(table));
    }

    ~TFixture() {
        Driver.Stop(true);
        TlsDriver.Stop(true);
    }

    TYdbQueryReadTable MakeRead() {
        const auto pos = Ctx.AppendPosition({});
        auto world = Ctx.NewWorld(pos);
        world->SetTypeAnn(Ctx.MakeType<TWorldExprType>());
        auto read = Build<TYdbQueryReadTable>(Ctx, pos)
            .World(world)
            .DataSource<TYdbQueryDataSource>()
                .Category().Build(YdbQueryProviderName)
                .Cluster().Build("remote")
            .Build()
            .Table().Build("items")
            .Columns<TCoVoid>().Build()
            .Done();
        // The core DataSource annotator runs before the provider in a real pipeline.
        read.DataSource().Ptr()->SetTypeAnn(Ctx.MakeType<TUnitExprType>());
        const auto* row = State->Tables.at(TState::TTableKey("remote", "items")).RowType;
        read.Ptr()->SetTypeAnn(Ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            world->GetTypeAnn(), Ctx.MakeType<TListExprType>(row)}));
        return read;
    }

    TExprNode::TPtr MakeRawRead(bool wrapKey, ui32 tableCount = 1, TStringBuf table = "items") {
        const auto typedRead = MakeRead();
        const auto pos = typedRead.Pos();
        auto key = Ctx.NewCallable(pos, "Key", {
            Ctx.NewList(pos, {Ctx.NewAtom(pos, "table"),
                Ctx.NewCallable(pos, "String", {Ctx.NewAtom(pos, table)})})});
        if (wrapKey) {
            key = Ctx.NewCallable(pos, "MrTableConcat", TExprNode::TListType(tableCount, key));
        }
        return Ctx.NewCallable(pos, "Read!", {
            typedRead.World().Ptr(), typedRead.DataSource().Ptr(), std::move(key),
            Ctx.NewCallable(pos, "Void", {}), Ctx.NewList(pos, {})});
    }

    TCoExtractMembers MakeProjection(TExprNode::TPtr input, const TVector<TString>& names) {
        const auto* tableRow = State->Tables.at(TState::TTableKey("remote", "items")).RowType;
        TExprNode::TListType members;
        TVector<const TItemExprType*> items;
        for (const auto& name : names) {
            members.emplace_back(Ctx.NewAtom(input->Pos(), name));
            items.push_back(tableRow->GetItems()[*tableRow->FindItem(name)]);
        }
        auto projection = Build<TCoExtractMembers>(Ctx, input->Pos())
            .Input(input)
            .Members(Ctx.NewList(input->Pos(), std::move(members)))
            .Done();
        projection.Ptr()->SetTypeAnn(Ctx.MakeType<TListExprType>(Ctx.MakeType<TStructExprType>(items)));
        return projection;
    }

    TSource SerializeSource(const TDqSourceWrap& wrap) {
        const auto source = Build<TDqSource>(Ctx, wrap.Pos())
            .DataSource(wrap.DataSource()).Settings(wrap.Input()).Done();
        google::protobuf::Any packed;
        TString sourceType;
        CreateDqIntegration(State)->FillSourceSettings(source.Ref(), packed, sourceType, 1, Ctx);
        TSource payload;
        UNIT_ASSERT_VALUES_EQUAL(packed.type_url(), "type.googleapis.com/NYql.NYdbQuery.TSource");
        UNIT_ASSERT(packed.UnpackTo(&payload));
        return payload;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TYdbProvider) {
    Y_UNIT_TEST(MetadataResourcesAreLazyAndFactoryFailuresAreSanitized) {
        for (const bool throws : {false, true}) {
            TExprContext ctx;
            auto types = MakeIntrusive<TTypeAnnotationContext>();
            ui32 requests = 0;
            auto providers = CreateYdbDataProviders(types.Get(),
                [&]() -> std::shared_ptr<IYdbMetadataClientCache> {
                    ++requests;
                    if (throws) {
                        throw yexception() << "private-factory-credentials";
                    }
                    return {};
                });
            providers.Source->AddCluster("remote", {
                {"location", "localhost:2135"}, {"database_name", "/Remote"}, {"authMethod", "NONE"}});
            UNIT_ASSERT_VALUES_EQUAL(requests, 0);
            const auto pos = ctx.AppendPosition({});
            const auto makeRead = [&](TStringBuf category) {
                return ctx.NewCallable(pos, "Read!", {
                    ctx.NewWorld(pos), ctx.NewCallable(pos, "DataSource", {
                        ctx.NewAtom(pos, category), ctx.NewAtom(pos, "remote")}),
                    ctx.NewCallable(pos, "Key", {ctx.NewList(pos, {ctx.NewAtom(pos, "table"),
                        ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "items")})})}),
                    ctx.NewCallable(pos, "Void", {}), ctx.NewList(pos, {})});
            };
            auto& metadata = providers.Source->GetLoadTableMetadataTransformer();
            TExprNode::TPtr output;
            const auto foreign = makeRead("kikimr");
            UNIT_ASSERT_VALUES_EQUAL(metadata.Transform(foreign, output, ctx).Level, IGraphTransformer::TStatus::Ok);
            UNIT_ASSERT_VALUES_EQUAL(requests, 0);

            const auto read = makeRead(YdbQueryProviderName);
            UNIT_ASSERT_VALUES_EQUAL(metadata.Transform(read, output, ctx).Level, IGraphTransformer::TStatus::Async);
            UNIT_ASSERT(metadata.GetAsyncFuture(*read).Wait(TDuration::Seconds(1)));
            UNIT_ASSERT_VALUES_EQUAL(metadata.ApplyAsyncChanges(read, output, ctx).Level, IGraphTransformer::TStatus::Error);
            UNIT_ASSERT_VALUES_EQUAL(requests, 1);
            const auto issues = ctx.IssueManager.GetIssues().ToString();
            UNIT_ASSERT_STRING_CONTAINS(issues, "Ydb metadata client initialization failed");
            UNIT_ASSERT(!issues.Contains("private-factory-credentials"));
        }
    }

    Y_UNIT_TEST(MetadataAcceptsLiteralKeyWithOrWithoutConcat) {
        for (const bool wrapKey : {false, true}) {
            TFixture f;
            auto transformer = CreateLoadMetadataTransformer(f.State);
            const auto input = f.MakeRawRead(wrapKey);
            TExprNode::TPtr output;
            const auto status = transformer->Transform(input, output, f.Ctx);
            UNIT_ASSERT(status.Level != IGraphTransformer::TStatus::Error);
            UNIT_ASSERT(status.Level != IGraphTransformer::TStatus::Async);
            UNIT_ASSERT(TYdbQueryReadTable::Match(output.Get()));
            const TYdbQueryReadTable read(output);
            UNIT_ASSERT_VALUES_EQUAL(read.Table().Value(), "items");
            UNIT_ASSERT_VALUES_EQUAL(read.World().Raw(), input->Child(0));
            UNIT_ASSERT_VALUES_EQUAL(read.DataSource().Raw(), input->Child(1));
        }
    }

    Y_UNIT_TEST(ForeignReadsAreIgnoredDuringMetadataLoading) {
        TFixture f;
        auto transformer = CreateLoadMetadataTransformer(f.State);
        const auto nativeRead = f.MakeRawRead(false);
        const auto pos = nativeRead->Pos();
        auto foreignChildren = nativeRead->ChildrenList();
        foreignChildren[1] = f.Ctx.NewCallable(pos, "DataSource", {
            f.Ctx.NewAtom(pos, "kikimr"), f.Ctx.NewAtom(pos, "local")});
        const auto foreignRead = f.Ctx.ChangeChildren(*nativeRead, std::move(foreignChildren));
        TExprNode::TPtr output;
        UNIT_ASSERT_VALUES_EQUAL(transformer->Transform(foreignRead, output, f.Ctx).Level, IGraphTransformer::TStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(output.Get(), foreignRead.Get());

        // The graph can contain both native and local reads in a federated join.
        const auto mixed = f.Ctx.NewList(pos, {foreignRead, nativeRead});
        const auto status = transformer->Transform(mixed, output, f.Ctx);
        UNIT_ASSERT(status.Level != IGraphTransformer::TStatus::Error);
        UNIT_ASSERT(status.Level != IGraphTransformer::TStatus::Async);
        UNIT_ASSERT_VALUES_EQUAL(output->Child(0), foreignRead.Get());
        UNIT_ASSERT(TYdbQueryReadTable::Match(output->Child(1)));
    }

    Y_UNIT_TEST(ProviderDispatchChecksDataSourceAndDataSinkCategories) {
        TFixture f;
        auto providers = CreateYdbDataProviders(f.Types.Get(), f.State->MetadataClientCacheFactory);
        const auto nativeRead = f.MakeRawRead(false);
        const auto pos = nativeRead->Pos();
        auto foreignChildren = nativeRead->ChildrenList();
        foreignChildren[1] = f.Ctx.NewCallable(pos, "DataSource", {
            f.Ctx.NewAtom(pos, "kikimr"), f.Ctx.NewAtom(pos, "local")});
        const auto foreignRead = f.Ctx.ChangeChildren(*nativeRead, std::move(foreignChildren));
        UNIT_ASSERT(providers.Source->CanParse(*nativeRead));
        UNIT_ASSERT(!providers.Source->CanParse(*foreignRead));
        for (const TStringBuf callable : {TStringBuf("Write!"), TStringBuf("Commit!")}) {
            for (const TStringBuf category : {YdbQueryProviderName, TStringBuf("kikimr")}) {
                const auto operation = f.Ctx.NewCallable(pos, callable, {
                    nativeRead->ChildPtr(0), f.Ctx.NewCallable(pos, "DataSink", {
                        f.Ctx.NewAtom(pos, category), f.Ctx.NewAtom(pos, "remote")})});
                UNIT_ASSERT_VALUES_EQUAL(providers.Sink->CanParse(*operation), category == YdbQueryProviderName);
                UNIT_ASSERT_VALUES_EQUAL(providers.Sink->CanExecute(*operation),
                    category == YdbQueryProviderName && callable == "Commit!");
            }
        }
    }

    Y_UNIT_TEST(MetadataRejectsMultipleTables) {
        TFixture f;
        auto transformer = CreateLoadMetadataTransformer(f.State);
        TExprNode::TPtr output;
        const auto status = transformer->Transform(f.MakeRawRead(true, 2), output, f.Ctx);
        UNIT_ASSERT_VALUES_EQUAL(status.Level, IGraphTransformer::TStatus::Error);
        UNIT_ASSERT(f.Ctx.IssueManager.GetIssues().ToString().Contains("single table read"));
    }

    Y_UNIT_TEST(MetadataRejectsExpiredDeadlineBeforeStartingNetwork) {
        TFixture f(TInstant::Now() - TDuration::Seconds(1));
        auto transformer = CreateLoadMetadataTransformer(f.State);
        TExprNode::TPtr output;
        const auto input = f.MakeRawRead(false, 1, "uncached");
        UNIT_ASSERT_VALUES_EQUAL(transformer->Transform(input, output, f.Ctx).Level, IGraphTransformer::TStatus::Error);
        UNIT_ASSERT(f.Ctx.IssueManager.GetIssues().ToString().Contains("metadata deadline exceeded"));
    }

    Y_UNIT_TEST(MetadataBoundsDistinctTableCountBeforeStartingNetwork) {
        TFixture f;
        TExprNode::TListType reads;
        for (ui64 i = 0; i < MaxMetadataTables; ++i) {
            reads.emplace_back(f.MakeRawRead(false, 1, TStringBuilder() << "uncached" << i));
        }
        auto transformer = CreateLoadMetadataTransformer(f.State);
        TExprNode::TPtr output;
        UNIT_ASSERT_VALUES_EQUAL(transformer->Transform(f.Ctx.NewList(f.Ctx.AppendPosition({}), std::move(reads)),
            output, f.Ctx).Level, IGraphTransformer::TStatus::Error);
        UNIT_ASSERT(f.Ctx.IssueManager.GetIssues().ToString().Contains("metadata table limit exceeded"));
    }

    Y_UNIT_TEST(MetadataSchemaLimitsAreCheckedBeforeCopying) {
        Ydb::Table::DescribeTableResult description;
        for (ui64 i = 0; i <= MaxMetadataColumns; ++i) {
            auto* column = description.add_columns();
            column->set_name(TStringBuilder() << "key" << i);
            column->mutable_type()->set_type_id(Ydb::Type::UINT64);
        }
        TMetadataSchema schema;
        TString error;
        UNIT_ASSERT(!ExtractMetadataSchema(description, schema, error));
        UNIT_ASSERT(error.Contains("column limit"));
        UNIT_ASSERT(schema.Columns.empty());

        description.clear_columns();
        for (ui64 i = 0; i < 65; ++i) {
            auto* column = description.add_columns();
            column->set_name(TString(1024, 'a'));
            column->mutable_type()->set_type_id(Ydb::Type::UINT64);
        }
        UNIT_ASSERT(!ExtractMetadataSchema(description, schema, error));
        UNIT_ASSERT(error.Contains("schema limit"));
        UNIT_ASSERT(schema.Columns.empty());
    }

    Y_UNIT_TEST(MetadataCompactSchemaPreservesNullabilityAndRejectsColumnTables) {
        Ydb::Table::DescribeTableResult description;
        auto* key = description.add_columns();
        key->set_name("key");
        key->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        key->set_not_null(true);
        auto* value = description.add_columns();
        value->set_name("value");
        value->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
        TMetadataSchema schema;
        TString error;
        UNIT_ASSERT(ExtractMetadataSchema(description, schema, error));
        UNIT_ASSERT_VALUES_EQUAL(schema.Columns.size(), 2);
        UNIT_ASSERT(schema.Columns[0].second.has_type_id());
        UNIT_ASSERT(schema.Columns[1].second.has_optional_type());
        description.set_store_type(Ydb::Table::STORE_TYPE_COLUMN);
        TMetadataSchema rejected;
        UNIT_ASSERT(!ExtractMetadataSchema(description, rejected, error));
        UNIT_ASSERT(error.Contains("only row tables"));
        UNIT_ASSERT(rejected.Columns.empty());
    }

    Y_UNIT_TEST(PrimitiveNullability) {
        TExprContext ctx;
        for (const auto id : {Ydb::Type::BOOL, Ydb::Type::INT8, Ydb::Type::INT16, Ydb::Type::INT32, Ydb::Type::INT64,
                             Ydb::Type::UINT8, Ydb::Type::UINT16, Ydb::Type::UINT32, Ydb::Type::UINT64,
                             Ydb::Type::FLOAT, Ydb::Type::DOUBLE, Ydb::Type::STRING, Ydb::Type::UTF8}) {
            Ydb::Type primitive;
            primitive.set_type_id(id);
            const auto* required = ParseColumnType(primitive, ctx);
            UNIT_ASSERT(required);
            UNIT_ASSERT_VALUES_EQUAL(required->GetKind(), ETypeAnnotationKind::Data);
            Ydb::Type optional;
            *optional.mutable_optional_type()->mutable_item() = primitive;
            const auto* nullable = ParseColumnType(optional, ctx);
            UNIT_ASSERT(nullable);
            UNIT_ASSERT_VALUES_EQUAL(nullable->GetKind(), ETypeAnnotationKind::Optional);
            UNIT_ASSERT_VALUES_EQUAL(nullable->Cast<TOptionalExprType>()->GetItemType(), required);
        }
    }

    Y_UNIT_TEST(UnsupportedTypesFailClosed) {
        TExprContext ctx;
        Ydb::Type type;
        type.set_type_id(Ydb::Type::UUID);
        UNIT_ASSERT(!ParseColumnType(type, ctx));
        type.Clear();
        type.mutable_decimal_type()->set_precision(22);
        type.mutable_decimal_type()->set_scale(9);
        UNIT_ASSERT(!ParseColumnType(type, ctx));
        type.Clear();
        type.mutable_optional_type()->mutable_item()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
        UNIT_ASSERT(!ParseColumnType(type, ctx));
    }

    Y_UNIT_TEST(SourceHasSchemaAndSecretReferenceOnly) {
        TFixture f;
        auto integration = CreateDqIntegration(f.State);
        auto read = f.MakeRead();
        const TDqSourceWrap wrap(integration->WrapRead(read.Ptr(), f.Ctx, {}));
        const auto source = Build<TDqSource>(f.Ctx, read.Pos())
            .DataSource(wrap.DataSource())
            .Settings(wrap.Input())
            .Done();
        google::protobuf::Any packed;
        TString sourceType;
        integration->FillSourceSettings(source.Ref(), packed, sourceType, 1, f.Ctx);
        UNIT_ASSERT_VALUES_EQUAL(sourceType, "Ydb");
        TSource payload;
        UNIT_ASSERT(packed.UnpackTo(&payload));
        UNIT_ASSERT_VALUES_EQUAL(payload.GetVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(payload.GetDatabase(), "/Remote");
        UNIT_ASSERT_VALUES_EQUAL(payload.GetTable(), "/Remote/items");
        UNIT_ASSERT_VALUES_EQUAL(payload.GetToken(), "cluster:default_remote");
        UNIT_ASSERT(payload.GetUseTls());
        UNIT_ASSERT(payload.HasReadTimeoutMs());
        UNIT_ASSERT_VALUES_EQUAL(payload.GetReadTimeoutMs(), 60000);
        UNIT_ASSERT_VALUES_EQUAL(payload.ColumnsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(payload.GetColumns(0).GetName(), "key");
        UNIT_ASSERT(payload.GetColumns(0).GetType().has_type_id());
        UNIT_ASSERT(payload.GetColumns(1).GetType().has_optional_type());
        UNIT_ASSERT(packed.SerializeAsString().find("private-token-value") == TString::npos);
        UNIT_ASSERT(packed.SerializeAsString().find("secret-name") == TString::npos);
        TVector<TString> partitions;
        integration->Partition(source.Ref(), partitions, nullptr, f.Ctx, {});
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
    }

    Y_UNIT_TEST(ZeroColumnReadPreservesRowCount) {
        TFixture f;
        auto integration = CreateDqIntegration(f.State);
        auto read = f.MakeRead();
        const auto* empty = f.Ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{});
        read.Ptr()->SetTypeAnn(f.Ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            read.World().Ref().GetTypeAnn(), f.Ctx.MakeType<TListExprType>(empty)}));
        const TDqSourceWrap wrap(integration->WrapRead(read.Ptr(), f.Ctx, {}));
        const auto settings = wrap.Input().Cast<TYdbQuerySourceSettings>();
        UNIT_ASSERT_VALUES_EQUAL(settings.Columns().Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.Columns().Item(0).Value(), "key");
        const auto source = Build<TDqSource>(f.Ctx, read.Pos())
            .DataSource(wrap.DataSource()).Settings(wrap.Input()).Done();
        google::protobuf::Any packed;
        TString sourceType;
        integration->FillSourceSettings(source.Ref(), packed, sourceType, 1, f.Ctx);
        TSource payload;
        UNIT_ASSERT(packed.UnpackTo(&payload));
        UNIT_ASSERT_VALUES_EQUAL(payload.ColumnsSize(), 1);
    }

    Y_UNIT_TEST(ProjectionPrunesRightAndReadWrapBeforeSerialization) {
        for (const bool readWrap : {false, true}) {
            for (const TVector<TString>& columns : {TVector<TString>{"key"}, TVector<TString>{"value"}, TVector<TString>{}}) {
                TFixture f;
                const auto read = f.MakeRead();
                TExprNode::TPtr wrapper = readWrap
                    ? Build<TDqReadWrap>(f.Ctx, read.Pos()).Input(read).Flags().Build().Done().Ptr()
                    : Build<TCoRight>(f.Ctx, read.Pos()).Input(read).Done().Ptr();
                const auto projection = f.MakeProjection(wrapper, columns);
                auto optimizer = CreateLogicalOptimizer(f.State);
                TExprNode::TPtr output;
                UNIT_ASSERT(optimizer->Transform(projection.Ptr(), output, f.Ctx).Level != IGraphTransformer::TStatus::Error);
                UNIT_ASSERT_VALUES_EQUAL(output->Content(), wrapper->Content());
                const TYdbQueryReadTable pruned(output->ChildPtr(0));
                UNIT_ASSERT_VALUES_EQUAL(pruned.Columns().Ref().ChildrenSize(), columns.size());
                auto annotation = CreateTypeAnnotationTransformer(f.State);
                TExprNode::TPtr annotated;
                UNIT_ASSERT_VALUES_EQUAL(annotation->Transform(pruned.Ptr(), annotated, f.Ctx).Level, IGraphTransformer::TStatus::Ok);
                const auto* row = pruned.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                UNIT_ASSERT_VALUES_EQUAL(row->GetSize(), columns.size());
                if (!columns.empty()) {
                    UNIT_ASSERT_VALUES_EQUAL(row->GetItems().front()->GetItemType()->GetKind(),
                        columns.front() == "value" ? ETypeAnnotationKind::Optional : ETypeAnnotationKind::Data);
                }
                const TDqSourceWrap source(CreateDqIntegration(f.State)->WrapRead(pruned.Ptr(), f.Ctx, {}));
                const auto payload = f.SerializeSource(source);
                UNIT_ASSERT_VALUES_EQUAL(payload.ColumnsSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(payload.GetColumns(0).GetName(), columns.empty() ? "key" : columns.front());
            }
        }
    }

    Y_UNIT_TEST(ProjectionPrunesSourceAndRetainsCountCarrier) {
        for (const TVector<TString>& columns : {TVector<TString>{"value"}, TVector<TString>{}}) {
            TFixture f;
            const auto read = f.MakeRead();
            const auto wrap = CreateDqIntegration(f.State)->WrapRead(read.Ptr(), f.Ctx, {});
            const auto projection = f.MakeProjection(wrap, columns);
            auto optimizer = CreateLogicalOptimizer(f.State);
            TExprNode::TPtr output;
            UNIT_ASSERT(optimizer->Transform(projection.Ptr(), output, f.Ctx).Level != IGraphTransformer::TStatus::Error);
            const TDqSourceWrap source(output);
            const auto payload = f.SerializeSource(source);
            UNIT_ASSERT_VALUES_EQUAL(payload.ColumnsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(payload.GetColumns(0).GetName(), columns.empty() ? "key" : "value");
            // ExpandType represents StructType as one child per public field.
            UNIT_ASSERT_VALUES_EQUAL(source.RowType().Ref().ChildrenSize(), columns.size());
            auto annotation = CreateTypeAnnotationTransformer(f.State);
            TExprNode::TPtr annotated;
            UNIT_ASSERT_VALUES_EQUAL(annotation->Transform(source.Input().Ptr(), annotated, f.Ctx).Level, IGraphTransformer::TStatus::Ok);
            const auto* physical = source.Input().Ref().GetTypeAnn()->Cast<TStreamExprType>()->GetItemType()->Cast<TStructExprType>();
            UNIT_ASSERT_VALUES_EQUAL(physical->GetSize(), 2);
            UNIT_ASSERT(physical->FindItem(BlockLengthColumnName));
            UNIT_ASSERT(physical->FindItem(columns.empty() ? "key" : "value"));
        }
    }

    Y_UNIT_TEST(ProjectionDoesNotCrossLocalFilter) {
        TFixture f;
        const auto read = f.MakeRead();
        const auto wrap = CreateDqIntegration(f.State)->WrapRead(read.Ptr(), f.Ctx, {});
        const auto filter = Build<TCoFilter>(f.Ctx, read.Pos())
            .Input(wrap)
            .Lambda()
                .Args({"row"})
                .Body<TCoExists>()
                    .Optional<TCoMember>()
                        .Struct("row")
                        .Name().Build("value")
                    .Build()
                .Build()
            .Build()
            .Done();
        const auto projection = f.MakeProjection(filter.Ptr(), {"key"});
        auto optimizer = CreateLogicalOptimizer(f.State);
        TExprNode::TPtr output;
        UNIT_ASSERT_VALUES_EQUAL(optimizer->Transform(projection.Ptr(), output, f.Ctx).Level, IGraphTransformer::TStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(output.Get(), projection.Raw());
        UNIT_ASSERT_VALUES_EQUAL(f.SerializeSource(TDqSourceWrap(wrap)).ColumnsSize(), 2);
    }

    Y_UNIT_TEST(LookupProducesQueryIssueAndGuardsPlannerBoundary) {
        TFixture f;
        const auto read = f.MakeRead();
        const TDqSourceWrap wrap(CreateDqIntegration(f.State)->WrapRead(read.Ptr(), f.Ctx, {}));
        const auto lookup = Build<TDqLookupSourceWrap>(f.Ctx, read.Pos())
            .Input(wrap.Input()).DataSource(wrap.DataSource()).RowType(wrap.RowType()).Done();
        auto optimizer = CreateLogicalOptimizer(f.State);
        TExprNode::TPtr output;
        UNIT_ASSERT_VALUES_EQUAL(optimizer->Transform(lookup.Ptr(), output, f.Ctx).Level, IGraphTransformer::TStatus::Error);
        UNIT_ASSERT(f.Ctx.IssueManager.GetIssues().ToString().Contains("Ydb streamlookup joins are not supported"));
        google::protobuf::Any packed;
        TString sourceType;
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreateDqIntegration(f.State)->FillLookupSourceSettings(lookup.Ref(), packed, sourceType),
            yexception, "Ydb streamlookup joins are not supported");
    }

    Y_UNIT_TEST(InternalReadTimeoutMatchesSourceAndPlan) {
        TFixture f;
        const TSource defaults;
        UNIT_ASSERT(!defaults.HasReadTimeoutMs());
        UNIT_ASSERT_VALUES_EQUAL(defaults.GetReadTimeoutMs(), 60000);

        auto integration = CreateDqIntegration(f.State);
        const auto read = f.MakeRead();
        const TDqSourceWrap wrap(integration->WrapRead(read.Ptr(), f.Ctx, {}));
        const auto payload = f.SerializeSource(wrap);
        UNIT_ASSERT(payload.HasReadTimeoutMs());
        UNIT_ASSERT_VALUES_EQUAL(payload.GetReadTimeoutMs(), defaults.GetReadTimeoutMs());

        const auto source = Build<TDqSource>(f.Ctx, read.Pos())
            .DataSource(wrap.DataSource()).Settings(wrap.Input()).Done();
        TMap<TString, NJson::TJsonValue> properties;
        UNIT_ASSERT(integration->FillSourcePlanProperties(source, properties));
        UNIT_ASSERT_VALUES_EQUAL(properties.at("SourceType").GetStringSafe(), "Ydb");
        UNIT_ASSERT_VALUES_EQUAL(properties.at("ReadTimeoutMs").GetUIntegerSafe(), payload.GetReadTimeoutMs());
    }

    Y_UNIT_TEST(ClusterErrorsDoNotContainSourcePathsOrCredentials) {
        TFixture f;
        const THashMap<TString, TString> valid{
            {"location", "localhost:2135"}, {"database_name", "/Remote"}, {"authMethod", "NONE"}};
        const TVector<std::tuple<TString, TString, TString>> cases{
            {"database_id", "private-managed-id", "Ydb currently requires explicit LOCATION and DATABASE_NAME; database ID resolution is not supported"},
            {"database_name", "/Remote/../private-db", "Ydb requires an absolute DATABASE_NAME without empty, '.' or '..' path components"},
            {"location", "grpc://secret@host:2135", "Ydb requires LOCATION in host:port format"},
            {"use_tls", "private-invalid-value", "Ydb USE_TLS must be true or false"},
            {"authMethod", "TOKEN", "Ydb TOKEN credentials are missing"},
            {"authMethod", "BASIC", "Ydb currently supports only TOKEN and NONE authentication"}};
        for (const auto& [property, value, expected] : cases) {
            auto properties = valid;
            properties[property] = value;
            TString error;
            try {
                AddCluster(*f.State, "bad", properties);
            } catch (const yexception& ex) {
                error = ex.what();
            }
            UNIT_ASSERT_VALUES_EQUAL(error, expected);
        }
    }

    Y_UNIT_TEST(SourceUsesArrowBlocksWithRequiredKey) {
        TFixture f;
        auto integration = CreateDqIntegration(f.State);
        auto read = f.MakeRead();
        const TDqSourceWrap wrap(integration->WrapRead(read.Ptr(), f.Ctx, {}));
        auto annotation = CreateTypeAnnotationTransformer(f.State);
        TExprNode::TPtr output;
        UNIT_ASSERT_VALUES_EQUAL(annotation->Transform(wrap.Input().Ptr(), output, f.Ctx).Level, IGraphTransformer::TStatus::Ok);
        const auto* row = wrap.Input().Ref().GetTypeAnn()->Cast<TStreamExprType>()->GetItemType()->Cast<TStructExprType>();
        const auto* keyType = row->GetItems()[*row->FindItem("key")]->GetItemType()->Cast<TBlockExprType>()->GetItemType();
        UNIT_ASSERT_VALUES_EQUAL(keyType->GetKind(), ETypeAnnotationKind::Data);
        UNIT_ASSERT(row->FindItem(BlockLengthColumnName));
    }

    Y_UNIT_TEST(UnsupportedAuthAndDatabaseIdRejected) {
        TFixture f;
        UNIT_ASSERT_EXCEPTION(AddCluster(*f.State, "bad", {{"database_id", "managed-id"}}), yexception);
        UNIT_ASSERT_EXCEPTION(AddCluster(*f.State, "bad", {
            {"location", "localhost:2135"}, {"database_name", "/Remote"}, {"authMethod", "BASIC"}}), yexception);
    }

    Y_UNIT_TEST(TableReadIgnoresTopicOnlyProperties) {
        TFixture f;
        AddCluster(*f.State, "shared", {{"location", "localhost:2135"}, {"database_name", "/Remote"},
            {"authMethod", "NONE"}, {"shared_reading", "true"}, {"shared_reading_group", "topic-group"}});
        UNIT_ASSERT(f.State->ValidClusters.contains("shared"));
        UNIT_ASSERT_VALUES_EQUAL(f.State->Clusters.at("shared").Database, "/Remote");
    }

    Y_UNIT_TEST(RelativeDatabaseIsNormalized) {
        TFixture f;
        AddCluster(*f.State, "legacy", {{"source_type", "Ydb"}, {"location", "localhost:2135"},
            {"database_name", "Remote"}, {"authMethod", "NONE"}});
        UNIT_ASSERT_VALUES_EQUAL(f.State->Clusters.at("legacy").Database, "/Remote");
        AddCluster(*f.State, "without_source_type", {{"location", "localhost:2135"},
            {"database_name", "Remote"}, {"authMethod", "NONE"}});
        UNIT_ASSERT_VALUES_EQUAL(f.State->Clusters.at("without_source_type").Database, "/Remote");
    }
}

} // namespace NYql::NYdbQuery
