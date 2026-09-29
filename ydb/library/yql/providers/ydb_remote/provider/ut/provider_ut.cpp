#include <ydb/library/yql/providers/ydb_remote/provider/yql_ydb_remote_provider_impl.h>
#include <ydb/library/yql/providers/ydb_remote/expr_nodes/yql_ydb_remote_expr_nodes.h>
#include <ydb/library/yql/providers/ydb_remote/proto/source.pb.h>
#include <ydb/library/yql/providers/native/operation_context.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>
#include <yql/essentials/core/sql_types/block.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NYdbRemote {
namespace {

using namespace NNodes;

struct TFixture {
    TExprContext Ctx;
    TIntrusivePtr<TTypeAnnotationContext> Types = MakeIntrusive<TTypeAnnotationContext>();
    NYdb::TDriver Driver{NYdb::TDriverConfig().SetNetworkThreadsNum(1).SetClientThreadsNum(1)};
    TState::TPtr State;

    explicit TFixture(TInstant deadline = TInstant::Max(), std::shared_ptr<NNative::IAsyncMemoryQuota> quota = {})
        : State(MakeIntrusive<TState>(Types.Get(), Driver, CreateStructuredTokenCredentialsFactory(), deadline, std::move(quota)))
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
    }

    TYdbRemoteReadTable MakeRead() {
        const auto pos = Ctx.AppendPosition({});
        auto world = Ctx.NewWorld(pos);
        world->SetTypeAnn(Ctx.MakeType<TWorldExprType>());
        auto read = Build<TYdbRemoteReadTable>(Ctx, pos)
            .World(world)
            .DataSource<TYdbRemoteDataSource>()
                .Category().Build(YdbRemoteProviderName)
                .Cluster().Build("remote")
            .Build()
            .Table().Build("items")
            .Columns<TCoVoid>().Build()
            .Done();
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
};

class TWaitingMetadataQuota final : public NNative::IAsyncMemoryQuota {
public:
    NThreading::TFuture<std::shared_ptr<void>> Acquire(
        ui64 bytes, TInstant deadline, NThreading::TCancellationToken cancellation) override {
        Requested.push_back(bytes);
        Deadline = deadline;
        Cancellation = cancellation;
        if (bytes == MetadataSchemaReservation) {
            auto lease = std::make_shared<int>(0);
            SchemaLease = lease;
            return NThreading::MakeFuture<std::shared_ptr<void>>(std::move(lease));
        }
        auto promise = NThreading::NewPromise<std::shared_ptr<void>>();
        cancellation.Future().Subscribe([promise](const NThreading::TFuture<void>&) mutable {
            promise.TrySetException(std::make_exception_ptr(yexception() << "cancelled admission"));
        });
        return promise.GetFuture();
    }

    void Shutdown() override {}

    TVector<ui64> Requested;
    TInstant Deadline;
    NThreading::TCancellationToken Cancellation = NThreading::TCancellationToken::Default();
    std::weak_ptr<void> SchemaLease;
};

} // namespace

Y_UNIT_TEST_SUITE(TYdbRemoteProvider) {
    Y_UNIT_TEST(MetadataAcceptsLiteralKeyWithOrWithoutConcat) {
        for (const bool wrapKey : {false, true}) {
            TFixture f;
            auto transformer = CreateLoadMetadataTransformer(f.State);
            const auto input = f.MakeRawRead(wrapKey);
            TExprNode::TPtr output;
            const auto status = transformer->Transform(input, output, f.Ctx);
            UNIT_ASSERT(status.Level != IGraphTransformer::TStatus::Error);
            UNIT_ASSERT(status.Level != IGraphTransformer::TStatus::Async);
            UNIT_ASSERT(TYdbRemoteReadTable::Match(output.Get()));
            const TYdbRemoteReadTable read(output);
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
        UNIT_ASSERT(TYdbRemoteReadTable::Match(output->Child(1)));
    }

    Y_UNIT_TEST(ProviderDispatchChecksDataSourceAndDataSinkCategories) {
        TFixture f;
        auto providers = CreateYdbRemoteDataProviders(f.Types.Get(), f.Driver);
        const auto nativeRead = f.MakeRawRead(false);
        const auto pos = nativeRead->Pos();
        auto foreignChildren = nativeRead->ChildrenList();
        foreignChildren[1] = f.Ctx.NewCallable(pos, "DataSource", {
            f.Ctx.NewAtom(pos, "kikimr"), f.Ctx.NewAtom(pos, "local")});
        const auto foreignRead = f.Ctx.ChangeChildren(*nativeRead, std::move(foreignChildren));
        UNIT_ASSERT(providers.Source->CanParse(*nativeRead));
        UNIT_ASSERT(!providers.Source->CanParse(*foreignRead));
        for (const TStringBuf callable : {TStringBuf("Write!"), TStringBuf("Commit!")}) {
            for (const TStringBuf category : {YdbRemoteProviderName, TStringBuf("kikimr")}) {
                const auto operation = f.Ctx.NewCallable(pos, callable, {
                    nativeRead->ChildPtr(0), f.Ctx.NewCallable(pos, "DataSink", {
                        f.Ctx.NewAtom(pos, category), f.Ctx.NewAtom(pos, "remote")})});
                UNIT_ASSERT_VALUES_EQUAL(providers.Sink->CanParse(*operation), category == YdbRemoteProviderName);
                UNIT_ASSERT_VALUES_EQUAL(providers.Sink->CanExecute(*operation),
                    category == YdbRemoteProviderName && callable == "Commit!");
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

    Y_UNIT_TEST(MetadataRequiresAdmissionForUncachedTables) {
        TFixture f;
        auto transformer = CreateLoadMetadataTransformer(f.State);
        TExprNode::TPtr output;
        UNIT_ASSERT_VALUES_EQUAL(transformer->Transform(f.MakeRawRead(false, 1, "uncached"), output, f.Ctx).Level,
            IGraphTransformer::TStatus::Error);
        UNIT_ASSERT(f.Ctx.IssueManager.GetIssues().ToString().Contains("memory quota is unavailable"));
    }

    Y_UNIT_TEST(MetadataBoundsDistinctTableCountBeforeAdmission) {
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

    Y_UNIT_TEST(MetadataRewindCancelsAdmissionAndRetainsTheSharedDeadline) {
        auto quota = std::make_shared<TWaitingMetadataQuota>();
        const auto deadline = TInstant::Now() + TDuration::Minutes(3);
        TFixture f(deadline, quota);
        auto transformer = CreateLoadMetadataTransformer(f.State);
        TExprNode::TPtr output;
        const auto input = f.MakeRawRead(false, 1, "uncached");
        UNIT_ASSERT_VALUES_EQUAL(transformer->Transform(input, output, f.Ctx).Level, IGraphTransformer::TStatus::Async);
        auto future = transformer->GetAsyncFuture(*input);
        UNIT_ASSERT(!future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(quota->Requested.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(quota->Requested[0], MetadataSchemaReservation);
        UNIT_ASSERT_VALUES_EQUAL(quota->Requested[1], MetadataResponseReservation);
        UNIT_ASSERT_VALUES_EQUAL(quota->Deadline, deadline);
        UNIT_ASSERT(!quota->SchemaLease.expired());
        transformer->Rewind();
        UNIT_ASSERT(quota->Cancellation.IsCancellationRequested());
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT(quota->SchemaLease.expired());
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
        UNIT_ASSERT_VALUES_EQUAL(sourceType, "YdbRemote");
        TSource payload;
        UNIT_ASSERT(packed.UnpackTo(&payload));
        UNIT_ASSERT_VALUES_EQUAL(payload.GetVersion(), 1);
        UNIT_ASSERT_VALUES_EQUAL(payload.GetDatabase(), "/Remote");
        UNIT_ASSERT_VALUES_EQUAL(payload.GetTable(), "/Remote/items");
        UNIT_ASSERT_VALUES_EQUAL(payload.GetToken(), "cluster:default_remote");
        UNIT_ASSERT(payload.GetUseTls());
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
        const auto settings = wrap.Input().Cast<TYdbRemoteSourceSettings>();
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
}

} // namespace NYql::NYdbRemote
