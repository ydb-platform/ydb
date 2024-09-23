#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/providers/generic/proto/source.pb.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_state.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_provider.h>

#include <ydb/library/yql/ast/yql_ast.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/core/services/yql_out_transformers.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/result/provider/yql_result_provider.h>
#include <ydb/library/yql/sql/sql.h>
#include <ydb/library/yql/utils/log/log.h>

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/random_provider/random_provider.h>

#include <google/protobuf/text_format.h>

#include <fmt/format.h>

using namespace NYql;
using namespace NNodes;
using namespace NKikimr::NMiniKQL;
using namespace fmt::literals;

// Template for program to optimize
static constexpr auto ProgramTemplate = R"ast((
(let $data_source (DataSource '"generic" '"test_cluster"))
(let $empty_lambda (lambda '($arg) (Bool '"true")))
(let $table
    (MrTableConcat (Key '('table (String '"test_table"))))
)
(let $read (Read! world $data_source $table))

(let $map_lambda (lambda '($row)
    (OptionalIf
        {lambda_text}
        $row
    )
))
(let $filtered_data (FlatMap (Right! $read) $map_lambda))

(let $resulte_data_sink (DataSink '"result"))
(let $result (ResWrite! (Left! $read) $resulte_data_sink (Key) $filtered_data '('('type))))
(return (Commit! $result $resulte_data_sink))
))ast";

struct TFakeDatabaseResolver: public IDatabaseAsyncResolver {
    NThreading::TFuture<TDatabaseResolverResponse> ResolveIds(const TDatabaseAuthMap& ids) const {
        TDatabaseResolverResponse resp;
        resp.Success = true;
        for (const auto& [databasePair, auth] : ids) {
            const auto& [database, type] = databasePair;
            TDatabaseResolverResponse::TDatabaseDescription& desc = resp.DatabaseDescriptionMap[std::pair(database, type)];
            desc.Database = database;
            desc.Endpoint = "endpoint";
            desc.Host = "host";
            desc.Port = 42;
        }
        return NThreading::MakeFuture(resp);
    }
};

struct TFakeGenericClient: public NConnector::IClient {
    NConnector::TDescribeTableAsyncResult DescribeTable(const NConnector::NApi::TDescribeTableRequest& request) {
        UNIT_ASSERT_VALUES_EQUAL(request.table(), "test_table");
        NConnector::TResult<NConnector::NApi::TDescribeTableResponse> result;
        auto& resp = result.Response.emplace();
        auto& schema = *resp.mutable_schema();

#define PRIMITIVE_TYPE_COL(name, type)                                          \
    {                                                                           \
        auto* col = schema.add_columns();                                       \
        col->set_name("col_" name);                                             \
        auto* t = col->mutable_type();                                          \
        t->set_type_id(Ydb::Type::type);                                        \
    }                                                                           \
    {                                                                           \
        auto* col = schema.add_columns();                                       \
        col->set_name("col_optional_" name);                                    \
        auto* t = col->mutable_type()->mutable_optional_type()->mutable_item(); \
        t->set_type_id(Ydb::Type::type);                                        \
    }

        PRIMITIVE_TYPE_COL("bool", BOOL);
        PRIMITIVE_TYPE_COL("int8", INT8);
        PRIMITIVE_TYPE_COL("uint8", UINT8);
        PRIMITIVE_TYPE_COL("int16", INT16);
        PRIMITIVE_TYPE_COL("uint16", UINT16);
        PRIMITIVE_TYPE_COL("int32", INT32);
        PRIMITIVE_TYPE_COL("uint32", UINT32);
        PRIMITIVE_TYPE_COL("int64", INT64);
        PRIMITIVE_TYPE_COL("uint64", UINT64);
        PRIMITIVE_TYPE_COL("float", FLOAT);
        PRIMITIVE_TYPE_COL("double", DOUBLE);
        PRIMITIVE_TYPE_COL("date", DATE);
        PRIMITIVE_TYPE_COL("datetime", DATETIME);
        PRIMITIVE_TYPE_COL("timestamp", TIMESTAMP);
        PRIMITIVE_TYPE_COL("interval", INTERVAL);
        PRIMITIVE_TYPE_COL("tz_date", TZ_DATE);
        PRIMITIVE_TYPE_COL("tz_datetime", TZ_DATETIME);
        PRIMITIVE_TYPE_COL("tz_timestamp", TZ_TIMESTAMP);
        PRIMITIVE_TYPE_COL("string", STRING);
        PRIMITIVE_TYPE_COL("utf8", UTF8);
        PRIMITIVE_TYPE_COL("yson", YSON);
        PRIMITIVE_TYPE_COL("json", JSON);
        PRIMITIVE_TYPE_COL("uuid", UUID);
        PRIMITIVE_TYPE_COL("json_document", JSON_DOCUMENT);
        PRIMITIVE_TYPE_COL("dynumber", DYNUMBER);

        return NThreading::MakeFuture<NConnector::TDescribeTableAsyncResult::value_type>(std::move(result));
    }

    NConnector::TListSplitsStreamIteratorAsyncResult ListSplits(const NConnector::NApi::TListSplitsRequest& request) {
        Y_UNUSED(request);
        try {
            throw std::runtime_error("ListSplits unimplemented");
        } catch (...) {
            return NThreading::MakeErrorFuture<NConnector::TListSplitsStreamIteratorAsyncResult::value_type>(std::current_exception());
        }
    }

    NConnector::TReadSplitsStreamIteratorAsyncResult ReadSplits(const NConnector::NApi::TReadSplitsRequest& request) {
        Y_UNUSED(request);
        try {
            throw std::runtime_error("ReadSplits unimplemented");
        } catch (...) {
            return NThreading::MakeErrorFuture<NConnector::TReadSplitsStreamIteratorAsyncResult::value_type>(std::current_exception());
        }
    }
};

class TBuildDqSourceSettingsTransformer: public TOptimizeTransformerBase {
public:
    explicit TBuildDqSourceSettingsTransformer(TTypeAnnotationContext* types, Generic::TSource* dqSourceSettings, bool* dqSourceSettingsWereBuilt)
        : TOptimizeTransformerBase(types, NLog::EComponent::ProviderGeneric, {})
        , DqSourceSettings_(dqSourceSettings)
        , DqSourceSettingsWereBuilt_(dqSourceSettingsWereBuilt)
    {
        AddHandler(0, TCoRight::Match, "BuildGenericDqSourceSettings", Hndl(&TBuildDqSourceSettingsTransformer::BuildDqSource));
    }

    TMaybeNode<TExprBase> BuildDqSource(TExprBase node, TExprContext& ctx) {
        TCoRight right = node.Cast<TCoRight>();
        TExprBase input = right.Input();
        if (!input.Maybe<TGenReadTable>()) {
            return node;
        }
        auto genericDataSource = Types->DataSourceMap.find(GenericProviderName);
        UNIT_ASSERT(genericDataSource != Types->DataSourceMap.end());
        auto dqIntegration = genericDataSource->second->GetDqIntegration();
        UNIT_ASSERT(dqIntegration);
        auto newRead = dqIntegration->WrapRead(TDqSettings(), input.Ptr(), ctx);
        BuildSettings(newRead, dqIntegration, ctx);
        return newRead;
    }

    void BuildSettings(const TExprNode::TPtr& read, IDqIntegration* dqIntegration, TExprContext& ctx) {
        UNIT_ASSERT(!*DqSourceSettingsWereBuilt_);
        // Hack: we need DqSource to build settings:
        // build node, call DqSourceWrap and throw it away
        TDqSourceWrap wrap(read);
        auto dqSourceNode =
            Build<TDqSource>(ctx, read->Pos())
                .DataSource(wrap.DataSource())
                .Settings(wrap.Input())
                .Done()
                .Ptr();
        ::google::protobuf::Any settings;
        TString sourceType;
        dqIntegration->FillSourceSettings(*dqSourceNode, settings, sourceType, 1, ctx);
        UNIT_ASSERT_STRINGS_EQUAL(sourceType, "PostgreSqlGeneric");
        UNIT_ASSERT(settings.Is<Generic::TSource>());
        settings.UnpackTo(DqSourceSettings_);
        *DqSourceSettingsWereBuilt_ = true;
    }

private:
    Generic::TSource* DqSourceSettings_;
    bool* DqSourceSettingsWereBuilt_;
};

struct TPushdownFixture: public NUnitTest::TBaseFixture {
    TExprContext Ctx;
    TTypeAnnotationContextPtr TypesCtx;

    TGenericState::TPtr GenericState;
    std::shared_ptr<TFakeDatabaseResolver> DatabaseResolver = std::make_shared<TFakeDatabaseResolver>();
    std::shared_ptr<TFakeGenericClient> GenericClient = std::make_shared<TFakeGenericClient>();
    TIntrusivePtr<IDataProvider> GenericDataSource;
    TIntrusivePtr<IDataProvider> GenericDataSink;

    TGatewaysConfig GatewaysCfg;
    IFunctionRegistry::TPtr FunctionRegistry;

    TAutoPtr<IGraphTransformer> Transformer;
    TAutoPtr<IGraphTransformer> BuildDqSourceSettingsTransformer;
    Generic::TSource DqSourceSettings;
    bool DqSourceSettingsWereBuilt = false;

    TExprNode::TPtr InitialExprRoot;
    TExprNode::TPtr ExprRoot;

    TPushdownFixture() {
        Init();
    }

    void Init() {
        TypesCtx = MakeIntrusive<TTypeAnnotationContext>();
        TypesCtx->RandomProvider = CreateDeterministicRandomProvider(1);

        FunctionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone(); // TODO: remove Clone()

        {
            auto* setting = GatewaysCfg.MutableGeneric()->AddDefaultSettings();
            setting->SetName("UsePredicatePushdown");
            setting->SetValue("true");

            auto* cluster = GatewaysCfg.MutableGeneric()->AddClusterMapping();
            cluster->SetName("test_cluster");
            cluster->SetKind(NConnector::NApi::POSTGRESQL);
            cluster->MutableEndpoint()->set_host("host");
            cluster->MutableEndpoint()->set_port(42);
            cluster->MutableCredentials()->mutable_basic()->set_username("user");
            cluster->MutableCredentials()->mutable_basic()->set_password("password");
            cluster->SetDatabaseName("database");
            cluster->SetProtocol(NConnector::NApi::NATIVE);
        }

        GenericState = MakeIntrusive<TGenericState>(
            TypesCtx.Get(),
            FunctionRegistry.Get(),
            DatabaseResolver,
            nullptr,
            GenericClient,
            GatewaysCfg.GetGeneric());

        GenericDataSource = CreateGenericDataSource(GenericState);
        GenericDataSink = CreateGenericDataSink(GenericState);

        TypesCtx->AddDataSource(GenericProviderName, GenericDataSource);
        TypesCtx->AddDataSink(GenericProviderName, GenericDataSink);

        {
            auto writerFactory = []() { return CreateYsonResultWriter(NYson::EYsonFormat::Binary); };
            auto cfg = MakeIntrusive<TResultProviderConfig>(*TypesCtx, *FunctionRegistry, IDataProvider::EResultFormat::Yson,
                                                            ToString((ui32)NYson::EYsonFormat::Binary), writerFactory);
            auto resultProvider = CreateResultProvider(cfg);
            TypesCtx->AddDataSink(ResultProviderName, resultProvider);
        }

        Transformer = TTransformationPipeline(TypesCtx)
                          .AddServiceTransformers()
                          .Add(TExprLogTransformer::Sync("Expr", NLog::EComponent::Core, NLog::ELevel::DEBUG), "LogExpr")
                          .AddPreTypeAnnotation()
                          .AddExpressionEvaluation(*FunctionRegistry)
                          .AddIOAnnotation()
                          .AddTypeAnnotation()
                          .AddPostTypeAnnotation()
                          .Add(TExprLogTransformer::Sync("Expr to optimize", NLog::EComponent::Core, NLog::ELevel::DEBUG), "LogExpr")
                          .AddOptimization(false, false)
                          .Add(TExprLogTransformer::Sync("Optimized expr", NLog::EComponent::Core, NLog::ELevel::DEBUG), "LogExpr")
                          .Build();

        TAutoPtr<IGraphTransformer> buildTransformer = new TBuildDqSourceSettingsTransformer(TypesCtx.Get(), &DqSourceSettings, &DqSourceSettingsWereBuilt);
        BuildDqSourceSettingsTransformer = TTransformationPipeline(TypesCtx)
                                               .AddServiceTransformers()
                                               .Add(buildTransformer, "BuildDqSourceSettings")
                                               .Add(TExprLogTransformer::Sync("Built settings", NLog::EComponent::Core, NLog::ELevel::DEBUG), "LogExpr")
                                               .Build();

        NLog::YqlLogger().ResetBackend(CreateLogBackend("cerr"));
        NLog::YqlLogger().SetComponentLevel(NLog::EComponent::Core, NLog::ELevel::TRACE);
        NLog::YqlLogger().SetComponentLevel(NLog::EComponent::ProviderCommon, NLog::ELevel::TRACE);
        NLog::YqlLogger().SetComponentLevel(NLog::EComponent::ProviderGeneric, NLog::ELevel::TRACE);
    }

    void Transform(const TString& program) {
        Cerr << "Initial program:\n"
             << program << Endl;
        TAstParseResult astRes = ParseAst(program);
        UNIT_ASSERT_C(astRes.IsOk(), astRes.Issues.ToString());
        UNIT_ASSERT_C(CompileExpr(*astRes.Root, InitialExprRoot, Ctx, nullptr, nullptr), astRes.Issues.ToString());

        ExprRoot = InitialExprRoot;
        auto status = SyncTransform(*Transformer, ExprRoot, Ctx);
        UNIT_ASSERT_C(status == IGraphTransformer::TStatus::Ok, Ctx.IssueManager.GetIssues().ToString());
    }

    void BuildDqSourceSettings() {
        auto root = ExprRoot;
        auto status = SyncTransform(*BuildDqSourceSettingsTransformer, root, Ctx);
        UNIT_ASSERT_C(status == IGraphTransformer::TStatus::Ok, Ctx.IssueManager.GetIssues().ToString());
        UNIT_ASSERT(DqSourceSettingsWereBuilt);
        Cerr << "Dq source filter settings:\n"
             << DqSourceSettings.select().where().Utf8DebugString() << Endl;
    }

    const NConnector::NApi::TPredicate& BuildProtoFilterFromProgram(const TString& program) {
        Transform(program);
        BuildDqSourceSettings();
        return DqSourceSettings.select().where().filter_typed();
    }

    static TString ProgramFromLambda(const TString& lambdaText) {
        return fmt::format(
            ProgramTemplate,
            "lambda_text"_a = lambdaText);
    }

    const NConnector::NApi::TPredicate& BuildProtoFilterFromLambda(const TString& lambdaText) {
        return BuildProtoFilterFromProgram(ProgramFromLambda(lambdaText));
    }

    void AssertFilter(const TString& lambdaText, const TString& filterText) {
        const auto& filter = BuildProtoFilterFromLambda(lambdaText);
        NConnector::NApi::TPredicate expectedFilter;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(filterText, &expectedFilter));
        UNIT_ASSERT_STRINGS_EQUAL(filter.Utf8DebugString(), expectedFilter.Utf8DebugString());
    }

    void AssertNoPush(const TString& lambdaText) {
        BuildProtoFilterFromLambda(lambdaText);
        UNIT_ASSERT(!DqSourceSettings.select().where().has_filter_typed());
    }
};

Y_UNIT_TEST_SUITE_F(PushdownTest, TPushdownFixture) {
    Y_UNIT_TEST(NoFilter) {
        AssertNoPush(R"ast((Bool '"true"))ast"); // Note that R"ast()ast" is empty string!
    }

    Y_UNIT_TEST(Equal) {
        AssertFilter(
            // Note that R"ast()ast" is empty string!
            R"ast((== (Member $row '"col_int16") (Int16 '42)))ast",
            R"proto(
                comparison {
                    operation: EQ
                    left_value {
                        column: "col_int16"
                    }
                    right_value {
                        typed_value {
                            type {
                                type_id: INT16
                            }
                            value {
                                int32_value: 42
                            }
                        }
                    }
                }
            )proto");
    }

    Y_UNIT_TEST(NotEqualInt32Int64) {
        AssertFilter(
            // Note that R"ast()ast" is empty string!
            R"ast(
                (Coalesce
                    (!= (Member $row '"col_optional_uint64") (Member $row '"col_uint32"))
                    (Bool '"true")
                )
                )ast",
            R"proto(
                comparison {
                    operation: NE
                    left_value {
                        column: "col_optional_uint64"
                    }
                    right_value {
                        column: "col_uint32"
                    }
                }
            )proto");
    }

    Y_UNIT_TEST(CmpInt16AndInt32) {
        AssertFilter(
            // Note that R"ast()ast" is empty string!
            R"ast(
                (<= (Member $row '"col_int32") (Member $row '"col_int16"))
                )ast",
            R"proto(
                comparison {
                    operation: LE
                    left_value {
                        column: "col_int32"
                    }
                    right_value {
                        column: "col_int16"
                    }
                }
            )proto");
    }

    Y_UNIT_TEST(PartialAnd) {
        AssertFilter(
            // Note that R"ast()ast" is empty string!
            // Unwrap must be excluded from pushdown, but the other parts of "And" statement - not
            R"ast(
                (Coalesce
                    (And
                        (Or
                            (Not (Member $row '"col_bool"))
                            (== (* (Member $row '"col_int64") (Member $row '"col_int32")) (Int64 '42))
                        )
                        (< (Unwrap (/ (Int64 '42) (Member $row '"col_int64"))) (Int64 '10))
                        (>= (Member $row '"col_uint32") (- (Uint32 '15) (Uint32 '1)))
                    )
                    (Bool '"true")
                )
                )ast",
            R"proto(
                conjunction {
                    operands {
                        disjunction {
                            operands {
                                negation {
                                    operand {
                                        bool_expression: {
                                            value {
                                                column: "col_bool"
                                            }
                                        }
                                    }
                                }
                            }
                            operands {
                                comparison {
                                    operation: EQ
                                    left_value {
                                        arithmetical_expression {
                                            operation: MUL
                                            left_value {
                                                column: "col_int64"
                                            }
                                            right_value {
                                                column: "col_int32"
                                            }
                                        }
                                    }
                                    right_value {
                                        typed_value {
                                            type {
                                                type_id: INT64
                                            }
                                            value {
                                                int64_value: 42
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    operands {
                        comparison {
                            operation: GE
                            left_value {
                                column: "col_uint32"
                            }
                            right_value {
                                arithmetical_expression {
                                    operation: SUB
                                    left_value {
                                        typed_value {
                                            type {
                                                type_id: UINT32
                                            }
                                            value {
                                                uint32_value: 15
                                            }
                                        }
                                    }
                                    right_value {
                                        typed_value {
                                            type {
                                                type_id: UINT32
                                            }
                                            value {
                                                uint32_value: 1
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            )proto");
    }

    Y_UNIT_TEST(PartialAndOneBranchPushdownable) {
        AssertFilter(
            // Note that R"ast()ast" is empty string!
            // Unwrap must be excluded from pushdown, but the other part of "And" statement - not.
            // So we expect only one branch of "And" to be pushed down.
            R"ast(
                (Coalesce
                    (And
                        (< (Unwrap (/ (Int64 '42) (Member $row '"col_int64"))) (Int64 '10))
                        (>= (Member $row '"col_uint32") (Uint32 '15))
                    )
                    (Bool '"true")
                )
                )ast",
            R"proto(
                comparison {
                    operation: GE
                    left_value {
                        column: "col_uint32"
                    }
                    right_value {
                        typed_value {
                            type {
                                type_id: UINT32
                            }
                            value {
                                uint32_value: 15
                            }
                        }
                    }
                }
            )proto");
    }

    Y_UNIT_TEST(NotNull) {
        AssertFilter(
            // Note that R"ast()ast" is empty string!
            R"ast(
                (Exists
                    (Member $row '"col_optional_utf8")
                )
                )ast",
            R"proto(
                is_not_null {
                    value {
                        column: "col_optional_utf8"
                    }
                }
            )proto");
    }

    Y_UNIT_TEST(NotNullForDatetime) {
        AssertFilter(
            // Note that R"ast()ast" is empty string!
            R"ast(
                (Exists
                    (Member $row '"col_optional_tz_datetime")
                )
                )ast",
            R"proto(
                is_not_null {
                    value {
                        column: "col_optional_tz_datetime"
                    }
                }
            )proto");
    }

    Y_UNIT_TEST(IsNull) {
        AssertFilter(
            // Note that R"ast()ast" is empty string!
            R"ast(
                (Not
                    (Exists
                        (Member $row '"col_optional_utf8")
                    )
                )
                )ast",
            R"proto(
                is_null {
                    value {
                        column: "col_optional_utf8"
                    }
                }
            )proto");
    }

    Y_UNIT_TEST(StringFieldsNotSupported) {
        AssertNoPush(
            // Note that R"ast()ast" is empty string!
            R"ast(
                (Coalesce
                    (==
                        (Member $row '"col_utf8")
                        (Member $row '"col_optional_utf8")
                    )
                    (Bool '"true")
                )
                )ast");
    }

    Y_UNIT_TEST(StringFieldsNotSupported2) {
        AssertNoPush(
            // Note that R"ast()ast" is empty string!
            R"ast(
                (!=
                    (Member $row '"col_string")
                    (String '"value")
                )
                )ast");
    }
}
