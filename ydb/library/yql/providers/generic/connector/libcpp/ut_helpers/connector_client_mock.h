#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/error.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/ut_helpers/defaults.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/ut_helpers/stream_iterator_mock.h>

#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

#include <arrow/api.h>

#include <google/protobuf/util/message_differencer.h>

#include <memory>
#include <type_traits>

namespace NYql::NConnector::NTest {
    using namespace testing;

#define EXPR_SETTER(name, set_expr)           \
    TBuilder& name(const auto& value) {       \
        this->Result_->set_expr(value);       \
        return static_cast<TBuilder&>(*this); \
    }

#define SETTER(name, protoName) EXPR_SETTER(name, Y_CAT(set_, protoName))

#define SUBPROTO_BUILDER(name, fieldExpr, protoType, builderType)                     \
    builderType name() {                                                              \
        return builderType(this->Result_->fieldExpr(), static_cast<TBuilder*>(this)); \
    }                                                                                 \
    TBuilder& name(const protoType& proto) {                                          \
        this->Result_->fieldExpr()->CopyFrom(proto);                                  \
        return static_cast<TBuilder&>(*this);                                         \
    }

#define DATA_SOURCE_INSTANCE_SUBBUILDER()                                           \
    TBuilder& DataSourceInstance(const NYql::TGenericDataSourceInstance& proto) {          \
        this->Result_->mutable_data_source_instance()->CopyFrom(proto);             \
        return static_cast<TBuilder&>(*this);                                       \
    }                                                                               \
    TPostgreSQLDataSourceInstanceBuilder<TBuilder> PostgreSQLDataSourceInstance() { \
        return TPostgreSQLDataSourceInstanceBuilder<TBuilder>(                      \
            this->Result_->mutable_data_source_instance(),                          \
            static_cast<TBuilder*>(this));                                          \
    }                                                                               \
    TClickHouseDataSourceInstanceBuilder<TBuilder> ClickHouseDataSourceInstance() { \
        return TClickHouseDataSourceInstanceBuilder<TBuilder>(                      \
            this->Result_->mutable_data_source_instance(),                          \
            static_cast<TBuilder*>(this));                                          \
    }                                                                               \
    TYdbDataSourceInstanceBuilder<TBuilder> YdbDataSourceInstance() {               \
        return TYdbDataSourceInstanceBuilder<TBuilder>(                             \
            this->Result_->mutable_data_source_instance(),                          \
            static_cast<TBuilder*>(this));                                          \
    }

    template <typename TProto>
    bool MatchProtos(const TProto& expected, const TProto& actual) {
        Cerr << "GENERIC-CONNECTOR-MOCK Expected: " << expected.DebugString() << Endl;
        Cerr << "GENERIC-CONNECTOR-MOCK Actual: " << actual.DebugString() << Endl;

        google::protobuf::util::MessageDifferencer differencer;
        TString differences;
        differencer.ReportDifferencesToString(&differences);

        bool result = differencer.Compare(actual, expected);

        if (!result) {
            Cerr << "GENERIC-CONNECTOR-MOCK Differences:" << Endl << differences << Endl;
        }

        return result;
    }

    MATCHER_P(ProtobufRequestMatcher, expected, "request does not match") {
        return MatchProtos(expected, arg);
    }

    MATCHER_P(RequestRelaxedMatcher, checker, "request does not match") {
        return checker(arg);
    }

#define MATCH_OPT_RESULT_WITH_VAL_IDX(VAL, RESULT_SET, GETTER, INDEX)               \
    {                                                                               \
            auto r = RESULT_SET.ColumnParser(INDEX).GETTER();                       \
            UNIT_ASSERT_VALUES_EQUAL(r.has_value(), VAL.has_value());               \
            if (r.has_value()) {                                                    \
                UNIT_ASSERT_VALUES_EQUAL(*r, *VAL);                                 \
            }                                                                       \
    }

#define MATCH_RESULT_WITH_INPUT_IDX(INPUT, RESULT_SET, GETTER, INDEX)               \
    {                                                                               \
        for (const auto& val : INPUT) {                                             \
            UNIT_ASSERT(RESULT_SET.TryNextRow());                                   \
            UNIT_ASSERT_VALUES_EQUAL(RESULT_SET.ColumnParser(INDEX).GETTER(), val); \
        }                                                                           \
    }

#define MATCH_RESULT_WITH_INPUT(INPUT, RESULT_SET, GETTER)\
    MATCH_RESULT_WITH_INPUT_IDX(INPUT, RESULT_SET, GETTER, 0)

    // Make arrow array for one column.
    // Returns field for schema and array with data.
    // Designed for call with MakeRecordBatch function.
    template <class TArrowBuilderType, class TColDataType>
    std::tuple<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::Array>>
    MakeArray(const TString& columnName,
              const std::vector<TColDataType>& input,
              std::shared_ptr<arrow::DataType> dataType) {
        TArrowBuilderType builder;
        UNIT_ASSERT_EQUAL(builder.AppendValues(input), arrow::Status::OK());
        std::shared_ptr<arrow::Array> columnData;
        UNIT_ASSERT_EQUAL(builder.Finish(&columnData), arrow::Status::OK());
        auto field = arrow::field(columnName, std::move(dataType));
        return {std::move(field), std::move(columnData)};
    }

    // Make record batch with the only column.
    template <class TArrowBuilderType, class TColDataType>
    std::shared_ptr<arrow::RecordBatch> MakeRecordBatch(
        const TString& columnName,
        const std::vector<TColDataType>& input,
        std::shared_ptr<arrow::DataType> dataType) {
        auto [field, columnData] = MakeArray<TArrowBuilderType, TColDataType>(columnName, input, dataType);
        auto schema = arrow::schema({field});
        return arrow::RecordBatch::Make(schema, columnData->length(), {columnData});
    }

    template <class T>
    concept TFieldAndArray = std::is_same_v<T, std::tuple<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::Array>>>;

    // Make record batch from several results of MakeArray calls with different params.
    template <TFieldAndArray... TArrayFieldTuple>
    std::shared_ptr<arrow::RecordBatch> MakeRecordBatch(const TArrayFieldTuple&... fields) {
        auto schema = arrow::schema({std::get<0>(fields)...});
        std::vector<std::shared_ptr<arrow::Array>> data = {std::get<1>(fields)...};
        UNIT_ASSERT(data.size());
        for (const auto& d : data) {
            UNIT_ASSERT_VALUES_EQUAL(d->length(), data[0]->length());
        }
        return arrow::RecordBatch::Make(schema, data[0]->length(), std::move(data));
    }

    // Make record batch with schema with no columns
    std::shared_ptr<arrow::RecordBatch> MakeEmptyRecordBatch(size_t rowsCount);

    template <class T>
    void SetSimpleValue(const T& value, Ydb::TypedValue* proto, bool optional = false);

    template <class T>
    void SetValue(const T& value, Ydb::TypedValue* proto,
        const ::Ydb::Type::PrimitiveTypeId& typeId, bool optional = false);

    template <class TParent>
    struct TWithParentBuilder {
        explicit TWithParentBuilder(TParent* parent)
            : Parent_(parent)
        {
        }

        TParent& Done() {
            Y_ABORT_UNLESS(Parent_); // Use only with parent builder
            return *Parent_;
        }

    protected:
        TParent* Parent_ = nullptr;
    };

    // No parent
    template <>
    struct TWithParentBuilder<void> {
        explicit TWithParentBuilder(void*)
        {
        }
    };

    template <class TParent, class TResultType>
    struct TResponseBuilder: public TWithParentBuilder<TParent> {
        explicit TResponseBuilder(std::shared_ptr<TResultType> result = nullptr, TParent* parent = nullptr)
            : TWithParentBuilder<TParent>(parent)
            , Result_(std::move(result))
        {
        }

        NThreading::TFuture<TResultType> GetResult() {
            return NThreading::MakeFuture<TResultType>(Result_);
        }

    protected:
        std::shared_ptr<TResultType> Result_;
    };

    template <class TParent, class TProto>
    struct TProtoBuilder: public TWithParentBuilder<TParent> {
        explicit TProtoBuilder(TProto* result = nullptr, TParent* parent = nullptr)
            : TWithParentBuilder<TParent>(parent)
            , Result_(result)
        {
            if (!Result_) {
                Result_ = &MaybeResult_.ConstructInPlace();
            }
        }

        TProto GetResult() {
            return *Result_;
        }

    protected:
        TProto* Result_ = nullptr;
        TMaybe<TProto> MaybeResult_;
    };

    void CreatePostgreSQLExternalDataSource(
        const std::shared_ptr<NKikimr::NKqp::TKikimrRunner>& kikimr,
        const TString& dataSourceName = DEFAULT_DATA_SOURCE_NAME,
        NYql::EGenericProtocol protocol = DEFAULT_PG_PROTOCOL,
        const TString& host = DEFAULT_PG_HOST,
        int port = DEFAULT_PG_PORT,
        const TString& login = DEFAULT_LOGIN,
        const TString& password = DEFAULT_PASSWORD,
        bool useTls = DEFAULT_USE_TLS,
        const TString& databaseName = DEFAULT_DATABASE,
        const TString& schema = DEFAULT_PG_SCHEMA);

    void CreateClickHouseExternalDataSource(
        const std::shared_ptr<NKikimr::NKqp::TKikimrRunner>& kikimr,
        const TString& dataSourceName = DEFAULT_DATA_SOURCE_NAME,
        NYql::EGenericProtocol protocol = DEFAULT_CH_PROTOCOL,
        const TString& clickHouseClusterId = DEFAULT_CH_CLUSTER_ID,
        const TString& login = DEFAULT_LOGIN,
        const TString& password = DEFAULT_PASSWORD,
        bool useTls = DEFAULT_USE_TLS,
        const TString& serviceAccountId = DEFAULT_CH_SERVICE_ACCOUNT_ID,
        const TString& serviceAccountIdSignature = DEFAULT_CH_SERVICE_ACCOUNT_ID_SIGNATURE,
        const TString& databaseName = DEFAULT_DATABASE);

    void CreateYdbExternalDataSource(
        const std::shared_ptr<NKikimr::NKqp::TKikimrRunner>& kikimr,
        const TString& dataSourceName = DEFAULT_DATA_SOURCE_NAME,
        const TString& login = DEFAULT_LOGIN,
        const TString& password = DEFAULT_PASSWORD,
        const TString& endpoint = DEFAULT_YDB_ENDPOINT,
        bool useTls = DEFAULT_USE_TLS,
        const TString& databaseName = DEFAULT_DATABASE);

    class TConnectorClientMock: public NYql::NConnector::IClient {
    public:
        MOCK_METHOD(TResult<NApi::TDescribeTableResponse>, DescribeTableImpl, (const NApi::TDescribeTableRequest& request));
        MOCK_METHOD(TIteratorResult<IListSplitsStreamIterator>, ListSplitsImpl, (const NApi::TListSplitsRequest& request));
        MOCK_METHOD(TIteratorResult<IReadSplitsStreamIterator>, ReadSplitsImpl, (const NApi::TReadSplitsRequest& request));

        //
        // Expectation helpers
        //

        enum class EArgsValidation {
            Strict,
            DataSourceInstance,
            Off,
        };

        template <class TDerived, class TParent = void /* no parent by default */>
        struct TBaseDataSourceInstanceBuilder: public TProtoBuilder<TParent, NYql::TGenericDataSourceInstance> {
            using TBuilder = TDerived;

            explicit TBaseDataSourceInstanceBuilder(NYql::TGenericDataSourceInstance* result = nullptr, TParent* parent = nullptr)
                : TProtoBuilder<TParent, NYql::TGenericDataSourceInstance>(result, parent)
            {
            }

            SETTER(Database, database);
            EXPR_SETTER(Login, mutable_credentials()->mutable_basic()->set_username);
            EXPR_SETTER(Password, mutable_credentials()->mutable_basic()->set_password);
            EXPR_SETTER(Host, mutable_endpoint()->set_host);
            EXPR_SETTER(Port, mutable_endpoint()->set_port);
            EXPR_SETTER(Schema, mutable_pg_options()->set_schema);
            SETTER(UseTls, use_tls);
            SETTER(Kind, kind);
            SETTER(Protocol, protocol);

        protected:
            void FillWithDefaults() {
                Database(DEFAULT_DATABASE);
                Login(DEFAULT_LOGIN);
                Password(DEFAULT_PASSWORD);
                UseTls(DEFAULT_USE_TLS);
            }
        };

        template <class TParent = void /* no parent by default */>
        struct TPostgreSQLDataSourceInstanceBuilder: public TBaseDataSourceInstanceBuilder<TPostgreSQLDataSourceInstanceBuilder<TParent>, TParent> {
            using TBase = TBaseDataSourceInstanceBuilder<TPostgreSQLDataSourceInstanceBuilder<TParent>, TParent>;

            explicit TPostgreSQLDataSourceInstanceBuilder(NYql::TGenericDataSourceInstance* result = nullptr, TParent* parent = nullptr)
                : TBase(result, parent)
            {
                FillWithDefaults();
            }

            void FillWithDefaults() {
                TBase::FillWithDefaults();
                this->Host(DEFAULT_PG_HOST);
                this->Port(DEFAULT_PG_PORT);
                this->Kind(NYql::EGenericDataSourceKind::POSTGRESQL);
                this->Protocol(DEFAULT_PG_PROTOCOL);
                this->Schema(DEFAULT_PG_SCHEMA);
            }
        };

        template <class TParent = void /* no parent by default */>
        struct TClickHouseDataSourceInstanceBuilder: public TBaseDataSourceInstanceBuilder<TClickHouseDataSourceInstanceBuilder<TParent>, TParent> {
            using TBase = TBaseDataSourceInstanceBuilder<TClickHouseDataSourceInstanceBuilder<TParent>, TParent>;

            explicit TClickHouseDataSourceInstanceBuilder(NYql::TGenericDataSourceInstance* result = nullptr, TParent* parent = nullptr)
                : TBase(result, parent)
            {
                FillWithDefaults();
            }

            void FillWithDefaults() {
                TBase::FillWithDefaults();
                this->Host(DEFAULT_CH_HOST);
                this->Port(DEFAULT_CH_PORT);
                this->Kind(NYql::EGenericDataSourceKind::CLICKHOUSE);
                this->Protocol(DEFAULT_CH_PROTOCOL);
            }
        };

        template <class TParent = void /* no parent by default */>
        struct TYdbDataSourceInstanceBuilder: public TBaseDataSourceInstanceBuilder<TYdbDataSourceInstanceBuilder<TParent>, TParent> {
            using TBase = TBaseDataSourceInstanceBuilder<TYdbDataSourceInstanceBuilder<TParent>, TParent>;

            explicit TYdbDataSourceInstanceBuilder(NYql::TGenericDataSourceInstance* result = nullptr, TParent* parent = nullptr)
                : TBase(result, parent)
            {
                FillWithDefaults();
            }

            void FillWithDefaults() {
                TBase::FillWithDefaults();
                this->Host(DEFAULT_YDB_HOST);
                this->Port(DEFAULT_YDB_PORT);
                this->Kind(NYql::EGenericDataSourceKind::YDB);
                this->Protocol(DEFAULT_YDB_PROTOCOL);
            }
        };

        template <class TParent = void /* no parent by default */>
        struct TDescribeTableResultBuilder: public TResponseBuilder<TParent, NApi::TDescribeTableResponse> {
            using TBuilder = TDescribeTableResultBuilder<TParent>;

            explicit TDescribeTableResultBuilder(
                std::shared_ptr<NApi::TDescribeTableResponse> result,
                TParent* parent = nullptr)
                : TResponseBuilder<TParent, NApi::TDescribeTableResponse>(std::move(result), parent)
            {
                FillWithDefaults();
            }

            TBuilder& Status(const Ydb::StatusIds_StatusCode value) {
                this->Result_->mutable_error()->set_status(value);
                return static_cast<TBuilder&>(*this);
            }

            // TODO: add nonprimitive types
            TBuilder& Column(const TString& name, Ydb::Type::PrimitiveTypeId typeId) {
                auto* col = this->Result_->mutable_schema()->add_columns();
                col->set_name(name);
                col->mutable_type()->set_type_id(typeId);
                return *this;
            }

            TBuilder& NullableColumn(const TString& name, Ydb::Type::PrimitiveTypeId typeId) {
                auto* col = this->Result_->mutable_schema()->add_columns();
                col->set_name(name);
                col->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(typeId);
                return *this;
            }

            void FillWithDefaults() {
                Status(Ydb::StatusIds::SUCCESS);
            }
        };

        struct TDescribeTableExpectationBuilder: public TProtoBuilder<int, NApi::TDescribeTableRequest> {
            using TBuilder = TDescribeTableExpectationBuilder;

            explicit TDescribeTableExpectationBuilder(NApi::TDescribeTableRequest* result = nullptr, TConnectorClientMock* mock = nullptr)
                : TProtoBuilder<int, NApi::TDescribeTableRequest>(result)
                , Mock_(mock)
            {
                FillWithDefaults();
            }

            explicit TDescribeTableExpectationBuilder(TConnectorClientMock* mock)
                : TDescribeTableExpectationBuilder(nullptr, mock)
            {
            }

            ~TDescribeTableExpectationBuilder() {
                SetExpectation();
            }

            SETTER(Table, table);
            DATA_SOURCE_INSTANCE_SUBBUILDER();

            TDescribeTableResultBuilder<TBuilder> Response() {
                return TDescribeTableResultBuilder<TBuilder>(ResponseResults_.emplace_back(std::make_shared<NApi::TDescribeTableResponse>()), this);
            }

            void FillWithDefaults() {
                Table(DEFAULT_TABLE);
            }

            TBuilder& TypeMappingSettings(const NApi::TTypeMappingSettings& proto) {
                *Result_->mutable_type_mapping_settings() = proto;
                return *this;
            }

        private:
            void SetExpectation() {
                if (ResponseResults_.empty()) {
                    Response();
                }

                auto& expectBuilder = EXPECT_CALL(*Mock_, DescribeTableImpl(ProtobufRequestMatcher(*Result_)));
                for (auto result : ResponseResults_) {
                    expectBuilder.WillOnce(Return(TResult<NApi::TDescribeTableResponse>({NYdbGrpc::TGrpcStatus(), *result})));
                }
            }

        private:
            TConnectorClientMock* Mock_ = nullptr;
            std::vector<std::shared_ptr<NApi::TDescribeTableResponse>> ResponseResults_;
        };

        template <class TParent = void /* no parent by default */>
        struct TExpressionBuilder: public TProtoBuilder<TParent, NApi::TExpression> {
            using TBuilder = TExpressionBuilder<TParent>;

            TExpressionBuilder(NApi::TExpression* result = nullptr, TParent* parent = nullptr)
                : TProtoBuilder<TParent, NApi::TExpression>(result, parent)
            {
                FillWithDefaults();
            }

            SETTER(Column, column);

            template <class T>
            TBuilder& Value(const T& value) {
                SetSimpleValue(value, this->Result_->mutable_typed_value());
                return *this;
            }

            template <class T>
            TBuilder& Value(const T& value, const ::Ydb::Type::PrimitiveTypeId& typeId) {
                SetValue(value, this->Result_->mutable_typed_value(), typeId);
                return *this;
            }

            template <class T>
            TBuilder& OptionalValue(const T& value) {
                SetSimpleValue(value, this->Result_->mutable_typed_value(), true);
                return *this;
            }

            template <class T>
            TBuilder& OptionalValue(const T& value, const ::Ydb::Type::PrimitiveTypeId& typeId) {
                SetValue(value, this->Result_->mutable_typed_value(), typeId, true);
                return *this;
            }

            void FillWithDefaults() {
            }
        };
        template <class TParent = void /* no parent by default */>
        struct TPredicateBuilder;

        template <class TParent = void /* no parent by default */>
        struct TConjunctionBuilder: public TProtoBuilder<TParent, NApi::TPredicate::TConjunction> {
            using TBuilder = TConjunctionBuilder<TParent>;
            TConjunctionBuilder(NApi::TPredicate::TConjunction* result = nullptr, TParent* parent = nullptr)
                : TProtoBuilder<TParent, NApi::TPredicate::TConjunction>(result, parent) {
            }
            SUBPROTO_BUILDER(Operand, add_operands, NApi::TPredicate, TPredicateBuilder<TBuilder>);
        };

        template <class TParent = void /* no parent by default */>
        struct TDisjunctionBuilder: public TProtoBuilder<TParent, NApi::TPredicate::TDisjunction> {
            using TBuilder = TDisjunctionBuilder<TParent>;
            TDisjunctionBuilder(NApi::TPredicate::TDisjunction* result = nullptr, TParent* parent = nullptr)
                : TProtoBuilder<TParent, NApi::TPredicate::TDisjunction>(result, parent) {
            }
            SUBPROTO_BUILDER(Operand, add_operands, NApi::TPredicate, TPredicateBuilder<TBuilder>);
        };

        template <class TParent = void /* no parent by default */>
        struct TComparisonBaseBuilder: public TProtoBuilder<TParent, NApi::TPredicate::TComparison> {
            using TBuilder = TComparisonBaseBuilder<TParent>;

            TComparisonBaseBuilder(NApi::TPredicate::TComparison* result = nullptr, TParent* parent = nullptr)
                : TProtoBuilder<TParent, NApi::TPredicate::TComparison>(result, parent)
            {
                FillWithDefaults();
            }

            TBuilder& Arg(const NApi::TExpression& expr) {
                MutableArg()->CopyFrom(expr);
                return *this;
            }
            TExpressionBuilder<TBuilder> Arg() {
                return TExpressionBuilder<TBuilder>(MutableArg(), this);
            }

            TBuilder& Column(const TString& name) {
                return Arg().Column(name).Done();
            }

            TBuilder& Value(const auto& value) {
                return Arg().Value(value).Done();
            }

            TBuilder& Value(const auto& value, const ::Ydb::Type::PrimitiveTypeId& typeId) {
                return Arg().Value(value, typeId).Done();
            }

            TBuilder& OptionalValue(const auto& value) {
                return Arg().OptionalValue(value).Done();
            }

            TBuilder& OptionalValue(const auto& value, const ::Ydb::Type::PrimitiveTypeId& typeId) {
                return Arg().OptionalValue(value, typeId).Done();
            }

            void FillWithDefaults() {
            }

        protected:
            SETTER(Operation, operation);

        private:
            NApi::TExpression* MutableArg() {
                if (!this->Result_->has_left_value()) {
                    return this->Result_->mutable_left_value();
                }
                UNIT_ASSERT(!this->Result_->has_right_value());
                return this->Result_->mutable_right_value();
            }
        };

        template <NApi::TPredicate::TComparison::EOperation operation, class TParent = void /* no parent by default */>
        struct TComparisonBuilder: public TComparisonBaseBuilder<TParent> {
            using TBuilder = TComparisonBuilder<operation, TParent>;

            TComparisonBuilder(NApi::TPredicate::TComparison* result = nullptr, TParent* parent = nullptr)
                : TComparisonBaseBuilder<TParent>(result, parent)
            {
                FillWithDefaults();
            }

            void FillWithDefaults() {
                this->Operation(operation);
            }
        };

        template <class TParent>
        using TEqualBuilder = TComparisonBuilder<NApi::TPredicate::TComparison::EQ, TParent>;

        template <class TParent>
        using TNotEqualBuilder = TComparisonBuilder<NApi::TPredicate::TComparison::NE, TParent>;

        template <class TParent>
        using TLessBuilder = TComparisonBuilder<NApi::TPredicate::TComparison::L, TParent>;

        template <class TParent>
        using TLessOrEqualBuilder = TComparisonBuilder<NApi::TPredicate::TComparison::LE, TParent>;

        template <class TParent>
        using TGreaterBuilder = TComparisonBuilder<NApi::TPredicate::TComparison::G, TParent>;

        template <class TParent>
        using TGreaterOrEqualBuilder = TComparisonBuilder<NApi::TPredicate::TComparison::GE, TParent>;

        template <class TParent>
        struct TPredicateBuilder: public TProtoBuilder<TParent, NApi::TPredicate> {
            using TBuilder = TPredicateBuilder<TParent>;

            explicit TPredicateBuilder(NApi::TPredicate* result = nullptr, TParent* parent = nullptr)
                : TProtoBuilder<TParent, NApi::TPredicate>(result, parent)
            {
                FillWithDefaults();
            }

            SUBPROTO_BUILDER(Conjunction, mutable_conjunction, NApi::TPredicate::TConjunction, TConjunctionBuilder<TBuilder>);
            SUBPROTO_BUILDER(Disjunction, mutable_disjunction, NApi::TPredicate::TDisjunction, TDisjunctionBuilder<TBuilder>);

            SUBPROTO_BUILDER(Equal, mutable_comparison, NApi::TPredicate::TComparison, TEqualBuilder<TBuilder>);
            SUBPROTO_BUILDER(NotEqual, mutable_comparison, NApi::TPredicate::TComparison, TNotEqualBuilder<TBuilder>);
            SUBPROTO_BUILDER(Less, mutable_comparison, NApi::TPredicate::TComparison, TLessBuilder<TBuilder>);
            SUBPROTO_BUILDER(LessOrEqual, mutable_comparison, NApi::TPredicate::TComparison, TLessOrEqualBuilder<TBuilder>);
            SUBPROTO_BUILDER(Greater, mutable_comparison, NApi::TPredicate::TComparison, TGreaterBuilder<TBuilder>);
            SUBPROTO_BUILDER(GreaterOrEqual, mutable_comparison, NApi::TPredicate::TComparison, TGreaterOrEqualBuilder<TBuilder>);

            void FillWithDefaults() {
            }
        };

        template <class TParent = void /* no parent by default */>
        struct TWhereBuilder: public TProtoBuilder<TParent, NApi::TSelect::TWhere> {
            using TBuilder = TWhereBuilder<TParent>;

            explicit TWhereBuilder(NApi::TSelect::TWhere* result = nullptr, TParent* parent = nullptr)
                : TProtoBuilder<TParent, NApi::TSelect::TWhere>(result, parent)
            {
                FillWithDefaults();
            }

            SUBPROTO_BUILDER(Filter, mutable_filter_typed, NApi::TPredicate, TPredicateBuilder<TBuilder>);

            void FillWithDefaults() {
            }
        };

        template <class TParent = void /* no parent by default */>
        struct TWhatBuilder: public TProtoBuilder<TParent, NApi::TSelect::TWhat> {
            using TBuilder = TWhatBuilder<TParent>;

            explicit TWhatBuilder(NApi::TSelect::TWhat* result = nullptr, TParent* parent = nullptr)
                : TProtoBuilder<TParent, NApi::TSelect::TWhat>(result, parent)
            {
                FillWithDefaults();
            }

            // TODO: add nonprimitive types
            TBuilder& Column(const TString& name, Ydb::Type::PrimitiveTypeId typeId) {
                auto* col = this->Result_->add_items()->mutable_column();
                col->set_name(name);
                col->mutable_type()->set_type_id(typeId);
                return *this;
            }

            TBuilder& NullableColumn(const TString& name, Ydb::Type::PrimitiveTypeId typeId) {
                auto* col = this->Result_->add_items()->mutable_column();
                col->set_name(name);
                col->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(typeId);
                return *this;
            }

            void FillWithDefaults() {
            }
        };

        template <class TParent = void /* no parent by default */>
        struct TSelectBuilder: public TProtoBuilder<TParent, NApi::TSelect> {
            using TBuilder = TSelectBuilder<TParent>;

            explicit TSelectBuilder(NApi::TSelect* result = nullptr, TParent* parent = nullptr)
                : TProtoBuilder<TParent, NApi::TSelect>(result, parent)
            {
                FillWithDefaults();
            }

            EXPR_SETTER(Table, mutable_from()->set_table);
            DATA_SOURCE_INSTANCE_SUBBUILDER();
            SUBPROTO_BUILDER(What, mutable_what, NApi::TSelect::TWhat, TWhatBuilder<TBuilder>);
            SUBPROTO_BUILDER(Where, mutable_where, NApi::TSelect::TWhere, TWhereBuilder<TBuilder>);

            void FillWithDefaults() {
                Table(DEFAULT_TABLE);
            }
        };

        template <class TParent = void /* no parent by default */>
        struct TSplitBuilder: public TProtoBuilder<TParent, NApi::TSplit> {
            using TBuilder = TSplitBuilder<TParent>;

            explicit TSplitBuilder(NApi::TSplit* result = nullptr, TParent* parent = nullptr)
                : TProtoBuilder<TParent, NApi::TSplit>(result, parent)
            {
                FillWithDefaults();
            }

            SETTER(Description, description);
            SUBPROTO_BUILDER(Select, mutable_select, NApi::TSelect, TSelectBuilder<TBuilder>);

            void FillWithDefaults() {
                Select();
            }
        };

        template <class TParent = void /* no parent by default */>
        struct TListSplitsResultBuilder: public TResponseBuilder<TParent, TListSplitsStreamIteratorMock> {
            using TBuilder = TListSplitsResultBuilder<TParent>;

            explicit TListSplitsResultBuilder(
                TListSplitsStreamIteratorMock::TPtr result = std::make_shared<TListSplitsStreamIteratorMock>(),
                TParent* parent = nullptr)
                : TResponseBuilder<TParent, TListSplitsStreamIteratorMock>(std::move(result), parent)
            {
                FillWithDefaults();
            }

            TSplitBuilder<TBuilder> AddResponse(const NApi::TError& error) {
                auto& response = this->Result_->Responses().emplace_back();
                response.mutable_error()->CopyFrom(error);
                auto split = response.mutable_splits()->Add();
                return TSplitBuilder<TBuilder>(split, this);
            }

            void FillWithDefaults() {
            }
        };

        struct TListSplitsExpectationBuilder: public TProtoBuilder<int, NApi::TListSplitsRequest> {
            using TBuilder = TListSplitsExpectationBuilder;

            explicit TListSplitsExpectationBuilder(NApi::TListSplitsRequest* result = nullptr, TConnectorClientMock* mock = nullptr)
                : TProtoBuilder<int, NApi::TListSplitsRequest>(result)
                , Mock_(mock)
            {
            }

            explicit TListSplitsExpectationBuilder(TConnectorClientMock* mock)
                : TListSplitsExpectationBuilder(nullptr, mock)
            {
            }

            ~TListSplitsExpectationBuilder() {
                SetExpectation();
            }

            SUBPROTO_BUILDER(Select, add_selects, NApi::TSelect, TSelectBuilder<TBuilder>);
            SETTER(MaxSplitCount, max_split_count);

            TListSplitsResultBuilder<TBuilder> Result() {
                return TListSplitsResultBuilder<TBuilder>(ResponseResults_.emplace_back(std::make_shared<TListSplitsStreamIteratorMock>()), this);
            }

            auto& Status(const NYdbGrpc::TGrpcStatus& status) {
                ResponseStatus_ = status;
                return *this;
            }

            auto& ValidateArgs(EArgsValidation validateCase) {
                ValidateArgs_ = validateCase;
                return *this;
            }

        private:
            void SetExpectation() {
                if (ResponseResults_.empty()) {
                    Result();
                }

                const auto setupResponse = [&](auto& expectBuilder) {
                    for (auto response : ResponseResults_) {
                        expectBuilder.WillOnce(Return(TIteratorResult<IListSplitsStreamIterator>{ResponseStatus_, response}));
                    }
                };

                switch (ValidateArgs_) {
                    case EArgsValidation::Strict:
                        setupResponse(EXPECT_CALL(*Mock_, ListSplitsImpl(ProtobufRequestMatcher(*Result_))));
                        break;
                    case EArgsValidation::DataSourceInstance:
                        if (!Result_->selects().empty()) {
                            setupResponse(EXPECT_CALL(*Mock_, ListSplitsImpl(RequestRelaxedMatcher([expected = Result_->selects(0).data_source_instance()](const NConnector::NApi::TListSplitsRequest& actual) {
                                for (const auto& select : actual.selects()) {
                                    if (!MatchProtos(expected, select.data_source_instance())) {
                                        return false;
                                    }
                                }
                                return true;
                            }))));
                            break;
                        }
                    case EArgsValidation::Off:
                        setupResponse(EXPECT_CALL(*Mock_, ListSplitsImpl(RequestRelaxedMatcher([](const auto&) { return true; }))));
                        break;
                }
            }

        private:
            TConnectorClientMock* Mock_ = nullptr;
            std::vector<TListSplitsStreamIteratorMock::TPtr> ResponseResults_;
            NYdbGrpc::TGrpcStatus ResponseStatus_ {};
            EArgsValidation ValidateArgs_ = EArgsValidation::Strict;
        };

        template <class TParent = void /* no parent by default */>
        struct TReadSplitsResultBuilder: public TResponseBuilder<TParent, TReadSplitsStreamIteratorMock> {
            using TBuilder = TReadSplitsResultBuilder<TParent>;

            explicit TReadSplitsResultBuilder(
                TReadSplitsStreamIteratorMock::TPtr result = std::make_shared<TReadSplitsStreamIteratorMock>(),
                TParent* parent = nullptr)
                : TResponseBuilder<TParent, TReadSplitsStreamIteratorMock>(std::move(result), parent)
            {
                FillWithDefaults();
            }

            TBuilder& AddResponse(
                const std::shared_ptr<arrow::RecordBatch>& recordBatch,
                const NApi::TError& error) {
                NKikimr::NArrow::NSerialization::TSerializerContainer ser = NKikimr::NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
                auto& response = this->Result_->Responses().emplace_back();
                response.mutable_error()->CopyFrom(error);
                response.set_arrow_ipc_streaming(ser->SerializeFull(recordBatch));
                return static_cast<TBuilder&>(*this);
            }

            void FillWithDefaults() {
            }
        };

        struct TReadSplitsExpectationBuilder: public TProtoBuilder<int, NApi::TReadSplitsRequest> {
            using TBuilder = TReadSplitsExpectationBuilder;

            explicit TReadSplitsExpectationBuilder(NApi::TReadSplitsRequest* result = nullptr, TConnectorClientMock* mock = nullptr)
                : TProtoBuilder<int, NApi::TReadSplitsRequest>(result)
                , Mock_(mock)

            {
                FillWithDefaults();
            }

            explicit TReadSplitsExpectationBuilder(TConnectorClientMock* mock)
                : TReadSplitsExpectationBuilder(nullptr, mock)
            {
            }

            ~TReadSplitsExpectationBuilder() {
                SetExpectation();
            }

            DATA_SOURCE_INSTANCE_SUBBUILDER();
            SUBPROTO_BUILDER(Split, add_splits, NApi::TSplit, TSplitBuilder<TBuilder>);
            SETTER(Format, format);
            SETTER(Filtering, filtering);

            TReadSplitsResultBuilder<TBuilder> Result() {
                return TReadSplitsResultBuilder<TBuilder>(ResponseResults_.emplace_back(std::make_shared<TReadSplitsStreamIteratorMock>()), this);
            }

            auto& Status(const NYdbGrpc::TGrpcStatus& status) {
                ResponseStatus_ = status;
                return *this;
            }

            auto& ValidateArgs(EArgsValidation validateCase) {
                ValidateArgs_ = validateCase;
                return *this;
            }

            void FillWithDefaults() {
                Format(NApi::TReadSplitsRequest::ARROW_IPC_STREAMING);
            }

        private:
            void SetExpectation() {
                if (ResponseResults_.empty()) {
                    Result();
                }

                const auto setupResponse = [&](auto& expectBuilder) {
                    for (auto response : ResponseResults_) {
                        expectBuilder.WillOnce(Return(TIteratorResult<IReadSplitsStreamIterator>{ResponseStatus_, response}));
                    }
                };

                switch (ValidateArgs_) {
                    case EArgsValidation::Strict:
                        setupResponse(EXPECT_CALL(*Mock_, ReadSplitsImpl(ProtobufRequestMatcher(*Result_))));
                        break;
                    case EArgsValidation::DataSourceInstance:
                        if (!Result_->splits().empty()) {
                            setupResponse(EXPECT_CALL(*Mock_, ReadSplitsImpl(RequestRelaxedMatcher([expected = Result_->splits(0).select().data_source_instance()](const NConnector::NApi::TReadSplitsRequest& actual) {
                                for (const auto& split : actual.splits()) {
                                    if (!MatchProtos(expected, split.select().data_source_instance())) {
                                        return false;
                                    }
                                }
                                return true;
                            }))));
                            break;
                        }
                    case EArgsValidation::Off:
                        setupResponse(EXPECT_CALL(*Mock_, ReadSplitsImpl(RequestRelaxedMatcher([](const auto&) { return true; }))));
                        break;
                }
            }

        private:
            TConnectorClientMock* Mock_ = nullptr;
            std::vector<TReadSplitsStreamIteratorMock::TPtr> ResponseResults_;
            NYdbGrpc::TGrpcStatus ResponseStatus_ {};
            EArgsValidation ValidateArgs_ = EArgsValidation::Strict;
        };

        TDescribeTableExpectationBuilder ExpectDescribeTable() {
            return TDescribeTableExpectationBuilder(this);
        }

        TListSplitsExpectationBuilder ExpectListSplits() {
            return TListSplitsExpectationBuilder(this);
        }

        TReadSplitsExpectationBuilder ExpectReadSplits() {
            return TReadSplitsExpectationBuilder(this);
        }

        TDescribeTableAsyncResult DescribeTable(const NApi::TDescribeTableRequest& request, TDuration = {}) override {
            Cerr << "Call DescribeTable.\n"
                 << request.Utf8DebugString() << Endl;
            auto result = DescribeTableImpl(request);
            Cerr << "DescribeTable result.\n"
                 << StatusToDebugString(result.Status);
            if (result.Response) {
                Cerr << '\n'
                     << result.Response->Utf8DebugString();
            }
            Cerr << Endl;
            return NThreading::MakeFuture(std::move(result));
        }

        TListSplitsStreamIteratorAsyncResult ListSplits(const NApi::TListSplitsRequest& request, TDuration = {}) override {
            Cerr << "Call ListSplits.\n"
                 << request.Utf8DebugString() << Endl;
            auto result = ListSplitsImpl(request);
            Cerr << "ListSplits result.\n"
                 << StatusToDebugString(result.Status) << Endl;
            return NThreading::MakeFuture(std::move(result));
        }

        TReadSplitsStreamIteratorAsyncResult ReadSplits(const NApi::TReadSplitsRequest& request, TDuration = {}) override {
            Cerr << "Call ReadSplits.\n" << request.Utf8DebugString() << Endl;
            auto resultPromise = NThreading::NewPromise<TIteratorResult<IReadSplitsStreamIterator>>();

            with_lock (Mutex_) {
                if (ReadingLocked_) {
                    Cerr << "Delay ReadSplits." << Endl;
                    PendingReadSplits.push_back({request, resultPromise});
                    return resultPromise.GetFuture();
                }
            }

            ProcessReadSplits(request, resultPromise);
            return resultPromise.GetFuture();
        }

        void LockReading() {
            with_lock (Mutex_) {
                ReadingLocked_ = true;
            }
        }

        void UnlockReading() {
            with_lock (Mutex_) {
                ReadingLocked_ = false;
                for (auto& pending : PendingReadSplits) {
                    Cerr << "Process pending ReadSplits." << Endl;
                    ProcessReadSplits(pending.Request, pending.ResultPromise);
                }
                PendingReadSplits.clear();
            }
        }

    private:
        void ProcessReadSplits(const NApi::TReadSplitsRequest& request, NThreading::TPromise<TIteratorResult<IReadSplitsStreamIterator>>& resultPromise) {
            auto result = ReadSplitsImpl(request);
            Cerr << "ReadSplits result.\n" << StatusToDebugString(result.Status) << Endl;
            resultPromise.SetValue(std::move(result));
        }

    protected:
        static TString StatusToDebugString(const NYdbGrpc::TGrpcStatus& status) {
            TStringBuilder s;
            s << "GRpcStatusCode: " << status.GRpcStatusCode << '\n';
            if (!status.Msg.empty()) {
                s << status.Msg;
            }
            if (!status.Details.empty()) {
                s << " (" << status.Details << ')';
            }
            if (status.InternalError) {
                s << " (internal error)";
            }
            return std::move(s);
        }

        template <typename TRequest, typename TResult>
        struct TPendingResult {
            TRequest Request;
            NThreading::TPromise<TResult> ResultPromise;
        };

        std::vector<TPendingResult<NApi::TReadSplitsRequest, TIteratorResult<IReadSplitsStreamIterator>>> PendingReadSplits;
        bool ReadingLocked_ = false;
        TMutex Mutex_;
    };
} // namespace NYql::NConnector::NTest
