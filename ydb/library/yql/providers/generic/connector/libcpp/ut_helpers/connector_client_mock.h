#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/library/yql/providers/generic/connector/api/common/data_source.pb.h>
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
    TBuilder& DataSourceInstance(const NApi::TDataSourceInstance& proto) {          \
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

    MATCHER_P(ProtobufRequestMatcher, expected, "request does not match") {
        return google::protobuf::util::MessageDifferencer::Equals(arg, expected);
    }

#define MATCH_RESULT_WITH_INPUT(INPUT, RESULT_SET, GETTER)                      \
    {                                                                           \
        for (const auto& val : INPUT) {                                         \
            UNIT_ASSERT(RESULT_SET.TryNextRow());                               \
            UNIT_ASSERT_VALUES_EQUAL(RESULT_SET.ColumnParser(0).GETTER(), val); \
        }                                                                       \
    }

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
        NApi::EProtocol protocol = DEFAULT_PG_PROTOCOL,
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
        NApi::EProtocol protocol = DEFAULT_CH_PROTOCOL,
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

        template <class TDerived, class TParent = void /* no parent by default */>
        struct TBaseDataSourceInstanceBuilder: public TProtoBuilder<TParent, NApi::TDataSourceInstance> {
            using TBuilder = TDerived;

            explicit TBaseDataSourceInstanceBuilder(NApi::TDataSourceInstance* result = nullptr, TParent* parent = nullptr)
                : TProtoBuilder<TParent, NApi::TDataSourceInstance>(result, parent)
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

            explicit TPostgreSQLDataSourceInstanceBuilder(NApi::TDataSourceInstance* result = nullptr, TParent* parent = nullptr)
                : TBase(result, parent)
            {
                FillWithDefaults();
            }

            void FillWithDefaults() {
                TBase::FillWithDefaults();
                this->Host(DEFAULT_PG_HOST);
                this->Port(DEFAULT_PG_PORT);
                this->Kind(NApi::EDataSourceKind::POSTGRESQL);
                this->Protocol(DEFAULT_PG_PROTOCOL);
                this->Schema(DEFAULT_PG_SCHEMA);
            }
        };

        template <class TParent = void /* no parent by default */>
        struct TClickHouseDataSourceInstanceBuilder: public TBaseDataSourceInstanceBuilder<TClickHouseDataSourceInstanceBuilder<TParent>, TParent> {
            using TBase = TBaseDataSourceInstanceBuilder<TClickHouseDataSourceInstanceBuilder<TParent>, TParent>;

            explicit TClickHouseDataSourceInstanceBuilder(NApi::TDataSourceInstance* result = nullptr, TParent* parent = nullptr)
                : TBase(result, parent)
            {
                FillWithDefaults();
            }

            void FillWithDefaults() {
                TBase::FillWithDefaults();
                this->Host(DEFAULT_CH_HOST);
                this->Port(DEFAULT_CH_PORT);
                this->Kind(NApi::EDataSourceKind::CLICKHOUSE);
                this->Protocol(DEFAULT_CH_PROTOCOL);
            }
        };

        template <class TParent = void /* no parent by default */>
        struct TYdbDataSourceInstanceBuilder: public TBaseDataSourceInstanceBuilder<TYdbDataSourceInstanceBuilder<TParent>, TParent> {
            using TBase = TBaseDataSourceInstanceBuilder<TYdbDataSourceInstanceBuilder<TParent>, TParent>;

            explicit TYdbDataSourceInstanceBuilder(NApi::TDataSourceInstance* result = nullptr, TParent* parent = nullptr)
                : TBase(result, parent)
            {
                FillWithDefaults();
            }

            void FillWithDefaults() {
                TBase::FillWithDefaults();
                this->Host(DEFAULT_YDB_HOST);
                this->Port(DEFAULT_YDB_PORT);
                this->Kind(NApi::EDataSourceKind::YDB);
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
                return TDescribeTableResultBuilder<TBuilder>(ResponseResult_, this);
            }

            void FillWithDefaults() {
                Table(DEFAULT_TABLE);
                Response();
            }

            TBuilder& TypeMappingSettings(const NApi::TTypeMappingSettings& proto) {
                *Result_->mutable_type_mapping_settings() = proto;
                return *this;
            }

        private:
            void SetExpectation() {
                EXPECT_CALL(*Mock_, DescribeTableImpl(ProtobufRequestMatcher(*Result_)))
                    .WillOnce(Return(
                        TResult<NApi::TDescribeTableResponse>(
                            {NYdbGrpc::TGrpcStatus(),
                             *ResponseResult_})));
            }

        private:
            TConnectorClientMock* Mock_ = nullptr;
            std::shared_ptr<NApi::TDescribeTableResponse> ResponseResult_ = std::make_shared<NApi::TDescribeTableResponse>();
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
            TBuilder& OptionalValue(const T& value) {
                SetSimpleValue(value, this->Result_->mutable_typed_value(), true);
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

            TBuilder& OptionalValue(const auto& value) {
                return Arg().OptionalValue(value).Done();
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
                FillWithDefaults();
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
                return TListSplitsResultBuilder<TBuilder>(ResponseResult_, this);
            }

            void FillWithDefaults() {
                Result();
            }

        private:
            void SetExpectation() {
                EXPECT_CALL(*Mock_, ListSplitsImpl(ProtobufRequestMatcher(*Result_)))
                    .WillOnce(Return(TIteratorResult<IListSplitsStreamIterator>{NYdbGrpc::TGrpcStatus(), ResponseResult_}));
            }

        private:
            TConnectorClientMock* Mock_ = nullptr;
            TListSplitsStreamIteratorMock::TPtr ResponseResult_ = std::make_shared<TListSplitsStreamIteratorMock>();
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

            TReadSplitsResultBuilder<TBuilder> Result() {
                return TReadSplitsResultBuilder<TBuilder>(ResponseResult_, this);
            }

            void FillWithDefaults() {
                Format(NApi::TReadSplitsRequest::ARROW_IPC_STREAMING);
            }

        private:
            void SetExpectation() {
                EXPECT_CALL(*Mock_, ReadSplitsImpl(ProtobufRequestMatcher(*Result_)))
                    .WillOnce(Return(TIteratorResult<IReadSplitsStreamIterator>{NYdbGrpc::TGrpcStatus(), ResponseResult_}));
            }

        private:
            TConnectorClientMock* Mock_ = nullptr;
            TReadSplitsStreamIteratorMock::TPtr ResponseResult_ = std::make_shared<TReadSplitsStreamIteratorMock>();
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

        TDescribeTableAsyncResult DescribeTable(const NApi::TDescribeTableRequest& request) override {
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

        TListSplitsStreamIteratorAsyncResult ListSplits(const NApi::TListSplitsRequest& request) override {
            Cerr << "Call ListSplits.\n"
                 << request.Utf8DebugString() << Endl;
            auto result = ListSplitsImpl(request);
            Cerr << "ListSplits result.\n"
                 << StatusToDebugString(result.Status) << Endl;
            return NThreading::MakeFuture(std::move(result));
        }

        TReadSplitsStreamIteratorAsyncResult ReadSplits(const NApi::TReadSplitsRequest& request) override {
            Cerr << "Call ReadSplits.\n"
                 << request.Utf8DebugString() << Endl;
            auto result = ReadSplitsImpl(request);
            Cerr << "ReadSplits result.\n"
                 << StatusToDebugString(result.Status) << Endl;
            return NThreading::MakeFuture(std::move(result));
        }

    protected:
        static TString StatusToDebugString(const NYdbGrpc::TGrpcStatus& status) {
            TStringBuilder s;
            s << "GRpcStatusCode: " << status.GRpcStatusCode << '\n';
            if (status.Msg) {
                s << status.Msg;
            }
            if (status.Details) {
                s << " (" << status.Details << ')';
            }
            if (status.InternalError) {
                s << " (internal error)";
            }
            return std::move(s);
        }
    };
} // namespace NYql::NConnector::NTest
