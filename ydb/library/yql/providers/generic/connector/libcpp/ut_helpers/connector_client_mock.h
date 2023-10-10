#pragma once
#include <ydb/library/yql/providers/generic/connector/libcpp/ut_helpers/defaults.h>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/yql/providers/generic/connector/api/common/data_source.pb.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>

#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

#include <arrow/api.h>

#include <google/protobuf/util/message_differencer.h>

#include <memory>

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

    template <class TArrowBuilderType, class TColDataType>
    std::shared_ptr<arrow::RecordBatch> MakeRecordBatch(
        const TString& columnName,
        const std::vector<TColDataType>& input,
        std::shared_ptr<arrow::DataType> dataType) {
        TArrowBuilderType builder;
        UNIT_ASSERT_EQUAL(builder.AppendValues(input), arrow::Status::OK());
        std::shared_ptr<arrow::Array> columnData;
        UNIT_ASSERT_EQUAL(builder.Finish(&columnData), arrow::Status::OK());
        auto field = arrow::field(columnName, std::move(dataType));
        auto schema = arrow::schema({field});
        return arrow::RecordBatch::Make(schema, columnData->length(), {columnData});
    }

    // Make record batch with schema with no columns
    std::shared_ptr<arrow::RecordBatch> MakeEmptyRecordBatch(size_t rowsCount);

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

    template <class TParent, class TResultPtrType>
    struct TResponseBuilder: public TWithParentBuilder<TParent> {
        explicit TResponseBuilder(TResultPtrType result = std::make_shared<typename TResultPtrType::element_type>(), TParent* parent = nullptr)
            : TWithParentBuilder<TParent>(parent)
            , Result_(std::move(result))
        {
        }

        TResultPtrType GetResult() {
            return Result_;
        }

    protected:
        TResultPtrType Result_;
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
        bool useTls = DEFAULT_USE_TLS);

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
        NYql::NConnector::EExternalDataSource sourceType = DEFAULT_CH_SOURCE_TYPE);

    class TConnectorClientMock: public NYql::NConnector::IClient {
    public:
        MOCK_METHOD(TDescribeTableResult::TPtr, DescribeTable, (const NApi::TDescribeTableRequest& request), (override));
        MOCK_METHOD(TListSplitsResult::TPtr, ListSplits, (const NApi::TListSplitsRequest& request), (override));
        MOCK_METHOD(TReadSplitsResult::TPtr, ReadSplits, (const NApi::TReadSplitsRequest& request), (override));

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
        struct TDescribeTableResultBuilder: public TResponseBuilder<TParent, TDescribeTableResult::TPtr> {
            using TBuilder = TDescribeTableResultBuilder<TParent>;

            explicit TDescribeTableResultBuilder(TDescribeTableResult::TPtr result = std::make_shared<TDescribeTableResult>(), TParent* parent = nullptr)
                : TResponseBuilder<TParent, TDescribeTableResult::TPtr>(std::move(result), parent)
            {
                FillWithDefaults();
            }

            EXPR_SETTER(Status, Error.set_status);

            // TODO: add nonprimitive types
            TBuilder& Column(const TString& name, Ydb::Type::PrimitiveTypeId typeId) {
                auto* col = this->Result_->Schema.add_columns();
                col->set_name(name);
                col->mutable_type()->set_type_id(typeId);
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

        private:
            void SetExpectation() {
                EXPECT_CALL(*Mock_, DescribeTable(ProtobufRequestMatcher(*Result_)))
                    .WillOnce(Return(ResponseResult_));
            }

        private:
            TConnectorClientMock* Mock_ = nullptr;
            TDescribeTableResult::TPtr ResponseResult_ = std::make_shared<TDescribeTableResult>();
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
        struct TListSplitsResultBuilder: public TResponseBuilder<TParent, TListSplitsResult::TPtr> {
            using TBuilder = TListSplitsResultBuilder<TParent>;

            explicit TListSplitsResultBuilder(TListSplitsResult::TPtr result = std::make_shared<TListSplitsResult>(), TParent* parent = nullptr)
                : TResponseBuilder<TParent, TListSplitsResult::TPtr>(std::move(result), parent)
            {
                FillWithDefaults();
            }

            EXPR_SETTER(Status, Error.set_status);

            TSplitBuilder<TBuilder> Split() {
                return TSplitBuilder<TBuilder>(&this->Result_->Splits.emplace_back(), this);
            }

            void FillWithDefaults() {
                Status(Ydb::StatusIds::SUCCESS);
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

            TListSplitsResultBuilder<TBuilder> Response() {
                return TListSplitsResultBuilder<TBuilder>(ResponseResult_, this);
            }

            void FillWithDefaults() {
                Response();
            }

        private:
            void SetExpectation() {
                EXPECT_CALL(*Mock_, ListSplits(ProtobufRequestMatcher(*Result_)))
                    .WillOnce(Return(ResponseResult_));
            }

        private:
            TConnectorClientMock* Mock_ = nullptr;
            TListSplitsResult::TPtr ResponseResult_ = std::make_shared<TListSplitsResult>();
        };

        template <class TParent = void /* no parent by default */>
        struct TReadSplitsResultBuilder: public TResponseBuilder<TParent, TReadSplitsResult::TPtr> {
            using TBuilder = TReadSplitsResultBuilder<TParent>;

            explicit TReadSplitsResultBuilder(TReadSplitsResult::TPtr result = std::make_shared<TReadSplitsResult>(), TParent* parent = nullptr)
                : TResponseBuilder<TParent, TReadSplitsResult::TPtr>(std::move(result), parent)
            {
                FillWithDefaults();
            }

            EXPR_SETTER(Status, Error.set_status);
            EXPR_SETTER(RecordBatch, RecordBatches.push_back);

            void FillWithDefaults() {
                Status(Ydb::StatusIds::SUCCESS);
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

            TReadSplitsResultBuilder<TBuilder> Response() {
                return TReadSplitsResultBuilder<TBuilder>(ResponseResult_, this);
            }

            void FillWithDefaults() {
                Format(NApi::TReadSplitsRequest::ARROW_IPC_STREAMING);
            }

        private:
            void SetExpectation() {
                EXPECT_CALL(*Mock_, ReadSplits(ProtobufRequestMatcher(*Result_)))
                    .WillOnce(Return(ResponseResult_));
            }

        private:
            TConnectorClientMock* Mock_ = nullptr;
            TReadSplitsResult::TPtr ResponseResult_ = std::make_shared<TReadSplitsResult>();
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
    };
} // namespace NYql::NConnector::NTest
