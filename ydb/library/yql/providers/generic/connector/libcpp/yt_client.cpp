#include "yt_client.h"
#include "error.h"
#include "utils.h"

#include <arrow/api.h>
#include <util/string/builder.h>
#include <util/string/split.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <library/cpp/yson/node/node.h>

namespace NYql::NConnector {

    namespace {

        // Maps a YT column value type to the corresponding YDB primitive type id.
        Ydb::Type::PrimitiveTypeId YtTypeToYdbPrimitive(NYT::EValueType type) {
            switch (type) {
                case NYT::VT_INT8:
                    return Ydb::Type::INT8;
                case NYT::VT_INT16:
                    return Ydb::Type::INT16;
                case NYT::VT_INT32:
                    return Ydb::Type::INT32;
                case NYT::VT_INT64:
                    return Ydb::Type::INT64;
                case NYT::VT_UINT8:
                    return Ydb::Type::UINT8;
                case NYT::VT_UINT16:
                    return Ydb::Type::UINT16;
                case NYT::VT_UINT32:
                    return Ydb::Type::UINT32;
                case NYT::VT_UINT64:
                    return Ydb::Type::UINT64;
                case NYT::VT_FLOAT:
                    return Ydb::Type::FLOAT;
                case NYT::VT_DOUBLE:
                    return Ydb::Type::DOUBLE;
                case NYT::VT_BOOLEAN:
                    return Ydb::Type::BOOL;
                case NYT::VT_STRING:
                    return Ydb::Type::STRING;
                case NYT::VT_UTF8:
                    return Ydb::Type::UTF8;
                case NYT::VT_JSON:
                    return Ydb::Type::JSON;
                case NYT::VT_DATE:
                    return Ydb::Type::DATE;
                case NYT::VT_DATETIME:
                    return Ydb::Type::DATETIME;
                case NYT::VT_TIMESTAMP:
                    return Ydb::Type::TIMESTAMP;
                case NYT::VT_INTERVAL:
                    return Ydb::Type::INTERVAL;
                default:
                    return Ydb::Type::STRING;
            }
        }

        // Creates an arrow field matching the given YDB column type.
        std::shared_ptr<arrow::DataType> YdbTypeToArrow(Ydb::Type::PrimitiveTypeId typeId) {
            switch (typeId) {
                case Ydb::Type::INT8:
                    return arrow::int8();
                case Ydb::Type::INT16:
                    return arrow::int16();
                case Ydb::Type::INT32:
                    return arrow::int32();
                case Ydb::Type::INT64:
                case Ydb::Type::INTERVAL:
                    return arrow::int64();
                case Ydb::Type::UINT8:
                    return arrow::uint8();
                case Ydb::Type::UINT16:
                case Ydb::Type::DATE:
                    return arrow::uint16();
                case Ydb::Type::UINT32:
                case Ydb::Type::DATETIME:
                    return arrow::uint32();
                case Ydb::Type::UINT64:
                case Ydb::Type::TIMESTAMP:
                    return arrow::uint64();
                case Ydb::Type::FLOAT:
                    return arrow::float32();
                case Ydb::Type::DOUBLE:
                    return arrow::float64();
                case Ydb::Type::BOOL:
                    return arrow::boolean();
                case Ydb::Type::STRING:
                    return arrow::binary();
                case Ydb::Type::UTF8:
                case Ydb::Type::JSON:
                    return arrow::utf8();
                default:
                    ythrow yexception() << "unsupported YDB type for arrow conversion: " << int(typeId);
            }
        }

        // Extracts the underlying primitive type id from a (possibly optional) YDB type.
        Ydb::Type::PrimitiveTypeId ExtractPrimitiveTypeId(const Ydb::Type& type) {
            if (type.has_optional_type()) {
                return ExtractPrimitiveTypeId(type.optional_type().item());
            }
            return type.type_id();
        }

        void CheckOk(const arrow::Status& status) {
            if (!status.ok()) {
                ythrow yexception() << "arrow error: " << status.ToString();
            }
        }

        // Appends a single YT node value to the arrow builder for the given YDB type.
        void AppendNode(arrow::ArrayBuilder* builder, Ydb::Type::PrimitiveTypeId typeId, const NYT::TNode& node) {
            if (node.IsNull() || node.IsUndefined()) {
                CheckOk(builder->AppendNull());
                return;
            }

            switch (typeId) {
                case Ydb::Type::INT8:
                    CheckOk(static_cast<arrow::Int8Builder*>(builder)->Append(node.AsInt64()));
                    break;
                case Ydb::Type::INT16:
                    CheckOk(static_cast<arrow::Int16Builder*>(builder)->Append(node.AsInt64()));
                    break;
                case Ydb::Type::INT32:
                    CheckOk(static_cast<arrow::Int32Builder*>(builder)->Append(node.AsInt64()));
                    break;
                case Ydb::Type::INT64:
                case Ydb::Type::INTERVAL:
                    CheckOk(static_cast<arrow::Int64Builder*>(builder)->Append(node.AsInt64()));
                    break;
                case Ydb::Type::UINT8:
                    CheckOk(static_cast<arrow::UInt8Builder*>(builder)->Append(node.AsUint64()));
                    break;
                case Ydb::Type::UINT16:
                case Ydb::Type::DATE:
                    CheckOk(static_cast<arrow::UInt16Builder*>(builder)->Append(node.AsUint64()));
                    break;
                case Ydb::Type::UINT32:
                case Ydb::Type::DATETIME:
                    CheckOk(static_cast<arrow::UInt32Builder*>(builder)->Append(node.AsUint64()));
                    break;
                case Ydb::Type::UINT64:
                case Ydb::Type::TIMESTAMP:
                    CheckOk(static_cast<arrow::UInt64Builder*>(builder)->Append(node.AsUint64()));
                    break;
                case Ydb::Type::FLOAT:
                    CheckOk(static_cast<arrow::FloatBuilder*>(builder)->Append(node.AsDouble()));
                    break;
                case Ydb::Type::DOUBLE:
                    CheckOk(static_cast<arrow::DoubleBuilder*>(builder)->Append(node.AsDouble()));
                    break;
                case Ydb::Type::BOOL:
                    CheckOk(static_cast<arrow::BooleanBuilder*>(builder)->Append(node.AsBool()));
                    break;
                case Ydb::Type::STRING: {
                    const TString& s = node.AsString();
                    CheckOk(static_cast<arrow::BinaryBuilder*>(builder)->Append(s.data(), static_cast<int32_t>(s.size())));
                    break;
                }
                case Ydb::Type::UTF8:
                case Ydb::Type::JSON: {
                    const TString& s = node.AsString();
                    CheckOk(static_cast<arrow::StringBuilder*>(builder)->Append(s.data(), static_cast<int32_t>(s.size())));
                    break;
                }
                default:
                    ythrow yexception() << "unsupported YDB type for value append: " << int(typeId);
            }
        }

        // Deserializes an Arrow IPC streaming block into an arrow record batch, using the
        // same default serializer the connector uses for read responses.
        std::shared_ptr<arrow::RecordBatch> DeserializeArrowIpc(const TString& arrowIpcStreaming) {
            auto deser = NKikimr::NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
            auto result = deser->Deserialize(arrowIpcStreaming);
            if (!result.ok()) {
                ythrow yexception() << "failed to deserialize arrow ipc block: " << result.status().ToString();
            }
            return *result;
        }

        // Converts a single arrow array value at the given index into a YT node, according
        // to the YDB primitive type. This is the reverse of AppendNode.
        NYT::TNode ArrowValueToNode(const arrow::Array& array, int64_t idx, Ydb::Type::PrimitiveTypeId typeId) {
            if (array.IsNull(idx)) {
                return NYT::TNode::CreateEntity();
            }

            switch (typeId) {
                case Ydb::Type::INT8:
                    return NYT::TNode(static_cast<const arrow::Int8Array&>(array).Value(idx));
                case Ydb::Type::INT16:
                    return NYT::TNode(static_cast<const arrow::Int16Array&>(array).Value(idx));
                case Ydb::Type::INT32:
                    return NYT::TNode(static_cast<const arrow::Int32Array&>(array).Value(idx));
                case Ydb::Type::INT64:
                case Ydb::Type::INTERVAL:
                    return NYT::TNode(static_cast<i64>(static_cast<const arrow::Int64Array&>(array).Value(idx)));
                case Ydb::Type::UINT8:
                    return NYT::TNode(static_cast<ui64>(static_cast<const arrow::UInt8Array&>(array).Value(idx)));
                case Ydb::Type::UINT16:
                case Ydb::Type::DATE:
                    return NYT::TNode(static_cast<ui64>(static_cast<const arrow::UInt16Array&>(array).Value(idx)));
                case Ydb::Type::UINT32:
                case Ydb::Type::DATETIME:
                    return NYT::TNode(static_cast<ui64>(static_cast<const arrow::UInt32Array&>(array).Value(idx)));
                case Ydb::Type::UINT64:
                case Ydb::Type::TIMESTAMP:
                    return NYT::TNode(static_cast<ui64>(static_cast<const arrow::UInt64Array&>(array).Value(idx)));
                case Ydb::Type::FLOAT:
                    return NYT::TNode(static_cast<double>(static_cast<const arrow::FloatArray&>(array).Value(idx)));
                case Ydb::Type::DOUBLE:
                    return NYT::TNode(static_cast<const arrow::DoubleArray&>(array).Value(idx));
                case Ydb::Type::BOOL:
                    return NYT::TNode(static_cast<const arrow::BooleanArray&>(array).Value(idx));
                case Ydb::Type::STRING: {
                    auto view = static_cast<const arrow::BinaryArray&>(array).GetView(idx);
                    return NYT::TNode(TString(view.data(), view.size()));
                }
                case Ydb::Type::UTF8:
                case Ydb::Type::JSON: {
                    auto view = static_cast<const arrow::StringArray&>(array).GetView(idx);
                    return NYT::TNode(TString(view.data(), view.size()));
                }
                default:
                    ythrow yexception() << "unsupported YDB type for value conversion: " << int(typeId);
            }
        }

        // Compares a YT node value against a YDB scalar value (for EQ predicate evaluation).
        bool NodeEqualsYdbValue(const NYT::TNode& node, const Ydb::Value& value) {
            switch (value.value_case()) {
                case Ydb::Value::kBoolValue:
                    return node.IsBool() && node.AsBool() == value.bool_value();
                case Ydb::Value::kInt32Value:
                    return node.IsInt64() && node.AsInt64() == value.int32_value();
                case Ydb::Value::kUint32Value:
                    return node.IsUint64() && node.AsUint64() == value.uint32_value();
                case Ydb::Value::kInt64Value:
                    return node.IsInt64() && node.AsInt64() == value.int64_value();
                case Ydb::Value::kUint64Value:
                    return node.IsUint64() && node.AsUint64() == value.uint64_value();
                case Ydb::Value::kFloatValue:
                    return node.IsDouble() && node.AsDouble() == value.float_value();
                case Ydb::Value::kDoubleValue:
                    return node.IsDouble() && node.AsDouble() == value.double_value();
                case Ydb::Value::kBytesValue:
                    return node.IsString() && node.AsString() == value.bytes_value();
                case Ydb::Value::kTextValue:
                    return node.IsString() && node.AsString() == value.text_value();
                default:
                    ythrow yexception() << "unsupported YDB value for predicate comparison: " << int(value.value_case());
            }
        }

        // Evaluates a strongly-typed predicate against a YT row.
        // Supports the subset produced by the lookup actor: disjunction / conjunction /
        // negation of EQ comparisons between a column and a typed constant.
        bool EvaluatePredicate(const NApi::TPredicate& predicate, const NYT::TNode& row);

        bool EvaluateComparison(const NApi::TPredicate::TComparison& cmp, const NYT::TNode& row) {
            // Expect "column <op> typed_value".
            const auto& left = cmp.left_value();
            const auto& right = cmp.right_value();

            if (left.payload_case() != NApi::TExpression::kColumn ||
                right.payload_case() != NApi::TExpression::kTypedValue) {
                ythrow yexception() << "unsupported comparison shape for YT pushdown";
            }

            const TString& columnName = left.column();
            if (!row.IsMap() || !row.HasKey(columnName)) {
                return false;
            }

            const auto& node = row[columnName];
            const auto& value = right.typed_value().value();
            const bool eq = NodeEqualsYdbValue(node, value);

            switch (cmp.operation()) {
                case NApi::TPredicate::TComparison::EQ:
                case NApi::TPredicate::TComparison::IND:
                    return eq;
                case NApi::TPredicate::TComparison::NE:
                case NApi::TPredicate::TComparison::ID:
                    return !eq;
                default:
                    ythrow yexception() << "unsupported comparison operation for YT pushdown: " << int(cmp.operation());
            }
        }

        bool EvaluatePredicate(const NApi::TPredicate& predicate, const NYT::TNode& row) {
            switch (predicate.payload_case()) {
                case NApi::TPredicate::kDisjunction: {
                    for (const auto& operand : predicate.disjunction().operands()) {
                        if (EvaluatePredicate(operand, row)) {
                            return true;
                        }
                    }
                    return false;
                }
                case NApi::TPredicate::kConjunction: {
                    for (const auto& operand : predicate.conjunction().operands()) {
                        if (!EvaluatePredicate(operand, row)) {
                            return false;
                        }
                    }
                    return true;
                }
                case NApi::TPredicate::kNegation:
                    return !EvaluatePredicate(predicate.negation().operand(), row);
                case NApi::TPredicate::kComparison:
                    return EvaluateComparison(predicate.comparison(), row);
                case NApi::TPredicate::PAYLOAD_NOT_SET:
                    // No predicate => matches everything.
                    return true;
                default:
                    ythrow yexception() << "unsupported predicate payload for YT pushdown: " << int(predicate.payload_case());
            }
        }

        // Encodes/decodes an opaque split description carrying the [lower, upper) row range.
        TString EncodeSplitRange(i64 lower, i64 upper) {
            return TStringBuilder() << lower << ":" << upper;
        }

        bool DecodeSplitRange(const TString& description, i64& lower, i64& upper) {
            if (description.empty()) {
                return false;
            }
            TStringBuf buf(description);
            TStringBuf lo, hi;
            if (!buf.TrySplit(':', lo, hi)) {
                return false;
            }
            lower = FromString<i64>(lo);
            upper = FromString<i64>(hi);
            return true;
        }

        // A stream iterator that emits a precomputed set of responses and then EOF.
        template <class TResponse>
        class TPrecomputedStreamIterator: public IStreamIterator<TResponse> {
        public:
            explicit TPrecomputedStreamIterator(TVector<TResponse>&& responses)
                : Responses_(std::move(responses))
                , Index_(0)
            {
            }

            TAsyncResult<TResponse> ReadNext() override {
                auto promise = NThreading::NewPromise<TResult<TResponse>>();
                if (Index_ < Responses_.size()) {
                    promise.SetValue({NYdbGrpc::TGrpcStatus(), std::move(Responses_[Index_++])});
                } else {
                    NYdbGrpc::TGrpcStatus status;
                    status.GRpcStatusCode = grpc::OUT_OF_RANGE;
                    status.Msg = "Read EOF";
                    promise.SetValue({std::move(status), std::nullopt});
                }
                return promise.GetFuture();
            }

        private:
            TVector<TResponse> Responses_;
            size_t Index_;
        };

        template <class TIterator, class TResponse>
        TIteratorAsyncResult<TIterator> MakeIteratorResult(TVector<TResponse>&& responses) {
            auto promise = NThreading::NewPromise<TIteratorResult<TIterator>>();
            typename TIterator::TPtr iterator =
                std::make_shared<TPrecomputedStreamIterator<TResponse>>(std::move(responses));
            promise.SetValue({NYdbGrpc::TGrpcStatus(), std::move(iterator)});
            return promise.GetFuture();
        }

        template <class TIterator>
        TIteratorAsyncResult<TIterator> MakeErrorIteratorResult(const TString& msg) {
            auto promise = NThreading::NewPromise<TIteratorResult<TIterator>>();
            auto status = NYdbGrpc::TGrpcStatus(grpc::Status(grpc::StatusCode::INTERNAL, msg));
            promise.SetValue({std::move(status), nullptr});
            return promise.GetFuture();
        }

    } // namespace

    TYtClient::TYtClient(const TGenericGatewayConfig& config)
        : Config_(config)
    {
    }

    TString TYtClient::GetClusterName(const NYql::TGenericDataSourceInstance& dsi) {
        if (dsi.has_yt_options() && dsi.yt_options().cluster()) {
            return dsi.yt_options().cluster();
        }
        return dsi.endpoint().host();
    }

    TString TYtClient::GetToken(const NYql::TGenericDataSourceInstance& dsi) {
        if (dsi.has_credentials() && dsi.credentials().has_token()) {
            return dsi.credentials().token().value();
        }
        return {};
    }

    NYT::IClientPtr TYtClient::GetYtClient(const NYql::TGenericDataSourceInstance& dsi) {
        const TString cluster = "hahn";
        const TString token = GetToken(dsi);
        const TString key = TStringBuilder() << cluster << '\0' << token;

        TGuard<TMutex> guard(Mutex_);
        auto it = ClientForCluster_.find(key);
        if (it != ClientForCluster_.end()) {
            return it->second;
        }

        auto options = NYT::TCreateClientOptions();
        if (token) {
            options.Token(token);
        }
        auto client = NYT::CreateClient(cluster, options);
        ClientForCluster_.emplace(key, client);
        return client;
    }

    NYT::TNode TYtClient::GetNode(const NYT::IClientPtr& client, const TString& path) {
        return client->Get(path);
    }

    TVector<NYT::TNode> TYtClient::ReadRows(const NYT::IClientPtr& client, const NYT::TRichYPath& path) {
        TVector<NYT::TNode> rows;
        auto reader = client->CreateTableReader<NYT::TNode>(path);
        for (; reader->IsValid(); reader->Next()) {
            rows.push_back(reader->GetRow());
        }
        return rows;
    }

    TDescribeTableAsyncResult TYtClient::DescribeTable(const NApi::TDescribeTableRequest& request, TDuration) {
        auto promise = NThreading::NewPromise<TResult<NApi::TDescribeTableResponse>>();

        if (!request.has_data_source_instance() || !request.data_source_instance().has_kind()) {
            auto msg = TStringBuilder() << "DescribeTable request is invalid: either data source or kind is missing";
            YQL_CLOG(WARN, ProviderGeneric) << msg;
            auto status = NYdbGrpc::TGrpcStatus(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, msg));
            promise.SetValue({std::move(status), NApi::TDescribeTableResponse()});
            return promise.GetFuture();
        }

        NApi::TDescribeTableResponse response;
        try {
            auto client = GetYtClient(request.data_source_instance());
            const TString table = request.table();

            auto schemaNode = GetNode(client, table + "/@schema");
            auto schema = NYT::TTableSchema::FromNode(schemaNode);

            auto* outSchema = response.mutable_schema();
            for (const auto& column : schema.Columns()) {
                auto* ydbColumn = outSchema->add_columns();
                ydbColumn->set_name(column.Name());
                auto typeId = YtTypeToYdbPrimitive(column.Type());
                if (column.Required()) {
                    ydbColumn->mutable_type()->set_type_id(typeId);
                } else {
                    ydbColumn->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(typeId);
                }
            }
        } catch (const std::exception& e) {
            auto msg = TStringBuilder() << "DescribeTable failed: " << e.what();
            YQL_CLOG(ERROR, ProviderGeneric) << msg;
            auto status = NYdbGrpc::TGrpcStatus(grpc::Status(grpc::StatusCode::INTERNAL, msg));
            promise.SetValue({std::move(status), std::move(response)});
            return promise.GetFuture();
        }

        promise.SetValue({NYdbGrpc::TGrpcStatus(), std::move(response)});
        return promise.GetFuture();
    }

    TListSplitsStreamIteratorAsyncResult TYtClient::ListSplits(const NApi::TListSplitsRequest& request, TDuration) {
        const auto& selects = request.selects();

        if (selects.empty() ||
            !selects.at(0).has_data_source_instance() ||
            !selects.at(0).data_source_instance().has_kind()) {
            auto msg = TStringBuilder() << "ListSplits request is invalid: either selects is empty or data source or kind is missing";
            YQL_CLOG(WARN, ProviderGeneric) << msg;
            return MakeErrorIteratorResult<IListSplitsStreamIterator>(msg);
        }

        try {
            TVector<NApi::TListSplitsResponse> responses;
            ui64 splitId = 0;

            for (const auto& select : selects) {
                auto client = GetYtClient(select.data_source_instance());
                const TString table = select.from().table();

                const i64 rowCount = GetNode(client, table + "/@row_count").AsInt64();

                // Determine how many splits to produce.
                ui32 maxSplitCount = request.max_split_count();
                ui64 splitSize = request.split_size();

                i64 splitCount = 1;
                if (maxSplitCount == 1 || rowCount == 0) {
                    splitCount = 1;
                } else if (splitSize > 0) {
                    splitCount = (rowCount + splitSize - 1) / splitSize;
                    if (maxSplitCount > 0 && splitCount > static_cast<i64>(maxSplitCount)) {
                        splitCount = maxSplitCount;
                    }
                } else if (maxSplitCount > 0) {
                    splitCount = maxSplitCount;
                }
                if (splitCount < 1) {
                    splitCount = 1;
                }

                NApi::TListSplitsResponse response;
                const i64 rowsPerSplit = (rowCount + splitCount - 1) / Max<i64>(splitCount, 1);

                for (i64 i = 0; i < splitCount; ++i) {
                    const i64 lower = i * rowsPerSplit;
                    if (lower >= rowCount && !(rowCount == 0 && i == 0)) {
                        break;
                    }
                    const i64 upper = Min<i64>(lower + rowsPerSplit, rowCount);

                    auto* split = response.add_splits();
                    *split->mutable_select() = select;
                    split->set_id(splitId++);
                    // Full-table single split leaves the range empty.
                    if (!(maxSplitCount == 1 || splitCount == 1)) {
                        split->set_description(EncodeSplitRange(lower, upper));
                    }
                }

                responses.push_back(std::move(response));
            }

            return MakeIteratorResult<IListSplitsStreamIterator, NApi::TListSplitsResponse>(std::move(responses));
        } catch (const std::exception& e) {
            auto msg = TStringBuilder() << "ListSplits failed: " << e.what();
            YQL_CLOG(ERROR, ProviderGeneric) << msg;
            return MakeErrorIteratorResult<IListSplitsStreamIterator>(msg);
        }
    }

    TReadSplitsStreamIteratorAsyncResult TYtClient::ReadSplits(const NApi::TReadSplitsRequest& request, TDuration) {
        const auto& splits = request.splits();

        if (splits.empty() ||
            !splits.at(0).has_select() ||
            !splits.at(0).select().has_data_source_instance() ||
            !splits.at(0).select().data_source_instance().has_kind()) {
            auto msg = TStringBuilder() << "ReadSplits request is invalid: either splits or select is empty or data source or kind is missing";
            YQL_CLOG(WARN, ProviderGeneric) << msg;
            return MakeErrorIteratorResult<IReadSplitsStreamIterator>(msg);
        }

        const bool mandatoryFiltering =
            request.filtering() == NApi::TReadSplitsRequest::FILTERING_MANDATORY;

        try {
            auto serializer = NKikimr::NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
            TVector<NApi::TReadSplitsResponse> responses;

            ui32 splitIndex = 0;
            for (const auto& split : splits) {
                const auto& select = split.select();
                auto client = GetYtClient(select.data_source_instance());
                const TString table = select.from().table();

                // Collect the requested column names and their YDB types (in TSelect.what order).
                TVector<TString> columnNames;
                TVector<Ydb::Type::PrimitiveTypeId> columnTypes;
                for (const auto& item : select.what().items()) {
                    if (item.payload_case() != NApi::TSelect::TWhat::TItem::kColumn) {
                        ythrow yexception() << "unsupported TWhat item (only columns are supported)";
                    }
                    columnNames.push_back(item.column().name());
                    columnTypes.push_back(ExtractPrimitiveTypeId(item.column().type()));
                }

                // Build the path with row-range and column projection.
                NYT::TRichYPath path(table);
                i64 lower = 0;
                i64 upper = 0;
                if (DecodeSplitRange(split.description(), lower, upper)) {
                    path.AddRange(NYT::TReadRange::FromRowIndices(lower, upper));
                }
                {
                    NYT::TColumnNames columns;
                    for (const auto& name : columnNames) {
                        columns.Parts_.push_back(name);
                    }
                    path.Columns(columns);
                }

                const bool hasPredicate =
                    select.has_where() && select.where().has_filter_typed();

                ui64 limit = 0;
                bool hasLimit = false;
                if (select.has_limit() && select.limit().limit() > 0) {
                    limit = select.limit().limit();
                    hasLimit = true;
                }

                if (mandatoryFiltering && !hasPredicate) {
                    ythrow yexception() << "FILTERING_MANDATORY requested but no predicate provided";
                }

                // Read matching rows into memory (applying predicate/limit).
                TVector<NYT::TNode> rows;
                for (const auto& row : ReadRows(client, path)) {
                    if (hasPredicate) {
                        if (!EvaluatePredicate(select.where().filter_typed(), row)) {
                            continue;
                        }
                    }
                    rows.push_back(row);
                    if (hasLimit && rows.size() >= limit) {
                        break;
                    }
                }

                // Build an arrow record batch (columns in TSelect.what order).
                std::vector<std::shared_ptr<arrow::Field>> fields;
                std::vector<std::shared_ptr<arrow::Array>> arrays;

                for (size_t c = 0; c < columnNames.size(); ++c) {
                    auto arrowType = YdbTypeToArrow(columnTypes[c]);
                    fields.push_back(arrow::field(columnNames[c], arrowType));

                    std::unique_ptr<arrow::ArrayBuilder> builder;
                    CheckOk(arrow::MakeBuilder(arrow::default_memory_pool(), arrowType, &builder));
                    CheckOk(builder->Reserve(rows.size()));

                    for (const auto& row : rows) {
                        if (row.IsMap() && row.HasKey(columnNames[c])) {
                            AppendNode(builder.get(), columnTypes[c], row[columnNames[c]]);
                        } else {
                            CheckOk(builder->AppendNull());
                        }
                    }

                    std::shared_ptr<arrow::Array> array;
                    CheckOk(builder->Finish(&array));
                    arrays.push_back(std::move(array));
                }

                auto arrowSchema = arrow::schema(fields);
                auto recordBatch = arrow::RecordBatch::Make(arrowSchema, rows.size(), arrays);

                NApi::TReadSplitsResponse response;
                response.set_arrow_ipc_streaming(serializer->SerializeFull(recordBatch));
                response.set_split_index_number(splitIndex);
                response.mutable_stats()->set_rows(rows.size());
                response.mutable_stats()->set_bytes(response.arrow_ipc_streaming().size());
                responses.push_back(std::move(response));

                ++splitIndex;
            }

            return MakeIteratorResult<IReadSplitsStreamIterator, NApi::TReadSplitsResponse>(std::move(responses));
        } catch (const std::exception& e) {
            auto msg = TStringBuilder() << "ReadSplits failed: " << e.what();
            YQL_CLOG(ERROR, ProviderGeneric) << msg;
            return MakeErrorIteratorResult<IReadSplitsStreamIterator>(msg);
        }
    }

    void TYtClient::WriteRows(const NApi::TSchema& schema,
                              const TString& table,
                              const TString& arrowIpcStreaming,
                              const NYql::TGenericDataSourceInstance& dataSourceInstance) {
        auto client = GetYtClient(dataSourceInstance);

        auto recordBatch = DeserializeArrowIpc(arrowIpcStreaming);
        const int64_t numRows = recordBatch->num_rows();
        const int numColumns = recordBatch->num_columns();

        // Resolve, for every arrow column, its name and the target YDB primitive type
        // (taken from the connector schema).
        const auto& arrowSchema = *recordBatch->schema();
        TVector<TString> columnNames(numColumns);
        TVector<Ydb::Type::PrimitiveTypeId> columnTypes(numColumns);
        TVector<std::shared_ptr<arrow::Array>> arrays(numColumns);

        for (int c = 0; c < numColumns; ++c) {
            columnNames[c] = arrowSchema.field(c)->name();
            columnTypes[c] = ExtractPrimitiveTypeId(GetColumnTypeByName(schema, columnNames[c]));
            arrays[c] = recordBatch->column(c);
        }

        auto writer = client->CreateTableWriter<NYT::TNode>(NYT::TRichYPath(table).Append(true));
        for (int64_t r = 0; r < numRows; ++r) {
            NYT::TNode row = NYT::TNode::CreateMap();
            for (int c = 0; c < numColumns; ++c) {
                row[columnNames[c]] = ArrowValueToNode(*arrays[c], r, columnTypes[c]);
            }
            writer->AddRow(row);
        }
        writer->Finish();
    }

    IClient::TPtr MakeYtClient(const ::NYql::TGenericGatewayConfig& cfg) {
        return std::make_shared<TYtClient>(cfg);
    }

} // namespace NYql::NConnector
