#include "utils.h"

#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <util/string/builder.h>
#include <util/system/type_name.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql::NConnector {
    arrow::Status MakeConversion(const Ydb::Column columnMeta,
                                 const NApi::TReadSplitsResponse_TColumnSet_TColumn& columnData,
                                 std::vector<std::shared_ptr<arrow::Field>>& fields,
                                 std::vector<std::shared_ptr<arrow::Array>>& arrays) {
        const auto t = columnMeta.type().type_id();
        switch (t) {
            case Ydb::Type::PrimitiveTypeId::Type_PrimitiveTypeId_INT32: {
                fields.emplace_back(arrow::field(columnMeta.name(), arrow::int32()));

                arrow::Int32Builder builder;
                ARROW_RETURN_NOT_OK(builder.Resize(columnData.data_size()));

                for (const auto& val : columnData.data()) {
                    ARROW_RETURN_NOT_OK(builder.Append(val.int32_value()));
                }

                std::shared_ptr<arrow::Array> array;
                ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
                arrays.push_back(array);
                break;
            }
            case Ydb::Type::PrimitiveTypeId::Type_PrimitiveTypeId_STRING: {
                fields.emplace_back(arrow::field(columnMeta.name(), arrow::utf8()));

                arrow::StringBuilder builder;
                ARROW_RETURN_NOT_OK(builder.Resize(columnData.data_size()));

                for (const auto& val : columnData.data()) {
                    ARROW_RETURN_NOT_OK(builder.Append(std::string(val.text_value())));
                }

                std::shared_ptr<arrow::Array> array;
                ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
                arrays.push_back(array);
                break;
            }
            default:
                ythrow yexception() << "unexpected type: " << Ydb::Type_PrimitiveTypeId_Name(t) << " ("
                                    << TypeName(t) << ")";
        }
        return arrow::Status::OK();
    }

    std::shared_ptr<arrow::RecordBatch> ColumnSetToArrowRecordBatch(const NApi::TReadSplitsResponse::TColumnSet& columnSet) {
        YQL_ENSURE(columnSet.meta_size() == columnSet.data_size(), "metadata and data size mismatch");

        // schema fields
        std::vector<std::shared_ptr<arrow::Field>> fields;
        // data columns
        std::vector<std::shared_ptr<arrow::Array>> arrays;

        for (auto i = 0; i < columnSet.meta_size(); i++) {
            const auto& columnMeta = columnSet.meta().Get(i);
            const auto& columnData = columnSet.data().Get(i);

            auto status = MakeConversion(columnMeta, columnData, fields, arrays);
            if (!status.ok()) {
                throw status.ToString();
            }
        }

        auto schema = arrow::schema(fields);
        return arrow::RecordBatch::Make(schema, arrays[0]->length(), arrays);
    }

    std::shared_ptr<arrow::RecordBatch> ArrowIPCStreamingToArrowRecordBatch(const TProtoStringType dump) {
        NKikimr::NArrow::NSerialization::TSerializerContainer deser = NKikimr::NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
        auto result = deser->Deserialize(dump);
        if (!result.ok()) {
            ythrow yexception() << result.status().ToString();
        }

        auto out = *result;

        return out;
    }

    std::shared_ptr<arrow::RecordBatch> ReadSplitsResponseToArrowRecordBatch(const NApi::TReadSplitsResponse& response) {
        const auto t = response.payload_case();
        switch (t) {
            case NApi::TReadSplitsResponse::PayloadCase::kColumnSet:
                return ColumnSetToArrowRecordBatch(response.column_set());
            case NApi::TReadSplitsResponse::PayloadCase::kArrowIpcStreaming:
                return ArrowIPCStreamingToArrowRecordBatch(response.arrow_ipc_streaming());
            default:
                ythrow yexception() << "unexpected payload case: " << int(t);
        }
    }

    Ydb::Type GetColumnTypeByName(const NApi::TSchema& schema, const TString& name) {
        const auto columns = schema.columns();

        auto res =
            std::find_if(columns.begin(), columns.end(), [&](const auto& column) -> bool { return column.name() == name; });

        if (res == schema.columns().end()) {
            ythrow yexception() << "schema " << schema.DebugString() << " does not have column '" << name << "'";
        }

        return res->type();
    }
}
