/* 
    This file contains code copied from core/formats/arrow_helpers.cpp in order to cut client dependecies
*/


#include "cli_arrow_helpers.h"
#include "cli_switch_type.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/folder/path.h>

#define Y_VERIFY_OK(status) Y_ABORT_UNLESS(status.ok(), "%s", status.ToString().c_str())

namespace NYdb_cli::NArrow {
    std::shared_ptr<arrow::RecordBatch> ToBatch(const std::shared_ptr<arrow::Table>& table) {
        std::vector<std::shared_ptr<arrow::Array>> columns;
        columns.reserve(table->num_columns());
        for (auto& col : table->columns()) {
            Y_ABORT_UNLESS(col->num_chunks() == 1);
            columns.push_back(col->chunk(0));
        }
        return arrow::RecordBatch::Make(table->schema(), table->num_rows(), columns);
    }
    ui64 GetBatchDataSize(const std::shared_ptr<arrow::RecordBatch>& batch) {
        if (!batch) {
            return 0;
        }
        ui64 bytes = 0;
        for (auto& column : batch->columns()) { // TODO: use column_data() instead of columns()
            bytes += GetArrayDataSize(column);
        }
        return bytes;
    }

    template <typename TType>
    ui64 GetArrayDataSizeImpl(const std::shared_ptr<arrow::Array>& column) {
        return sizeof(typename TType::c_type) * column->length();
    }

    template <>
    ui64 GetArrayDataSizeImpl<arrow::NullType>(const std::shared_ptr<arrow::Array>& column) {
        return column->length() * 8; // Special value for empty lines
    }

    template <>
    ui64 GetArrayDataSizeImpl<arrow::StringType>(const std::shared_ptr<arrow::Array>& column) {
        auto typedColumn = std::static_pointer_cast<arrow::StringArray>(column);
        return typedColumn->total_values_length();
    }

    template <>
    ui64 GetArrayDataSizeImpl<arrow::LargeStringType>(const std::shared_ptr<arrow::Array>& column) {
        auto typedColumn = std::static_pointer_cast<arrow::StringArray>(column);
        return typedColumn->total_values_length();
    }

    template <>
    ui64 GetArrayDataSizeImpl<arrow::BinaryType>(const std::shared_ptr<arrow::Array>& column) {
        auto typedColumn = std::static_pointer_cast<arrow::BinaryArray>(column);
        return typedColumn->total_values_length();
    }

    template <>
    ui64 GetArrayDataSizeImpl<arrow::LargeBinaryType>(const std::shared_ptr<arrow::Array>& column) {
        auto typedColumn = std::static_pointer_cast<arrow::BinaryArray>(column);
        return typedColumn->total_values_length();
    }

    template <>
    ui64 GetArrayDataSizeImpl<arrow::FixedSizeBinaryType>(const std::shared_ptr<arrow::Array>& column) {
        auto typedColumn = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(column);
        return typedColumn->byte_width() * typedColumn->length();
    }

    template <>
    ui64 GetArrayDataSizeImpl<arrow::Decimal128Type>(const std::shared_ptr<arrow::Array>& column) {
        return sizeof(ui64) * 2 * column->length();
    }

    ui64 GetArrayDataSize(const std::shared_ptr<arrow::Array>& column) {
        auto type = column->type();
        ui64 bytes = 0;
        bool success = SwitchTypeWithNull(type->id(), [&]<typename TType>(TTypeWrapper<TType> typeHolder) {
            Y_UNUSED(typeHolder);
            bytes = GetArrayDataSizeImpl<TType>(column);
            return true;
        });

        // Add null bit mask overhead if any.
        if (HasNulls(column)) {
            bytes += column->length() / 8 + 1;
        }

        Y_DEBUG_ABORT_UNLESS(success, "Unsupported arrow type %s", type->ToString().data());
        return bytes;
    }

    namespace {
        class TFixedStringOutputStream final : public arrow::io::OutputStream {
        public:
            TFixedStringOutputStream(TString* out)
                : Out(out)
                , Position(0)
            { }

            arrow::Status Close() override {
                Out = nullptr;
                return arrow::Status::OK();
            }

            bool closed() const override {
                return Out == nullptr;
            }

            arrow::Result<int64_t> Tell() const override {
                return Position;
            }

            arrow::Status Write(const void* data, int64_t nbytes) override {
                if (Y_LIKELY(nbytes > 0)) {
                    Y_ABORT_UNLESS(Out && Out->size() - Position >= ui64(nbytes));
                    char* dst = &(*Out)[Position];
                    ::memcpy(dst, data, nbytes);
                    Position += nbytes;
                }

                return arrow::Status::OK();
            }

            size_t GetPosition() const {
                return Position;
            }

        private:
            TString* Out;
            size_t Position;
        };
    }

    TString SerializeSchema(const arrow::Schema& schema) {
        auto buffer = arrow::ipc::SerializeSchema(schema);
        if (!buffer.ok()) {
            return {};
        }
        return TString((const char*)(*buffer)->data(), (*buffer)->size());
    }

    TString SerializeBatch(const std::shared_ptr<arrow::RecordBatch>& batch, const arrow::ipc::IpcWriteOptions& options) {
        arrow::ipc::IpcPayload payload;
        auto status = arrow::ipc::GetRecordBatchPayload(*batch, options, &payload);
        Y_VERIFY_OK(status);

        int32_t metadata_length = 0;
        arrow::io::MockOutputStream mock;
        status = arrow::ipc::WriteIpcPayload(payload, options, &mock, &metadata_length);
        Y_VERIFY_OK(status);

        TString str;
        str.resize(mock.GetExtentBytesWritten());

        TFixedStringOutputStream out(&str);
        status = arrow::ipc::WriteIpcPayload(payload, options, &out, &metadata_length);
        Y_VERIFY_OK(status);
        Y_ABORT_UNLESS(out.GetPosition() == str.size());

        return str;
    }

    TString SerializeBatchNoCompression(const std::shared_ptr<arrow::RecordBatch>& batch) {
        auto writeOptions = arrow::ipc::IpcWriteOptions::Defaults();
        writeOptions.use_threads = false;
        return SerializeBatch(batch, writeOptions);
    }

}