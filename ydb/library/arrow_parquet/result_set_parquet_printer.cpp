#include "result_set_parquet_printer.h"

#include <ydb/public/sdk/cpp/client/ydb_value/value.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/file.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/stdio.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/stream_writer.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/schema.h>

#include <util/folder/path.h>

namespace NYdb {

    class TResultSetParquetPrinter::TImpl {
    public:
        explicit TImpl(const std::string& outputPath, ui64 rowGroupSize);
        void Reset();
        void Print(const TResultSet& resultSet);

    private:
        void InitStream(const TResultSet& resultSet);
        static parquet::schema::NodePtr ToParquetType(const char* name, const TTypeParser& type, bool nullable);

    private:
        std::unique_ptr<parquet::StreamWriter> Stream;
        const std::string OutputPath;
        const ui64 RowGroupSize;
    };

    void TResultSetParquetPrinter::TImpl::InitStream(const TResultSet& resultSet) {
        parquet::schema::NodeVector fields;
        for (const auto& field : resultSet.GetColumnsMeta()) {
            TTypeParser type(field.Type);
            bool nullable = false;
            if (type.GetKind() == TTypeParser::ETypeKind::Optional) {
                nullable = true;
                type.OpenOptional();
            }
            fields.emplace_back(ToParquetType(field.Name.c_str(), type, nullable));
        }
        auto schema = std::static_pointer_cast<parquet::schema::GroupNode>(
            parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
        parquet::WriterProperties::Builder builder;
        builder.compression(parquet::Compression::ZSTD);
        builder.disable_dictionary();
        std::shared_ptr<arrow::io::OutputStream> outstream;
        if (OutputPath.empty()) {
            outstream = std::make_shared<arrow::io::StdoutStream>();
        } else {
            if (auto parent = TFsPath(OutputPath.c_str()).Parent()) {
                parent.MkDirs();
            }
            outstream = *arrow::io::FileOutputStream::Open(OutputPath);
        }
        Stream = std::make_unique<parquet::StreamWriter>(parquet::ParquetFileWriter::Open(outstream, schema, builder.build()));
        Stream->SetMaxRowGroupSize(RowGroupSize);
    }

    parquet::schema::NodePtr TResultSetParquetPrinter::TImpl::ToParquetType(const char* name, const TTypeParser& type, bool nullable) {
        if (type.GetKind() != TTypeParser::ETypeKind::Primitive) {
            ythrow yexception() << "Cannot save not primitive type to parquet: " << type.GetKind();
        }
        const auto repType = nullable ? parquet::Repetition::OPTIONAL : parquet::Repetition::REQUIRED;
        switch (type.GetPrimitive()) {
        case EPrimitiveType::Bool:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::BOOLEAN);
        case EPrimitiveType::Int8:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::INT32, parquet::ConvertedType::INT_8);
        case EPrimitiveType::Uint8:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::INT32, parquet::ConvertedType::UINT_8);
        case EPrimitiveType::Int16:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::INT32, parquet::ConvertedType::INT_16);
        case EPrimitiveType::Uint16:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::INT32, parquet::ConvertedType::UINT_16);
        case EPrimitiveType::Int32:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::INT32, parquet::ConvertedType::INT_32);
        case EPrimitiveType::Uint32:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::INT32, parquet::ConvertedType::UINT_32);
        case EPrimitiveType::Int64:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::INT64, parquet::ConvertedType::INT_64);
        case EPrimitiveType::Uint64:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::INT64, parquet::ConvertedType::UINT_64);
        case EPrimitiveType::Float:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::FLOAT);
        case EPrimitiveType::Double:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::DOUBLE);
        case EPrimitiveType::Date:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::INT32, parquet::ConvertedType::UINT_32);
        case EPrimitiveType::Timestamp:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::INT64, parquet::ConvertedType::INT_64);
        case EPrimitiveType::Interval:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::INT64, parquet::ConvertedType::INT_64);
        case EPrimitiveType::String:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8);
        case EPrimitiveType::Utf8:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8);
        case EPrimitiveType::Yson:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8);
        case EPrimitiveType::Json:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8);
        case EPrimitiveType::JsonDocument:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8);
        case EPrimitiveType::DyNumber:
            return parquet::schema::PrimitiveNode::Make(name, repType, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8);
        default:
            ythrow yexception() << "Cannot save type to parquet: " << type.GetPrimitive();
        }
    }

    TResultSetParquetPrinter::TImpl::TImpl(const std::string& outputPath, ui64 rowGroupSize)
        : OutputPath(outputPath)
        , RowGroupSize(rowGroupSize)
    {}

    void TResultSetParquetPrinter::TImpl::Reset() {
        Stream.reset();
    }

    void TResultSetParquetPrinter::TImpl::Print(const TResultSet& resultSet) {
        if (!Stream) {
            InitStream(resultSet);
        }
        auto& os = *Stream;
        TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            for (ui32 i = 0; i < resultSet.GetColumnsMeta().size(); ++i) {
                TValueParser value(parser.GetValue(i));
                bool nullable = value.GetKind() == TTypeParser::ETypeKind::Optional;
                if (nullable) {
                    value.OpenOptional();
                    if (value.IsNull()) {
                        os.SkipColumns(1);
                        continue;
                    }
                }
                if (value.GetKind() != TTypeParser::ETypeKind::Primitive) {
                    ythrow yexception() << "Cannot save not primitive type to parquet: " << value.GetKind();
                }
                switch (value.GetPrimitiveType()) {
                case EPrimitiveType::Bool:
                    os << value.GetBool();
                    break;
                case EPrimitiveType::Int8:
                    os << value.GetInt8();
                    break;
                case EPrimitiveType::Uint8:
                    os << value.GetUint8();
                    break;
                case EPrimitiveType::Int16:
                    os << value.GetInt16();
                    break;
                case EPrimitiveType::Uint16:
                    os << value.GetUint16();
                    break;
                case EPrimitiveType::Int32:
                    os << value.GetInt32();
                    break;
                case EPrimitiveType::Uint32:
                    os << value.GetUint32();
                    break;
                case EPrimitiveType::Int64:
                    os << (std::int64_t)value.GetInt64();
                    break;
                case EPrimitiveType::Uint64:
                    os << (std::uint64_t)value.GetUint64();
                    break;
                case EPrimitiveType::Float:
                    os << value.GetFloat();
                    break;
                case EPrimitiveType::Double:
                    os << value.GetDouble();
                    break;
                case EPrimitiveType::Date:
                    os << (ui32)value.GetDate().Seconds();
                    break;
                case EPrimitiveType::Timestamp:
                    os << (std::int64_t)value.GetTimestamp().MicroSeconds();
                    break;
                case EPrimitiveType::Interval:
                    os << (std::int64_t)value.GetInterval();
                    break;
                case EPrimitiveType::String:
                    os << arrow::util::string_view(value.GetString().c_str(), value.GetString().length());
                    break;
                case EPrimitiveType::Utf8:
                    os << arrow::util::string_view(value.GetUtf8().c_str(), value.GetUtf8().length());
                    break;
                case EPrimitiveType::Yson:
                    os << arrow::util::string_view(value.GetYson().c_str(), value.GetYson().length());
                    break;
                case EPrimitiveType::Json:
                    os << arrow::util::string_view(value.GetJson().c_str(), value.GetJson().length());
                    break;
                case EPrimitiveType::JsonDocument:
                    os << arrow::util::string_view(value.GetJsonDocument().c_str(), value.GetJsonDocument().length());
                    break;
                case EPrimitiveType::DyNumber:
                    os << arrow::util::string_view(value.GetDyNumber().c_str(), value.GetDyNumber().length());
                    break;
                default:
                    ythrow yexception() << "Cannot save type to parquet: " << value.GetPrimitiveType();
                }
            }
            os.EndRow();
        }
    }

    TResultSetParquetPrinter::TResultSetParquetPrinter(const std::string& outputPath, ui64 rowGroupSize /*= 100000*/)
        : Impl(std::make_unique<TImpl>(outputPath, rowGroupSize))
    {}

    TResultSetParquetPrinter::~TResultSetParquetPrinter() {
    }

    void TResultSetParquetPrinter::Reset() {
        Impl->Reset();
    }

    void TResultSetParquetPrinter::Print(const TResultSet& resultSet) {
        Impl->Print(resultSet);
    }

}
