#include "parquet.h"

#include "factories.h"
#include "kqp_setup.h"
#include "subprocess.h"

#include <ydb/library/yql/dq/comp_nodes/ut/utils/preallocated_spiller.h>

#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/public/udf/udf_string.h>
#include <yql/essentials/utils/log/log.h>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>

#include <util/stream/output.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <memory>
#include <numeric>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

namespace NKikimr::NMiniKQL {
namespace {

using NUdf::EDataSlot;
using NUdf::TUnboxedValue;
using NUdf::TUnboxedValuePod;

struct TParquetColumn {
    std::string Name;
    EDataSlot Slot;
    bool Optional;
};

struct TParquetBatch {
    std::vector<std::shared_ptr<arrow::Array>> Columns;
    size_t Rows = 0;
};

struct TParquetData {
    std::vector<TParquetColumn> Columns;
    std::vector<TParquetBatch> Batches;
    size_t Rows = 0;
};

enum class EAggregationKind {
    Sum,
    Count,
};

struct TAggregation {
    EAggregationKind Kind;
    size_t Column = 0;
    EDataSlot ResultSlot = EDataSlot::Uint64;
};

void EnsureArrowStatus(const arrow::Status& status, TStringBuf operation)
{
    Y_ENSURE(status.ok(), operation << ": " << status.ToString());
}

EDataSlot ArrowTypeToDataSlot(const std::shared_ptr<arrow::DataType>& type)
{
    switch (type->id()) {
        case arrow::Type::BOOL: return EDataSlot::Bool;
        case arrow::Type::INT8: return EDataSlot::Int8;
        case arrow::Type::UINT8: return EDataSlot::Uint8;
        case arrow::Type::INT16: return EDataSlot::Int16;
        case arrow::Type::UINT16: return EDataSlot::Uint16;
        case arrow::Type::INT32: return EDataSlot::Int32;
        case arrow::Type::UINT32: return EDataSlot::Uint32;
        case arrow::Type::INT64: return EDataSlot::Int64;
        case arrow::Type::UINT64: return EDataSlot::Uint64;
        case arrow::Type::FLOAT: return EDataSlot::Float;
        case arrow::Type::DOUBLE: return EDataSlot::Double;
        case arrow::Type::STRING: return EDataSlot::Utf8;
        case arrow::Type::BINARY: return EDataSlot::String;
        case arrow::Type::DATE32: return EDataSlot::Date32;
        case arrow::Type::DATE64: return EDataSlot::Datetime64;
        case arrow::Type::TIMESTAMP: return EDataSlot::Timestamp64;
        default:
            ythrow yexception() << "Unsupported Parquet/Arrow type: " << type->ToString();
    }
}

std::shared_ptr<arrow::DataType> DataSlotArrowType(EDataSlot slot)
{
    switch (slot) {
        case EDataSlot::Bool:
        case EDataSlot::Uint8: return arrow::uint8();
        case EDataSlot::Int8: return arrow::int8();
        case EDataSlot::Int16: return arrow::int16();
        case EDataSlot::Uint16: return arrow::uint16();
        case EDataSlot::Int32:
        case EDataSlot::Date32: return arrow::int32();
        case EDataSlot::Uint32: return arrow::uint32();
        case EDataSlot::Int64:
        case EDataSlot::Datetime64:
        case EDataSlot::Timestamp64: return arrow::int64();
        case EDataSlot::Uint64: return arrow::uint64();
        case EDataSlot::Float: return arrow::float32();
        case EDataSlot::Double: return arrow::float64();
        case EDataSlot::Utf8: return arrow::utf8();
        case EDataSlot::String: return arrow::binary();
        default:
            ythrow yexception() << "No Arrow representation for data slot "
                                << NUdf::GetDataTypeInfo(slot).Name;
    }
}

std::shared_ptr<arrow::Array> CanonicalizeArray(
    const std::shared_ptr<arrow::Array>& array,
    EDataSlot slot)
{
    const auto targetType = DataSlotArrowType(slot);
    if (array->type()->Equals(targetType)) {
        return array;
    }

    auto cast = arrow::compute::Cast(*array, targetType);
    Y_ENSURE(cast.ok(), "Cannot convert Arrow column from " << array->type()->ToString()
        << " to the MKQL block representation " << targetType->ToString() << ": "
        << cast.status().ToString());
    return cast.ValueOrDie();
}

TParquetData ReadParquetData(const TRunParams& params)
{
    auto fileResult = arrow::io::ReadableFile::Open(params.ParquetFile);
    Y_ENSURE(fileResult.ok(), "Cannot open Parquet file " << params.ParquetFile << ": "
        << fileResult.status().ToString());
    auto file = fileResult.ValueOrDie();

    std::unique_ptr<parquet::arrow::FileReader> fileReader;
    EnsureArrowStatus(
        parquet::arrow::OpenFile(file, arrow::default_memory_pool(), &fileReader),
        "Cannot create Parquet reader");

    std::shared_ptr<arrow::Schema> schema;
    EnsureArrowStatus(fileReader->GetSchema(&schema), "Cannot read Parquet schema");

    TParquetData data;
    std::vector<int> columnIndices;
    std::unordered_set<std::string> seenColumns;
    for (const auto& name : params.ParquetColumns) {
        Y_ENSURE(!name.empty(), "Empty name in --parquet-columns");
        Y_ENSURE(seenColumns.emplace(name).second,
            "Duplicate column in --parquet-columns: " << name);
        const int index = schema->GetFieldIndex(name);
        Y_ENSURE(index >= 0, "Column not found in Parquet schema: " << name);
        const auto& field = schema->field(index);
        data.Columns.push_back({name, ArrowTypeToDataSlot(field->type()), field->nullable()});
        columnIndices.push_back(index);
    }

    std::vector<int> rowGroups(fileReader->num_row_groups());
    std::iota(rowGroups.begin(), rowGroups.end(), 0);
    fileReader->set_batch_size(params.BlockSize);

    std::unique_ptr<arrow::RecordBatchReader> batchReader;
    EnsureArrowStatus(
        fileReader->GetRecordBatchReader(rowGroups, columnIndices, &batchReader),
        "Cannot create Parquet record batch reader");

    while (!params.ParquetRowLimit || data.Rows < params.ParquetRowLimit) {
        std::shared_ptr<arrow::RecordBatch> batch;
        EnsureArrowStatus(batchReader->ReadNext(&batch), "Cannot read Parquet record batch");
        if (!batch) {
            break;
        }

        size_t rows = batch->num_rows();
        if (params.ParquetRowLimit) {
            rows = std::min(rows, params.ParquetRowLimit - data.Rows);
        }
        if (!rows) {
            break;
        }

        TParquetBatch savedBatch;
        savedBatch.Rows = rows;
        savedBatch.Columns.reserve(data.Columns.size());
        Y_ENSURE(static_cast<size_t>(batch->num_columns()) == data.Columns.size(),
            "Parquet reader returned an unexpected number of columns");
        for (size_t column = 0; column < data.Columns.size(); ++column) {
            auto array = batch->column(column);
            if (rows != static_cast<size_t>(array->length())) {
                array = array->Slice(0, rows);
            }
            savedBatch.Columns.push_back(CanonicalizeArray(array, data.Columns[column].Slot));
        }
        data.Batches.push_back(std::move(savedBatch));
        data.Rows += rows;
    }

    Y_ENSURE(data.Rows > 0, "The selected Parquet input is empty");
    Cerr << "Preloaded " << data.Rows << " rows in " << data.Batches.size()
         << " Arrow blocks from " << params.ParquetFile << Endl;
    for (const auto& column : data.Columns) {
        Cerr << "  " << column.Name << ": " << NUdf::GetDataTypeInfo(column.Slot).Name
             << (column.Optional ? "?" : "") << Endl;
    }
    return data;
}

bool IsSummable(EDataSlot slot)
{
    switch (slot) {
        case EDataSlot::Int8:
        case EDataSlot::Uint8:
        case EDataSlot::Int16:
        case EDataSlot::Uint16:
        case EDataSlot::Int32:
        case EDataSlot::Uint32:
        case EDataSlot::Int64:
        case EDataSlot::Uint64:
        case EDataSlot::Float:
        case EDataSlot::Double:
            return true;
        default:
            return false;
    }
}

std::vector<size_t> ResolveKeys(const TRunParams& params, const TParquetData& data)
{
    Y_ENSURE(!params.ParquetKeyColumns.empty(), "At least one --parquet-keys column is required");
    std::vector<size_t> result;
    std::unordered_set<std::string> seen;
    for (const auto& name : params.ParquetKeyColumns) {
        Y_ENSURE(seen.emplace(name).second, "Duplicate key column: " << name);
        auto it = std::find_if(data.Columns.begin(), data.Columns.end(), [&](const auto& column) {
            return column.Name == name;
        });
        Y_ENSURE(it != data.Columns.end(), "Key column was not selected by --parquet-columns: " << name);
        result.push_back(std::distance(data.Columns.begin(), it));
    }
    return result;
}

std::vector<TAggregation> ResolveAggregations(const TRunParams& params, const TParquetData& data)
{
    Y_ENSURE(!params.ParquetAggregations.empty(), "At least one aggregation is required");
    std::vector<TAggregation> result;
    for (const auto& text : params.ParquetAggregations) {
        if (text == "count") {
            result.push_back({EAggregationKind::Count, 0, EDataSlot::Uint64});
            continue;
        }

        constexpr TStringBuf sumPrefix = "sum:";
        Y_ENSURE(TStringBuf(text).StartsWith(sumPrefix),
            "Unsupported aggregation '" << text << "'; expected sum:column_name or count");
        const std::string name = text.substr(sumPrefix.size());
        auto it = std::find_if(data.Columns.begin(), data.Columns.end(), [&](const auto& column) {
            return column.Name == name;
        });
        Y_ENSURE(it != data.Columns.end(),
            "Sum column was not selected by --parquet-columns: " << name);
        Y_ENSURE(IsSummable(it->Slot), "Cannot sum column " << name << " of type "
            << NUdf::GetDataTypeInfo(it->Slot).Name);
        result.push_back({
            EAggregationKind::Sum,
            static_cast<size_t>(std::distance(data.Columns.begin(), it)),
            it->Slot,
        });
    }
    return result;
}

class TPrebuiltBlockStream final : public NUdf::TBoxedValue {
public:
    TPrebuiltBlockStream(std::vector<std::vector<TUnboxedValue>> values, size_t iterations)
        : Values_(std::move(values))
        , Iterations_(iterations)
    {
    }

    NUdf::EFetchStatus Fetch(TUnboxedValue&) final
    {
        ythrow yexception() << "Only WideFetch is supported";
    }

    NUdf::EFetchStatus WideFetch(TUnboxedValue* result, ui32 width) final
    {
        if (Iteration_ == Iterations_) {
            return NUdf::EFetchStatus::Finish;
        }
        Y_ENSURE(width == Values_[Batch_].size(), "Unexpected block stream width");
        std::copy(Values_[Batch_].begin(), Values_[Batch_].end(), result);
        if (++Batch_ == Values_.size()) {
            Batch_ = 0;
            ++Iteration_;
        }
        return NUdf::EFetchStatus::Ok;
    }

private:
    std::vector<std::vector<TUnboxedValue>> Values_;
    const size_t Iterations_;
    size_t Batch_ = 0;
    size_t Iteration_ = 0;
};

TUnboxedValuePod ArrowValueToUnboxed(
    const std::shared_ptr<arrow::Array>& array,
    size_t row,
    EDataSlot slot)
{
    if (array->IsNull(row)) {
        return {};
    }

#define PARQUET_NUMERIC_VALUE(dataSlot, arrowArray, cppType) \
    case EDataSlot::dataSlot: \
        return TUnboxedValuePod(static_cast<cppType>( \
            std::static_pointer_cast<arrow::arrowArray>(array)->Value(row)))

    switch (slot) {
        PARQUET_NUMERIC_VALUE(Bool, UInt8Array, bool);
        PARQUET_NUMERIC_VALUE(Int8, Int8Array, i8);
        PARQUET_NUMERIC_VALUE(Uint8, UInt8Array, ui8);
        PARQUET_NUMERIC_VALUE(Int16, Int16Array, i16);
        PARQUET_NUMERIC_VALUE(Uint16, UInt16Array, ui16);
        PARQUET_NUMERIC_VALUE(Int32, Int32Array, i32);
        PARQUET_NUMERIC_VALUE(Uint32, UInt32Array, ui32);
        PARQUET_NUMERIC_VALUE(Int64, Int64Array, i64);
        PARQUET_NUMERIC_VALUE(Uint64, UInt64Array, ui64);
        PARQUET_NUMERIC_VALUE(Float, FloatArray, float);
        PARQUET_NUMERIC_VALUE(Double, DoubleArray, double);
        PARQUET_NUMERIC_VALUE(Date32, Int32Array, i32);
        PARQUET_NUMERIC_VALUE(Datetime64, Int64Array, i64);
        PARQUET_NUMERIC_VALUE(Timestamp64, Int64Array, i64);
        case EDataSlot::Utf8: {
            const auto value = std::static_pointer_cast<arrow::StringArray>(array)->GetView(row);
            if (value.empty()) {
                return TUnboxedValuePod::Embedded(0);
            }
            return TUnboxedValuePod(NUdf::TStringValue(NUdf::TStringRef(value.data(), value.size())));
        }
        case EDataSlot::String: {
            const auto value = std::static_pointer_cast<arrow::BinaryArray>(array)->GetView(row);
            if (value.empty()) {
                return TUnboxedValuePod::Embedded(0);
            }
            return TUnboxedValuePod(NUdf::TStringValue(NUdf::TStringRef(value.data(), value.size())));
        }
        default:
            ythrow yexception() << "Cannot scalarize data slot " << NUdf::GetDataTypeInfo(slot).Name;
    }

#undef PARQUET_NUMERIC_VALUE
}

class TScalarParquetStream final : public NUdf::TBoxedValue {
public:
    TScalarParquetStream(const TParquetData& data, size_t iterations)
        : Data_(data)
        , Iterations_(iterations)
    {
    }

    NUdf::EFetchStatus Fetch(TUnboxedValue&) final
    {
        ythrow yexception() << "Only WideFetch is supported";
    }

    NUdf::EFetchStatus WideFetch(TUnboxedValue* result, ui32 width) final
    {
        if (Iteration_ == Iterations_) {
            return NUdf::EFetchStatus::Finish;
        }
        Y_ENSURE(width == Data_.Columns.size(), "Unexpected scalar stream width");
        const auto& batch = Data_.Batches[Batch_];
        for (size_t column = 0; column < Data_.Columns.size(); ++column) {
            result[column] = ArrowValueToUnboxed(
                batch.Columns[column], Row_, Data_.Columns[column].Slot);
        }
        if (++Row_ == batch.Rows) {
            Row_ = 0;
            if (++Batch_ == Data_.Batches.size()) {
                Batch_ = 0;
                ++Iteration_;
            }
        }
        return NUdf::EFetchStatus::Ok;
    }

private:
    const TParquetData& Data_;
    const size_t Iterations_;
    size_t Batch_ = 0;
    size_t Row_ = 0;
    size_t Iteration_ = 0;
};

template<bool LLVM, bool Spilling>
THolder<IComputationGraph> BuildGraph(
    TKqpSetup<LLVM, Spilling>& setup,
    const TParquetData& data,
    const std::vector<size_t>& keys,
    const std::vector<TAggregation>& aggregations,
    bool blocks,
    bool dqAggregate)
{
    auto& pb = setup.GetKqpBuilder();
    std::vector<TType*> inputTypes;
    inputTypes.reserve(data.Columns.size() + (blocks ? 1 : 0));
    for (const auto& column : data.Columns) {
        auto* type = pb.NewDataType(column.Slot, column.Optional);
        inputTypes.push_back(blocks ? pb.NewBlockType(type, TBlockType::EShape::Many) : type);
    }
    if (blocks) {
        inputTypes.push_back(pb.NewBlockType(
            pb.NewDataType(EDataSlot::Uint64), TBlockType::EShape::Scalar));
    }

    auto* streamType = pb.NewStreamType(pb.NewMultiType(inputTypes));
    auto streamCallable = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", streamType).Build();

    auto keyExtractor = [&](TRuntimeNode::TList items) {
        TRuntimeNode::TList result;
        result.reserve(keys.size());
        for (size_t column : keys) {
            result.push_back(items[column]);
        }
        return result;
    };
    auto init = [&](TRuntimeNode::TList, TRuntimeNode::TList items) {
        TRuntimeNode::TList result;
        result.reserve(aggregations.size());
        for (const auto& aggregation : aggregations) {
            if (aggregation.Kind == EAggregationKind::Sum) {
                result.push_back(items[aggregation.Column]);
            } else {
                result.push_back(pb.template NewDataLiteral<ui64>(1));
            }
        }
        return result;
    };
    auto update = [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) {
        TRuntimeNode::TList result;
        result.reserve(aggregations.size());
        for (size_t i = 0; i < aggregations.size(); ++i) {
            if (aggregations[i].Kind == EAggregationKind::Sum) {
                result.push_back(pb.AggrAdd(items[aggregations[i].Column], state[i]));
            } else {
                result.push_back(pb.AggrAdd(pb.template NewDataLiteral<ui64>(1), state[i]));
            }
        }
        return result;
    };
    auto finish = [](TRuntimeNode::TList keyNodes, TRuntimeNode::TList state) {
        keyNodes.insert(keyNodes.end(), state.begin(), state.end());
        return keyNodes;
    };

    const auto input = pb.ToFlow(TRuntimeNode(streamCallable, false), {});
    TRuntimeNode output;
    if (dqAggregate) {
        output = pb.FromFlow(pb.DqHashAggregate(input, Spilling, keyExtractor, init, update, finish));
    } else {
        output = pb.FromFlow(pb.WideCombiner(input, 0, keyExtractor, init, update, finish));
    }
    return setup.BuildGraph(output, {streamCallable});
}

std::vector<std::vector<TUnboxedValue>> WrapBlockValues(
    const TParquetData& data,
    const TComputationContext& context)
{
    std::vector<std::vector<TUnboxedValue>> result;
    result.reserve(data.Batches.size());
    for (const auto& batch : data.Batches) {
        std::vector<TUnboxedValue> values;
        values.reserve(data.Columns.size() + 1);
        for (const auto& column : batch.Columns) {
            values.push_back(context.HolderFactory.CreateArrowBlock(
                arrow::Datum(column), context.RuntimeSettings.DatumValidation.Get()));
        }
        values.push_back(MakeBlockCount(
            context.HolderFactory, batch.Rows, context.RuntimeSettings.DatumValidation.Get()));
        result.push_back(std::move(values));
    }
    return result;
}

template<typename T>
void AppendPod(std::string& result, T value)
{
    result.append(reinterpret_cast<const char*>(&value), sizeof(value));
}

void AppendUnboxed(std::string& result, const TUnboxedValue& value, EDataSlot slot)
{
    if (!value) {
        result.push_back(0);
        return;
    }
    result.push_back(1);

#define PARQUET_APPEND_VALUE(dataSlot, cppType) \
    case EDataSlot::dataSlot: AppendPod(result, value.Get<cppType>()); return

    switch (slot) {
        PARQUET_APPEND_VALUE(Bool, bool);
        PARQUET_APPEND_VALUE(Int8, i8);
        PARQUET_APPEND_VALUE(Uint8, ui8);
        PARQUET_APPEND_VALUE(Int16, i16);
        PARQUET_APPEND_VALUE(Uint16, ui16);
        PARQUET_APPEND_VALUE(Int32, i32);
        PARQUET_APPEND_VALUE(Uint32, ui32);
        PARQUET_APPEND_VALUE(Int64, i64);
        PARQUET_APPEND_VALUE(Uint64, ui64);
        PARQUET_APPEND_VALUE(Float, float);
        PARQUET_APPEND_VALUE(Double, double);
        PARQUET_APPEND_VALUE(Date32, i32);
        PARQUET_APPEND_VALUE(Datetime64, i64);
        PARQUET_APPEND_VALUE(Timestamp64, i64);
        case EDataSlot::Utf8:
        case EDataSlot::String: {
            const auto string = value.AsStringRef();
            AppendPod(result, string.Size());
            result.append(string.Data(), string.Size());
            return;
        }
        default:
            ythrow yexception() << "Cannot encode data slot " << NUdf::GetDataTypeInfo(slot).Name;
    }

#undef PARQUET_APPEND_VALUE
}

using TResultMap = std::unordered_map<std::string, std::string>;

template<typename T>
T ReadPod(const char*& position)
{
    T result;
    std::memcpy(&result, position, sizeof(result));
    position += sizeof(result);
    return result;
}

template<typename T>
bool ValuesEqual(T left, T right)
{
    if constexpr (std::is_floating_point_v<T>) {
        if (std::isnan(left) || std::isnan(right)) {
            return std::isnan(left) && std::isnan(right);
        }
        const T scale = std::max<T>({1, std::abs(left), std::abs(right)});
        return std::abs(left - right) <= 100 * std::numeric_limits<T>::epsilon() * scale;
    } else {
        return left == right;
    }
}

template<typename T>
bool EncodedValueEqual(const char*& left, const char*& right)
{
    const bool hasLeft = *left++;
    const bool hasRight = *right++;
    if (hasLeft != hasRight) {
        return false;
    }
    if (!hasLeft) {
        return true;
    }
    return ValuesEqual(ReadPod<T>(left), ReadPod<T>(right));
}

bool EncodedAggregatesEqual(
    const std::string& leftValues,
    const std::string& rightValues,
    const std::vector<EDataSlot>& slots)
{
    const char* left = leftValues.data();
    const char* right = rightValues.data();
    for (EDataSlot slot : slots) {
        bool equal = false;
#define PARQUET_COMPARE_VALUE(dataSlot, cppType) \
        case EDataSlot::dataSlot: equal = EncodedValueEqual<cppType>(left, right); break

        switch (slot) {
            PARQUET_COMPARE_VALUE(Int8, i8);
            PARQUET_COMPARE_VALUE(Uint8, ui8);
            PARQUET_COMPARE_VALUE(Int16, i16);
            PARQUET_COMPARE_VALUE(Uint16, ui16);
            PARQUET_COMPARE_VALUE(Int32, i32);
            PARQUET_COMPARE_VALUE(Uint32, ui32);
            PARQUET_COMPARE_VALUE(Int64, i64);
            PARQUET_COMPARE_VALUE(Uint64, ui64);
            PARQUET_COMPARE_VALUE(Float, float);
            PARQUET_COMPARE_VALUE(Double, double);
            default:
                ythrow yexception() << "Cannot compare aggregate data slot "
                                    << NUdf::GetDataTypeInfo(slot).Name;
        }
#undef PARQUET_COMPARE_VALUE
        if (!equal) {
            return false;
        }
    }
    return left == leftValues.data() + leftValues.size() &&
           right == rightValues.data() + rightValues.size();
}

std::vector<EDataSlot> MakeOutputSlots(
    const TParquetData& data,
    const std::vector<size_t>& keys,
    const std::vector<TAggregation>& aggregations)
{
    std::vector<EDataSlot> result;
    result.reserve(keys.size() + aggregations.size());
    for (size_t key : keys) {
        result.push_back(data.Columns[key].Slot);
    }
    for (const auto& aggregation : aggregations) {
        result.push_back(aggregation.ResultSlot);
    }
    return result;
}

TResultMap CollectScalarResults(
    const TUnboxedValue& stream,
    const std::vector<EDataSlot>& outputSlots,
    size_t keyWidth)
{
    TResultMap result;
    std::vector<TUnboxedValue> values(outputSlots.size());
    NUdf::EFetchStatus status;
    while ((status = stream.WideFetch(values.data(), values.size())) != NUdf::EFetchStatus::Finish) {
        if (status == NUdf::EFetchStatus::Yield) {
            continue;
        }
        std::string key;
        std::string aggregates;
        for (size_t i = 0; i < outputSlots.size(); ++i) {
            AppendUnboxed(i < keyWidth ? key : aggregates, values[i], outputSlots[i]);
        }
        Y_ENSURE(result.emplace(std::move(key), std::move(aggregates)).second,
            "Reference combiner produced a duplicate key");
    }
    return result;
}

TResultMap CollectBlockResults(
    const TUnboxedValue& stream,
    const std::vector<EDataSlot>& outputSlots,
    size_t keyWidth)
{
    TResultMap result;
    std::vector<TUnboxedValue> values(outputSlots.size() + 1);
    NUdf::EFetchStatus status;
    while ((status = stream.WideFetch(values.data(), values.size())) != NUdf::EFetchStatus::Finish) {
        if (status == NUdf::EFetchStatus::Yield) {
            continue;
        }
        const size_t rows = TArrowBlock::From(values.back()).GetDatum()
            .scalar_as<arrow::UInt64Scalar>().value;
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        arrays.reserve(outputSlots.size());
        for (size_t i = 0; i < outputSlots.size(); ++i) {
            arrays.push_back(TArrowBlock::From(values[i]).GetDatum().make_array());
        }
        for (size_t row = 0; row < rows; ++row) {
            std::string key;
            std::string aggregates;
            for (size_t i = 0; i < outputSlots.size(); ++i) {
                const auto value = ArrowValueToUnboxed(arrays[i], row, outputSlots[i]);
                AppendUnboxed(i < keyWidth ? key : aggregates, value, outputSlots[i]);
            }
            Y_ENSURE(result.emplace(std::move(key), std::move(aggregates)).second,
                "DqHashAggregate produced a duplicate key");
        }
    }
    return result;
}

size_t CountBlockResults(const TUnboxedValue& stream, size_t outputWidth)
{
    std::vector<TUnboxedValue> values(outputWidth + 1);
    size_t rows = 0;
    NUdf::EFetchStatus status;
    while ((status = stream.WideFetch(values.data(), values.size())) != NUdf::EFetchStatus::Finish) {
        if (status == NUdf::EFetchStatus::Ok) {
            rows += TArrowBlock::From(values.back()).GetDatum()
                .scalar_as<arrow::UInt64Scalar>().value;
        }
    }
    return rows;
}

template<bool LLVM, bool Spilling>
TRunResult MeasureGraph(IComputationGraph& graph, size_t outputWidth)
{
    const long maxRssBefore = GetMaxRSS();
    const auto start = GetThreadCPUTime();
    const size_t outputRows = CountBlockResults(graph.GetValue(), outputWidth);
    TRunResult result;
    result.ResultTime = GetThreadCPUTimeDelta(start);
    result.MaxRSSDelta = GetMaxRSSDelta(maxRssBefore);
    Cerr << "Output row count: " << outputRows << Endl;
    return result;
}

template<bool LLVM, bool Spilling>
void Verify(
    IComputationGraph& blockGraph,
    const TParquetData& data,
    const std::vector<size_t>& keys,
    const std::vector<TAggregation>& aggregations,
    size_t iterations)
{
    const auto outputSlots = MakeOutputSlots(data, keys, aggregations);
    const std::vector<EDataSlot> aggregateSlots(
        outputSlots.begin() + keys.size(), outputSlots.end());
    auto actual = CollectBlockResults(blockGraph.GetValue(), outputSlots, keys.size());

    TKqpSetup<false, false> referenceSetup(GetPerfTestFactory());
    auto referenceGraph = BuildGraph(
        referenceSetup, data, keys, aggregations, false, false);
    auto scalarStream = TUnboxedValuePod(new TScalarParquetStream(data, iterations));
    referenceGraph->GetEntryPoint(0, true)->SetValue(
        referenceGraph->GetContext(), std::move(scalarStream));
    auto expected = CollectScalarResults(referenceGraph->GetValue(), outputSlots, keys.size());

    Y_ENSURE(actual.size() == expected.size(), "Verification failed: DqHashAggregate produced "
        << actual.size() << " groups, reference produced " << expected.size());
    for (const auto& [key, value] : expected) {
        const auto it = actual.find(key);
        Y_ENSURE(it != actual.end(), "Verification failed: result is missing a key");
        Y_ENSURE(EncodedAggregatesEqual(it->second, value, aggregateSlots),
            "Verification failed: aggregate values differ for a key");
    }
    Cerr << "Verification passed for " << actual.size() << " groups" << Endl;
}

} // namespace

template<bool LLVM, bool Spilling>
void RunTestParquet(TRunParams params, TTestResultCollector& printout)
{
    NYql::NLog::InitLogger("cerr", false);

    auto data = ReadParquetData(params);
    const auto keys = ResolveKeys(params, data);
    const auto aggregations = ResolveAggregations(params, data);
    params.RowsPerRun = data.Rows;

    TKqpSetup<LLVM, Spilling> setup(GetPerfTestFactory());
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(Spilling);
    auto graph = BuildGraph(setup, data, keys, aggregations, true, true);
    if constexpr (Spilling) {
        graph->GetContext().SpillerFactory = std::make_shared<TPreallocatedSpillerFactory>();
    }

    auto blockStream = TUnboxedValuePod(new TPrebuiltBlockStream(
        WrapBlockValues(data, graph->GetContext()), params.NumRuns));
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(blockStream));

    std::optional<TRunResult> finalResult;
    for (int attempt = 1; attempt <= params.NumAttempts; ++attempt) {
        Cerr << "------ Parquet run " << attempt << " of " << params.NumAttempts << Endl;
        auto result = RunForked([&] {
            return MeasureGraph<LLVM, Spilling>(*graph, keys.size() + aggregations.size());
        });
        if (finalResult) {
            MergeRunResults(result, *finalResult);
        } else {
            finalResult = result;
        }
    }

    if (params.EnableVerification) {
        RunForked([&] {
            Verify<LLVM, Spilling>(*graph, data, keys, aggregations, params.NumRuns);
            return TRunResult{};
        });
    }

    printout.SubmitMetrics(params, *finalResult, "DqHashAggregateParquet", LLVM, Spilling);
}

template void RunTestParquet<false, false>(TRunParams params, TTestResultCollector& printout);
template void RunTestParquet<false, true>(TRunParams params, TTestResultCollector& printout);
template void RunTestParquet<true, false>(TRunParams params, TTestResultCollector& printout);
template void RunTestParquet<true, true>(TRunParams params, TTestResultCollector& printout);

} // namespace NKikimr::NMiniKQL
