#include <yt/yt/core/test_framework/framework.h>

#include "row_helpers.h"

#include <yt/yt/library/formats/arrow_parser.h>

#include <yt/yt/client/formats/config.h>
#include <yt/yt/client/formats/parser.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/validate_logical_type.h>
#include <yt/yt/library/formats/format.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

namespace NYT::NFormats {
namespace {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

using namespace std::string_literals;

////////////////////////////////////////////////////////////////////////////////

std::string GetEos()
{
    std::string eos;
    eos.assign(4, 0);
    return eos;
}

void Verify(const arrow::Status& status)
{
    YT_VERIFY(status.ok());
}

////////////////////////////////////////////////////////////////////////////////

std::string MakeOutputFromRecordBatch(const std::shared_ptr<arrow::RecordBatch>& recordBatch)
{
    auto outputStream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto arrowWriter = arrow::ipc::MakeStreamWriter(outputStream, recordBatch->schema()).ValueOrDie();
    Verify(arrowWriter->WriteRecordBatch(*recordBatch));
    auto buffer = outputStream->Finish().ValueOrDie();
    return buffer->ToString();
}

std::string MakeIntegerArrow(const std::vector<int8_t>& data)
{
    arrow::Int8Builder builder;

    for (const auto& value : data) {
        Verify(builder.Append(value));
    }

    auto intArray = builder.Finish();

    auto arrowSchema = arrow::schema({arrow::field("integer", arrow::int8())});
    std::vector<std::shared_ptr<arrow::Array>> columns = {*intArray};
    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);
    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeOptionalIntegerArrow()
{
    arrow::Int8Builder builder;

    Verify(builder.Append(1));
    Verify(builder.AppendNull());
    Verify(builder.AppendNull());

    auto data = builder.Finish();

    auto arrowSchema = arrow::schema({arrow::field("opt", arrow::int8())});
    std::vector<std::shared_ptr<arrow::Array>> columns = {*data};
    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);
    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeBooleanArrow(const std::vector<bool>& data)
{
    arrow::BooleanBuilder builder;

    for (const auto& value : data) {
        Verify(builder.Append(value));
    }

    auto boolArray = builder.Finish();

    auto arrowSchema = arrow::schema({arrow::field("bool", arrow::boolean())});
    std::vector<std::shared_ptr<arrow::Array>> columns = {*boolArray};
    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);
    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeIntAndStringArrow(const std::vector<int8_t>& data, const std::vector<std::string>& stringData)
{
    arrow::Int8Builder builder;

    for (const auto& value : data) {
        Verify(builder.Append(value));
    }
    auto intArray = builder.Finish();

    arrow::StringBuilder stringBuilder;

    for (const auto& value : stringData) {
        Verify(stringBuilder.Append(value));
    }

    auto stringArray = stringBuilder.Finish();

    auto arrowSchema = arrow::schema({
        arrow::field("integer", arrow::int8()),
        arrow::field("string", arrow::binary()),
    });

    std::vector<std::shared_ptr<arrow::Array>> columns = {*intArray, *stringArray};
    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);

    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeIntListArrow(const std::vector<std::optional<std::vector<int32_t>>>& data)
{
    auto* pool = arrow::default_memory_pool();
    auto valueBuilder = std::make_shared<arrow::Int32Builder>(pool);
    auto listBuilder = std::make_unique<arrow::ListBuilder>(pool, valueBuilder);

    for (const auto& list : data) {
        if (list) {
            Verify(listBuilder->Append());
            for (const auto& value : *list) {
                Verify(valueBuilder->Append(value));
            }
        } else {
            Verify(listBuilder->AppendNull());
        }
    }

    auto arrowSchema = arrow::schema({arrow::field("list", listBuilder->type())});

    std::shared_ptr<arrow::Array> listArray;
    Verify(listBuilder->Finish(&listArray));
    std::vector<std::shared_ptr<arrow::Array>> columns = {listArray};

    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);

    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeStringListArrow(const std::vector<std::vector<std::string>>& data)
{
    auto* pool = arrow::default_memory_pool();

    auto valueBuilder = std::make_shared<arrow::StringBuilder>(pool);
    auto listBuilder = std::make_unique<arrow::ListBuilder>(pool, valueBuilder);

    for (const auto& list : data) {
        Verify(listBuilder->Append());
        for (const auto& value : list) {
            Verify(valueBuilder->Append(value));
        }
    }

    auto arrowSchema = arrow::schema({arrow::field("list", listBuilder->type())});

    std::shared_ptr<arrow::Array> listArray;
    Verify(listBuilder->Finish(&listArray));
    std::vector<std::shared_ptr<arrow::Array>> columns = {listArray};

    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);

    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeMapArrow(const std::vector<std::vector<int32_t>>& key, const std::vector<std::vector<int32_t>>& value)
{
    auto* pool = arrow::default_memory_pool();

    auto keyBuilder = std::make_shared<arrow::Int32Builder>(pool);
    auto valueBuilder = std::make_shared<arrow::UInt32Builder>(pool);
    auto mapBuilder = std::make_unique<arrow::MapBuilder>(pool, keyBuilder, valueBuilder);

    for (ssize_t mapIndex = 0; mapIndex < std::ssize(key); mapIndex++) {
        Verify(mapBuilder->Append());
        for (int valueNumber = 0; valueNumber < std::ssize(key[mapIndex]); valueNumber++) {
            Verify(keyBuilder->Append(key[mapIndex][valueNumber]));
            Verify(valueBuilder->Append(value[mapIndex][valueNumber]));
        }
    }

    auto arrowSchema = arrow::schema({arrow::field("map", mapBuilder->type())});

    std::shared_ptr<arrow::Array> mapArray;
    Verify(mapBuilder->Finish(&mapArray));
    std::vector<std::shared_ptr<arrow::Array>> columns = {mapArray};

    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);

    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeDictionaryArrow(bool addExtraValues = false)
{
    auto* pool = arrow::default_memory_pool();

    arrow::DictionaryBuilder<arrow::Int32Type> dictionaryBuilder(pool);

    std::vector<int32_t> values = {1, 2, 1};

    for (auto value : values) {
        Verify(dictionaryBuilder.Append(value));
    }

    if (addExtraValues) {
        arrow::Int32Builder builder;
        Verify(builder.Append(3));
        Verify(builder.Append(4));
        Verify(builder.Append(5));
        auto intArray = *builder.Finish();
        Verify(dictionaryBuilder.InsertMemoValues(*intArray));
    }

    auto arrowSchema = arrow::schema({arrow::field("integer", dictionaryBuilder.type())});

    std::shared_ptr<arrow::Array> array;
    Verify(dictionaryBuilder.Finish(&array));

    std::vector<std::shared_ptr<arrow::Array>> columns = {array};

    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);

    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeStructArrow(const std::vector<std::string>& stringData, const std::vector<int64_t>& intData)
{
    auto* pool = arrow::default_memory_pool();

    auto stringBuilder = std::make_shared<arrow::StringBuilder>(pool);
    auto intBuilder = std::make_shared<arrow::Int64Builder>(pool);

    std::vector<std::shared_ptr<arrow::Field>> fields = {
        std::make_shared<arrow::Field>("bar", std::make_shared<arrow::StringType>()),
        std::make_shared<arrow::Field>("foo", std::make_shared<arrow::Int64Type>())
    };

    arrow::StructBuilder structBuilder(
        std::make_shared<arrow::StructType>(fields),
        pool,
        {stringBuilder, intBuilder});

    for (int index = 0; index < std::ssize(stringData); index++) {
        Verify(structBuilder.Append());
        Verify(stringBuilder->Append(stringData[index]));
        Verify(intBuilder->Append(intData[index]));
    }

    std::shared_ptr<arrow::Schema> arrowSchema = arrow::schema({arrow::field("struct", structBuilder.type())});

    std::shared_ptr<arrow::Array> structArray;
    Verify(structBuilder.Finish(&structArray));
    std::vector<std::shared_ptr<arrow::Array>> columns = {structArray};

    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);

    return MakeOutputFromRecordBatch(recordBatch);
}

struct TTzRow
{
    ui16 DateValue = 0;
    ui32 DatetimeValue = 0;
    ui64 TimestampValue = 0;
    i32 Date32Value = 0;
    i64 Datetime64Value = 0;
    i64 Timestamp64Value = 0;
    ui16 TzIndex = 0;
};

std::string MakeTzTypeArrow(const std::vector<TTzRow>& dateValue)
{
    auto* pool = arrow::default_memory_pool();

    auto dateBuilder = std::make_shared<arrow::UInt16Builder>(pool);
    auto dateTzIndexBuilder = std::make_shared<arrow::UInt16Builder>(pool);
    std::vector<std::shared_ptr<arrow::Field>> dateFields = {
        std::make_shared<arrow::Field>("Timestamp", std::make_shared<arrow::UInt16Type>()),
        std::make_shared<arrow::Field>("TzIndex", std::make_shared<arrow::UInt16Type>()),
    };
    arrow::StructBuilder tzDateBuilder(
        std::make_shared<arrow::StructType>(dateFields),
        pool,
        {dateBuilder, dateTzIndexBuilder});

    auto datetimeBuilder = std::make_shared<arrow::UInt32Builder>(pool);
    auto datetimeTzIndexBuilder = std::make_shared<arrow::UInt16Builder>(pool);
    std::vector<std::shared_ptr<arrow::Field>> datetimeFields = {
        std::make_shared<arrow::Field>("Timestamp", std::make_shared<arrow::UInt32Type>()),
        std::make_shared<arrow::Field>("TzIndex", std::make_shared<arrow::UInt16Type>()),
    };
    arrow::StructBuilder tzDatetimeBuilder(
        std::make_shared<arrow::StructType>(datetimeFields),
        pool,
        {datetimeBuilder, datetimeTzIndexBuilder});

    auto timestampBuilder = std::make_shared<arrow::UInt64Builder>(pool);
    auto timestampTzIndexBuilder = std::make_shared<arrow::UInt16Builder>(pool);
    std::vector<std::shared_ptr<arrow::Field>> timestampFields = {
        std::make_shared<arrow::Field>("Timestamp", std::make_shared<arrow::UInt64Type>()),
        std::make_shared<arrow::Field>("TzIndex", std::make_shared<arrow::UInt16Type>()),
    };
    arrow::StructBuilder tzTimestampBuilder(
        std::make_shared<arrow::StructType>(timestampFields),
        pool,
        {timestampBuilder, timestampTzIndexBuilder});

    auto date32Builder = std::make_shared<arrow::Int32Builder>(pool);
    auto date32TzIndexBuilder = std::make_shared<arrow::UInt16Builder>(pool);
    std::vector<std::shared_ptr<arrow::Field>> date32Fields = {
        std::make_shared<arrow::Field>("Timestamp", std::make_shared<arrow::Int32Type>()),
        std::make_shared<arrow::Field>("TzIndex", std::make_shared<arrow::UInt16Type>()),
    };
    arrow::StructBuilder tzDate32Builder(
        std::make_shared<arrow::StructType>(date32Fields),
        pool,
        {date32Builder, date32TzIndexBuilder});

    auto datetime64Builder = std::make_shared<arrow::Int64Builder>(pool);
    auto datetime64TzIndexBuilder = std::make_shared<arrow::UInt16Builder>(pool);
    std::vector<std::shared_ptr<arrow::Field>> datetime64Fields = {
        std::make_shared<arrow::Field>("Timestamp", std::make_shared<arrow::Int64Type>()),
        std::make_shared<arrow::Field>("TzIndex", std::make_shared<arrow::UInt16Type>()),
    };
    arrow::StructBuilder tzDatetime64Builder(
        std::make_shared<arrow::StructType>(datetime64Fields),
        pool,
        {datetime64Builder, datetime64TzIndexBuilder});

    auto timestamp64Builder = std::make_shared<arrow::Int64Builder>(pool);
    auto timestamp64TzIndexBuilder = std::make_shared<arrow::UInt16Builder>(pool);
    std::vector<std::shared_ptr<arrow::Field>> timestamp64Fields = {
        std::make_shared<arrow::Field>("Timestamp", std::make_shared<arrow::Int64Type>()),
        std::make_shared<arrow::Field>("TzIndex", std::make_shared<arrow::UInt16Type>()),
    };
    arrow::StructBuilder tzTimestamp64Builder(
        std::make_shared<arrow::StructType>(timestamp64Fields),
        pool,
        {timestamp64Builder, timestamp64TzIndexBuilder});

    for (int index = 0; index < std::ssize(dateValue); index++) {
        Verify(tzDateBuilder.Append());
        Verify(dateBuilder->Append(dateValue[index].DateValue));
        Verify(dateTzIndexBuilder->Append(dateValue[index].TzIndex));

        Verify(tzDatetimeBuilder.Append());
        Verify(datetimeBuilder->Append(dateValue[index].DatetimeValue));
        Verify(datetimeTzIndexBuilder->Append(dateValue[index].TzIndex));

        Verify(tzTimestampBuilder.Append());
        Verify(timestampBuilder->Append(dateValue[index].TimestampValue));
        Verify(timestampTzIndexBuilder->Append(dateValue[index].TzIndex));

        Verify(tzDate32Builder.Append());
        Verify(date32Builder->Append(dateValue[index].Date32Value));
        Verify(date32TzIndexBuilder->Append(dateValue[index].TzIndex));

        Verify(tzDatetime64Builder.Append());
        Verify(datetime64Builder->Append(dateValue[index].Datetime64Value));
        Verify(datetime64TzIndexBuilder->Append(dateValue[index].TzIndex));

        Verify(tzTimestamp64Builder.Append());
        Verify(timestamp64Builder->Append(dateValue[index].Timestamp64Value));
        Verify(timestamp64TzIndexBuilder->Append(dateValue[index].TzIndex));
    }

    std::shared_ptr<arrow::Schema> arrowSchema = arrow::schema({
        arrow::field("tzDateColumn", tzDateBuilder.type()),
        arrow::field("tzDatetimeColumn", tzDatetimeBuilder.type()),
        arrow::field("tzTimestampColumn", tzTimestampBuilder.type()),
        arrow::field("tzDate32Column", tzDate32Builder.type()),
        arrow::field("tzDatetime64Column", tzDatetime64Builder.type()),
        arrow::field("tzTimestamp64Column", tzTimestamp64Builder.type()),
    });

    std::shared_ptr<arrow::Array> dateArray;
    Verify(tzDateBuilder.Finish(&dateArray));

    std::shared_ptr<arrow::Array> datetimeArray;
    Verify(tzDatetimeBuilder.Finish(&datetimeArray));

    std::shared_ptr<arrow::Array> timestampArray;
    Verify(tzTimestampBuilder.Finish(&timestampArray));

    std::shared_ptr<arrow::Array> date32Array;
    Verify(tzDate32Builder.Finish(&date32Array));

    std::shared_ptr<arrow::Array> datetime64Array;
    Verify(tzDatetime64Builder.Finish(&datetime64Array));

    std::shared_ptr<arrow::Array> timestamp64Array;
    Verify(tzTimestamp64Builder.Finish(&timestamp64Array));

    auto columns = std::vector{
        dateArray,
        datetimeArray,
        timestampArray,
        date32Array,
        datetime64Array,
        timestamp64Array
    };

    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);

    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeTzDateTypeArrow(const std::vector<int>& dateValue)
{
    auto* pool = arrow::default_memory_pool();

    auto dateBuilder = std::make_shared<arrow::UInt16Builder>(pool);
    auto dateTzNameBuilder = std::make_shared<arrow::BinaryBuilder>(pool);

    std::vector<std::shared_ptr<arrow::Field>> dateFields = {
        std::make_shared<arrow::Field>("Timestamp", std::make_shared<arrow::UInt16Type>()),
        std::make_shared<arrow::Field>("TzIndex", std::make_shared<arrow::BinaryType>()),
    };
    arrow::StructBuilder tzDateBuilder(
        std::make_shared<arrow::StructType>(dateFields),
        pool,
        {dateBuilder, dateTzNameBuilder});

    for (int index = 0; index < std::ssize(dateValue); index++) {
        Verify(tzDateBuilder.Append());
        Verify(dateBuilder->Append(dateValue[index]));
        Verify(dateTzNameBuilder->Append("Europe/Moscow"));
    }

    std::shared_ptr<arrow::Schema> arrowSchema = arrow::schema({
        arrow::field("tzDateColumn", tzDateBuilder.type()),
    });

    std::shared_ptr<arrow::Array> dateArray;
    Verify(tzDateBuilder.Finish(&dateArray));

    std::vector<std::shared_ptr<arrow::Array>> columns = {
        dateArray
    };

    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);

    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeTzTypesListArrow(const std::vector<std::vector<i64>>& dateColumn)
{
    auto* pool = arrow::default_memory_pool();

    auto dateBuilder = std::make_shared<arrow::UInt16Builder>(pool);
    auto dateTzIndexBuilder = std::make_shared<arrow::UInt16Builder>(pool);
    std::vector<std::shared_ptr<arrow::Field>> dateFields = {
        std::make_shared<arrow::Field>("Timestamp", std::make_shared<arrow::UInt16Type>()),
        std::make_shared<arrow::Field>("TzIndex", std::make_shared<arrow::UInt16Type>()),
    };
    arrow::StructBuilder tzDateBuilder(
        std::make_shared<arrow::StructType>(dateFields),
        pool,
        {dateBuilder, dateTzIndexBuilder});

    auto aa = std::make_shared<arrow::StructBuilder>(std::move(tzDateBuilder));

    auto listBuilder = std::make_unique<arrow::ListBuilder>(pool, aa);

    for (const auto& list : dateColumn) {
        Verify(listBuilder->Append());
        for (const auto& value : list) {
            Verify(aa->Append());
            Verify(dateBuilder->Append(value));
            Verify(dateTzIndexBuilder->Append(1));
        }
    }

    auto arrowSchema = arrow::schema({arrow::field("listOfTzTypes", listBuilder->type())});

    std::shared_ptr<arrow::Array> listArray;
    Verify(listBuilder->Finish(&listArray));
    std::vector<std::shared_ptr<arrow::Array>> columns = {listArray};

    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);

    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeDateArrow(
    const std::vector<i32>& date32Column,
    const std::vector<i64>& date64Column,
    const std::vector<i64>& timestampColumn)
{
    arrow::Date32Builder date32Builder;

    for (const auto& value : date32Column) {
        Verify(date32Builder.Append(value));
    }

    auto date32Array = date32Builder.Finish();

    arrow::Date64Builder date64Builder;

    for (const auto& value : date64Column) {
        Verify(date64Builder.Append(value));
    }

    auto date64Array = date64Builder.Finish();

    arrow::TimestampBuilder timestampBuilder(arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), arrow::default_memory_pool());

    for (const auto& value : timestampColumn) {
        Verify(timestampBuilder.Append(value));
    }

    auto timestampArray = timestampBuilder.Finish();

    auto arrowSchema = arrow::schema({
        arrow::field("date", arrow::date32()),
        arrow::field("datetime", arrow::date64()),
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MICRO)),
    });
    std::vector<std::shared_ptr<arrow::Array>> columns = {*date32Array, *date64Array, *timestampArray};
    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);
    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeDatetimeListArrow(const std::vector<std::vector<i64>>& date64Column)
{
    auto* pool = arrow::default_memory_pool();

    auto valueBuilder = std::make_shared<arrow::Date64Builder>(pool);
    auto listBuilder = std::make_unique<arrow::ListBuilder>(pool, valueBuilder);

    for (const auto& list : date64Column) {
        Verify(listBuilder->Append());
        for (const auto& value : list) {
            Verify(valueBuilder->Append(value));
        }
    }

    auto arrowSchema = arrow::schema({arrow::field("list", listBuilder->type())});

    std::shared_ptr<arrow::Array> listArray;
    Verify(listBuilder->Finish(&listArray));
    std::vector<std::shared_ptr<arrow::Array>> columns = {listArray};

    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);

    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeDecimalArrows(std::vector<TString> values, std::vector<std::tuple<int, int, int>> columnParameters)
{
    auto* pool = arrow::default_memory_pool();

    auto makeColumn = [&]<class TBuilder, class TType, class TValue>(int precision, int scale) {
        auto builder = std::make_shared<TBuilder>(std::make_shared<TType>(precision, scale), pool);
        for (const auto& value : values) {
            Verify(builder->Append(TValue(std::string(value))));
        }
        return builder->Finish().ValueOrDie();
    };

    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (const auto& [bitness, precision, scale] : columnParameters) {
        if (bitness == 128) {
            columns.push_back(makeColumn.template operator()<arrow::Decimal128Builder, arrow::Decimal128Type, arrow::Decimal128>(precision, scale));
        } else if (bitness == 256) {
            columns.push_back(makeColumn.template operator()<arrow::Decimal256Builder, arrow::Decimal256Type, arrow::Decimal256>(precision, scale));
        } else {
            YT_ABORT();
        }
    }

    arrow::FieldVector fields;
    for (const auto& [bitness, precision, scale] : columnParameters) {
        std::shared_ptr<arrow::DataType> type;
        if (bitness == 128) {
            type = std::make_shared<arrow::Decimal128Type>(precision, scale);
        } else if (bitness == 256) {
            type = std::make_shared<arrow::Decimal256Type>(precision, scale);
        } else {
            YT_ABORT();
        }
        fields.push_back(std::make_shared<arrow::Field>(Format("decimal%v_%v_%v", bitness, precision, scale), type));
    }

    auto recordBatch = arrow::RecordBatch::Make(arrow::schema(std::move(fields)), columns[0]->length(), columns);

    return MakeOutputFromRecordBatch(recordBatch);
}

std::string MakeDecimalListArrow(std::vector<TString> values)
{
    // Create a single column with one value, which is a list containing all the #values.
    // Type of the list is Decimal128(10, 3).
    auto* pool = arrow::default_memory_pool();
    auto decimalBuilder = std::make_shared<arrow::Decimal128Builder>(std::make_shared<arrow::Decimal128Type>(10, 3), pool);
    auto listBuilder = std::make_unique<arrow::ListBuilder>(pool, decimalBuilder);

    Verify(listBuilder->Append());
    for (const auto& value : values) {
        Verify(decimalBuilder->Append(arrow::Decimal128(std::string(value))));
    }
    std::shared_ptr<arrow::Array> listArray;
    Verify(listBuilder->Finish(&listArray));
    auto arrowSchema = arrow::schema({arrow::field("list", listArray->type())});
    std::vector<std::shared_ptr<arrow::Array>> columns = {listArray};
    auto recordBatch = arrow::RecordBatch::Make(arrowSchema, columns[0]->length(), columns);
    return MakeOutputFromRecordBatch(recordBatch);
}

void TestArrowParserWithDictionary(bool addExtraValues = false)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("integer", EValueType::Int64)
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeDictionaryArrow(addExtraValues);
    parser->Read(data);
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 3);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "integer")), 1);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(1, "integer")), 2);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(2, "integer")), 1);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TArrowParserTest, Simple)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("integer", EValueType::Int64)
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeIntegerArrow({1, 2, 3});
    parser->Read(data);
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 3);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "integer")), 1);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(1, "integer")), 2);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(2, "integer")), 3);
}

TEST(TArrowParserTest, Optional)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("opt", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)))
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeOptionalIntegerArrow();
    parser->Read(data);
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 3);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "opt")), 1);
    ASSERT_TRUE(IsNull(collectedRows.GetRowValue(1, "opt")));
    ASSERT_TRUE(IsNull(collectedRows.GetRowValue(2, "opt")));
}

TEST(TArrowParserTest, Dictionary)
{
    TestArrowParserWithDictionary(false);
    TestArrowParserWithDictionary(true);
}

TEST(TArrowParserTest, Bool)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("bool", EValueType::Boolean),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeBooleanArrow({true, false, true});
    parser->Read(data);
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 3);

    ASSERT_EQ(GetBoolean(collectedRows.GetRowValue(0, "bool")), true);
    ASSERT_EQ(GetBoolean(collectedRows.GetRowValue(1, "bool")), false);
    ASSERT_EQ(GetBoolean(collectedRows.GetRowValue(2, "bool")), true);
}

TEST(TArrowParserTest, String)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("integer", EValueType::Any),
        TColumnSchema("string", EValueType::String),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeIntAndStringArrow({1, 2, 3}, {"foo", "bar", "yt"});
    parser->Read(data);
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 3);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "integer")), 1);
    ASSERT_EQ(GetString(collectedRows.GetRowValue(0, "string")), "foo");

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(1, "integer")), 2);
    ASSERT_EQ(GetString(collectedRows.GetRowValue(1, "string")), "bar");

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(2, "integer")), 3);
    ASSERT_EQ(GetString(collectedRows.GetRowValue(2, "string")), "yt");
}


TString ConvertToYsonTextStringStable(const INodePtr& node, bool binary = false)
{
    TStringStream out;
    auto format = binary ? EYsonFormat::Binary : EYsonFormat::Text;
    TYsonWriter writer(&out, format);
    VisitTree(node, &writer, true, TAttributeFilter());
    writer.Flush();
    return out.Str();
}

TEST(TArrowParserTest, ListOfIntegers)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("list", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeIntListArrow({std::vector{1, 2, 3}, std::nullopt, std::vector{4, 5}});
    parser->Read(data);
    parser->Finish();

    auto firstNode = GetComposite(collectedRows.GetRowValue(0, "list"));
    ASSERT_EQ(ConvertToYsonTextStringStable(firstNode), "[1;2;3;]");

    ASSERT_EQ(EValueType::Null, collectedRows.GetRowValue(1, "list").Type);

    auto thirdNode = GetComposite(collectedRows.GetRowValue(2, "list"));
    ASSERT_EQ(ConvertToYsonTextStringStable(thirdNode), "[4;5;]");
}

TEST(TArrowParserTest, ListOfStrings)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("list", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeStringListArrow({{"foo", "bar"}, {"42", "universe"}});
    parser->Read(data);
    parser->Finish();

    auto firstNode =  GetComposite(collectedRows.GetRowValue(0, "list"));
    ASSERT_EQ(ConvertToYsonTextStringStable(firstNode), "[\"foo\";\"bar\";]");

    auto secondNode = GetComposite(collectedRows.GetRowValue(1, "list"));
    ASSERT_EQ(ConvertToYsonTextStringStable(secondNode), "[\"42\";\"universe\";]");
}

TEST(TArrowParserTest, Map)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema(
            "map",
            DictLogicalType(
                SimpleLogicalType(ESimpleLogicalValueType::Int64),
                SimpleLogicalType(ESimpleLogicalValueType::Uint64))),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeMapArrow({{1, 3}, {3}}, {{2, 2}, {2}});
    parser->Read(data);
    parser->Finish();

    auto firstNode = GetComposite(collectedRows.GetRowValue(0, "map"));
    ASSERT_EQ(ConvertToYsonTextStringStable(firstNode), "[[1;2u;];[3;2u;];]");

    auto secondNode = GetComposite(collectedRows.GetRowValue(1, "map"));
    ASSERT_EQ(ConvertToYsonTextStringStable(secondNode), "[[3;2u;];]");
}

TEST(TArrowParserTest, SeveralIntArrays)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("integer", EValueType::Int64),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);
    auto data = Format("%v%v%v", MakeIntegerArrow({1, 2, 3}), GetEos(), MakeIntegerArrow({5, 6}));

    parser->Read(data);
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 5);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "integer")), 1);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(1, "integer")), 2);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(2, "integer")), 3);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(3, "integer")), 5);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(4, "integer")), 6);
}

TEST(TArrowParserTest, Struct)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("struct", StructLogicalType({
            {"bar", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"foo", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        })),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    parser->Read(MakeStructArrow({"one", "two"}, {1, 2}));
    parser->Finish();

    auto firstNode = GetComposite(collectedRows.GetRowValue(0, "struct"));
    ASSERT_EQ(ConvertToYsonTextStringStable(firstNode), "[\"one\";1;]");

    auto secondNode = GetComposite(collectedRows.GetRowValue(1, "struct"));
    ASSERT_EQ(ConvertToYsonTextStringStable(secondNode), "[\"two\";2;]");
}

TEST(TArrowParserTest, StructError)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("struct", StructLogicalType({
            {"bar", SimpleLogicalType(ESimpleLogicalValueType::String)},
        })),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        parser->Read(MakeStructArrow({"one", "two"}, {1, 2})),
        std::exception,
        "The number of fields in the Arrow \"struct\" type does not match the number of fields in the YT \"struct\" type");
}

TEST(TArrowParserTest, DecimalVariousPrecisions)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("decimal128_10_3", DecimalLogicalType(10, 3)),
        TColumnSchema("decimal128_35_3", DecimalLogicalType(35, 3)),
        TColumnSchema("decimal128_38_3", DecimalLogicalType(38, 3)),
        TColumnSchema("decimal256_10_3", DecimalLogicalType(10, 3)),
        TColumnSchema("decimal256_35_3", DecimalLogicalType(35, 3)),
        TColumnSchema("decimal256_38_3", DecimalLogicalType(38, 3)),
        TColumnSchema("decimal256_76_3", DecimalLogicalType(76, 3)),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    std::vector<TString> values = {"3.141", "0.000", "-2.718", "9999999.999"};

    auto parser = CreateParserForArrow(&collectedRows);

    parser->Read(MakeDecimalArrows(values, {{128, 10, 3}, {128, 35, 3}, {128, 38, 3}, {256, 10, 3}, {256, 35, 3}, {256, 38, 3}, {256, 76, 3}}));
    parser->Finish();

    auto collectStrings = [&] (TStringBuf columnName) {
        std::vector<TString> result;
        for (size_t index = 0; index < values.size(); ++index) {
            result.push_back(collectedRows.GetRowValue(index, columnName).AsString());
        }
        return result;
    };

    std::vector<TString> expectedValues_10_3 =
        {"\x80\x00\x00\x00\x00\x00\x0c\x45"s, "\x80\x00\x00\x00\x00\x00\x00\x00"s, "\x7f\xff\xff\xff\xff\xff\xf5\x62"s, "\x80\x00\x00\x02\x54\x0b\xe3\xff"s};
    std::vector<TString> expectedValues_35_3 =
        {
            "\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0c\x45"s, "\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"s,
            "\x7f\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xf5\x62"s, "\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x54\x0b\xe3\xff"s,
        };
    std::vector<TString> expectedValues_38_3 =
        {
            "\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0c\x45"s, "\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"s,
            "\x7f\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xf5\x62"s, "\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x54\x0b\xe3\xff"s
        };
    std::vector<TString> expectedValues_76_3 =
        {
            "\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0c\x45"s,
            "\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"s,
            "\x7f\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xf5\x62"s,
            "\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x54\x0b\xe3\xff"s,
        };
    ASSERT_EQ(expectedValues_10_3, collectStrings("decimal128_10_3"));
    ASSERT_EQ(expectedValues_35_3, collectStrings("decimal128_35_3"));
    ASSERT_EQ(expectedValues_38_3, collectStrings("decimal128_38_3"));
    ASSERT_EQ(expectedValues_10_3, collectStrings("decimal256_10_3"));
    ASSERT_EQ(expectedValues_35_3, collectStrings("decimal256_35_3"));
    ASSERT_EQ(expectedValues_38_3, collectStrings("decimal256_38_3"));
    ASSERT_EQ(expectedValues_76_3, collectStrings("decimal256_76_3"));
}

TEST(TArrowParserTest, Datetime)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("date", ESimpleLogicalValueType::Date),
        TColumnSchema("datetime", ESimpleLogicalValueType::Datetime),
        TColumnSchema("timestamp", ESimpleLogicalValueType::Timestamp),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    parser->Read(MakeDateArrow({18367}, {1586966302000}, {1586966302504185}));
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 1);

    ASSERT_EQ(GetUint64(collectedRows.GetRowValue(0, "date")), 18367u);
    ASSERT_EQ(GetUint64(collectedRows.GetRowValue(0, "datetime")), 1586966302u);
    ASSERT_EQ(GetUint64(collectedRows.GetRowValue(0, "timestamp")), 1586966302504185u);
}

TEST(TArrowParserTest, Datetime64)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("date", ESimpleLogicalValueType::Date32),
        TColumnSchema("datetime", ESimpleLogicalValueType::Datetime64),
        TColumnSchema("timestamp", ESimpleLogicalValueType::Timestamp64),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    parser->Read(MakeDateArrow({-18367}, {-1586966302000}, {-1586966302504185}));
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 1);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "date")), -18367);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "datetime")), -1586966302);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "timestamp")), -1586966302504185);
}

TEST(TArrowParserTest, TzType)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("tzDateColumn", ESimpleLogicalValueType::TzDate),
        TColumnSchema("tzDatetimeColumn", ESimpleLogicalValueType::TzDatetime),
        TColumnSchema("tzTimestampColumn", ESimpleLogicalValueType::TzTimestamp),
        TColumnSchema("tzDate32Column", ESimpleLogicalValueType::TzDate32),
        TColumnSchema("tzDatetime64Column", ESimpleLogicalValueType::TzDatetime64),
        TColumnSchema("tzTimestamp64Column", ESimpleLogicalValueType::TzTimestamp64),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    TTzRow row = {42, 100, 1, 2, 3, 4, 1};
    parser->Read(MakeTzTypeArrow({row}));
    parser->Finish();

    TString stringValue;

    stringValue = GetString(collectedRows.GetRowValue(0, "tzDateColumn"));
    auto dateValue = NTzTypes::ParseTzValue<ui16>(stringValue);
    ASSERT_EQ(dateValue.first, row.DateValue);
    ASSERT_EQ(dateValue.second, "Europe/Moscow");

    stringValue = GetString(collectedRows.GetRowValue(0, "tzDatetimeColumn"));
    auto datetimeValue = NTzTypes::ParseTzValue<ui32>(stringValue);
    ASSERT_EQ(datetimeValue.first, row.DatetimeValue);
    ASSERT_EQ(datetimeValue.second, "Europe/Moscow");

    stringValue = GetString(collectedRows.GetRowValue(0, "tzTimestampColumn"));
    auto timestampValue = NTzTypes::ParseTzValue<ui64>(stringValue);
    ASSERT_EQ(timestampValue.first, row.TimestampValue);
    ASSERT_EQ(timestampValue.second, "Europe/Moscow");

    stringValue = GetString(collectedRows.GetRowValue(0, "tzDate32Column"));
    auto date32Value = NTzTypes::ParseTzValue<i32>(stringValue);
    ASSERT_EQ(date32Value.first, row.Date32Value);
    ASSERT_EQ(date32Value.second, "Europe/Moscow");

    stringValue = GetString(collectedRows.GetRowValue(0, "tzDatetime64Column"));
    auto datetime64Value = NTzTypes::ParseTzValue<i64>(stringValue);
    ASSERT_EQ(datetime64Value.first, row.Datetime64Value);
    ASSERT_EQ(datetime64Value.second, "Europe/Moscow");

    stringValue = GetString(collectedRows.GetRowValue(0, "tzTimestamp64Column"));
    auto timestamp64Value = NTzTypes::ParseTzValue<i64>(stringValue);
    ASSERT_EQ(timestamp64Value.first, row.Timestamp64Value);
    ASSERT_EQ(timestamp64Value.second, "Europe/Moscow");
}

TEST(TArrowParserTest, TzTypeName)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("tzDateColumn", ESimpleLogicalValueType::TzDate),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    parser->Read(MakeTzDateTypeArrow({42}));
    parser->Finish();

    TString stringValue;

    stringValue = GetString(collectedRows.GetRowValue(0, "tzDateColumn"));
    auto dateValue = NTzTypes::ParseTzValue<ui16>(stringValue);
    ASSERT_EQ(dateValue.first, 42);
    ASSERT_EQ(dateValue.second, "Europe/Moscow");
}

TEST(TArrowParserTest, WrongTzIndex)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("tzDateColumn", ESimpleLogicalValueType::TzDate),
        TColumnSchema("tzDatetimeColumn", ESimpleLogicalValueType::TzDatetime),
        TColumnSchema("tzTimestampColumn", ESimpleLogicalValueType::TzTimestamp),
        TColumnSchema("tzDate32Column", ESimpleLogicalValueType::TzDate32),
        TColumnSchema("tzDatetime64Column", ESimpleLogicalValueType::TzDatetime64),
        TColumnSchema("tzTimestamp64Column", ESimpleLogicalValueType::TzTimestamp64),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    TTzRow row = {42, 100, 1, 2, 3, 4, 1000};
    EXPECT_THROW_WITH_SUBSTRING(parser->Read(MakeTzTypeArrow({row})), "Failed to parse column");
}

TEST(TArrowParserTest, WrongTzType)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("tzDateColumn", ESimpleLogicalValueType::TzDatetime),
        TColumnSchema("tzDatetimeColumn", ESimpleLogicalValueType::TzDatetime),
        TColumnSchema("tzTimestampColumn", ESimpleLogicalValueType::TzTimestamp),
        TColumnSchema("tzDate32Column", ESimpleLogicalValueType::TzDate32),
        TColumnSchema("tzDatetime64Column", ESimpleLogicalValueType::TzDatetime64),
        TColumnSchema("tzTimestamp64Column", ESimpleLogicalValueType::TzTimestamp64),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    TTzRow row = {42, 100, 1, 2, 3, 4, 10};
    EXPECT_THROW_WITH_SUBSTRING(parser->Read(MakeTzTypeArrow({row})), "Failed to parse column");
}

TEST(TArrowParserTest, ListOfTzTypes)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("listOfTzTypes", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::TzDate))),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeTzTypesListArrow({{42}});
    parser->Read(data);
    parser->Finish();

    auto firstNode = GetComposite(collectedRows.GetRowValue(0, "listOfTzTypes"));
    auto result = ConvertToYsonTextStringStable(firstNode, true);

    auto tzString = std::string_view(result.begin() + 3, result.end() - 2);

    ASSERT_EQ(tzString, NTzTypes::MakeTzString<ui16>(42, NTzTypes::GetTzName(1)));
}

TEST(TArrowParserTest, ListOfDatetimes)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("list", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Datetime64))),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeDatetimeListArrow({{18367000, 1586966302000}, {}});
    parser->Read(data);
    parser->Finish();

    auto firstNode = GetComposite(collectedRows.GetRowValue(0, "list"));
    ASSERT_EQ(ConvertToYsonTextStringStable(firstNode), "[18367;1586966302;]");

    auto secondNode = GetComposite(collectedRows.GetRowValue(1, "list"));
    ASSERT_EQ(ConvertToYsonTextStringStable(secondNode), "[]");
}

TEST(TArrowParserTest, ListOfDecimals)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("list", ListLogicalType(DecimalLogicalType(10, 3))),
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    std::vector<TString> values = {"3.141", "0.000", "-2.718", "9999999.999"};

    auto parser = CreateParserForArrow(&collectedRows);

    parser->Read(MakeDecimalListArrow(values));
    parser->Finish();

    auto firstList = ConvertTo<std::vector<TString>>(GetComposite(collectedRows.GetRowValue(0, "list")));
    std::vector<TString> secondList = {
        "\x80\x00\x00\x00\x00\x00\x0c\x45"s, "\x80\x00\x00\x00\x00\x00\x00\x00"s,
        "\x7f\xff\xff\xff\xff\xff\xf5\x62"s, "\x80\x00\x00\x02\x54\x0b\xe3\xff"s
    };
    ASSERT_EQ(firstList, secondList);
}

TEST(TArrowParserTest, BlockingInput)
{
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("integer", EValueType::Int64)
    });

    TCollectingValueConsumer collectedRows(tableSchema);

    auto parser = CreateParserForArrow(&collectedRows);

    auto data = MakeIntegerArrow({1, 2, 3});
    for (auto i : data) {
        std::string s(1, i);
        parser->Read(s);
    }
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 3);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "integer")), 1);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(1, "integer")), 2);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(2, "integer")), 3);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFormats
