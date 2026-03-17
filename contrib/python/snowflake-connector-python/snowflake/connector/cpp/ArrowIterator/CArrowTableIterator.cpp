//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#include "CArrowTableIterator.hpp"
#include "SnowflakeType.hpp"
#include "Python/Common.hpp"
#include "Util/time.hpp"
#include <memory>
#include <string>
#include <vector>

namespace sf
{

/**
 * This function is to make sure the arrow table can be successfully converted to pandas dataframe
 * using arrow's to_pandas method. Since some Snowflake arrow columns are not supported, this method
 * can map those to supported ones.
 * Specifically,
 *    All Snowflake fixed number with scale > 0 (expect decimal) will be converted to Arrow float64/double column
 *    All Snowflake time columns will be converted to Arrow Time column with unit = second, milli, or, micro.
 *    All Snowflake timestamp columns will be converted to Arrow timestamp columns
 *    Specifically,
 *    timestampntz will be converted to Arrow timestamp with UTC
 *    timestampltz will be converted to Arrow timestamp with session time zone
 *    timestamptz will be converted to Arrow timestamp with UTC
 *    Since Arrow timestamp use int64_t internally so it may be out of range for small and large timestamps
 */
void CArrowTableIterator::reconstructRecordBatches()
{
  // Type conversion, the code needs to be optimized
  for (unsigned int batchIdx = 0; batchIdx <  m_cRecordBatches->size(); batchIdx++)
  {
    std::shared_ptr<arrow::RecordBatch> currentBatch = (*m_cRecordBatches)[batchIdx];
    std::shared_ptr<arrow::Schema> schema = currentBatch->schema();
    // These copies will be used if rebuilding the RecordBatch if necessary
    bool needsRebuild = false;
    std::vector<std::shared_ptr<arrow::Field>> futureFields;
    std::vector<std::shared_ptr<arrow::Array>> futureColumns;

    for (int colIdx = 0; colIdx < currentBatch->num_columns(); colIdx++)
    {
      std::shared_ptr<arrow::Array> columnArray = currentBatch->column(colIdx);
      std::shared_ptr<arrow::Field> field = schema->field(colIdx);
      std::shared_ptr<arrow::DataType> dt = field->type();
      std::shared_ptr<const arrow::KeyValueMetadata> metaData = field->metadata();
      SnowflakeType::Type st = SnowflakeType::snowflakeTypeFromString(
          metaData->value(metaData->FindKey("logicalType")));

      // reconstruct columnArray in place
      switch (st)
      {
        case SnowflakeType::Type::FIXED:
        {
          int scale = metaData
                          ? std::stoi(metaData->value(metaData->FindKey("scale")))
                          : 0;
          if (scale > 0 && dt->id() != arrow::Type::type::DECIMAL)
          {
            logger->debug(
              __FILE__,
              __func__,
              __LINE__,
              "Convert fixed number column to double column, column scale %d, column type id: %d",
              scale,
              dt->id()
            );
            convertScaledFixedNumberColumn(
                batchIdx,
                colIdx,
                field,
                columnArray,
                scale,
                futureFields,
                futureColumns,
                needsRebuild
            );
          }
          break;
        }

        case SnowflakeType::Type::ANY:
        case SnowflakeType::Type::ARRAY:
        case SnowflakeType::Type::BOOLEAN:
        case SnowflakeType::Type::CHAR:
        case SnowflakeType::Type::OBJECT:
        case SnowflakeType::Type::BINARY:
        case SnowflakeType::Type::VARIANT:
        case SnowflakeType::Type::TEXT:
        case SnowflakeType::Type::REAL:
        case SnowflakeType::Type::DATE:
        {
          // Do not need to convert
          break;
        }

        case SnowflakeType::Type::TIME:
        {
          int scale = metaData
                          ? std::stoi(metaData->value(metaData->FindKey("scale")))
                          : 9;

          convertTimeColumn(batchIdx, colIdx, field, columnArray, scale, futureFields, futureColumns, needsRebuild);
          break;
        }

        case SnowflakeType::Type::TIMESTAMP_NTZ:
        {
          int scale = metaData
                          ? std::stoi(metaData->value(metaData->FindKey("scale")))
                          : 9;

          convertTimestampColumn(batchIdx, colIdx, field, columnArray, scale, futureFields, futureColumns, needsRebuild);
          break;
        }

        case SnowflakeType::Type::TIMESTAMP_LTZ:
        {
          int scale = metaData
                          ? std::stoi(metaData->value(metaData->FindKey("scale")))
                          : 9;

          convertTimestampColumn(batchIdx, colIdx, field, columnArray, scale, futureFields, futureColumns, needsRebuild, m_timezone);
          break;
        }

        case SnowflakeType::Type::TIMESTAMP_TZ:
        {
          int scale = metaData
                          ? std::stoi(metaData->value(metaData->FindKey("scale")))
                          : 9;
          int byteLength =
            metaData
                ? std::stoi(metaData->value(metaData->FindKey("byteLength")))
                : 16;

          convertTimestampTZColumn(batchIdx, colIdx, field, columnArray, scale, byteLength, futureFields, futureColumns, needsRebuild, m_timezone);
          break;
        }

        default:
        {
          std::string errorInfo = Logger::formatString(
              "[Snowflake Exception] unknown snowflake data type : %s",
              metaData->value(metaData->FindKey("logicalType")).c_str());
          logger->error(__FILE__, __func__, __LINE__, errorInfo.c_str());
          PyErr_SetString(PyExc_Exception, errorInfo.c_str());
          return;
        }
      }
    }

    if (needsRebuild)
    {
      std::shared_ptr<arrow::Schema> futureSchema = arrow::schema(futureFields, schema->metadata());
      (*m_cRecordBatches)[batchIdx] = arrow::RecordBatch::Make(futureSchema, currentBatch->num_rows(), futureColumns);
    }
  }
}

CArrowTableIterator::CArrowTableIterator(
PyObject* context,
std::vector<std::shared_ptr<arrow::RecordBatch>>* batches,
const bool number_to_decimal
)
: CArrowIterator(batches),
m_context(context),
m_pyTableObjRef(nullptr),
m_convert_number_to_decimal(number_to_decimal)
{
  py::UniqueRef tz(PyObject_GetAttrString(m_context, "_timezone"));
  PyArg_Parse(tz.get(), "s", &m_timezone);
}

std::shared_ptr<ReturnVal> CArrowTableIterator::next()
{
  bool firstDone = this->convertRecordBatchesToTable();
  if (firstDone && m_cTable)
  {
    m_pyTableObjRef.reset(arrow::py::wrap_table(m_cTable));
    return std::make_shared<ReturnVal>(m_pyTableObjRef.get(), nullptr);
  }
  else
  {
    return std::make_shared<ReturnVal>(Py_None, nullptr);
  }
}

void CArrowTableIterator::replaceColumn(
    const unsigned int batchIdx,
    const int colIdx,
    const std::shared_ptr<arrow::Field>& newField,
    const std::shared_ptr<arrow::Array>& newColumn,
    std::vector<std::shared_ptr<arrow::Field>>& futureFields,
    std::vector<std::shared_ptr<arrow::Array>>& futureColumns,
    bool& needsRebuild)
{
  // replace the targeted column
  if (needsRebuild == false)
  {
    // First time of modifying batches, we have to make a deep copy of fields and columns
    std::shared_ptr<arrow::RecordBatch> currentBatch = (*m_cRecordBatches)[batchIdx];
    futureFields = currentBatch->schema()->fields();
    futureColumns = currentBatch->columns();
    needsRebuild = true;
  }
  futureFields[colIdx] = newField;
  futureColumns[colIdx] = newColumn;
}

template <typename T>
double CArrowTableIterator::convertScaledFixedNumberToDouble(
  const unsigned int scale,
  T originalValue
)
{
  if (scale < 9)
  {
    // simply use divide to convert decimal value in double
    return (double) originalValue / sf::internal::powTenSB4[scale];
  }
  else
  {
    // when scale is large, convert the value to string first and then convert it to double
    // otherwise, it may loss precision
    std::string valStr = std::to_string(originalValue);
    int negative = valStr.at(0) == '-' ? 1:0;
    unsigned int digits = valStr.length() - negative;
    if (digits <= scale)
    {
      int numOfZeroes = scale - digits + 1;
      valStr.insert(negative, std::string(numOfZeroes, '0'));
    }
    valStr.insert(valStr.length() - scale, ".");
    std::size_t offset = 0;
    return std::stod(valStr, &offset);
  }
}

void CArrowTableIterator::convertScaledFixedNumberColumn(
  const unsigned int batchIdx,
  const int colIdx,
  const std::shared_ptr<arrow::Field> field,
  const std::shared_ptr<arrow::Array> columnArray,
  const unsigned int scale,
  std::vector<std::shared_ptr<arrow::Field>>& futureFields,
  std::vector<std::shared_ptr<arrow::Array>>& futureColumns,
  bool& needsRebuild
)
{
// Convert scaled fixed number to either Double, or Decimal based on setting
  if (m_convert_number_to_decimal){
    convertScaledFixedNumberColumnToDecimalColumn(
      batchIdx,
      colIdx,
      field,
      columnArray,
      scale,
      futureFields,
      futureColumns,
      needsRebuild
      );
  } else {
    convertScaledFixedNumberColumnToDoubleColumn(
      batchIdx,
      colIdx,
      field,
      columnArray,
      scale,
      futureFields,
      futureColumns,
      needsRebuild
      );
  }
}

void CArrowTableIterator::convertScaledFixedNumberColumnToDecimalColumn(
  const unsigned int batchIdx,
  const int colIdx,
  const std::shared_ptr<arrow::Field> field,
  const std::shared_ptr<arrow::Array> columnArray,
  const unsigned int scale,
  std::vector<std::shared_ptr<arrow::Field>>& futureFields,
  std::vector<std::shared_ptr<arrow::Array>>& futureColumns,
  bool& needsRebuild
)
{
  // Convert to decimal columns
  const std::shared_ptr<arrow::DataType> field_type = field->type();
  const std::shared_ptr<arrow::DataType> destType = arrow::decimal128(38, scale);
  std::shared_ptr<arrow::Field> doubleField = std::make_shared<arrow::Field>(
      field->name(), destType, field->nullable());
  arrow::Decimal128Builder builder(destType, m_pool);
  arrow::Status ret;
  for(int64_t rowIdx = 0; rowIdx < columnArray->length(); rowIdx++)
  {
    if (columnArray->IsValid(rowIdx))
    {
      arrow::Decimal128 val;
      switch (field_type->id())
      {
        case arrow::Type::type::INT8:
        {
          auto originalVal = std::static_pointer_cast<arrow::Int8Array>(columnArray)->Value(rowIdx);
          val = arrow::Decimal128(originalVal);
          break;
        }
        case arrow::Type::type::INT16:
        {
          auto originalVal = std::static_pointer_cast<arrow::Int16Array>(columnArray)->Value(rowIdx);
          val = arrow::Decimal128(originalVal);
          break;
        }
        case arrow::Type::type::INT32:
        {
          auto originalVal = std::static_pointer_cast<arrow::Int32Array>(columnArray)->Value(rowIdx);
          val = arrow::Decimal128(originalVal);
          break;
        }
        case arrow::Type::type::INT64:
        {
          auto originalVal = std::static_pointer_cast<arrow::Int64Array>(columnArray)->Value(rowIdx);
          val = arrow::Decimal128(originalVal);
          break;
        }
        default:
          std::string errorInfo = Logger::formatString(
              "[Snowflake Exception] unknown arrow internal data type(%d) "
              "for FIXED data",
              field_type->id());
          logger->error(__FILE__, __func__, __LINE__, errorInfo.c_str());
          return;
      }
      ret = builder.Append(val);
    }
    else
    {
      ret = builder.AppendNull();
    }
    SF_CHECK_ARROW_RC(ret,
      "[Snowflake Exception] arrow failed to append Decimal value: internal data type(%d), errorInfo: %s",
      field_type->id(),  ret.message().c_str());
  }

  std::shared_ptr<arrow::Array> doubleArray;
  ret = builder.Finish(&doubleArray);
  SF_CHECK_ARROW_RC(ret,
    "[Snowflake Exception] arrow failed to finish Decimal array, errorInfo: %s",
    ret.message().c_str());

  // replace the targeted column
  replaceColumn(batchIdx, colIdx, doubleField, doubleArray, futureFields, futureColumns, needsRebuild);
}

void CArrowTableIterator::convertScaledFixedNumberColumnToDoubleColumn(
  const unsigned int batchIdx,
  const int colIdx,
  const std::shared_ptr<arrow::Field> field,
  const std::shared_ptr<arrow::Array> columnArray,
  const unsigned int scale,
  std::vector<std::shared_ptr<arrow::Field>>& futureFields,
  std::vector<std::shared_ptr<arrow::Array>>& futureColumns,
  bool& needsRebuild
)
{
  // Convert to arrow double/float64 column
  std::shared_ptr<arrow::Field> doubleField = std::make_shared<arrow::Field>(
      field->name(), arrow::float64(), field->nullable());
  arrow::DoubleBuilder builder(m_pool);
  arrow::Status ret;
  auto dt = field->type();
  for(int64_t rowIdx = 0; rowIdx < columnArray->length(); rowIdx++)
  {
    if (columnArray->IsValid(rowIdx))
    {
      double val;
      switch (dt->id())
      {
        case arrow::Type::type::INT8:
        {
          auto originalVal = std::static_pointer_cast<arrow::Int8Array>(columnArray)->Value(rowIdx);
          val = convertScaledFixedNumberToDouble(scale, originalVal);
          break;
        }
        case arrow::Type::type::INT16:
        {
          auto originalVal = std::static_pointer_cast<arrow::Int16Array>(columnArray)->Value(rowIdx);
          val = convertScaledFixedNumberToDouble(scale, originalVal);
          break;
        }
        case arrow::Type::type::INT32:
        {
          auto originalVal = std::static_pointer_cast<arrow::Int32Array>(columnArray)->Value(rowIdx);
          val = convertScaledFixedNumberToDouble(scale, originalVal);
          break;
        }
        case arrow::Type::type::INT64:
        {
          auto originalVal = std::static_pointer_cast<arrow::Int64Array>(columnArray)->Value(rowIdx);
          val = convertScaledFixedNumberToDouble(scale, originalVal);
          break;
        }
        default:
          std::string errorInfo = Logger::formatString(
              "[Snowflake Exception] unknown arrow internal data type(%d) "
              "for FIXED data",
              dt->id());
          logger->error(__FILE__, __func__, __LINE__, errorInfo.c_str());
          return;
      }
      ret = builder.Append(val);
    }
    else
    {
      ret = builder.AppendNull();
    }
    SF_CHECK_ARROW_RC(ret,
      "[Snowflake Exception] arrow failed to append Double value: internal data type(%d), errorInfo: %s",
      dt->id(),  ret.message().c_str());
  }

  std::shared_ptr<arrow::Array> doubleArray;
  ret = builder.Finish(&doubleArray);
  SF_CHECK_ARROW_RC(ret,
    "[Snowflake Exception] arrow failed to finish Double array, errorInfo: %s",
    ret.message().c_str());

  // replace the targeted column
  replaceColumn(batchIdx, colIdx, doubleField, doubleArray, futureFields, futureColumns, needsRebuild);
}

void CArrowTableIterator::convertTimeColumn(
  const unsigned int batchIdx,
  const int colIdx,
  const std::shared_ptr<arrow::Field> field,
  const std::shared_ptr<arrow::Array> columnArray,
  const int scale,
  std::vector<std::shared_ptr<arrow::Field>>& futureFields,
  std::vector<std::shared_ptr<arrow::Array>>& futureColumns,
  bool& needsRebuild
)
{
  std::shared_ptr<arrow::Field> tsField;
  std::shared_ptr<arrow::Array> tsArray;
  arrow::Status ret;
  auto dt = field->type();
  // Convert to arrow time column
  if (scale == 0)
  {
    auto timeType = arrow::time32(arrow::TimeUnit::SECOND);
    tsField = std::make_shared<arrow::Field>(
      field->name(), timeType, field->nullable());
    arrow::Time32Builder builder(timeType, m_pool);


    for(int64_t rowIdx = 0; rowIdx < columnArray->length(); rowIdx++)
    {
      if (columnArray->IsValid(rowIdx))
      {
        int32_t originalVal = std::static_pointer_cast<arrow::Int32Array>(columnArray)->Value(rowIdx);
        // unit is second
        ret = builder.Append(originalVal);
      }
      else
      {
        ret = builder.AppendNull();
      }
      SF_CHECK_ARROW_RC(ret,
        "[Snowflake Exception] arrow failed to append value: internal data type(%d)"
        ", errorInfo: %s",
        dt->id(), ret.message().c_str());
    }

    ret = builder.Finish(&tsArray);
    SF_CHECK_ARROW_RC(ret,
      "[Snowflake Exception] arrow failed to finish array, errorInfo: %s",
      ret.message().c_str());
  }
  else if (scale <= 3)
  {
    auto timeType = arrow::time32(arrow::TimeUnit::MILLI);
    tsField = std::make_shared<arrow::Field>(
      field->name(), timeType, field->nullable());
    arrow::Time32Builder builder(timeType, m_pool);

    arrow::Status ret;
    for(int64_t rowIdx = 0; rowIdx < columnArray->length(); rowIdx++)
    {
      if (columnArray->IsValid(rowIdx))
      {
        int32_t val = std::static_pointer_cast<arrow::Int32Array>(columnArray)->Value(rowIdx)
          * sf::internal::powTenSB4[3 - scale];
        // unit is millisecond
        ret = builder.Append(val);
      }
      else
      {
        ret = builder.AppendNull();
      }
      SF_CHECK_ARROW_RC(ret,
        "[Snowflake Exception] arrow failed to append value: internal data type(%d)"
        ", errorInfo: %s",
        dt->id(), ret.message().c_str());
    }

    ret = builder.Finish(&tsArray);
    SF_CHECK_ARROW_RC(ret,
      "[Snowflake Exception] arrow failed to finish array, errorInfo: %s",
      ret.message().c_str());
  }
  else if (scale <= 6)
  {
    auto timeType = arrow::time64(arrow::TimeUnit::MICRO);
    tsField = std::make_shared<arrow::Field>(
      field->name(), timeType, field->nullable());
    arrow::Time64Builder builder(timeType, m_pool);

    arrow::Status ret;
    for(int64_t rowIdx = 0; rowIdx < columnArray->length(); rowIdx++)
    {
      if (columnArray->IsValid(rowIdx))
      {
        int64_t val;
        switch (dt->id())
        {
          case arrow::Type::type::INT32:
            val = std::static_pointer_cast<arrow::Int32Array>(columnArray)->Value(rowIdx);
            break;
          case arrow::Type::type::INT64:
            val = std::static_pointer_cast<arrow::Int64Array>(columnArray)->Value(rowIdx);
            break;
          default:
            std::string errorInfo = Logger::formatString(
                "[Snowflake Exception] unknown arrow internal data type(%d) "
                "for FIXED data",
                dt->id());
            logger->error(__FILE__, __func__, __LINE__, errorInfo.c_str());
            return;
        }
        val *= sf::internal::powTenSB4[6 - scale];
        // unit is microsecond
        ret = builder.Append(val);
      }
      else
      {
        ret = builder.AppendNull();
      }
      SF_CHECK_ARROW_RC(ret,
        "[Snowflake Exception] arrow failed to append value: internal data type(%d), errorInfo: %s",
        dt->id(),  ret.message().c_str());
    }

    ret = builder.Finish(&tsArray);
    SF_CHECK_ARROW_RC(ret,
      "[Snowflake Exception] arrow failed to finish array, errorInfo: %s",
      ret.message().c_str());
  }
  else
  {
    // Note: Python/Pandas Time does not support nanoseconds,
    // So truncate the time values to microseconds
    auto timeType = arrow::time64(arrow::TimeUnit::MICRO);
    tsField = std::make_shared<arrow::Field>(
      field->name(), timeType, field->nullable());
    arrow::Time64Builder builder(timeType, m_pool);

    arrow::Status ret;
    for(int64_t rowIdx = 0; rowIdx < columnArray->length(); rowIdx++)
    {
      if (columnArray->IsValid(rowIdx))
      {
        int64_t val;
        switch (dt->id())
        {
          case arrow::Type::type::INT32:
            val = std::static_pointer_cast<arrow::Int32Array>(columnArray)->Value(rowIdx);
            break;
          case arrow::Type::type::INT64:
            val = std::static_pointer_cast<arrow::Int64Array>(columnArray)->Value(rowIdx);
            break;
          default:
            std::string errorInfo = Logger::formatString(
                "[Snowflake Exception] unknown arrow internal data type(%d) "
                "for FIXED data",
                dt->id());
            logger->error(__FILE__, __func__, __LINE__, errorInfo.c_str());
            return;
        }
        val /= sf::internal::powTenSB4[scale - 6];
        // unit is microsecond
        ret = builder.Append(val);
      }
      else
      {
        ret = builder.AppendNull();
      }
      SF_CHECK_ARROW_RC(ret,
        "[Snowflake Exception] arrow failed to append value: internal data type(%d), errorInfo: %s",
        dt->id(),  ret.message().c_str());
    }

    ret = builder.Finish(&tsArray);
    SF_CHECK_ARROW_RC(ret,
      "[Snowflake Exception] arrow failed to finish array, errorInfo: %s",
      ret.message().c_str());
  }

  // replace the targeted column
  replaceColumn(batchIdx, colIdx, tsField, tsArray, futureFields, futureColumns, needsRebuild);
}

void CArrowTableIterator::convertTimestampColumn(
  const unsigned int batchIdx,
  const int colIdx,
  const std::shared_ptr<arrow::Field> field,
  const std::shared_ptr<arrow::Array> columnArray,
  const int scale,
  std::vector<std::shared_ptr<arrow::Field>>& futureFields,
  std::vector<std::shared_ptr<arrow::Array>>& futureColumns,
  bool& needsRebuild,
  const std::string timezone
)
{
  std::shared_ptr<arrow::Field> tsField;
  std::shared_ptr<arrow::Array> tsArray;
  arrow::Status ret;
  std::shared_ptr<arrow::DataType> timeType;
  auto dt = field->type();
  // Convert to arrow time column
  if (scale == 0)
  {
    if (!timezone.empty())
    {
      timeType = arrow::timestamp(arrow::TimeUnit::SECOND, timezone);
    }
    else
    {
      timeType = arrow::timestamp(arrow::TimeUnit::SECOND);
    }
    tsField = std::make_shared<arrow::Field>(
      field->name(), timeType, field->nullable());
    arrow::TimestampBuilder builder(timeType, m_pool);


    for(int64_t rowIdx = 0; rowIdx < columnArray->length(); rowIdx++)
    {
      if (columnArray->IsValid(rowIdx))
      {
        int64_t originalVal = std::static_pointer_cast<arrow::Int64Array>(columnArray)->Value(rowIdx);
        // unit is second
        ret = builder.Append(originalVal);
      }
      else
      {
        ret = builder.AppendNull();
      }
      SF_CHECK_ARROW_RC(ret,
        "[Snowflake Exception] arrow failed to append value: internal data type(%d), errorInfo: %s",
        dt->id(),  ret.message().c_str());
    }

    ret = builder.Finish(&tsArray); SF_CHECK_ARROW_RC(ret,
      "[Snowflake Exception] arrow failed to finish array, errorInfo: %s",
      ret.message().c_str());
  }
  else if (scale <= 3)
  {
    if (!timezone.empty())
    {
      timeType = arrow::timestamp(arrow::TimeUnit::MILLI, timezone);
    }
    else
    {
      timeType = arrow::timestamp(arrow::TimeUnit::MILLI);
    }
    tsField = std::make_shared<arrow::Field>(
      field->name(), timeType, field->nullable());
    arrow::TimestampBuilder builder(timeType, m_pool);

    arrow::Status ret;
    for(int64_t rowIdx = 0; rowIdx < columnArray->length(); rowIdx++)
    {
      if (columnArray->IsValid(rowIdx))
      {
        int64_t val = std::static_pointer_cast<arrow::Int64Array>(columnArray)->Value(rowIdx)
          * sf::internal::powTenSB4[3 - scale];
        // unit is millisecond
        ret = builder.Append(val);
      }
      else
      {
        ret = builder.AppendNull();
      }
      SF_CHECK_ARROW_RC(ret,
        "[Snowflake Exception] arrow failed to append value: internal data type(%d), errorInfo: %s",
        dt->id(),  ret.message().c_str());
    }

    ret = builder.Finish(&tsArray);
    SF_CHECK_ARROW_RC(ret,
      "[Snowflake Exception] arrow failed to finish array, errorInfo: %s",
      ret.message().c_str());
  }
  else if (scale <= 6)
  {
    if (!timezone.empty())
    {
      timeType = arrow::timestamp(arrow::TimeUnit::MICRO, timezone);
    }
    else
    {
      timeType = arrow::timestamp(arrow::TimeUnit::MICRO);
    }
    tsField = std::make_shared<arrow::Field>(
      field->name(), timeType, field->nullable());
    arrow::TimestampBuilder builder(timeType, m_pool);

    arrow::Status ret;
    for(int64_t rowIdx = 0; rowIdx < columnArray->length(); rowIdx++)
    {
      if (columnArray->IsValid(rowIdx))
      {
        int64_t val;
        switch (dt->id())
        {
          case arrow::Type::type::INT64:
            val = std::static_pointer_cast<arrow::Int64Array>(columnArray)->Value(rowIdx);
            break;
          default:
            std::string errorInfo = Logger::formatString(
                "[Snowflake Exception] unknown arrow internal data type(%d) "
                "for FIXED data",
                dt->id());
            logger->error(__FILE__, __func__, __LINE__, errorInfo.c_str());
            return;
        }
        val *= sf::internal::powTenSB4[6 - scale];
        // unit is microsecond
        ret = builder.Append(val);
      }
      else
      {
        ret = builder.AppendNull();
      }
      SF_CHECK_ARROW_RC(ret,
        "[Snowflake Exception] arrow failed to append value: internal data type(%d), errorInfo: %s",
        dt->id(),  ret.message().c_str());
    }

    ret = builder.Finish(&tsArray);
    SF_CHECK_ARROW_RC(ret,
      "[Snowflake Exception] arrow failed to finish array, errorInfo: %s",
      ret.message().c_str());
  }
  else
  {
     bool has_overflow_to_downscale = false;
     if (dt->id() == arrow::Type::type::STRUCT)
     {
         auto structArray = std::dynamic_pointer_cast<arrow::StructArray>(columnArray);
         for(int64_t rowIdx = 0; rowIdx < columnArray->length(); rowIdx++)
           {
              if (columnArray->IsValid(rowIdx))
              {
                  int64_t epoch = std::static_pointer_cast<arrow::Int64Array>(
                    structArray->GetFieldByName(sf::internal::FIELD_NAME_EPOCH))->Value(rowIdx);
                  int32_t fraction = std::static_pointer_cast<arrow::Int32Array>(
                    structArray->GetFieldByName(sf::internal::FIELD_NAME_FRACTION))->Value(rowIdx);
                  int powTenSB4 = sf::internal::powTenSB4[9];
                  if (epoch > (INT64_MAX / powTenSB4) || epoch < (INT64_MIN / powTenSB4))
                  {
                    if (fraction % 1000 != 0) {
                        std::string errorInfo = Logger::formatString(
                          "The total number of nanoseconds %d%d overflows int64 range. If you use a timestamp with "
                          "the nanosecond part over 6-digits in the Snowflake database, the timestamp must be "
                          "between '1677-09-21 00:12:43.145224192' and '2262-04-11 23:47:16.854775807' to not overflow."
                          , epoch, fraction);
                        throw std::overflow_error(errorInfo.c_str());
                    } else {
                        has_overflow_to_downscale = true;
                    }
                  }
                }
           }
     }
    auto timeUnit = has_overflow_to_downscale? arrow::TimeUnit::MICRO: arrow::TimeUnit::NANO;
    if (!timezone.empty())
    {
      timeType = arrow::timestamp(timeUnit, timezone);
    }
    else
    {
      timeType = arrow::timestamp(timeUnit);
    }
    tsField = std::make_shared<arrow::Field>(
      field->name(), timeType, field->nullable());
    arrow::TimestampBuilder builder(timeType, m_pool);
    std::shared_ptr<arrow::StructArray> structArray;
    if (dt->id() == arrow::Type::type::STRUCT)
    {
      structArray = std::dynamic_pointer_cast<arrow::StructArray>(columnArray);
    }
    arrow::Status ret;
    for(int64_t rowIdx = 0; rowIdx < columnArray->length(); rowIdx++)
    {
      if (columnArray->IsValid(rowIdx))
      {
        int64_t val;
        switch (dt->id())
        {
          case arrow::Type::type::INT64:
            val = std::static_pointer_cast<arrow::Int64Array>(columnArray)->Value(rowIdx);
            val *= sf::internal::powTenSB4[9 - scale];
            break;
          case arrow::Type::type::STRUCT:
            {
              int64_t epoch = std::static_pointer_cast<arrow::Int64Array>(
                structArray->GetFieldByName(sf::internal::FIELD_NAME_EPOCH))->Value(rowIdx);
              int32_t fraction = std::static_pointer_cast<arrow::Int32Array>(
                structArray->GetFieldByName(sf::internal::FIELD_NAME_FRACTION))->Value(rowIdx);
              if (has_overflow_to_downscale)
              {
                val = epoch * sf::internal::powTenSB4[6] + fraction / 1000;
              } else
              {
                val = epoch * sf::internal::powTenSB4[9] + fraction;
              }
            }
            break;
          default:
            std::string errorInfo = Logger::formatString(
                "[Snowflake Exception] unknown arrow internal data type(%d) "
                "for FIXED data",
                dt->id());
            logger->error(__FILE__, __func__, __LINE__, errorInfo.c_str());
            return;
        }
        // unit is nanosecond
        ret = builder.Append(val);
      }
      else
      {
        ret = builder.AppendNull();
      }
      SF_CHECK_ARROW_RC(ret,
        "[Snowflake Exception] arrow failed to append value: internal data type(%d), errorInfo: %s",
        dt->id(),  ret.message().c_str());
    }

    ret = builder.Finish(&tsArray);
    SF_CHECK_ARROW_RC(ret,
      "[Snowflake Exception] arrow failed to finish array, errorInfo: %s",
      ret.message().c_str());
  }

  // replace the targeted column
  replaceColumn(batchIdx, colIdx, tsField, tsArray, futureFields, futureColumns, needsRebuild);
}

void CArrowTableIterator::convertTimestampTZColumn(
  const unsigned int batchIdx,
  const int colIdx,
  const std::shared_ptr<arrow::Field> field,
  const std::shared_ptr<arrow::Array> columnArray,
  const int scale,
  const int byteLength,
  std::vector<std::shared_ptr<arrow::Field>>& futureFields,
  std::vector<std::shared_ptr<arrow::Array>>& futureColumns,
  bool& needsRebuild,
  const std::string timezone
)
{
  std::shared_ptr<arrow::Field> tsField;
  std::shared_ptr<arrow::Array> tsArray;
  std::shared_ptr<arrow::DataType> timeType;
  auto dt = field->type();
  // Convert to arrow time column
  std::shared_ptr<arrow::StructArray> structArray;
  structArray = std::dynamic_pointer_cast<arrow::StructArray>(columnArray);
  auto epochArray = std::static_pointer_cast<arrow::Int64Array>(
          structArray->GetFieldByName(sf::internal::FIELD_NAME_EPOCH));
  auto fractionArray = std::static_pointer_cast<arrow::Int32Array>(
          structArray->GetFieldByName(sf::internal::FIELD_NAME_FRACTION));

  if (scale == 0)
  {
    if (!timezone.empty())
    {
      timeType = arrow::timestamp(arrow::TimeUnit::SECOND, timezone);
    }
    else
    {
      timeType = arrow::timestamp(arrow::TimeUnit::SECOND);
    }
    tsField = std::make_shared<arrow::Field>(
      field->name(), timeType, field->nullable());
  }
  else if (scale <= 3)
  {
    if (!timezone.empty())
    {
      timeType = arrow::timestamp(arrow::TimeUnit::MILLI, timezone);
    }
    else
    {
      timeType = arrow::timestamp(arrow::TimeUnit::MILLI);
    }
    tsField = std::make_shared<arrow::Field>(
      field->name(), timeType, field->nullable());
  }
  else if (scale <= 6)
  {
    if (!timezone.empty())
    {
      timeType = arrow::timestamp(arrow::TimeUnit::MICRO, timezone);
    }
    else
    {
      timeType = arrow::timestamp(arrow::TimeUnit::MICRO);
    }
    tsField = std::make_shared<arrow::Field>(
      field->name(), timeType, field->nullable());
  }
  else
  {
    if (!timezone.empty())
    {
      timeType = arrow::timestamp(arrow::TimeUnit::NANO, timezone);
    }
    else
    {
      timeType = arrow::timestamp(arrow::TimeUnit::NANO);
    }
    tsField = std::make_shared<arrow::Field>(
      field->name(), timeType, field->nullable());
  }

  arrow::TimestampBuilder builder(timeType, m_pool);
  arrow::Status ret;
  for(int64_t rowIdx = 0; rowIdx < columnArray->length(); rowIdx++)
  {
    if (columnArray->IsValid(rowIdx))
    {
      if (byteLength == 8)
      {
        // two fields
        int64_t epoch = epochArray->Value(rowIdx);
        // append value
        if (scale == 0)
        {
          ret = builder.Append(epoch);
        }
        else if (scale <= 3)
        {
          ret = builder.Append(epoch * sf::internal::powTenSB4[3-scale]);
        }
        else if (scale <= 6)
        {
          ret = builder.Append(epoch * sf::internal::powTenSB4[6-scale]);
        }
        else
        {
          ret = builder.Append(epoch * sf::internal::powTenSB4[9 - scale]);
        }
      }
      else if (byteLength == 16)
      {
        // three fields
        int64_t epoch = epochArray->Value(rowIdx);
        int32_t fraction = fractionArray->Value(rowIdx);
        if (scale == 0)
        {
          ret = builder.Append(epoch);
        }
        else if (scale <= 3)
        {
          ret = builder.Append(epoch * sf::internal::powTenSB4[3-scale]
                  + fraction / sf::internal::powTenSB4[6]);
        }
        else if (scale <= 6)
        {
          ret = builder.Append(epoch * sf::internal::powTenSB4[6] + fraction / sf::internal::powTenSB4[3]);
        }
        else
        {
          ret = builder.Append(epoch * sf::internal::powTenSB4[9] + fraction);
        }
      }
      else
      {
        std::string errorInfo = Logger::formatString(
          "[Snowflake Exception] unknown arrow internal data type(%d) "
          "for TIMESTAMP_TZ data",
          dt->id());
        logger->error(__FILE__, __func__, __LINE__, errorInfo.c_str());
        PyErr_SetString(PyExc_Exception, errorInfo.c_str());
        return;
      }
    }
    else
    {
      ret = builder.AppendNull();
    }
    SF_CHECK_ARROW_RC(ret,
      "[Snowflake Exception] arrow failed to append value: internal data type(%d), errorInfo: %s",
      dt->id(),  ret.message().c_str());
  }

  ret = builder.Finish(&tsArray);
  SF_CHECK_ARROW_RC(ret,
    "[Snowflake Exception] arrow failed to finish array, errorInfo: %s",
    ret.message().c_str());

  // replace the targeted column
  replaceColumn(batchIdx, colIdx, tsField, tsArray, futureFields, futureColumns, needsRebuild);
}

bool CArrowTableIterator::convertRecordBatchesToTable()
{
  // only do conversion once and there exist some record batches
  if (!m_cTable && !m_cRecordBatches->empty())
  {
    reconstructRecordBatches();
    arrow::Result<std::shared_ptr<arrow::Table>> ret = arrow::Table::FromRecordBatches(*m_cRecordBatches);
    SF_CHECK_ARROW_RC_AND_RETURN(ret, false,
      "[Snowflake Exception] arrow failed to build table from batches, errorInfo: %s",
      ret.status().message().c_str());
    m_cTable = ret.ValueOrDie();

    return true;
  }
  return false;
}

} // namespace sf
