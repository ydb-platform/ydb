//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#include "TimeStampConverter.hpp"
#include "Python/Helpers.hpp"
#include "Util/time.hpp"

#include <cstdint>
#include <memory>
#include <type_traits>

template <typename T>
constexpr char toType() {
    static_assert(
        std::is_same<T, signed char>::value
        || std::is_same<T, short>::value
        || std::is_same<T, int>::value
        || std::is_same<T, long>::value
        || std::is_same<T, long long>::value
    , "Unknown type");
    return std::is_same<T, signed char>::value ? 'b'
    : std::is_same<T, short>::value ? 'h'
    : std::is_same<T, int>::value ? 'i'
    : std::is_same<T, long>::value ? 'l'
    : std::is_same<T, long long>::value ? 'L'
    // Should not get here. Error.
    : '?';
}

template <typename T1>
struct FormatArgs1 {
  char format[2];
  constexpr FormatArgs1()
  : format{toType<T1>(), '\0'}
  {}
};
template <typename T1, typename T2>
struct FormatArgs2 {
  char format[3];
  constexpr FormatArgs2()
  : format{toType<T1>(), toType<T2>(), '\0'}
  {}
};
template <typename T1, typename T2, typename T3>
struct FormatArgs3 {
  char format[4];
  constexpr FormatArgs3()
  : format{toType<T1>(), toType<T2>(), toType<T3>(), '\0'}
  {}
};

namespace sf
{
TimeStampBaseConverter::TimeStampBaseConverter(PyObject* context, int32_t scale)
: m_context(context), m_scale(scale)
{
}

OneFieldTimeStampNTZConverter::OneFieldTimeStampNTZConverter(
    std::shared_ptr<arrow::Array> array, int32_t scale, PyObject* context)
: TimeStampBaseConverter(context, scale),
  m_array(std::dynamic_pointer_cast<arrow::Int64Array>(array))
{
}

PyObject* OneFieldTimeStampNTZConverter::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    internal::TimeSpec ts(m_array->Value(rowIndex), m_scale);

    static constexpr FormatArgs2<decltype(ts.seconds), decltype(ts.microseconds)> format;
#ifdef _WIN32
    return PyObject_CallMethod(m_context, "TIMESTAMP_NTZ_to_python_windows", format.format,
                               ts.seconds, ts.microseconds);
#else
    return PyObject_CallMethod(m_context, "TIMESTAMP_NTZ_to_python", format.format,
                               ts.seconds, ts.microseconds);
#endif
  }
  else
  {
    Py_RETURN_NONE;
  }
}

NumpyOneFieldTimeStampNTZConverter::NumpyOneFieldTimeStampNTZConverter(
    std::shared_ptr<arrow::Array> array, int32_t scale, PyObject* context)
: TimeStampBaseConverter(context, scale),
  m_array(std::dynamic_pointer_cast<arrow::Int64Array>(array))
{
}

PyObject* NumpyOneFieldTimeStampNTZConverter::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    int64_t val = m_array->Value(rowIndex);
    return PyObject_CallMethod(m_context, "TIMESTAMP_NTZ_ONE_FIELD_to_numpy_datetime64", "Li", val, m_scale);
  }
  else
  {
    Py_RETURN_NONE;
  }
}

TwoFieldTimeStampNTZConverter::TwoFieldTimeStampNTZConverter(
    std::shared_ptr<arrow::Array> array, int32_t scale, PyObject* context)
: TimeStampBaseConverter(context, scale),
  m_array(std::dynamic_pointer_cast<arrow::StructArray>(array)),
  m_epoch(std::dynamic_pointer_cast<arrow::Int64Array>(
      m_array->GetFieldByName(internal::FIELD_NAME_EPOCH))),
  m_fraction(std::dynamic_pointer_cast<arrow::Int32Array>(
      m_array->GetFieldByName(internal::FIELD_NAME_FRACTION)))
{
}

PyObject* TwoFieldTimeStampNTZConverter::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    int64_t seconds = m_epoch->Value(rowIndex);
    int64_t microseconds = m_fraction->Value(rowIndex) / 1000;

    static constexpr FormatArgs2<decltype(seconds), decltype(microseconds)> format;
#ifdef _WIN32
    return PyObject_CallMethod(m_context, "TIMESTAMP_NTZ_to_python_windows", format.format,
                               seconds, microseconds);
#else
    return PyObject_CallMethod(m_context, "TIMESTAMP_NTZ_to_python", format.format,
                               seconds, microseconds);
#endif
  }
  else
  {
    Py_RETURN_NONE;
  }
}

NumpyTwoFieldTimeStampNTZConverter::NumpyTwoFieldTimeStampNTZConverter(
    std::shared_ptr<arrow::Array> array, int32_t scale, PyObject* context)
: TimeStampBaseConverter(context, scale),
  m_array(std::dynamic_pointer_cast<arrow::StructArray>(array)),
  m_epoch(std::dynamic_pointer_cast<arrow::Int64Array>(
      m_array->GetFieldByName(internal::FIELD_NAME_EPOCH))),
  m_fraction(std::dynamic_pointer_cast<arrow::Int32Array>(
      m_array->GetFieldByName(internal::FIELD_NAME_FRACTION)))
{
}

PyObject* NumpyTwoFieldTimeStampNTZConverter::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    int64_t epoch = m_epoch->Value(rowIndex);
    int32_t frac = m_fraction->Value(rowIndex);
    return PyObject_CallMethod(m_context, "TIMESTAMP_NTZ_TWO_FIELD_to_numpy_datetime64", "Li", epoch, frac);
  }
  else
  {
    Py_RETURN_NONE;
  }
}


OneFieldTimeStampLTZConverter::OneFieldTimeStampLTZConverter(
    std::shared_ptr<arrow::Array> array, int32_t scale, PyObject* context)
: TimeStampBaseConverter(context, scale),
  m_array(std::dynamic_pointer_cast<arrow::Int64Array>(array))
{
}

PyObject* OneFieldTimeStampLTZConverter::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    internal::TimeSpec ts(m_array->Value(rowIndex), m_scale);

    static constexpr FormatArgs2<decltype(ts.seconds), decltype(ts.microseconds)> format;

#ifdef _WIN32
        return PyObject_CallMethod(m_context, "TIMESTAMP_LTZ_to_python_windows", format.format,
                                   ts.seconds, ts.microseconds);
#else
        return PyObject_CallMethod(m_context, "TIMESTAMP_LTZ_to_python", format.format,
                                   ts.seconds, ts.microseconds);
#endif
  }

  Py_RETURN_NONE;
}

TwoFieldTimeStampLTZConverter::TwoFieldTimeStampLTZConverter(
    std::shared_ptr<arrow::Array> array, int32_t scale, PyObject* context)
: TimeStampBaseConverter(context, scale),
  m_array(std::dynamic_pointer_cast<arrow::StructArray>(array)),
  m_epoch(std::dynamic_pointer_cast<arrow::Int64Array>(
      m_array->GetFieldByName(internal::FIELD_NAME_EPOCH))),
  m_fraction(std::dynamic_pointer_cast<arrow::Int32Array>(
      m_array->GetFieldByName(internal::FIELD_NAME_FRACTION)))
{
}

PyObject* TwoFieldTimeStampLTZConverter::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    int64_t seconds = m_epoch->Value(rowIndex);
    int64_t microseconds = m_fraction->Value(rowIndex) / 1000;

    static constexpr FormatArgs2<decltype(seconds), decltype(microseconds)> format;
#ifdef _WIN32
    return PyObject_CallMethod(m_context, "TIMESTAMP_LTZ_to_python_windows", format.format,
                               seconds, microseconds);
#else
    return PyObject_CallMethod(m_context, "TIMESTAMP_LTZ_to_python", format.format,
                               seconds, microseconds);
#endif
  }

  Py_RETURN_NONE;
}

TwoFieldTimeStampTZConverter::TwoFieldTimeStampTZConverter(
    std::shared_ptr<arrow::Array> array, int32_t scale, PyObject* context)
: TimeStampBaseConverter(context, scale),
  m_array(std::dynamic_pointer_cast<arrow::StructArray>(array)),
  m_epoch(std::dynamic_pointer_cast<arrow::Int64Array>(
      m_array->GetFieldByName(internal::FIELD_NAME_EPOCH))),
  m_timezone(std::dynamic_pointer_cast<arrow::Int32Array>(
      m_array->GetFieldByName(internal::FIELD_NAME_TIME_ZONE)))
{
}

PyObject* TwoFieldTimeStampTZConverter::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    int32_t timezone = m_timezone->Value(rowIndex);
    internal::TimeSpec ts(m_epoch->Value(rowIndex), m_scale);

    static constexpr FormatArgs3<decltype(ts.seconds), decltype(ts.microseconds), decltype(timezone)> format;
#if _WIN32
    return PyObject_CallMethod(m_context, "TIMESTAMP_TZ_to_python_windows", format.format,
                               ts.seconds, ts.microseconds, timezone);
#else
    return PyObject_CallMethod(m_context, "TIMESTAMP_TZ_to_python", format.format,
                               ts.seconds, ts.microseconds, timezone);
#endif
  }

  Py_RETURN_NONE;
}

ThreeFieldTimeStampTZConverter::ThreeFieldTimeStampTZConverter(
    std::shared_ptr<arrow::Array> array, int32_t scale, PyObject* context)
: TimeStampBaseConverter(context, scale),
  m_array(std::dynamic_pointer_cast<arrow::StructArray>(array)),
  m_epoch(std::dynamic_pointer_cast<arrow::Int64Array>(
      m_array->GetFieldByName(internal::FIELD_NAME_EPOCH))),
  m_timezone(std::dynamic_pointer_cast<arrow::Int32Array>(
      m_array->GetFieldByName(internal::FIELD_NAME_TIME_ZONE))),
  m_fraction(std::dynamic_pointer_cast<arrow::Int32Array>(
      m_array->GetFieldByName(internal::FIELD_NAME_FRACTION)))
{
}

PyObject* ThreeFieldTimeStampTZConverter::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    int32_t timezone = m_timezone->Value(rowIndex);
    int64_t seconds = m_epoch->Value(rowIndex);
    int64_t microseconds = m_fraction->Value(rowIndex) / 1000;

    static constexpr FormatArgs3<decltype(seconds), decltype(microseconds), decltype(timezone)> format;
#ifdef _WIN32
    return PyObject_CallMethod(m_context, "TIMESTAMP_TZ_to_python_windows", format.format,
                               seconds, microseconds, timezone);
#else
    return PyObject_CallMethod(m_context, "TIMESTAMP_TZ_to_python", format.format,
                               seconds, microseconds, timezone);
#endif
  }

  Py_RETURN_NONE;
}

}  // namespace sf
