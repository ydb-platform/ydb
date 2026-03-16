//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#ifndef PC_TIMESTAMPCONVERTER_HPP
#define PC_TIMESTAMPCONVERTER_HPP

#include "IColumnConverter.hpp"
#include "Python/Common.hpp"
#include "Python/Helpers.hpp"
#include "Util/time.hpp"
#include <memory>

namespace sf
{

// correspond to python datetime.time and datetime.time has only support 6 bit
// precision, which is millisecond

class TimeStampBaseConverter : public IColumnConverter
{
public:
  TimeStampBaseConverter(PyObject* context, int32_t scale);
  virtual ~TimeStampBaseConverter() = default;

protected:
  PyObject* m_context;

  int32_t m_scale;
};

class OneFieldTimeStampNTZConverter : public TimeStampBaseConverter
{
public:
  explicit OneFieldTimeStampNTZConverter(std::shared_ptr<arrow::Array> array,
                                         int32_t scale, PyObject* context);

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<arrow::Int64Array> m_array;
};

class NumpyOneFieldTimeStampNTZConverter : public TimeStampBaseConverter
{
public:
  explicit NumpyOneFieldTimeStampNTZConverter(std::shared_ptr<arrow::Array> array,
                                              int32_t scale, PyObject* context);

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<arrow::Int64Array> m_array;
};

class TwoFieldTimeStampNTZConverter : public TimeStampBaseConverter
{
public:
  explicit TwoFieldTimeStampNTZConverter(std::shared_ptr<arrow::Array> array,
                                         int32_t scale, PyObject* context);

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<arrow::StructArray> m_array;
  std::shared_ptr<arrow::Int64Array> m_epoch;
  std::shared_ptr<arrow::Int32Array> m_fraction;
};


class NumpyTwoFieldTimeStampNTZConverter : public TimeStampBaseConverter
{
public:
  explicit NumpyTwoFieldTimeStampNTZConverter(std::shared_ptr<arrow::Array> array,
                                              int32_t scale, PyObject* context);

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<arrow::StructArray> m_array;
  std::shared_ptr<arrow::Int64Array> m_epoch;
  std::shared_ptr<arrow::Int32Array> m_fraction;
};

class OneFieldTimeStampLTZConverter : public TimeStampBaseConverter
{
public:
  explicit OneFieldTimeStampLTZConverter(std::shared_ptr<arrow::Array> array,
                                         int32_t scale, PyObject* context);

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<arrow::Int64Array> m_array;
};

class TwoFieldTimeStampLTZConverter : public TimeStampBaseConverter
{
public:
  explicit TwoFieldTimeStampLTZConverter(std::shared_ptr<arrow::Array> array,
                                         int32_t scale, PyObject* context);

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<arrow::StructArray> m_array;
  std::shared_ptr<arrow::Int64Array> m_epoch;
  std::shared_ptr<arrow::Int32Array> m_fraction;
};

class TwoFieldTimeStampTZConverter : public TimeStampBaseConverter
{
public:
  explicit TwoFieldTimeStampTZConverter(std::shared_ptr<arrow::Array> array,
                                        int32_t scale, PyObject* context);

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<arrow::StructArray> m_array;
  std::shared_ptr<arrow::Int64Array> m_epoch;
  std::shared_ptr<arrow::Int32Array> m_timezone;
};

class ThreeFieldTimeStampTZConverter : public TimeStampBaseConverter
{
public:
  explicit ThreeFieldTimeStampTZConverter(std::shared_ptr<arrow::Array> array,
                                          int32_t scale, PyObject* context);

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<arrow::StructArray> m_array;
  std::shared_ptr<arrow::Int64Array> m_epoch;
  std::shared_ptr<arrow::Int32Array> m_timezone;
  std::shared_ptr<arrow::Int32Array> m_fraction;
};

}  // namespace sf

#endif  // PC_TIMESTAMPCONVERTER_HPP
