//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#ifndef PC_DATECONVERTER_HPP
#define PC_DATECONVERTER_HPP

#include "IColumnConverter.hpp"
#include "Python/Common.hpp"
#include "logging.hpp"
#include <memory>

namespace sf
{

class DateConverter : public IColumnConverter
{
public:
  explicit DateConverter(std::shared_ptr<arrow::Array> array);

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  static py::UniqueRef& initPyDatetimeDate();

  std::shared_ptr<arrow::Date32Array> m_array;

  /** from Python Ordinal to 1970-01-01 */
  static constexpr int epochDay = 719163;

  static Logger* logger;

  py::UniqueRef& m_pyDatetimeDate;
};

class NumpyDateConverter : public IColumnConverter
{
public:
  explicit NumpyDateConverter(std::shared_ptr<arrow::Array> array, PyObject * context);

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<arrow::Date32Array> m_array;

  PyObject * m_context;
};

}  // namespace sf

#endif  // PC_DATECONVERTER_HPP
