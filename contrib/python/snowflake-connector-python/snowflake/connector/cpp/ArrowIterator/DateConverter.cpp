//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#include "DateConverter.hpp"
#include "Python/Helpers.hpp"
#include <memory>

namespace sf
{
Logger* DateConverter::logger = new Logger("snowflake.connector.DateConverter");

py::UniqueRef& DateConverter::initPyDatetimeDate()
{
  static py::UniqueRef pyDatetimeDate;
  if (pyDatetimeDate.empty())
  {
    py::UniqueRef pyDatetimeModule;
    py::importPythonModule("datetime", pyDatetimeModule);
    py::importFromModule(pyDatetimeModule, "date", pyDatetimeDate);
    Py_XINCREF(pyDatetimeDate.get());
  }
  return pyDatetimeDate;
}

DateConverter::DateConverter(std::shared_ptr<arrow::Array> array)
: m_array(std::dynamic_pointer_cast<arrow::Date32Array>(array)),
  m_pyDatetimeDate(initPyDatetimeDate())
{
}

PyObject* DateConverter::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    int32_t deltaDays = m_array->Value(rowIndex);
    return PyObject_CallMethod(m_pyDatetimeDate.get(), "fromordinal", "i",
                               epochDay + deltaDays);
  }
  else
  {
    Py_RETURN_NONE;
  }
}

NumpyDateConverter::NumpyDateConverter(std::shared_ptr<arrow::Array> array, PyObject * context)
: m_array(std::dynamic_pointer_cast<arrow::Date32Array>(array)),
  m_context(context)
{
}

PyObject* NumpyDateConverter::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    int32_t deltaDays = m_array->Value(rowIndex);
    return PyObject_CallMethod(m_context, "DATE_to_numpy_datetime64", "i", deltaDays);
  }
  else
  {
    Py_RETURN_NONE;
  }
}

}  // namespace sf
