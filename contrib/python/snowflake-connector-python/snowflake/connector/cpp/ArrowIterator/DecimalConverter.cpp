//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#include "Python/Common.hpp"
#include "DecimalConverter.hpp"
#include "Python/Helpers.hpp"
#include <memory>
#include <string>

namespace sf
{

DecimalBaseConverter::DecimalBaseConverter()
: m_pyDecimalConstructor(initPyDecimalConstructor())
{
}

py::UniqueRef& DecimalBaseConverter::initPyDecimalConstructor()
{
  static py::UniqueRef pyDecimalConstructor;
  if (pyDecimalConstructor.empty())
  {
    py::UniqueRef decimalModule;
    py::importPythonModule("decimal", decimalModule);
    py::importFromModule(decimalModule, "Decimal", pyDecimalConstructor);
    Py_XINCREF(pyDecimalConstructor.get());
  }

  return pyDecimalConstructor;
}

DecimalFromDecimalConverter::DecimalFromDecimalConverter(
    std::shared_ptr<arrow::Array> array, int scale)
: m_array(std::dynamic_pointer_cast<arrow::Decimal128Array>(array)),
  m_scale(scale)
{
}

PyObject* DecimalFromDecimalConverter::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    std::string formatDecimalString = m_array->FormatValue(rowIndex);
    if (m_scale == 0)
    {
      return PyLong_FromString(formatDecimalString.c_str(), nullptr, 0);
    }

    /** the reason we use c_str() instead of std::string here is that we may
     * meet some encoding problem with std::string */
    return PyObject_CallFunction(m_pyDecimalConstructor.get(), "s#",
                                 formatDecimalString.c_str(),
                                 static_cast<Py_ssize_t>(formatDecimalString.size()));
  }
  else
  {
    Py_RETURN_NONE;
  }
}

}  // namespace sf
