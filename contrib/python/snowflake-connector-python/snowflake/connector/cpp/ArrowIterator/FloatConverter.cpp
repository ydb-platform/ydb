//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#include "FloatConverter.hpp"
#include <memory>

namespace sf
{

/** snowflake float is 64-precision, which refers to double here */
FloatConverter::FloatConverter(std::shared_ptr<arrow::Array> array)
: m_array(std::dynamic_pointer_cast<arrow::DoubleArray>(array))
{
}

PyObject* FloatConverter::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    return PyFloat_FromDouble(m_array->Value(rowIndex));
  }
  else
  {
    Py_RETURN_NONE;
  }
}

NumpyFloat64Converter::NumpyFloat64Converter(std::shared_ptr<arrow::Array> array, PyObject * context)
: m_array(std::dynamic_pointer_cast<arrow::DoubleArray>(array)), m_context(context)
{
}

PyObject* NumpyFloat64Converter::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    double val = m_array->Value(rowIndex);

    return PyObject_CallMethod(m_context, "REAL_to_numpy_float64", "d", val);
  }
  else
  {
    Py_RETURN_NONE;
  }
}

}  // namespace sf
