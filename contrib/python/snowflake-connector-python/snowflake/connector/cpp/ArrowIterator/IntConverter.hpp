//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#ifndef PC_INTCONVERTER_HPP
#define PC_INTCONVERTER_HPP

#include "IColumnConverter.hpp"
#include <memory>

namespace sf
{

template <typename T>
class IntConverter : public IColumnConverter
{
public:
  explicit IntConverter(std::shared_ptr<arrow::Array> array)
  : m_array(std::dynamic_pointer_cast<T>(array))
  {
  }

  PyObject* pyLongForward(int64_t value) const
  {
    return PyLong_FromLongLong(value);
  }

  PyObject* pyLongForward(int32_t value) const
  {
    return PyLong_FromLong(value);
  }

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<T> m_array;
};

template <typename T>
PyObject* IntConverter<T>::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    // TODO : this forward function need to be tested in Win64
    return pyLongForward(m_array->Value(rowIndex));
  }
  else
  {
    Py_RETURN_NONE;
  }
}

template <typename T>
class NumpyIntConverter : public IColumnConverter
{
public:
  explicit NumpyIntConverter(std::shared_ptr<arrow::Array> array, PyObject * context)
  : m_array(std::dynamic_pointer_cast<T>(array)),
    m_context(context)
  {
  }

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<T> m_array;

  PyObject * m_context;
};

template <typename T>
PyObject* NumpyIntConverter<T>::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    int64_t val = m_array->Value(rowIndex);
    return PyObject_CallMethod(m_context, "FIXED_to_numpy_int64", "L", val);
  }
  else
  {
    Py_RETURN_NONE;
  }
}

}  // namespace sf

#endif  // PC_INTCONVERTER_HPP
