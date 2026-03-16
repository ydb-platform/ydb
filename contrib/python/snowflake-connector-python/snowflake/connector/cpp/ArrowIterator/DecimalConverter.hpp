//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#ifndef PC_DECIMALCONVERTER_HPP
#define PC_DECIMALCONVERTER_HPP

#include "IColumnConverter.hpp"
#include "Python/Common.hpp"
#include <memory>

namespace sf
{

class DecimalBaseConverter : public IColumnConverter
{
public:
  DecimalBaseConverter();
  virtual ~DecimalBaseConverter() = default;

protected:
  py::UniqueRef& m_pyDecimalConstructor;

private:
  static py::UniqueRef& initPyDecimalConstructor();
};

class DecimalFromDecimalConverter : public DecimalBaseConverter
{
public:
  explicit DecimalFromDecimalConverter(std::shared_ptr<arrow::Array> array,
                                       int scale);

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<arrow::Decimal128Array> m_array;

  int m_scale;
  /** no need for this converter to store precision*/
};

template <typename T>
class DecimalFromIntConverter : public DecimalBaseConverter
{
public:
  explicit DecimalFromIntConverter(std::shared_ptr<arrow::Array> array,
                                   int precision, int scale)
  : m_array(std::dynamic_pointer_cast<T>(array)),
    m_precision(precision),
    m_scale(scale)
  {
  }

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<T> m_array;

  int m_precision;  // looks like the precision here is not useful, and this
                    // will be removed soon when it's been confirmed

  int m_scale;
};

template <typename T>
PyObject* DecimalFromIntConverter<T>::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    int64_t val = m_array->Value(rowIndex);

    py::UniqueRef decimal(
        PyObject_CallFunction(m_pyDecimalConstructor.get(), "L", val));
    return PyObject_CallMethod(decimal.get(), "scaleb", "i", -m_scale);
  }
  else
  {
    Py_RETURN_NONE;
  }
}


template <typename T>
class NumpyDecimalConverter : public IColumnConverter
{
public:
  explicit NumpyDecimalConverter(std::shared_ptr<arrow::Array> array,
                                 int precision, int scale, PyObject * context)
  : m_array(std::dynamic_pointer_cast<T>(array)),
    m_precision(precision),
    m_scale(scale),
    m_context(context)
  {
  }

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<T> m_array;

  int m_precision;

  int m_scale;

  PyObject * m_context;
};

template <typename T>
PyObject* NumpyDecimalConverter<T>::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    int64_t val = m_array->Value(rowIndex);

    return PyObject_CallMethod(m_context, "FIXED_to_numpy_float64", "Li", val, m_scale);
  }
  else
  {
    Py_RETURN_NONE;
  }
}


}  // namespace sf

#endif  // PC_DECIMALCONVERTER_HPP
