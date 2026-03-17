//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#ifndef PC_FLOATCONVERTER_HPP
#define PC_FLOATCONVERTER_HPP

#include "IColumnConverter.hpp"
#include <memory>

namespace sf
{

class FloatConverter : public IColumnConverter
{
public:
  explicit FloatConverter(std::shared_ptr<arrow::Array> array);

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<arrow::DoubleArray> m_array;
};

class NumpyFloat64Converter : public IColumnConverter
{
public:
  explicit NumpyFloat64Converter(std::shared_ptr<arrow::Array> array, PyObject * context);

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<arrow::DoubleArray> m_array;

  PyObject * m_context;
};

}  // namespace sf

#endif  // PC_FLOATCONVERTER_HPP
