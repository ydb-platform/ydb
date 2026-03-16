//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#ifndef PC_BOOLEANCONVERTER_HPP
#define PC_BOOLEANCONVERTER_HPP

#include "IColumnConverter.hpp"
#include <memory>

namespace sf
{

class BooleanConverter : public IColumnConverter
{
public:
  explicit BooleanConverter(std::shared_ptr<arrow::Array> array);

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<arrow::BooleanArray> m_array;
};

}  // namespace sf

#endif  // PC_BOOLEANCONVERTER_HPP
