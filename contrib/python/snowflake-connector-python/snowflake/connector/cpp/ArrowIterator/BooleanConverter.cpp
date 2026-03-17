//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#include "BooleanConverter.hpp"
#include <memory>

namespace sf
{

BooleanConverter::BooleanConverter(std::shared_ptr<arrow::Array> array)
: m_array(std::dynamic_pointer_cast<arrow::BooleanArray>(array))
{
}

PyObject* BooleanConverter::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    if (m_array->Value(rowIndex))
    {
      Py_RETURN_TRUE;
    }
    else
    {
      Py_RETURN_FALSE;
    }
  }
  else
  {
    Py_RETURN_NONE;
  }
}

}  // namespace sf
