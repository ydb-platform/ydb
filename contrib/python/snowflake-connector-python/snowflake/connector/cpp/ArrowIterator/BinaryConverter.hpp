//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#ifndef PC_BINARYCONVERTER_HPP
#define PC_BINARYCONVERTER_HPP

#include "IColumnConverter.hpp"
#include "logging.hpp"
#include <memory>

namespace sf
{

class BinaryConverter : public IColumnConverter
{
public:
  explicit BinaryConverter(std::shared_ptr<arrow::Array> array);

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  std::shared_ptr<arrow::BinaryArray> m_array;

  static Logger* logger;
};

}  // namespace sf

#endif  // PC_BINARYCONVERTER_HPP
