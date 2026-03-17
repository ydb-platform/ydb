//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#ifndef PC_TIMECONVERTER_HPP
#define PC_TIMECONVERTER_HPP

#include "IColumnConverter.hpp"
#include "Python/Common.hpp"
#include "Python/Helpers.hpp"
#include "Util/time.hpp"
#include <memory>

namespace sf
{

template <typename T>
class TimeConverter : public IColumnConverter
{
public:
  explicit TimeConverter(std::shared_ptr<arrow::Array> array, int32_t scale)
  : m_array(std::dynamic_pointer_cast<T>(array)), m_scale(scale)
  {
  }

  PyObject* toPyObject(int64_t rowIndex) const override;

private:
  /** can be arrow::Int32Array and arrow::Int64Array */
  std::shared_ptr<T> m_array;

  int32_t m_scale;

  static py::UniqueRef& m_pyDatetimeTime();
};

template <typename T>
PyObject* TimeConverter<T>::toPyObject(int64_t rowIndex) const
{
  if (m_array->IsValid(rowIndex))
  {
    int64_t seconds = m_array->Value(rowIndex);
    using namespace internal;
    py::PyUniqueLock lock;
    return PyObject_CallFunction(m_pyDatetimeTime().get(), "iiii",
                                 getHourFromSeconds(seconds, m_scale),
                                 getMinuteFromSeconds(seconds, m_scale),
                                 getSecondFromSeconds(seconds, m_scale),
                                 getMicrosecondFromSeconds(seconds, m_scale));
  }
  else
  {
    Py_RETURN_NONE;
  }
}

template <typename T>
py::UniqueRef& TimeConverter<T>::m_pyDatetimeTime()
{
  static py::UniqueRef pyDatetimeTime;
  if (pyDatetimeTime.empty())
  {
    py::PyUniqueLock lock;
    py::UniqueRef pyDatetimeModule;
    py::importPythonModule("datetime", pyDatetimeModule);
    /** TODO : to check status here */

    py::importFromModule(pyDatetimeModule, "time", pyDatetimeTime);
  }
  return pyDatetimeTime;
}

}  // namespace sf

#endif  // PC_TIMECONVERTER_HPP
