//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#ifndef PC_ARROWITERATOR_HPP
#define PC_ARROWITERATOR_HPP

#include "Python/Common.hpp"
#include "logging.hpp"
#include <memory>
#include <string>
#include <vector>

#define SF_CHECK_ARROW_RC(arrow_status, format_string, ...) \
  if (!arrow_status.ok()) \
  { \
    std::string errorInfo = Logger::formatString(format_string, ##__VA_ARGS__); \
    logger->error(__FILE__, __func__, __LINE__, errorInfo.c_str()); \
    PyErr_SetString(PyExc_Exception, errorInfo.c_str()); \
    return; \
  }

#define SF_CHECK_ARROW_RC_AND_RETURN(arrow_status, ret_val, format_string, ...) \
  if (!arrow_status.ok()) \
  { \
    std::string errorInfo = Logger::formatString(format_string, ##__VA_ARGS__); \
    logger->error(__FILE__, __func__, __LINE__, errorInfo.c_str()); \
    PyErr_SetString(PyExc_Exception, errorInfo.c_str()); \
    return ret_val; \
  }

namespace sf
{

/**
 * A simple struct to contain return data back cython.
 * PyObject would be nullptr if failed and cause string will be populated
 */
class ReturnVal
{
public:
  ReturnVal(PyObject * obj, PyObject *except) :
    successObj(obj), exception(except)
  {
  }

  PyObject * successObj;

  PyObject * exception;
};

/**
 * Arrow base iterator implementation in C++.
 */

class CArrowIterator
{
public:
  CArrowIterator(std::vector<std::shared_ptr<arrow::RecordBatch>> * batches);

  virtual ~CArrowIterator() = default;

  /**
   * @return a python object which might be current row or an Arrow Table
   */
  virtual std::shared_ptr<ReturnVal> next() = 0;

protected:
   /** list of all record batch in current chunk */
  std::vector<std::shared_ptr<arrow::RecordBatch>> *m_cRecordBatches;

  static Logger* logger;
};
}

#endif  // PC_ARROWITERATOR_HPP
