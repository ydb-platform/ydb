//
// Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
//

#ifndef PC_ARROWCHUNKITERATOR_HPP
#define PC_ARROWCHUNKITERATOR_HPP

#include "CArrowIterator.hpp"
#include "IColumnConverter.hpp"
#include "Python/Common.hpp"
#include <memory>
#include <vector>

namespace sf
{

/**
 * Arrow chunk iterator implementation in C++. The caller (python arrow chunk
 * iterator object)
 * will ask for nextRow to be returned back to Python
 */
class CArrowChunkIterator : public CArrowIterator
{
public:
  /**
   * Constructor
   */
  CArrowChunkIterator(PyObject* context, std::vector<std::shared_ptr<arrow::RecordBatch>> * batches, PyObject *use_numpy);

  /**
   * Desctructor
   */
  virtual ~CArrowChunkIterator() = default;

  /**
   * @return a python tuple object which contains all data in current row
   */
  std::shared_ptr<ReturnVal> next() override;

protected:
  /**
   * @return python object of tuple which is tuple of all row values
   */
  virtual void createRowPyObject();

  /** pointer to the latest returned python tuple(row) result */
  py::UniqueRef m_latestReturnedRow;

  /** list of column converters*/
  std::vector<std::shared_ptr<sf::IColumnConverter>> m_currentBatchConverters;

  /** row index inside current record batch (start from 0) */
  int m_rowIndexInBatch;

  /** schema of current record batch */
  std::shared_ptr<arrow::Schema> m_currentSchema;

private:
  /** number of columns */
  int m_columnCount;

  /** number of record batch in current chunk */
  int m_batchCount;

  /** current index that iterator points to */
  int m_currentBatchIndex;

  /** total number of rows inside current record batch */
  int64_t m_rowCountInBatch;

  /** pointer to the current python exception object */
  py::UniqueRef m_currentPyException;

  /** arrow format convert context for the current session */
  PyObject* m_context;

  /** true if return numpy int64 float64 datetime*/
  bool m_useNumpy;

  void initColumnConverters();
};

class DictCArrowChunkIterator : public CArrowChunkIterator
{
public:
  DictCArrowChunkIterator(PyObject* context, std::vector<std::shared_ptr<arrow::RecordBatch>> * batches, PyObject *use_numpy);

  ~DictCArrowChunkIterator() = default;

private:

  void createRowPyObject() override;

};


}

#endif  // PC_ARROWCHUNKITERATOR_HPP
