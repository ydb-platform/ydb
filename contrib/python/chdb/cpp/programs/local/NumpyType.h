#pragma once

#include "PybindWrapper.h"

#include <cstdint>
#include <DataTypes/IDataType.h>

namespace CHDB {

enum class NumpyNullableType : uint8_t {
	BOOL,
	INT_8,
	UINT_8,
	INT_16,
	UINT_16,
	INT_32,
	UINT_32,
	INT_64,
	UINT_64,
	FLOAT_16,
	FLOAT_32,
	FLOAT_64,
	OBJECT,
	UNICODE,
	DATETIME_S,
	DATETIME_MS,
	DATETIME_NS,
	DATETIME_US,
	TIMEDELTA,

	CATEGORY,
	STRING,
};

struct NumpyType {
	NumpyNullableType type;
	bool has_timezone = false;

	String toString() const;
};

enum class NumpyObjectType : uint8_t {
	INVALID,
	NDARRAY1D,
	NDARRAY2D,
	LIST,
	DICT,
};

NumpyType ConvertNumpyType(const py::handle & col_type);
std::shared_ptr<DB_CHDB::IDataType> NumpyToDataType(const NumpyType & col_type);

} // namespace CHDB
