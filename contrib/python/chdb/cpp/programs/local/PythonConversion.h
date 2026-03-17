#pragma once

#include "PybindWrapper.h"
#include "PythonUtils.h"

#include <base/types.h>
#include <rapidjson/document.h>

namespace CHDB {

enum class PythonObjectType {
	Other,
	None,
	Integer,
	Float,
	Bool,
	Decimal,
	Uuid,
	Datetime,
	Date,
	Time,
	Timedelta,
	String,
	ByteArray,
	MemoryView,
	Bytes,
	List,
	Tuple,
	Dict,
	NdArray,
	NdDatetime
};

PythonObjectType GetPythonObjectType(const py::handle & obj);

bool isInteger(const py::handle & obj);

bool isNone(const py::handle & obj);

bool isFloat(const py::handle & obj);

bool isBoolean(const py::handle & obj);

bool isDecimal(const py::handle & obj);

bool isString(const py::handle & obj);

bool isByteArray(const py::handle & obj);

bool isMemoryView(const py::handle & obj);

void convert_to_json_str(const py::handle & obj, String & ret);

bool tryInsertJsonResult(
    const py::handle & handle,
    const DB_CHDB::FormatSettings & format_settings,
    DB_CHDB::MutableColumnPtr & column,
    DB_CHDB::SerializationPtr & serialization);

} // namespace CHDB
