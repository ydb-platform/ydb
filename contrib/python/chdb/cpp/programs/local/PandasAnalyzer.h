#pragma once

#include "NumpyType.h"
#include "PybindWrapper.h"

#include <DataTypes/IDataType.h>
#include <Core/Settings.h>

namespace CHDB {

class PandasAnalyzer {
public:
	explicit PandasAnalyzer(const DB_CHDB::Settings & settings) {
		analyzed_type = {};

		sample_size = settings.pandas_analyze_sample;
	}

public:
	DB_CHDB::DataTypePtr getItemType(py::object obj, bool & can_convert);
	bool Analyze(py::object column);

	DB_CHDB::DataTypePtr analyzedType()
	{
		return analyzed_type;
	}

private:
	DB_CHDB::DataTypePtr innerAnalyze(py::object column, bool & can_convert, size_t increment);
	size_t getSampleIncrement(size_t rows);

private:
	int64_t sample_size;
	PythonGILWrapper gil;
	DB_CHDB::DataTypePtr analyzed_type;
};

} // namespace CHDB
