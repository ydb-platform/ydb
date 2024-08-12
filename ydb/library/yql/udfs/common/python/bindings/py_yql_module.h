#pragma once

#include <ydb/library/yql/udfs/common/python/python_udf/python_udf.h>

namespace NPython {

void PrepareYqlModule();
void InitYqlModule(NYql::NUdf::EPythonFlavor pythonFlavor, bool standalone = true);
void TermYqlModule();

} // namspace NPython
