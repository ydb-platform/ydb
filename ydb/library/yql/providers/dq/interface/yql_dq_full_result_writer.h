#pragma once

#include <yql/essentials/public/udf/udf_value.h>

namespace NYql {

class IDqFullResultWriter {
public:
    virtual ~IDqFullResultWriter() = default;
    virtual void AddRow(const NYql::NUdf::TUnboxedValuePod& row) = 0;
    virtual void Finish() = 0;
    virtual void Abort() = 0;
    virtual ui64 GetRowCount() const = 0;
};

} // NYql
