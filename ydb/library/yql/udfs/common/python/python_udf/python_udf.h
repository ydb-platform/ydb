#pragma once

#include <ydb/library/yql/public/udf/udf_registrator.h>

namespace NYql {
namespace NUdf {

enum class EPythonFlavor {
    System,
    Arcadia,
};

void RegisterYqlPythonUdf(
        IRegistrator& registrator,
        ui32 flags,
        TStringBuf moduleName,
        TStringBuf resourceName,
        EPythonFlavor pythonFlavor);

TUniquePtr<IUdfModule> GetYqlPythonUdfModule(
    TStringBuf resourceName,
    EPythonFlavor pythonFlavor,
    bool standalone);

} // namespace NUdf
} // namespace NYql
