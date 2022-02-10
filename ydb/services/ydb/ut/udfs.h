#pragma once
#include <ydb/library/yql/public/udf/udf_registrator.h>

NYql::NUdf::TUniquePtr<NYql::NUdf::IUdfModule> CreateRe2Module();
NYql::NUdf::TUniquePtr<NYql::NUdf::IUdfModule> CreateJson2Module();
