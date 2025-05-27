// Copied from ydb/core/kqp/ut/common/re2_udf.cpp

#include <yql/essentials/udfs/common/re2/re2_udf.cpp>

NYql::NUdf::TUniquePtr<NYql::NUdf::IUdfModule> CreateRe2Module() {
    return new TRe2Module<true>();
} 
