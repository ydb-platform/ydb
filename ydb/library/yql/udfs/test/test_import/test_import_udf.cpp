#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

#include <util/string/cast.h>
#include <util/generic/string.h>

using namespace NKikimr;
using namespace NUdf;

namespace {

SIMPLE_UDF(TConcat, char*(char*, char*)) {
    return valueBuilder->ConcatStrings(args[0], args[1]);
}

SIMPLE_UDF(TRepeat, char*(char*, ui64)) {
    TString orig(args[0].AsStringRef());
    ui64 times = args[1].Get<ui64>();
    TString res = "";
    for (ui64 i = 0; i < times; i++) {
        res += orig;
    }
    return valueBuilder->NewString(res);
}

SIMPLE_MODULE(TTestImportUdfModule,
                TConcat,
                TRepeat
              )

} // namespace

REGISTER_MODULES(TTestImportUdfModule)
