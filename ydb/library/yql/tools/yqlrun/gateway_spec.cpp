#include <ydb/library/yql/tools/yqlrun/gateway_spec.h>

#include <library/cpp/getopt/last_getopt.h>

using namespace NYql;
using namespace NKikimr::NMiniKQL;

void ExtProviderSpecific(const IFunctionRegistry* funcRegistry,
        TVector<TDataProviderInitializer>& dataProvidersInit,
        const THashMap<std::pair<TString, TString>, TVector<std::pair<TString, TString>>>& rtmrTableAttributes) {
    Y_UNUSED(funcRegistry);
    Y_UNUSED(dataProvidersInit);
    Y_UNUSED(rtmrTableAttributes);
}
