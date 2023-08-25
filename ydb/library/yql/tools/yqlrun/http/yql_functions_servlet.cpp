#include "yql_functions_servlet.h"

#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>
#include <ydb/library/yql/providers/config/yql_config_provider.h>
#include <ydb/library/yql/providers/result/provider/yql_result_provider.h>
#include <ydb/library/yql/core/type_ann/type_ann_core.h>


namespace {

void OutputKnownFunctions(TStringStream& out)
{
    using namespace NYql;

    out <<
            "window.yql = {};\n"
            "window.yql.builtinFunctions = [";

    const auto& builtinFuncs = GetBuiltinFunctions();
    for (const auto& func: builtinFuncs) {
        out << '"' << func << "\",";
    }
    out << "];\n";

    out << "window.yql.supportedFunctions = [";

    const THashSet<TStringBuf>* functionsSet[] = {
        &YtDataSourceFunctions(),
        &YtDataSinkFunctions(),
        &ResultProviderFunctions(),
        &ConfigProviderFunctions()
    };

    for (const auto& funcs: functionsSet) {
        for (const auto& func: *funcs) {
            out << '"' << func << "\",";
        }
    }

    out << "];";
}

} // namspace

namespace NYql {
namespace NHttp {

void TYqlFunctoinsServlet::DoGet(const TRequest& req, TResponse& resp) const
{
    Y_UNUSED(req);

    TStringStream out;
    OutputKnownFunctions(out);
    resp.Body = TBlob::FromString(out.Str());
    resp.Headers.AddHeader(THttpInputHeader("Content-Type: text/javascript"));
}

} // namspace NNttp
} // namspace NYql
