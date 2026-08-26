#include "args.h"

#include <yql/essentials/tools/yql_language_server/api/api.h>
#include <yql/essentials/tools/yql_language_server/service/layer.h>
#include <yql/essentials/tools/yql_language_server/lsp/server/server.h>

namespace NLsp::NYql {

int Main(TArgs args) {
    auto service = MakeServiceLayer();
    auto api = MakeYqlLspApi(std::move(service));
    LspServe(Cin, Cout, {.Threads = args.Threads}, std::move(api));
    return 0;
}

int Main(int argc, char** argv) {
    return Main(TArgs::Parse(argc, argv));
}

} // namespace NLsp::NYql

int main(int argc, char** argv) try {
    return NLsp::NYql::Main(argc, argv);
} catch (...) {
    Cerr << CurrentExceptionMessage();
    return 1;
}
