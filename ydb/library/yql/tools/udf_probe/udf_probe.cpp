#include <ydb/library/yql/minikql/mkql_function_registry.h>

#include <util/generic/yexception.h>
#include <util/generic/string.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

void ListModules(const TString& dir) {
    TVector<TString> udfPaths;
    NMiniKQL::FindUdfsInDir(dir, &udfPaths);
    auto funcRegistry = CreateFunctionRegistry(nullptr, IBuiltinFunctionRegistry::TPtr(), false, udfPaths,
        NUdf::IRegistrator::TFlags::TypesOnly);

    for (auto& m : funcRegistry->GetAllModuleNames()) {
        auto path = *funcRegistry->FindUdfPath(m);
        Cout << m << '\t' << path << Endl;
    }
}

int main(int argc, char **argv) {
    try {
        if (argc != 2) {
            Cerr << "Expected directory path\n";
        }

        TString dir = argv[1];
        Y_UNUSED(NUdf::GetStaticSymbols());
        ListModules(dir);
        return 0;
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
