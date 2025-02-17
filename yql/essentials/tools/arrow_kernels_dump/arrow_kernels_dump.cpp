#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/public/udf/udf_version.h>

#include <util/stream/output.h>
#include <util/generic/algorithm.h>
#include <util/folder/path.h>

int main(int argc, char **argv) {
    Y_UNUSED(argc);
    Cerr << TFsPath(argv[0]).GetName() << " ABI version: " << NKikimr::NUdf::CurrentAbiVersionStr() << Endl;

    auto builtins = NKikimr::NMiniKQL::CreateBuiltinRegistry();
    auto families = builtins->GetAllKernelFamilies();
    Sort(families, [](const auto& x, const auto& y) { return x.first < y.first; });
    ui64 totalKernels = 0;
    for (const auto& f : families) {
        auto numKernels = f.second->GetAllKernels().size();
        Cout << f.first << ": " << numKernels << " kernels" << Endl;
        totalKernels += numKernels;
    }

    Cout << "Total kernel families: " << families.size() << ", kernels: " << totalKernels << Endl;
    return 0;
}
