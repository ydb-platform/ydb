import sys

TEMPLATE = """
#include <@YQL_BASE_DIR@/udfs/common/python/python_udf/python_udf.h>

#include <@YQL_BASE_DIR@/public/udf/udf_registrator.h>

#if @WITH_LIBRA@
#include <yql/udfs/quality/libra/module/module.h>
#endif

using namespace NKikimr::NUdf;

#ifdef BUILD_UDF

#if @WITH_LIBRA@
LIBRA_MODULE(TLibraModule, "Libra@MODULE_NAME@");
#endif

extern "C" UDF_API void Register(IRegistrator& registrator, ui32 flags) {
    RegisterYqlPythonUdf(registrator, flags, TStringBuf("@MODULE_NAME@"), TStringBuf("@PACKAGE_NAME@"), EPythonFlavor::@FLAVOR@);
#if @WITH_LIBRA@
    RegisterHelper<TLibraModule>(registrator);
#endif
}

extern "C" UDF_API ui32 AbiVersion() {
    return CurrentAbiVersion();
}

extern "C" UDF_API void SetBackTraceCallback(TBackTraceCallback callback) {
    SetBackTraceCallbackImpl(callback);
}

#endif
"""


def main():
    assert len(sys.argv) == 7
    flavor, module_name, package_name, path, libra_flag, yql_base_dir = sys.argv[1:]
    with open(path, 'w') as f:
        f.write(
            TEMPLATE.strip()
            .replace('@MODULE_NAME@', module_name)
            .replace('@PACKAGE_NAME@', package_name)
            .replace('@FLAVOR@', flavor)
            .replace('@WITH_LIBRA@', libra_flag)
            .replace('@YQL_BASE_DIR@', yql_base_dir)
        )
        f.write('\n')


if __name__ == "__main__":
    main()
