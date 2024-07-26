#include "../pg_compat.h"

extern "C" {
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/include/postgres.h>
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/include/c.h>
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/include/fmgr.h>
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/include/varatt.h>
}

#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#include <ydb/library/yql/parser/pg_wrapper/utils.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TProcTests) {
    Y_UNIT_TEST(BuiltinsHasRuntimeFuncs) {
        if (NPg::AreAllFunctionsAllowed()) {
            return;
        }
        
        NPg::EnumProc([](ui32 oid, const NPg::TProcDesc& desc) {
            if (desc.ExtensionIndex == 0 && desc.Kind == NPg::EProcKind::Function &&
                desc.Lang == NPg::LangInternal) {
                FmgrInfo finfo;
                UNIT_ASSERT(GetPgFuncAddr(oid, finfo));
                UNIT_ASSERT(finfo.fn_addr);
            }
        });
    }
}

}
