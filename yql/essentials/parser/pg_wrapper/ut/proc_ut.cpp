#include "../pg_compat.h"
#include "../comp_factory_utils.h"

extern "C" {
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/postgres.h>
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/c.h>
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/fmgr.h>
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/varatt.h>
}

#include <yql/essentials/parser/pg_catalog/catalog.h>

#include <yql/essentials/parser/pg_wrapper/utils.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TProcTests) {
Y_UNIT_TEST(ConcatNullArgument) {
    const auto& proc = NPg::LookupProc("concat", {TEXTOID, 0});
    const TVector<std::optional<ui32>> argumentTypes = {TEXTOID, std::nullopt};
    const TPgResolvedCallBase call("concat", proc.ProcId, argumentTypes, TEXTOID, false, nullptr);
    UNIT_ASSERT_VALUES_EQUAL(call.GetArgCount(), 2);
    UNIT_ASSERT_VALUES_EQUAL(call.GetArgTypeId(0), TEXTOID);
    UNIT_ASSERT_VALUES_EQUAL(call.GetArgTypeId(1), proc.VariadicType);
}

Y_UNIT_TEST(ConcatVariadicArgumentTypes) {
    const auto& proc = NPg::LookupProc("concat", {TEXTOID, 0});
    const TVector<TVector<std::optional<ui32>>> testCases = {
        {}, {std::nullopt}, {std::nullopt, std::nullopt},
        {std::nullopt, TEXTOID},
        {TEXTOID, std::nullopt, INT4OID, std::nullopt}};
    for (const auto& argumentTypes : testCases) {
        const TPgResolvedCallBase call("concat", proc.ProcId, argumentTypes, TEXTOID, false, nullptr);
        UNIT_ASSERT_VALUES_EQUAL(call.GetArgCount(), argumentTypes.size());
        for (size_t index = 0; index < argumentTypes.size(); ++index) {
            UNIT_ASSERT_VALUES_EQUAL(call.GetArgTypeId(index), argumentTypes[index].value_or(proc.VariadicType));
        }
    }
}

Y_UNIT_TEST(ConcatWsNullArgumentTypes) {
    const auto& proc = NPg::LookupProc("concat_ws", {TEXTOID, TEXTOID, 0});
    const TVector<std::optional<ui32>> argumentTypes = {std::nullopt, TEXTOID, std::nullopt};
    const TPgResolvedCallBase call("concat_ws", proc.ProcId, argumentTypes, TEXTOID, false, nullptr);
    UNIT_ASSERT_VALUES_EQUAL(call.GetArgTypeId(0), TEXTOID);
    UNIT_ASSERT_VALUES_EQUAL(call.GetArgTypeId(1), TEXTOID);
    UNIT_ASSERT_VALUES_EQUAL(call.GetArgTypeId(2), proc.VariadicType);
}

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
} // Y_UNIT_TEST_SUITE(TProcTests)

} // namespace NYql
