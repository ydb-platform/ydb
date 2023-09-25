#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/comp_factory.h>

#include <ydb/library/yql/minikql/comp_nodes/ut/mkql_computation_node_ut.h>

#include <random>
#include <ctime>
#include <algorithm>

namespace NKikimr {
namespace NMiniKQL {

using namespace NYql;

namespace {

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLPGSortTest) {
    Y_UNIT_TEST_LLVM(TestListSort) {
        const std::array<TString, 3U> values = {{"2000-01-01","1979-12-12","2010-12-01"}};
        const std::array<TString, 3U> sortedValues = {{"1979-12-12","2000-01-01","2010-12-01"} };

        TSetup<LLVM> setup(GetTestFactory(GetPgFactory()));
        TProgramBuilder& pgmBuilder = *setup.PgmBuilder;

        auto pgDateType = static_cast<TPgType*>(pgmBuilder.NewPgType(NPg::LookupType("date").TypeId));
        auto pgTextType = static_cast<TPgType*>(pgmBuilder.NewPgType(NPg::LookupType("text").TypeId));
        auto utf8Type = pgmBuilder.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id);

        std::vector<TRuntimeNode> data;
        for (const auto& x : values) {
            data.push_back(pgmBuilder.PgConst(pgDateType, x));
        }

        const auto list = pgmBuilder.NewList(pgDateType, data);

        const auto pgmReturn = pgmBuilder.Map(pgmBuilder.Sort(list, pgmBuilder.NewDataLiteral<bool>(true),
            [&](TRuntimeNode item) { return item; }),
            [&](TRuntimeNode item) {
                return pgmBuilder.FromPg(pgmBuilder.PgCast(item, pgTextType), utf8Type);
            });

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto& result = graph->GetValue();

        UNIT_ASSERT_VALUES_EQUAL(result.GetListLength(), sortedValues.size());
        for (ui32 i = 0; i < sortedValues.size(); ++i) {
            auto elem = result.GetElement(i);
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf(elem.AsStringRef()), sortedValues[i]);
        }
    }
}
} // NMiniKQL
} // NKikimr
