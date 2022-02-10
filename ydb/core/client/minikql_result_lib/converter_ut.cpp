#include "converter.h"

#include <ydb/core/testlib/test_client.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {

using namespace NResultLib;

Y_UNIT_TEST_SUITE(TMiniKQLResultConverterTest) {

Y_UNIT_TEST(TTestWithSimpleProgram) {
    TPortManager pm;
    ui16 port = pm.GetPort(2134);
    auto settings = Tests::TServerSettings(port);
    Tests::TServer server = Tests::TServer(settings);
    Tests::TClient client(settings);
    client.InitRootScheme();

    const TString pgmText = R"___(
(
(let list (AsList (Uint32 '20) (Uint32 '10) (Uint32 '0)))
(let opt (Just (String 'i_am_opt)))
(let tuple '((Double '12) (Float '22)))
(return (AsList
    (SetResult 'list list)
    (SetResult 'opt opt)
    (SetResult 'emptyOpt (Nothing (OptionalType (DataType 'Int32))))
    (SetResult 'tuple tuple)
))
)
)___";

    NKikimrMiniKQL::TResult result;
    UNIT_ASSERT(client.FlatQuery(pgmText, result));

    TStruct s = ConvertResult(result.GetValue(), result.GetType());
    {
        TListType l = s.GetMember<TOptional>("list").GetItem<TListType>();
        TVector<ui32> v;
        for (ui32 item : l.MakeIterable<ui32>()) {
            v.push_back(item);
        }
        UNIT_ASSERT_EQUAL(v.size(), 3);
        UNIT_ASSERT_EQUAL(v[0], 20);
        UNIT_ASSERT_EQUAL(v[1], 10);
        UNIT_ASSERT_EQUAL(v[2], 0);
    }
    {
        TOptional opt = s.GetMember<TOptional>("opt").GetItem<TOptional>();
        UNIT_ASSERT_EQUAL(opt.GetItem<TStringBuf>(), "i_am_opt");
    }
    {
        TOptional opt = s.GetMember<TOptional>("emptyOpt").GetItem<TOptional>();
        UNIT_ASSERT(!opt.HasItem());
    }
    {
        TTuple tuple = s.GetMember<TOptional>("tuple").GetItem<TTuple>();
        UNIT_ASSERT_DOUBLES_EQUAL(tuple.GetElement<double>(0), 12.0, 1e-3);
        UNIT_ASSERT_DOUBLES_EQUAL(tuple.GetElement<float>(1), 22.0, 1e-3);
    }
}

}


} // namespace NKikimr
