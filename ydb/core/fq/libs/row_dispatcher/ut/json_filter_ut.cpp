#include <ydb/core/base/backtrace.h>

#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/core/fq/libs/row_dispatcher/common.h>
#include <ydb/core/fq/libs/row_dispatcher/json_filter.h>
#include <ydb/core/fq/libs/row_dispatcher/purecalc_compilation/compile_service.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>

#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <library/cpp/testing/unittest/registar.h>

namespace {

using namespace NKikimr;
using namespace NFq;

class TFixture : public NUnitTest::TBaseFixture {

public:
    TFixture()
        : Runtime(true)
        , Alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), true, false)
    {
        Alloc.Ref().UseRefLocking = true;
    }

    static void SegmentationFaultHandler(int) {
        Cerr << "segmentation fault call stack:" << Endl;
        FormatBackTrace(&Cerr);
        abort();
    }

    void SetUp(NUnitTest::TTestContext&) override {
        NKikimr::EnableYDBBacktraceFormat();
        signal(SIGSEGV, &SegmentationFaultHandler);

        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Runtime.Initialize(app->Unwrap());
        Runtime.SetLogPriority(NKikimrServices::FQ_ROW_DISPATCHER, NLog::PRI_DEBUG);
        Runtime.SetDispatchTimeout(TDuration::Seconds(5));

        CompileServiceActorId = Runtime.Register(NRowDispatcher::CreatePurecalcCompileService());
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
        with_lock (Alloc) {
            for (const auto& holder : Holders) {
                for (const auto& value : holder) {
                    Alloc.Ref().UnlockObject(value);
                }
            }
            Holders.clear();
        }
        Filter.reset();
    }

    void MakeFilter(
        const TVector<TString>& columns,
        const TVector<TString>& types,
        const TString& whereFilter,
        NFq::TJsonFilter::TCallback callback) {
        Filter = NFq::NewJsonFilter(
            columns,
            types,
            whereFilter,
            callback,
            {.EnabledLLVM = false});

        const auto edgeActor = Runtime.AllocateEdgeActor();
        Runtime.Send(CompileServiceActorId, edgeActor, Filter->GetCompileRequest().release());
        auto response = Runtime.GrabEdgeEvent<TEvRowDispatcher::TEvPurecalcCompileResponse>(edgeActor, TDuration::Seconds(5));

        UNIT_ASSERT_C(response, "Failed to get compile response");
        UNIT_ASSERT_C(response->Get()->ProgramHolder, "Failed to compile program, error: " << response->Get()->Error);
        Filter->OnCompileResponse(std::move(response));
    }

    void Push(const TVector<ui64>& offsets, const TVector<const TVector<NYql::NUdf::TUnboxedValue>*>& values) {
        Filter->Push(offsets, values, 0, values.front()->size());
    }

    const TVector<NYql::NUdf::TUnboxedValue>* MakeVector(size_t size, std::function<NYql::NUdf::TUnboxedValuePod(size_t)> valueCreator) {
        with_lock (Alloc) {
            Holders.emplace_front();
            for (size_t i = 0; i < size; ++i) {
                Holders.front().emplace_back(valueCreator(i));
                Alloc.Ref().LockObject(Holders.front().back());
            }
            return &Holders.front();
        }
    }

    template <typename TValue>
    const TVector<NYql::NUdf::TUnboxedValue>* MakeVector(const TVector<TValue>& values, bool optional = false) {
        return MakeVector(values.size(), [&](size_t i) {
            NYql::NUdf::TUnboxedValuePod unboxedValue = NYql::NUdf::TUnboxedValuePod(values[i]);
            return optional ? unboxedValue.MakeOptional() : unboxedValue;
        });
    }

    const TVector<NYql::NUdf::TUnboxedValue>* MakeStringVector(const TVector<TString>& values, bool optional = false) {
        return MakeVector(values.size(), [&](size_t i) {
            NYql::NUdf::TUnboxedValuePod stringValue = NKikimr::NMiniKQL::MakeString(values[i]);
            return optional ? stringValue.MakeOptional() : stringValue;
        });
    }

    const TVector<NYql::NUdf::TUnboxedValue>* MakeEmptyVector(size_t size) {
        return MakeVector(size, [&](size_t) {
            return NYql::NUdf::TUnboxedValuePod();
        });
    }

    NActors::TTestActorRuntime Runtime;
    TActorSystemStub ActorSystemStub;
    TActorId CompileServiceActorId;
    std::unique_ptr<NFq::TJsonFilter> Filter;

    NKikimr::NMiniKQL::TScopedAlloc Alloc;
    TList<TVector<NYql::NUdf::TUnboxedValue>> Holders;
};

Y_UNIT_TEST_SUITE(TJsonFilterTests) {
    Y_UNIT_TEST_F(Simple1, TFixture) {
        TMap<ui64, TString> result;
        MakeFilter(
            {"a1", "a2", "a@3"},
            {"[DataType; String]", "[DataType; Uint64]", "[OptionalType; [DataType; String]]"},
            "where a2 > 100",
            [&](ui64 offset, const TString& json) {
                result[offset] = json;
            });
        Push({5}, {MakeStringVector({"hello1"}), MakeVector<ui64>({99}), MakeStringVector({"zapuskaem"}, true)});
        Push({6}, {MakeStringVector({"hello2"}), MakeVector<ui64>({101}), MakeStringVector({"gusya"}, true)});
        UNIT_ASSERT_VALUES_EQUAL(1, result.size());
        UNIT_ASSERT_VALUES_EQUAL(R"({"a1":"hello2","a2":101,"a@3":"gusya"})", result[6]);
    }

    Y_UNIT_TEST_F(Simple2, TFixture) {
        TMap<ui64, TString> result;
        MakeFilter(
            {"a2", "a1"},
            {"[DataType; Uint64]", "[DataType; String]"},
            "where a2 > 100",
            [&](ui64 offset, const TString& json) {
                result[offset] = json;
            });
        Push({5}, {MakeVector<ui64>({99}), MakeStringVector({"hello1"})});
        Push({6}, {MakeVector<ui64>({101}), MakeStringVector({"hello2"})});
        UNIT_ASSERT_VALUES_EQUAL(1, result.size());
        UNIT_ASSERT_VALUES_EQUAL(R"({"a1":"hello2","a2":101})", result[6]);
    }

    Y_UNIT_TEST_F(ManyValues, TFixture) {
        TMap<ui64, TString> result;
        MakeFilter(
            {"a1", "a2", "a3"},
            {"[DataType; String]", "[DataType; Uint64]", "[DataType; String]"},
            "where a2 > 100",
            [&](ui64 offset, const TString& json) {
                result[offset] = json;
            });
        const TString largeString = "abcdefghjkl1234567890+abcdefghjkl1234567890";
        for (ui64 i = 0; i < 5; ++i) {
            Push({2 * i, 2 * i + 1}, {MakeStringVector({"hello1", "hello2"}), MakeVector<ui64>({99, 101}), MakeStringVector({largeString, largeString})});
            UNIT_ASSERT_VALUES_EQUAL_C(i + 1, result.size(), i);
            UNIT_ASSERT_VALUES_EQUAL_C(TStringBuilder() << "{\"a1\":\"hello2\",\"a2\":101,\"a3\":\"" << largeString << "\"}", result[2 * i + 1], i);
        }
    }

    Y_UNIT_TEST_F(NullValues, TFixture) {
        TMap<ui64, TString> result;
        MakeFilter(
            {"a1", "a2"},
            {"[OptionalType; [DataType; Uint64]]", "[DataType; String]"},
            "where a1 is null",
            [&](ui64 offset, const TString& json) {
                result[offset] = json;
            });
        Push({5}, {MakeEmptyVector(1), MakeStringVector({"str"})});
        UNIT_ASSERT_VALUES_EQUAL(1, result.size());
        UNIT_ASSERT_VALUES_EQUAL(R"({"a1":null,"a2":"str"})", result[5]);
    }

    Y_UNIT_TEST_F(PartialPush, TFixture) {
        TMap<ui64, TString> result;
        MakeFilter(
            {"a1", "a2", "a@3"},
            {"[DataType; String]", "[DataType; Uint64]", "[OptionalType; [DataType; String]]"},
            "where a2 > 50",
            [&](ui64 offset, const TString& json) {
                result[offset] = json;
            });
        Filter->Push({5, 6, 7}, {MakeStringVector({"hello1", "hello2"}), MakeVector<ui64>({99, 101}), MakeStringVector({"zapuskaem", "gusya"}, true)}, 1, 1);
        UNIT_ASSERT_VALUES_EQUAL(1, result.size());
        UNIT_ASSERT_VALUES_EQUAL(R"({"a1":"hello1","a2":99,"a@3":"zapuskaem"})", result[6]);
    }
}

}
