#include "all.h"

#include <library/cpp/lwtrace/protos/lwtrace.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>

enum ESimpleEnum {
    ValueA,
    ValueB,
};

enum class EEnumClass {
    ValueC,
    ValueD,
};

#define LWTRACE_UT_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)                                          \
    PROBE(NoParam, GROUPS("Group"), TYPES(), NAMES())                                                    \
    PROBE(IntParam, GROUPS("Group"), TYPES(ui32), NAMES("value"))                                        \
    PROBE(StringParam, GROUPS("Group"), TYPES(TString), NAMES("value"))                                  \
    PROBE(SymbolParam, GROUPS("Group"), TYPES(NLWTrace::TSymbol), NAMES("symbol"))                       \
    PROBE(CheckParam, GROUPS("Group"), TYPES(NLWTrace::TCheck), NAMES("value"))                          \
    PROBE(EnumParams, GROUPS("Group"), TYPES(ESimpleEnum, EEnumClass), NAMES("simpleEnum", "enumClass")) \
    PROBE(InstantParam, GROUPS("Group"), TYPES(TInstant), NAMES("value"))                                \
    PROBE(DurationParam, GROUPS("Group"), TYPES(TDuration), NAMES("value"))                              \
    PROBE(ProtoEnum, GROUPS("Group"), TYPES(NLWTrace::EOperatorType), NAMES("value"))                    \
    PROBE(IntIntParams, GROUPS("Group"), TYPES(ui32, ui64), NAMES("value1", "value2"))                   \
    /**/

LWTRACE_DECLARE_PROVIDER(LWTRACE_UT_PROVIDER)
LWTRACE_DEFINE_PROVIDER(LWTRACE_UT_PROVIDER)
LWTRACE_USING(LWTRACE_UT_PROVIDER)

using namespace NLWTrace;

Y_UNIT_TEST_SUITE(LWTraceTrace) {
#ifndef LWTRACE_DISABLE
    Y_UNIT_TEST(Smoke) {
        TManager mngr(*Singleton<TProbeRegistry>(), true);
        TQuery q;
        bool parsed = NProtoBuf::TextFormat::ParseFromString(R"END(
            Blocks {
                ProbeDesc {
                    Name: "NoParam"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Action {
                    LogAction { }
                }
            }
        )END",
                                                             &q);
        UNIT_ASSERT(parsed);
        mngr.New("Query1", q);
        LWPROBE(NoParam);
        struct {
            void Push(TThread::TId, const TLogItem& item) {
                UNIT_ASSERT(TString(item.Probe->Event.Name) == "NoParam");
            }
        } reader;
        mngr.ReadLog("Query1", reader);

        LWPROBE(EnumParams, ValueA, EEnumClass::ValueC);
        LWPROBE(InstantParam, TInstant::Seconds(42));
        LWPROBE(DurationParam, TDuration::MilliSeconds(146));
        LWPROBE(ProtoEnum, OT_EQ);
    }

    Y_UNIT_TEST(Predicate) {
        TManager mngr(*Singleton<TProbeRegistry>(), true);
        TQuery q;
        bool parsed = NProtoBuf::TextFormat::ParseFromString(R"END(
            Blocks {
                ProbeDesc {
                    Name: "IntParam"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Predicate {
                    Operators {
                        Type: OT_NE
                        Argument { Param: "value" }
                        Argument { Value: "1" }
                    }
                }
                Action {
                    LogAction { }
                }
            }
        )END", &q);
        UNIT_ASSERT(parsed);
        mngr.New("QueryName", q);
        LWPROBE(IntParam, 3);
        LWPROBE(IntParam, 1);
        LWPROBE(IntParam, 4);
        LWPROBE(IntParam, 1);
        LWPROBE(IntParam, 1);
        LWPROBE(IntParam, 5);
        struct {
            ui32 expected = 3;
            ui32 logsCount = 0;
            void Push(TThread::TId, const TLogItem& item) {
                UNIT_ASSERT(TString(item.Probe->Event.Name) == "IntParam");
                ui32 value = item.GetParam("value").GetParam().Get<ui32>();
                UNIT_ASSERT(value == expected);
                expected++;
                logsCount++;
            }
        } reader;
        mngr.ReadLog("QueryName", reader);
        UNIT_ASSERT(reader.logsCount == 3);
    }

    Y_UNIT_TEST(StatementAction) {
        TManager mngr(*Singleton<TProbeRegistry>(), true);
        TQuery q;
        bool parsed = NProtoBuf::TextFormat::ParseFromString(R"END(
            Blocks {
                ProbeDesc {
                    Name: "IntParam"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Action {
                    StatementAction {
                        Type: ST_INC
                        Argument { Variable: "varInc" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_DEC
                        Argument { Variable: "varDec" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_MOV
                        Argument { Variable: "varMov" }
                        Argument { Value: "3" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_ADD_EQ
                        Argument { Variable: "varAddEq" }
                        Argument { Value: "2" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_ADD_EQ
                        Argument { Variable: "varAddEq" }
                        Argument { Value: "3" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_SUB_EQ
                        Argument { Variable: "varSubEq" }
                        Argument { Value: "5" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_ADD
                        Argument { Variable: "varAdd" }
                        Argument { Value: "3" }
                        Argument { Value: "2" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_SUB
                        Argument { Variable: "varSub" }
                        Argument { Value: "3" }
                        Argument { Value: "2" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_MUL
                        Argument { Variable: "varMul" }
                        Argument { Value: "6" }
                        Argument { Value: "2" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_DIV
                        Argument { Variable: "varDiv" }
                        Argument { Value: "6" }
                        Argument { Value: "2" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_MOD
                        Argument { Variable: "varMod" }
                        Argument { Value: "17" }
                        Argument { Value: "5" }
                    }
                }
            }
            Blocks {
                ProbeDesc {
                    Name: "IntParam"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Predicate {
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varInc" }
                        Argument { Value: "1" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varDec" }
                        Argument { Value: "-1" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varMov" }
                        Argument { Value: "3" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varAddEq" }
                        Argument { Value: "5" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varSubEq" }
                        Argument { Value: "-5" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varAdd" }
                        Argument { Value: "5" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varSub" }
                        Argument { Value: "1" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varMul" }
                        Argument { Value: "12" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varDiv" }
                        Argument { Value: "3" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varMod" }
                        Argument { Value: "2" }
                    }
                }
                Action {
                    LogAction { }
                }
            }
        )END", &q);
        UNIT_ASSERT(parsed);
        mngr.New("QueryName", q);
        LWPROBE(IntParam, 1);
        LWPROBE(IntParam, 2);
        struct {
            int logsCount = 0;
            void Push(TThread::TId, const TLogItem& item) {
                UNIT_ASSERT(TString(item.Probe->Event.Name) == "IntParam");
                ui32 value = item.GetParam("value").GetParam().Get<ui32>();
                UNIT_ASSERT(value == 1);
                logsCount++;
            }
        } reader;
        mngr.ReadLog("QueryName", reader);
        UNIT_ASSERT(reader.logsCount == 1);
    }

    Y_UNIT_TEST(StatementActionWithParams) {
        TManager mngr(*Singleton<TProbeRegistry>(), true);
        TQuery q;
        bool parsed = NProtoBuf::TextFormat::ParseFromString(R"END(
            Blocks {
                ProbeDesc {
                    Name: "IntIntParams"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Action {
                    StatementAction {
                        Type: ST_MOV
                        Argument { Variable: "varMov" }
                        Argument { Param: "value1" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_ADD_EQ
                        Argument { Variable: "varAddEq" }
                        Argument { Param: "value1" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_SUB_EQ
                        Argument { Variable: "varSubEq" }
                        Argument { Param: "value1" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_ADD
                        Argument { Variable: "varAdd" }
                        Argument { Param: "value1" }
                        Argument { Param: "value2" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_SUB
                        Argument { Variable: "varSub" }
                        Argument { Param: "value1" }
                        Argument { Param: "value2" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_MUL
                        Argument { Variable: "varMul" }
                        Argument { Param: "value1" }
                        Argument { Param: "value2" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_DIV
                        Argument { Variable: "varDiv" }
                        Argument { Param: "value1" }
                        Argument { Param: "value2" }
                    }
                }
                Action {
                    StatementAction {
                        Type: ST_MOD
                        Argument { Variable: "varMod" }
                        Argument { Param: "value1" }
                        Argument { Param: "value2" }
                    }
                }
            }
            Blocks {
                ProbeDesc {
                    Name: "IntIntParams"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Predicate {
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varMov" }
                        Argument { Param: "value1" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varAddEq" }
                        Argument { Param: "value1" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varSubEq" }
                        Argument { Value: "-22" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varAdd" }
                        Argument { Value: "25" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varSub" }
                        Argument { Value: "19" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varMul" }
                        Argument { Value: "66" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varDiv" }
                        Argument { Value: "7" }
                    }
                    Operators {
                        Type: OT_EQ
                        Argument { Variable: "varMod" }
                        Argument { Value: "1" }
                    }
                }
                Action {
                    LogAction { }
                }
            }
        )END", &q);
        UNIT_ASSERT(parsed);
        mngr.New("QueryName", q);
        LWPROBE(IntIntParams, 22, 3);
        struct {
            int logsCount = 0;
            void Push(TThread::TId, const TLogItem& item) {
                UNIT_ASSERT(TString(item.Probe->Event.Name) == "IntIntParams");
                logsCount++;
            }
        } reader;
        mngr.ReadLog("QueryName", reader);
        UNIT_ASSERT(reader.logsCount == 1);
    }

    Y_UNIT_TEST(PerThreadLogSize) {
        TManager mngr(*Singleton<TProbeRegistry>(), true);
        TQuery q;
        bool parsed = NProtoBuf::TextFormat::ParseFromString(R"END(
            PerThreadLogSize: 3
            Blocks {
                ProbeDesc {
                    Name: "IntParam"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Action {
                    LogAction { }
                }
            }
        )END", &q);
        UNIT_ASSERT(parsed);
        mngr.New("QueryRandom", q);
        LWPROBE(IntParam, 1);
        LWPROBE(IntParam, 2);
        LWPROBE(IntParam, 3);
        LWPROBE(IntParam, 4);
        struct {
            ui32 logsCount = 0;
            ui32 expected = 2;
            void Push(TThread::TId, const TLogItem& item) {
                UNIT_ASSERT(TString(item.Probe->Event.Name) == "IntParam");
                ui32 value = item.GetParam("value").GetParam().Get<ui32>();
                UNIT_ASSERT(value == expected);
                logsCount++;
                expected++;
            }
        } reader;
        mngr.ReadLog("QueryRandom", reader);
        UNIT_ASSERT(reader.logsCount == 3);
    }

    Y_UNIT_TEST(CustomAction) {
        static ui32 nCustomActionsCalls = 0;
        class TMyActionExecutor: public TCustomActionExecutor {
        public:
            TMyActionExecutor(TProbe* probe, const TCustomAction&, TSession*)
                : TCustomActionExecutor(probe, false /* not destructive */)
            {}
        private:
            bool DoExecute(TOrbit&, const TParams& params) override {
                (void)params;
                nCustomActionsCalls++;
                return true;
            }
        };

        TManager mngr(*Singleton<TProbeRegistry>(), true);
        mngr.RegisterCustomAction("MyCustomAction", [](TProbe* probe,
                                                       const TCustomAction& action,
                                                       TSession* session) {
                return new TMyActionExecutor(probe, action, session);
            }
        );
        TQuery q;
        bool parsed = NProtoBuf::TextFormat::ParseFromString(R"END(
            Blocks {
                ProbeDesc {
                    Name: "NoParam"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Action {
                    CustomAction {
                        Name: "MyCustomAction"
                    }
                }
            }
        )END", &q);
        UNIT_ASSERT(parsed);
        mngr.New("Query1", q);
        LWPROBE(NoParam);
        UNIT_ASSERT(nCustomActionsCalls == 1);
    }

    Y_UNIT_TEST(SafeModeSleepException) {
        TManager mngr(*Singleton<TProbeRegistry>(), false);
        TQuery q;
        bool parsed = NProtoBuf::TextFormat::ParseFromString(R"END(
            Blocks {
                ProbeDesc {
                    Name: "NoParam"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Action {
                    SleepAction {
                        NanoSeconds: 1000000000
                    }
                }
            }
        )END", &q);
        UNIT_ASSERT(parsed);
        UNIT_ASSERT_EXCEPTION(mngr.New("QueryName", q), yexception);
    }

    Y_UNIT_TEST(Sleep) {
        TManager mngr(*Singleton<TProbeRegistry>(), true);
        TQuery q;
        bool parsed = NProtoBuf::TextFormat::ParseFromString(R"END(
            Blocks {
                ProbeDesc {
                    Name: "NoParam"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Action {
                    SleepAction {
                        NanoSeconds: 1000
                    }
                }
            }
        )END", &q);
        UNIT_ASSERT(parsed);
        mngr.New("QueryName", q);
        const ui64 sleepTimeNs = 1000;  // 1 us

        TInstant t0 = Now();
        LWPROBE(NoParam);
        TInstant t1 = Now();
        UNIT_ASSERT(t1.NanoSeconds() - t0.NanoSeconds() >= sleepTimeNs);
    }

    Y_UNIT_TEST(ProtoEnumTraits) {
        using TPbEnumTraits = TParamTraits<EOperatorType>;
        TString str;
        TPbEnumTraits::ToString(TPbEnumTraits::ToStoreType(OT_EQ), &str);
        UNIT_ASSERT_STRINGS_EQUAL(str, "OT_EQ (0)");
    }

    Y_UNIT_TEST(Track) {
        TManager mngr(*Singleton<TProbeRegistry>(), true);
        TQuery q;
        bool parsed = NProtoBuf::TextFormat::ParseFromString(R"END(
            Blocks {
                ProbeDesc {
                    Name: "IntParam"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Action {
                    RunLogShuttleAction { }
                }
            }
        )END", &q);
        UNIT_ASSERT(parsed);
        mngr.New("Query1", q);

        {
            TOrbit orbit;
            LWTRACK(IntParam, orbit, 1);
            LWTRACK(StringParam, orbit, "str");
        }

        struct {
            void Push(TThread::TId, const TTrackLog& tl) {
                UNIT_ASSERT(tl.Items.size() == 2);
                UNIT_ASSERT(TString(tl.Items[0].Probe->Event.Name) == "IntParam");
                UNIT_ASSERT(TString(tl.Items[1].Probe->Event.Name) == "StringParam");
            }
        } reader;
        mngr.ReadDepot("Query1", reader);
    }

    Y_UNIT_TEST(ShouldSerializeTracks)
    {
        TManager manager(*Singleton<TProbeRegistry>(), false);

        TOrbit orbit;
        TTraceRequest req;
        req.SetIsTraced(true);
        manager.HandleTraceRequest(req, orbit);

        LWTRACK(NoParam, orbit);
        LWTRACK(IntParam, orbit, 1);
        LWTRACK(StringParam, orbit, "str");
        LWTRACK(EnumParams, orbit, ValueA, EEnumClass::ValueC);
        LWTRACK(InstantParam, orbit, TInstant::Seconds(42));
        LWTRACK(DurationParam, orbit, TDuration::MilliSeconds(146));
        LWTRACK(ProtoEnum, orbit, OT_EQ);
        LWTRACK(IntIntParams, orbit, 1, 2);

        TTraceResponse resp;
        orbit.Serialize(0, *resp.MutableTrace());
        auto& r = resp.GetTrace();

        UNIT_ASSERT_VALUES_EQUAL(8, r.EventsSize());

        const auto& p0 = r.GetEvents(0);
        UNIT_ASSERT_VALUES_EQUAL("NoParam", p0.GetName());
        UNIT_ASSERT_VALUES_EQUAL("LWTRACE_UT_PROVIDER", p0.GetProvider());
        UNIT_ASSERT_VALUES_EQUAL(0 , p0.ParamsSize());

        const auto& p1 = r.GetEvents(1);
        UNIT_ASSERT_VALUES_EQUAL("IntParam", p1.GetName());
        UNIT_ASSERT_VALUES_EQUAL("LWTRACE_UT_PROVIDER", p1.GetProvider());
        UNIT_ASSERT_VALUES_EQUAL(1, p1.GetParams(0).GetUintValue());

        const auto& p2 = r.GetEvents(2);
        UNIT_ASSERT_VALUES_EQUAL("StringParam", p2.GetName());
        UNIT_ASSERT_VALUES_EQUAL("LWTRACE_UT_PROVIDER", p2.GetProvider());
        UNIT_ASSERT_VALUES_EQUAL("str", p2.GetParams(0).GetStrValue());

        const auto& p3 = r.GetEvents(3);
        UNIT_ASSERT_VALUES_EQUAL("EnumParams", p3.GetName());
        UNIT_ASSERT_VALUES_EQUAL("LWTRACE_UT_PROVIDER", p3.GetProvider());
        UNIT_ASSERT_VALUES_EQUAL((ui32)ValueA, p3.GetParams(0).GetIntValue());
        UNIT_ASSERT_VALUES_EQUAL((ui32)EEnumClass::ValueC, p3.GetParams(1).GetIntValue());

        const auto& p4 = r.GetEvents(4);
        UNIT_ASSERT_VALUES_EQUAL("InstantParam", p4.GetName());
        UNIT_ASSERT_VALUES_EQUAL("LWTRACE_UT_PROVIDER", p4.GetProvider());
        UNIT_ASSERT_VALUES_EQUAL(42, p4.GetParams(0).GetDoubleValue());

        const auto& p5 = r.GetEvents(5);
        UNIT_ASSERT_VALUES_EQUAL("DurationParam", p5.GetName());
        UNIT_ASSERT_VALUES_EQUAL("LWTRACE_UT_PROVIDER", p5.GetProvider());
        UNIT_ASSERT_VALUES_EQUAL(146, p5.GetParams(0).GetDoubleValue());

        const auto& p6 = r.GetEvents(6);
        UNIT_ASSERT_VALUES_EQUAL("ProtoEnum", p6.GetName());
        UNIT_ASSERT_VALUES_EQUAL("LWTRACE_UT_PROVIDER", p6.GetProvider());
        UNIT_ASSERT_VALUES_EQUAL((int)OT_EQ, p6.GetParams(0).GetIntValue());

        const auto& p7 = r.GetEvents(7);
        UNIT_ASSERT_VALUES_EQUAL("IntIntParams", p7.GetName());
        UNIT_ASSERT_VALUES_EQUAL("LWTRACE_UT_PROVIDER", p7.GetProvider());
        UNIT_ASSERT_VALUES_EQUAL(1, p7.GetParams(0).GetUintValue());
        UNIT_ASSERT_VALUES_EQUAL(2, p7.GetParams(1).GetUintValue());
    }

    Y_UNIT_TEST(ShouldDeserializeTracks)
    {
        TManager manager(*Singleton<TProbeRegistry>(), false);

        TTraceResponse resp;
        auto& r = *resp.MutableTrace()->MutableEvents();

        auto& p0 = *r.Add();
        p0.SetName("NoParam");
        p0.SetProvider("LWTRACE_UT_PROVIDER");

        auto& p1 = *r.Add();
        p1.SetName("IntParam");
        p1.SetProvider("LWTRACE_UT_PROVIDER");
        auto& p1param = *p1.MutableParams()->Add();
        p1param.SetUintValue(1);

        auto& p2 = *r.Add();
        p2.SetName("StringParam");
        p2.SetProvider("LWTRACE_UT_PROVIDER");
        auto& p2param = *p2.MutableParams()->Add();
        p2param.SetStrValue("str");

        auto& p3 = *r.Add();
        p3.SetName("EnumParams");
        p3.SetProvider("LWTRACE_UT_PROVIDER");
        auto& p3param1 = *p3.MutableParams()->Add();
        p3param1.SetUintValue((ui64)EEnumClass::ValueC);
        auto& p3param2 = *p3.MutableParams()->Add();
        p3param2.SetIntValue((ui64)EEnumClass::ValueC);

        auto& p4 = *r.Add();
        p4.SetName("InstantParam");
        p4.SetProvider("LWTRACE_UT_PROVIDER");
        auto& p4param = *p4.MutableParams()->Add();
        p4param.SetDoubleValue(42);

        auto& p5 = *r.Add();
        p5.SetName("DurationParam");
        p5.SetProvider("LWTRACE_UT_PROVIDER");
        auto& p5param = *p5.MutableParams()->Add();
        p5param.SetDoubleValue(146);

        auto& p6 = *r.Add();
        p6.SetName("ProtoEnum");
        p6.SetProvider("LWTRACE_UT_PROVIDER");
        auto& p6param = *p6.MutableParams()->Add();
        p6param.SetIntValue((i64)OT_EQ);

        auto& p7 = *r.Add();
        p7.SetName("IntIntParams");
        p7.SetProvider("LWTRACE_UT_PROVIDER");
        auto& p7param1 = *p7.MutableParams()->Add();
        p7param1.SetIntValue(1);
        auto& p7param2 = *p7.MutableParams()->Add();
        p7param2.SetIntValue(2);

        TOrbit orbit;
        UNIT_ASSERT_VALUES_EQUAL(
            manager.HandleTraceResponse(resp, manager.GetProbesMap(), orbit).IsSuccess,
            true);
    }

    Y_UNIT_TEST(ShouldDeserializeWhatSerialized)
    {
        TManager manager(*Singleton<TProbeRegistry>(), false);

        TOrbit orbit;
        TOrbit child;

        TTraceRequest req;
        req.SetIsTraced(true);
        bool traced = manager.HandleTraceRequest(req, orbit);
        UNIT_ASSERT(traced);

        LWTRACK(NoParam, orbit);

        orbit.Fork(child);

        LWTRACK(IntParam, orbit, 1);
        LWTRACK(IntParam, child, 2);

        LWTRACK(StringParam, orbit, "str1");
        LWTRACK(StringParam, child, "str2");

        LWTRACK(EnumParams, orbit, ValueA, EEnumClass::ValueC);
        LWTRACK(InstantParam, orbit, TInstant::Seconds(42));
        LWTRACK(DurationParam, orbit, TDuration::MilliSeconds(146));
        LWTRACK(ProtoEnum, orbit, OT_EQ);
        LWTRACK(IntIntParams, orbit, 1, 2);

        orbit.Join(child);

        TTraceResponse resp1;
        auto& r1 = *resp1.MutableTrace();
        orbit.Serialize(0, r1);

        UNIT_ASSERT_VALUES_EQUAL(r1.EventsSize(), 12);

        TOrbit other;
        traced = manager.HandleTraceRequest(req, other);
        UNIT_ASSERT(traced);

        UNIT_ASSERT_VALUES_EQUAL(
            manager.HandleTraceResponse(resp1, manager.GetProbesMap(), other).IsSuccess,
            true);

        TTraceResponse resp2;
        auto& r2 = *resp2.MutableTrace();
        other.Serialize(0, r2);
        UNIT_ASSERT_VALUES_EQUAL(r2.EventsSize(), 12);

        TString proto1;
        bool parsed = NProtoBuf::TextFormat::PrintToString(resp1, &proto1);
        UNIT_ASSERT(parsed);

        TString proto2;
        parsed = NProtoBuf::TextFormat::PrintToString(resp2, &proto2);
        UNIT_ASSERT(parsed);

        UNIT_ASSERT_VALUES_EQUAL(proto1, proto2);
    }

    Y_UNIT_TEST(TrackForkJoin) {
        TManager mngr(*Singleton<TProbeRegistry>(), true);
        TQuery q;
        bool parsed = NProtoBuf::TextFormat::ParseFromString(R"END(
            Blocks {
                ProbeDesc {
                    Name: "NoParam"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Action {
                    RunLogShuttleAction { }
                }
            }
        )END", &q);

        UNIT_ASSERT(parsed);
        mngr.New("Query1", q);

        {
            TOrbit a, b, c, d;

            // Graph:
            //         c
            //        / \
            //     b-f-b-j-b
            //    /         \
            // a-f-a-f-a-j-a-j-a
            //        \ /
            //         d
            //
            // Merged track:
            //   a-f(b)-a-f(d)-a-j(d,1)-d-a-j(b,6)-b-f(c)-b-j(c,1)-c-b-a

            LWTRACK(NoParam, a);
            a.Fork(b);
            LWTRACK(IntParam, a, 1);
            a.Fork(d);
            LWTRACK(IntParam, a, 2);

            LWTRACK(IntParam, b, 3);
            b.Fork(c);
            LWTRACK(IntParam, b, 4);

            LWTRACK(IntParam, c, 5);
            b.Join(c);
            LWTRACK(IntParam, b, 6);

            LWTRACK(IntParam, d, 7);
            a.Join(d);
            LWTRACK(IntParam, a, 8);

            a.Join(b);
            LWTRACK(IntParam, a, 9);
        }

        struct {
            void Push(TThread::TId, const TTrackLog& tl) {
                UNIT_ASSERT(tl.Items.size() == 16);
                UNIT_ASSERT(TString(tl.Items[0].Probe->Event.Name) == "NoParam");
                UNIT_ASSERT(TString(tl.Items[1].Probe->Event.Name) == "Fork");
                UNIT_ASSERT(tl.Items[2].Params.Param[0].Get<ui64>() == 1);
                UNIT_ASSERT(TString(tl.Items[3].Probe->Event.Name) == "Fork");
                UNIT_ASSERT(tl.Items[4].Params.Param[0].Get<ui64>() == 2);
                UNIT_ASSERT(TString(tl.Items[5].Probe->Event.Name) == "Join");
                UNIT_ASSERT(tl.Items[6].Params.Param[0].Get<ui64>() == 7);
                UNIT_ASSERT(tl.Items[7].Params.Param[0].Get<ui64>() == 8);
                UNIT_ASSERT(TString(tl.Items[8].Probe->Event.Name) == "Join");
                UNIT_ASSERT(tl.Items[9].Params.Param[0].Get<ui64>() == 3);
                UNIT_ASSERT(TString(tl.Items[10].Probe->Event.Name) == "Fork");
                UNIT_ASSERT(tl.Items[11].Params.Param[0].Get<ui64>() == 4);
                UNIT_ASSERT(TString(tl.Items[12].Probe->Event.Name) == "Join");
                UNIT_ASSERT(tl.Items[13].Params.Param[0].Get<ui64>() == 5);
                UNIT_ASSERT(tl.Items[14].Params.Param[0].Get<ui64>() == 6);
                UNIT_ASSERT(tl.Items[15].Params.Param[0].Get<ui64>() == 9);
            }
        } reader;
        mngr.ReadDepot("Query1", reader);
    }

    Y_UNIT_TEST(TrackForkError) {
        TManager mngr(*Singleton<TProbeRegistry>(), true);
        TQuery q;
        bool parsed = NProtoBuf::TextFormat::ParseFromString(R"END(
            Blocks {
                ProbeDesc {
                    Name: "NoParam"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Action {
                    RunLogShuttleAction {
                        MaxTrackLength: 100
                    }
                }
            }
        )END", &q);

        UNIT_ASSERT(parsed);
        mngr.New("Query1", q);

        constexpr size_t n = (100 + 2) / 3 + 1;

        {
            TVector<TVector<TOrbit>> span;

            while (1) {
                TVector<TOrbit> orbit(n);

                LWTRACK(NoParam, orbit[0]);
                if (!orbit[0].HasShuttles()) {
                    break;
                }
                for (size_t i = 1; i < n; i++) {
                    if (!orbit[i - 1].Fork(orbit[i])) {
                        break;
                    }
                    LWTRACK(IntParam, orbit[i], i);
                }

                span.emplace_back(std::move(orbit));
            }

            for (auto& orbit: span) {
                for (auto it = orbit.rbegin(); it + 1 != orbit.rend(); it++) {
                    (it + 1)->Join(*it);
                }
            }
        }

        struct {
            void Push(TThread::TId, const TTrackLog& tl) {
                UNIT_ASSERT(tl.Items.size() == 100);
                UNIT_ASSERT(tl.Truncated);
            }
        } reader;
        mngr.ReadDepot("Query1", reader);
    }

    Y_UNIT_TEST(ShouldResetFailedForksCounterUponShuttleParking) {
        TManager mngr(*Singleton<TProbeRegistry>(), true);
        TQuery q;
        bool parsed = NProtoBuf::TextFormat::ParseFromString(R"END(
            Blocks {
                ProbeDesc {
                    Name: "NoParam"
                    Provider: "LWTRACE_UT_PROVIDER"
                }
                Action {
                    RunLogShuttleAction {
                        MaxTrackLength: 100
                        ShuttlesCount: 2
                    }
                }
            }
        )END", &q);

        UNIT_ASSERT(parsed);
        mngr.New("Query1", q);

        struct {
            ui32 cnt = 0;
            void Push(TThread::TId, const TTrackLog&) {
                ++cnt;
            }
        } reader;

        {
            // Run shuttle ans fail fork
            TOrbit initial;
            TOrbit fork1;
            TOrbit fork2;

            LWTRACK(NoParam, initial);
            UNIT_ASSERT_VALUES_EQUAL(initial.HasShuttles(), true);
            UNIT_ASSERT_VALUES_EQUAL(initial.Fork(fork1), true);
            LWTRACK(IntParam, fork1, 1);
            UNIT_ASSERT_VALUES_EQUAL(fork1.Fork(fork2), false);
            initial.Join(fork1);
        }

        mngr.ReadDepot("Query1", reader);
        UNIT_ASSERT_VALUES_EQUAL(reader.cnt, 0);

        reader.cnt = 0;

        {
            TOrbit initial;

            LWTRACK(NoParam, initial);
            UNIT_ASSERT_VALUES_EQUAL(initial.HasShuttles(), true);
        }

        mngr.ReadDepot("Query1", reader);
        UNIT_ASSERT_VALUES_EQUAL(reader.cnt, 1);
    }
#endif // LWTRACE_DISABLE
}
