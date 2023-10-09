#include "queue_backpressure_server.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>


#define STR Cnull
#define VERBOSE_STR Cnull


namespace NKikimr {

    Y_UNIT_TEST_SUITE(TQueueBackpressureTest) {

        using namespace NBackpressure;
        using TFeedback = ::NKikimr::NBackpressure::TFeedback<ui64>;

        Y_UNIT_TEST(CreateDelete) {
            TQueueBackpressure<ui64> qb(true, 100u, 10u);

            TInstant now = Now();
            TActorId actorId(1, 1, 1, 1);
            qb.Push(5, actorId, TMessageId(0, 0), 1, now);
            qb.Push(5, actorId, TMessageId(0, 1), 1, now);
            qb.Push(5, actorId, TMessageId(0, 2), 1, now);
            qb.Push(5, actorId, TMessageId(0, 3), 1, now);
            qb.Processed(actorId, TMessageId(0, 0), 1, now);
            qb.Push(5, actorId, TMessageId(0, 4), 1, now);
            qb.Processed(actorId, TMessageId(0, 2), 1, now);
            qb.Processed(actorId, TMessageId(0, 1), 1, now);
            qb.Push(5, actorId, TMessageId(0, 4), 1, now);
            qb.Output(STR, now);
        }


        Y_UNIT_TEST(IncorrectMessageId) {
            TQueueBackpressure<ui64> qb(true, 100u, 10u);

            TInstant now = Now();
            TActorId actorId(1, 1, 1, 1);
            qb.Push(5, actorId, TMessageId(0, 0), 1, now);
            qb.Push(5, actorId, TMessageId(0, 1), 1, now);
            auto feedback = qb.Push(5, actorId, TMessageId(0, 1), 1, now);
            TString res = "{Status# 4 Notify# 1 ActualWindowSize# 2 MaxWindowSize# 20 "
                                "ExpectedMsgId# [1 2] FailedMsgId# [0 1]}";
            TStringStream str;
            feedback.Output(str);
            STR << res << "\n";
            STR << str.Str() << "\n";
            UNIT_ASSERT_STRINGS_EQUAL(str.Str(), res);
        }


        struct IClient : public virtual TThrRefBase {
            virtual TFeedback Work(TQueueBackpressure<ui64> &qb) = 0;
        };

        using IClientPtr = TIntrusivePtr<IClient>;

        struct TTrivialClient : public IClient {
            TTrivialClient(ui64 id)
                : Id(id)
                , MsgId()
                , State(true)
                , ActorId(1, 1, Id, 1)
            {}

            TFeedback Work(TQueueBackpressure<ui64> &qb) {
                TInstant now = Now();
                if (State) {
                    State = !State;
                    return qb.Push(Id, ActorId, MsgId, 1, now);
                } else {
                    State = !State;
                    const TMessageId res = MsgId;
                    ++MsgId.MsgId;
                    return qb.Processed(ActorId, res, 1, now);
                }
            }

            ui64 Id;
            TMessageId MsgId;
            bool State;
            TActorId ActorId;
        };

        struct TInFlightClient : public IClient {
            TInFlightClient(ui64 id, ui32 maxInFlight)
                : Id(id)
                , MsgId()
                , MaxInFlight(maxInFlight)
                , InFlight(0)
                , FullLoadObtained(false)
                , ActorId(1, 1, Id, 1)
            {}

            TFeedback Work(TQueueBackpressure<ui64> &qb) {
                TInstant now = Now();
                if (!FullLoadObtained) {
                    // full load
                    while (true) {
                        TFeedback res = qb.Push(Id, ActorId, MsgId, 1, now);
                        if (Good(res.first.Status)) {
                            VERBOSE_STR << "UNDERLOAD: Push OK MsgId# " << MsgId.ToString() << "\n";
                            InFlight++;
                            MsgId.MsgId++;
                            if (InFlight == MaxInFlight) {
                                FullLoadObtained = true;
                                return res;
                            }
                        } else {
                            VERBOSE_STR << "UNDERLOAD: Push FAILED\n";
                            return res;
                        }
                    }
                } else {
                    if (InFlight == MaxInFlight) {
                        const TMessageId temp(MsgId.SequenceId, MsgId.MsgId - InFlight);
                        auto res = qb.Processed(ActorId, temp, 1, now);
                        Y_ABORT_UNLESS(Good(res.first.Status));
                        VERBOSE_STR << "LOAD: Processed: MsgId# " << temp.ToString() << "\n";
                        InFlight--;
                        return res;
                    } else {
                        auto res = qb.Push(Id, ActorId, MsgId, 1, now);
                        Y_ABORT_UNLESS(Good(res.first.Status));
                        VERBOSE_STR << "LOAD: Push: MsgId# " << MsgId.ToString() << "\n";
                        InFlight++;
                        MsgId.MsgId++;
                        return res;
                    }
                }
            }

            ui64 Id;
            TMessageId MsgId;
            const ui32 MaxInFlight;
            ui32 InFlight;
            bool FullLoadObtained;
            TActorId ActorId;
        };


        Y_UNIT_TEST(PerfTrivial) {
            TQueueBackpressure<ui64> qb(true, 100u, 10u);

            TVector<IClientPtr> clients;
            ui32 i = 0;
            for (i = 0; i < 5; i++) {
                clients.emplace_back(new TTrivialClient(i));
            }

            TInstant now = Now();
            for (i = 0; i < 1000000; i++) {
                for (auto &c : clients) {
                    c->Work(qb);
                }
                if (i % 100000 == 0) {
                    STR << "=========================\n";
                    qb.Output(STR, now);
                }
            }


            for (i = 0; i < 5; i++) {
                auto feedback = clients[i]->Work(qb);
                TStringStream s;
                s << "{Status# 1 Notify# 0 "
                    << "ActualWindowSize# 1 MaxWindowSize# 20 ExpectedMsgId# [0 500001] FailedMsgId# [0 0]}";
                TString res = feedback.first.ToString();
                STR << res << "\n";
                UNIT_ASSERT(res == s.Str());
            }
        }

        Y_UNIT_TEST(PerfInFlight) {
            TQueueBackpressure<ui64> qb(true, 100u, 10u);

            TInstant now = Now();

            TVector<IClientPtr> clients;
            clients.emplace_back(new TInFlightClient(0, 30));
            clients.emplace_back(new TInFlightClient(1, 10));

            ui32 i = 0;
            for (i = 0; i < 1000000; i++) {
                for (auto &c : clients) {
                    c->Work(qb);
                }
                if (i % 100000 == 0) {
                    STR << "=========================\n";
                    qb.Output(STR, now);
                }
            }

            TStringStream s;
            qb.Output(s, now);
            TString res = "MaxCost# 100 ActualCost# 38 activeWindows# 2 fadingWindows# 0 "
                                "frozenWindows# 0 deadWindows# 0\n"
                            "GlobalStat: NSuccess# 1000038 NWindowUpdate# 400005 NProcessed# 1000000 "
                                "NIncorrectMsgId# 0 NHighWatermarkOverflow# 0\n"
                            "ClientId# 0 ExpectedMsgId# [0 500029] Cost# 29 LowWatermark# 20 HighWatermark# 76 "
                                "CostChangeUntilFrozenCountdown# 20 CostChangeUntilDeathCountdown# 30\n"
                            "ClientId# 1 ExpectedMsgId# [0 500009] Cost# 9 LowWatermark# 20 HighWatermark# 23 "
                                "CostChangeUntilFrozenCountdown# 20 CostChangeUntilDeathCountdown# 30\n";
            STR << s.Str() << "\n";
            UNIT_ASSERT_STRINGS_EQUAL(s.Str(), res);
        }
    }

} // NKikimr
