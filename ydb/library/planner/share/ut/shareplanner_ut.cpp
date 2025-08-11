#include <ydb/library/planner/share/shareplanner.h>
#include <ydb/library/planner/share/history.h>
#include <library/cpp/threading/future/legacy_future.h>

#include <util/random/random.h>
#include <util/generic/list.h>
#include <util/string/vector.h>
#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/lwtrace/all.h>
#define SHAREPLANNER_UT_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(TracePrint, GROUPS(), \
      TYPES(TString), \
      NAMES("message")) \
    /**/

LWTRACE_DECLARE_PROVIDER(SHAREPLANNER_UT_PROVIDER)
LWTRACE_DEFINE_PROVIDER(SHAREPLANNER_UT_PROVIDER)
LWTRACE_USING(SHAREPLANNER_UT_PROVIDER)

Y_UNIT_TEST_SUITE(SchedulingSharePlanner) {
    using namespace NScheduling;

    class TMyAccount;
    class TMyGroup;
    class TMyPlanner;

    ////////////////////////////
    ///                      ///
    ///   Planner for test   ///
    ///                      ///
    ////////////////////////////

    class TMyAccount: public TShareAccount {
    private:
        double DemandShare = 1.0; // Max share that account can consume (1.0 - is whole cluster)
    public:
        TMyAccount(const TString& name, FWeight w, FWeight wmax, TEnergy v = 1)
            : TShareAccount(name, w, wmax, v)
        {}
        TMyPlanner* GetPlanner();
        TMyGroup* GetParent();
        const TMyGroup* GetParent() const;
        void Distribute(TEnergy cost);
        double GatherDemands();
        double P();
        double C();
        double GetDemandShare() const { return DemandShare; }
        void SetDemandShare(double value) { DemandShare = value; }
    };

    class TMyGroup: public TShareGroup {
    private:
        double DemandShare = 1.0; // Max share that account can consume (1.0 - is whole cluster)
    public:
        TMyGroup(const TString& name, FWeight w, FWeight wmax, TEnergy v = 1)
            : TShareGroup(name, w, wmax, v)
        {}
        TMyPlanner* GetPlanner();
        TMyGroup* GetParent();
        const TMyGroup* GetParent() const;
        void Distribute(TEnergy cost);
        double GatherDemands();
        double GetDemandShare() const { return DemandShare; }
        void SetDemandShare(double value) { DemandShare = value; }
        TEnergy Esum() const
        {
            TEnergy result = 0;
            CUSTOMSHAREPLANNER_FOR(TMyAccount, TMyGroup, node,
                result += node->E();
            );
            return result;
        }
    };

    class TMyPlanner: public TCustomSharePlanner<TMyAccount, TMyGroup> {
    public:
        double GetRepaymentRate() const;
        double GetMaxRepaymentSpeed() const;
        void CheckRepayment(double debt1, double debt2, double dx, double maxErr, const TString& desc = TString()) const;
        double GetX() const;
        friend class TMyGroup;
        friend class TMyAccount;
    };

    ///////////////////////////////
    ///                         ///
    ///        Accessors        ///
    ///                         ///
    ///////////////////////////////

    TMyPlanner* TMyAccount::GetPlanner() { return static_cast<TMyPlanner*>(Planner); }
    TMyGroup* TMyAccount::GetParent()  { return static_cast<TMyGroup*>(Parent); }
    const TMyGroup* TMyAccount::GetParent() const  { return static_cast<const TMyGroup*>(Parent); }

    TMyPlanner* TMyGroup::GetPlanner() { return static_cast<TMyPlanner*>(Planner); }
    TMyGroup* TMyGroup::GetParent()  { return static_cast<TMyGroup*>(Parent); }
    const TMyGroup* TMyGroup::GetParent() const  { return static_cast<const TMyGroup*>(Parent); }

    ////////////////////////////////
    ///                          ///
    ///   Main class for tests   ///
    ///                          ///
    ////////////////////////////////

    struct TTester {
        TMyPlanner Planner;
        void Evolve(TEnergy cost, size_t steps);
    };

    ///////////////////////////////
    ///                         ///
    ///   Scheduler emulation   ///
    ///                         ///
    ///////////////////////////////

    void TMyAccount::Distribute(TEnergy cost)
    {
        Planner->Done(this, cost);
    }

    struct TDistrItem {
        TMyAccount* Account = nullptr;
        TMyGroup* Group = nullptr;
        double Nordem; // Demands normalized by weight

        TDistrItem(TMyAccount* node)
            : Account(node)
            , Nordem(node->GetDemandShare() / node->Ctx()->GetWeight())
        {}

        TDistrItem(TMyGroup* node)
            : Group(node)
            , Nordem(node->GetDemandShare() / node->Ctx()->GetWeight())
        {}

        bool operator<(const TDistrItem& o) const
        {
            return Nordem < o.Nordem;
        }
    };

#define CUSTOMSHAREPLANNER_FOR_DST(items, node, expr) \
    do { \
        for (auto& item : (items)) { \
            if (auto* node = item.Account) { expr; } \
            else if (auto* node = item.Group) { expr; } \
        } \
    } while (false) \
    /**/

    void TMyGroup::Distribute(TEnergy cost)
    {
        TEnergy slot = cost;
        FWeight wsum = 0;
        TVector<TDistrItem> Items;
        CUSTOMSHAREPLANNER_FOR(TMyAccount, TMyGroup, node,
            Items.push_back(TDistrItem(node));
            wsum += node->Ctx()->GetWeight());

        Sort(Items);
        CUSTOMSHAREPLANNER_FOR_DST(Items, node,
            node->Distribute(WMaxCut(node->Ctx()->GetWeight(), TEnergy(node->GetDemandShare() * slot), wsum, cost)));
        GetPlanner()->Waste(cost);
    }

    double TMyAccount::GatherDemands()
    {
        return DemandShare;
    }

    double TMyGroup::GatherDemands()
    {
        DemandShare = 0.0;
        CUSTOMSHAREPLANNER_FOR(TMyAccount, TMyGroup, node,
            DemandShare += node->GatherDemands());
        return DemandShare;
    }

    void TTester::Evolve(TEnergy cost, size_t steps = 1)
    {
        Planner.GetRoot()->GatherDemands();
        for (; steps > 0; steps--) {
            TEnergy c = cost / steps;
            cost -= c;
            Planner.GetRoot()->Distribute(c);
            Planner.Commit();
        }
    }

    ///////////////////////////////
    ///                         ///
    ///      Measurements       ///
    ///                         ///
    ///////////////////////////////

    double TMyAccount::P()
    {
        return E() - double(GetParent()->Esum()) / gs * s0();
    }

    double TMyAccount::C()
    {
        return fabs(P());
    }

    double TMyPlanner::GetRepaymentRate() const
    {
        return Config.GetDenseLength() > 0.0 ? 1.0 / Config.GetDenseLength() : 0.0;
    }

    double TMyPlanner::GetMaxRepaymentSpeed() const
    {
        double gR = 128*81*125*7*11*13; // WTF?
        return gR / 2;
    }

    void TMyPlanner::CheckRepayment(double debt1, double debt2, double dx, double maxErr, const TString& desc) const
    {
        UNIT_ASSERT(debt1 >= 0 && debt2 >= 0);
        double minDebt = 10000;
        double debt2_hi_threshold = debt1 * exp(-(GetRepaymentRate()/maxErr) * dx);
        double debt2_lo_threshold = debt1 * exp(-(GetRepaymentRate()*maxErr) * dx);
        double debt2_target = debt1 * exp(-GetRepaymentRate() * dx);
        Y_UNUSED(debt2_target);
        bool tooLoDebts = debt2 < minDebt && debt2 < minDebt;
        double repaySpeed = debt1 * GetRepaymentRate();
        bool tooHiSpeed = repaySpeed > 0.3 * GetMaxRepaymentSpeed();
        auto tracegen = [=, this]() {
                return Sprintf("CheckRepayment: %s DenseLength=%g speed=%g maxspeed=%g debt1=%g debt2=%g dx=%g -%s%s-> %g < %g < %g",
                               desc.data(), Config.GetDenseLength(), repaySpeed, GetMaxRepaymentSpeed(),
                               debt1, debt2, dx,  tooLoDebts? "L": "-",  tooHiSpeed? "H": "-",
                               debt2_lo_threshold / debt2_target, debt2 / debt2_target,
                               debt2_hi_threshold / debt2_target);
        };
        if (!tooLoDebts && !tooHiSpeed) {
            UNIT_ASSERT_C(debt2 < debt2_hi_threshold && debt2_lo_threshold < debt2,
                          tracegen());
        }
        LWPROBE(TracePrint, tracegen());
    }

    double TMyPlanner::GetX() const
    {
        return GetRoot()->Esum()/gs;
    }

    ////////////////////////////
    ///                      ///
    ///      Unit tests      ///
    ///                      ///
    ////////////////////////////

    Y_UNIT_TEST(StartLwtrace) {
        NLWTrace::StartLwtraceFromEnv();
    }

    Y_UNIT_TEST(Smoke) {
        TTester t;

        TMyAccount* A = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "A", 1000, 2000).get();
        TMyAccount* B = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "B", 1000, 2000).get();
        float N = 100;
        TEnergy c = 1.0 / N;
        for (int i = 0; i<50; i++) {
            t.Planner.Done(A, c);
        }
        t.Planner.Done(B, 50*c);
        t.Planner.Commit();
    }

    Y_UNIT_TEST(Pull) {
        TTester t;

        TMyAccount* A = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "A", 2000, 100000, 3).get();
        TMyAccount* B = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "B", 1000, 100000, 2).get();
        TMyAccount* C = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "C", 1000, 100000, 1).get();
        float N = 100;
        TEnergy c = 6.0 / N;
        t.Planner.Done(A, 10*c);
        t.Planner.Done(B, 20*c);
        t.Planner.Done(C, 30*c);
        t.Planner.Commit();
        for (int i = 0; i<10; i++) {
            t.Planner.Done(C, 10*c);
            t.Planner.Commit();
        }
        for (int i = 0; i<20; i++) {
            t.Planner.Done(B, 10*c);
            t.Planner.Commit();
        }
        for (int i = 0; i<50; i++) {
            t.Planner.Done(A, 10*c);
            t.Planner.Commit();
        }
    }

    Y_UNIT_TEST(RelativeHistorySmoke) {
        TStringStream ss;
        TTester t;

        TMyAccount* A = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "A", 1000, 100000).get();
        TMyAccount* B = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "B", 2000, 200000).get();
        TMyAccount* C = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "C", 3000, 300000).get();

        // Create some history
        float N = 100;
        TEnergy c = 3.0 / N;
        t.Planner.Done(A, 10*c);
        t.Planner.Done(B, 20*c);
        t.Planner.Done(C, 30*c);
        t.Planner.Commit();
        THistorySaver::Save(&t.Planner, ss);

        // Change state
        t.Planner.Done(A, 30*c);
        t.Planner.Done(B, 20*c);
        t.Planner.Done(C, 10*c);
        t.Planner.Commit();

        // Load and check history
        bool ok = THistoryLoader::Load(&t.Planner, ss);
        UNIT_ASSERT(ok);
        t.Planner.Commit();
        UNIT_ASSERT(fabs(A->e() - B->e()) < 1e-5);
        UNIT_ASSERT(fabs(B->e() - C->e()) < 1e-5);

        // Create another history
        t.Planner.Done(A, 25*c);
        t.Planner.Done(B, 36*c);
        t.Planner.Done(C, 49*c);
        t.Planner.Commit();
        ss.Clear();
        THistorySaver::Save(&t.Planner, ss);
        double eAB = A->e() - B->e();
        double eBC = B->e() - C->e();

        // Change state
        t.Planner.Done(A, 44*c);
        t.Planner.Done(B, 33*c);
        t.Planner.Done(C, 22*c);
        t.Planner.Commit();

        // Load and check history
        ok = THistoryLoader::Load(&t.Planner, ss);
        UNIT_ASSERT(ok);
        t.Planner.Commit();
        UNIT_ASSERT(fabs(eAB - (A->e() - B->e())) < 1e-5);
        UNIT_ASSERT(fabs(eBC - (B->e() - C->e())) < 1e-5);
    }

    Y_UNIT_TEST(RelativeHistoryWithDelete) {
        TStringStream ss;
        TTester t;

        TMyAccount* A = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "A", 1000, 100000).get();
        TMyAccount* B = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "B", 2000, 200000).get();
        TMyAccount* C = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "C", 3000, 300000).get();

        // Create some history
        float N = 100;
        TEnergy c = 3.0 / N;
        t.Planner.Done(A, 10*c);
        t.Planner.Done(B, 20*c);
        t.Planner.Done(C, 30*c);
        t.Planner.Commit();
        THistorySaver::Save(&t.Planner, ss);

        // Change state
        t.Planner.Done(A, 30*c);
        t.Planner.Done(B, 20*c);
        t.Planner.Commit();

        // Delete one account
        t.Planner.Delete(C);
        t.Planner.Commit();

        // Load and check history
        bool ok = THistoryLoader::Load(&t.Planner, ss);
        UNIT_ASSERT(ok);
        t.Planner.Commit();
        UNIT_ASSERT(fabs(A->e() - B->e()) < 1e-5);
    }

    double denseLengths[] = {1.0, 0.2, 0.4, 0.5, 0.6, 0.9, 0.99, 1.01, 1.1, 1.5, 2.0, 4.0, 8.0, 10.0, 0.0};
    int checks[] = {1, 5, 10, 20, 40, 50, 80};
    int steps = 1000;

    Y_UNIT_TEST(RepaymentConvergence) {
        for (const auto& denseLength : denseLengths) {
            TTester t;

            TMyAccount* A = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "A", 1000, 100000).get();
            TMyAccount* B = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "B", 1000, 100000).get();

            TSharePlannerConfig cfg;
            cfg.SetDenseLength(denseLength);
            t.Planner.Configure(cfg);
            LWPROBE(TracePrint, Sprintf("SetDenseLength: %g", denseLength));

            // Create some history
            float N = 100;
            TEnergy c = 2.0 / N;
            t.Planner.Done(A, 40*c);
            t.Planner.Done(B, 10*c);
            t.Planner.Commit();

            // Emulate dynamics
            TVector<std::pair<double, double>> state; // <debt, x> pairs
            for (int i = 0; i < steps; ++i) {
                double debt = (A->C() + B->C()) / 2;
                double x = t.Planner.GetX();
                state.push_back(std::make_pair(debt, x));
                LWPROBE(TracePrint, Sprintf("i=%d x=%g debt=%g", i, x, debt));
                t.Evolve(c);
            }

            // Check convergence
            for (const auto& check : checks) {
                for (int i = check; i < steps; ++i) {
                    const auto& s1 = state[i - check];
                    const auto& s2 = state[i];
                    t.Planner.CheckRepayment(s1.first, s2.first, s2.second - s1.second, 2.0, Sprintf("i1=%d i2=%d", i-check, i));
                }
            }
        }
    }

    Y_UNIT_TEST(RepaymentConvergence4Users) {
        for (const auto& denseLength : denseLengths) {
            TTester t;

            TMyAccount* A = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "A", 1000, 100000).get();
            TMyAccount* B = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "B", 2000, 200000).get();
            TMyAccount* C = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "C", 3000, 300000).get();
            TMyAccount* D = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "D", 4000, 400000).get();

            TSharePlannerConfig cfg;
            cfg.SetDenseLength(denseLength);
            t.Planner.Configure(cfg);
            LWPROBE(TracePrint, Sprintf("SetDenseLength: %g", denseLength));

            // Create some history
            float N = 100;
            TEnergy c = 4.0 / N;
            t.Planner.Done(A, 20*c);
            t.Planner.Done(B, 10*c);
            t.Planner.Done(C, 40*c);
            t.Planner.Done(D, 30*c);
            t.Planner.Commit();

            // Emulate dynamics
            TVector<std::pair<double, double>> state; // <debt, x> pairs
            for (int i = 0; i < steps; ++i) {
                double debt = (A->C() + B->C() + C->C() + D->C()) / 2;
                double x = t.Planner.GetX();
                state.push_back(std::make_pair(debt, x));
                LWPROBE(TracePrint, Sprintf("i=%d x=%g debt=%g", i, x, debt));
                t.Evolve(c);
            }

            // Check convergence
            for (const auto& check : checks) {
                for (int i = check; i < steps; ++i) {
                    const auto& s1 = state[i - check];
                    const auto& s2 = state[i];
                    t.Planner.CheckRepayment(s1.first, s2.first, s2.second - s1.second, 2.0, Sprintf("i1=%d i2=%d", i-check, i));
                }
            }

            /*
            TMyAccount* accounts[] = {A, B, C, D};
            // Emulate dynamics
            typedef THashMap<TMyAccount*, double> TAccs;
            TAccs prev;
            double prevx;
            for (int i = 0; i < steps; ++i) {
                // Check
                double x = t.Planner.GetX();
                if (!prev.empty()) {
                    for (auto acc : accounts) {
                        //t.Planner.CheckRepayment(prev[acc], acc->P(), x - prevx, 2.0e10, Sprintf("i=%d", i));
                        Y_UNUSED(acc); Y_UNUSED(prevx);
                    }
                }

                // Save prev state
                for (auto acc : accounts) {
                    prev[acc] = acc->P();
                }
                prevx = x;

                // Emulate
                t.Evolve(c);
            }
            */
        }
    }

    Y_UNIT_TEST(OuterGroupInsensitivity) {
        for (const auto& denseLength : denseLengths) {
            if (denseLength == 0.0)
                continue; // This test should not work for discontinuous h-function
            TTester t;

            TMyAccount* A = t.Planner.Add<TMyAccount>(t.Planner.GetRoot(), "A", 1000, 2000).get();
            TMyGroup* G = t.Planner.Add<TMyGroup>(t.Planner.GetRoot(), "G", 1000, 2000).get();
            TMyAccount* B = t.Planner.Add<TMyAccount>(G, "B", 1000, 100000).get();
            TMyAccount* C = t.Planner.Add<TMyAccount>(G, "C", 1000, 100000).get();

            B->SetDemandShare(0.0);

            TSharePlannerConfig cfg;
            cfg.SetDenseLength(denseLength);
            t.Planner.Configure(cfg);
            LWPROBE(TracePrint, Sprintf("SetDenseLength: %g", denseLength));

            // Create some history
            float N = 100;
            TEnergy c = 3.0 / N;
            t.Planner.Done(A, 50*c);
            t.Planner.Done(B, 25*c);
            t.Planner.Done(C, 25*c);
            t.Planner.Commit();

            // Emulate dynamics
            int idleSteps = 100;
            for (int i = 0; i < steps; ++i) {
                UNIT_ASSERT(fabs(A->w() - 1000.0) < 50.0);
                UNIT_ASSERT(fabs(G->w() - 1000.0) < 50.0);
                LWPROBE(TracePrint, Sprintf("wA=%g wG=%g", A->w(), G->w()));
                t.Evolve(c);
                if (B->pdS() > 0) {
                    if (--idleSteps == 0) {
                        B->SetDemandShare(1.0);
                    }
                }
            }
        }
    }
}
