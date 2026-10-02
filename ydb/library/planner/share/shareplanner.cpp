#include "shareplanner.h"
#include <util/stream/str.h>
#include <util/string/hex.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <ydb/library/planner/share/protos/shareplanner.pb.h>
#include "pull.h"
#include "billing.h"
#include "step.h"
#include <ydb/library/planner/share/models/static.h>
#include <ydb/library/planner/share/models/max.h>
#include <ydb/library/planner/share/models/density.h>
#include "history.h"
#include "monitoring.h"

namespace NScheduling {

TSharePlanner::TSharePlanner(const TSharePlannerConfig& cfg)
    : Root(nullptr)
    //, UtilMeter(new TUtilizationMeter())
    , Stepper(new TStepper())
{
    Configure(cfg);
}

void TSharePlanner::Configure(const TSharePlannerConfig& cfg)
{
    PLANNER_PROBE(Configure, GetName(), cfg.DebugString());
    Config = cfg;
    EPullType pullType = Config.GetPull();
    EBillingType billingType = Config.GetBilling();
    if (pullType == PT_INSENSITIVE && Config.GetModel() != PM_DENSITY) {
        // Insensitive puller can be used only with density model
        // because it uses some data (p0) from its context.
        // Otherwise fallback to strict puller
        pullType = PT_STRICT;
    }

    TDimless staticTariff = Config.GetStaticTariff();
    if ((billingType == BT_PRESENT_SHARE)
            && Config.GetModel() != PM_DENSITY) {
        // Present share billing can be used only with density model
        // because they use some data (sigma/isigma) from its context.
        // Otherwise do not use billing at all
        billingType = BT_STATIC;
        staticTariff = 1.0;
    }
    switch (pullType) {
    default: // for forward compatibility
    case PT_STRICT:      Puller.Reset(new TRecursivePuller<TStrictPuller>()); break;
    case PT_INSENSITIVE: Puller.Reset(new TRecursivePuller<TInsensitivePuller>(Config.GetPullLength())); break;
    case PT_NONE:        Puller.Destroy(); break;
    }
    switch (Config.GetModel()) {
    case PM_STATIC:     Model.Reset(new TStaticModel(this)); break;
    case PM_MAX:        Model.Reset(new TMaxModel(this)); break;
    default: // for forward compatibility
    case PM_DENSITY:    Model.Reset(new TDensityModel(this)); break;
    }
    switch (billingType) {
    default: // for forward compatibility
    case BT_STATIC:         Billing.Reset(new TStaticBilling(staticTariff)); break;
    case BT_PRESENT_SHARE:  Billing.Reset(new TPresentShareBilling(Config.GetBillingIsMemoryless())); break;
    }
}

void TSharePlanner::Advance(TEnergy cost, bool wasted)
{
    PLANNER_PROBE(Advance, GetName(), cost, wasted);
    Y_ASSERT(cost >= 0);

    // Adjust totals
    if (!wasted) {
        D_ += cost;
        dD_ += cost;
        Stats.ResUsed += cost;
    } else {
        W_ += cost;
        dW_ += cost;
        Stats.ResWasted += cost;
    }

    //UtilMeter->Add(cost, wasted); // Adjust utilization history
}

void TSharePlanner::Done(TShareAccount* acc, TEnergy cost)
{
    Y_ASSERT(cost >= 0);
    acc->Done(cost);
    PLANNER_PROBE(Done, GetName(), acc->GetName(), cost, acc->Normalize(cost));
    Advance(cost, false);
}

void TSharePlanner::Waste(TEnergy cost)
{
    PLANNER_PROBE(Waste, GetName(), cost);
    Advance(cost, true);
}

void TSharePlanner::Commit(bool model)
{
    if (!Puller && !Model && !Billing) {
        return; // We are not configured yet
    }
    if (Puller) {
        Root->Accept(Puller.Get());
    }
    bool run = false;
    if (model && (dD_ > 0 || dW_ > 0)) {
        PLANNER_PROBE(Run, GetName());
        if (Model) {
            Model->Run(Root);
            if (Billing) {
                Root->Accept(Billing.Get());
            }
        }
        Root->Accept(Stepper.Get());
        pdD_ = dD_;
        pdW_ = dW_;
        dD_ = dW_ = 0;
        run = true;
    }
    PLANNER_PROBE(CommitInfo, GetName(), model, run, PrintInfo());
}

void TSharePlanner::OnDetach(TShareNode* node)
{
    if (Model) {
        Model->OnDetach(node);
    }
    node->Detach();
}

void TSharePlanner::OnAttach(TShareNode* node, TShareGroup* parent)
{
    node->Attach(this, parent);
    if (Model) {
        Model->OnAttach(node);
    }
}

TString TSharePlanner::PrintStatus() const
{
    return TStatusPrinter::Print(this);
}

TString TSharePlanner::PrintTree(bool band, bool model, const TArtParams& art) const
{
    return TTreePrinter::Print(this, band, model, art);
}

TString TSharePlanner::PrintStats() const
{
    TStringStream ss;
    ss << "             === PLANNER ===\n"
       << "                   D:" << D_ << "\n"
       << "                   W:" << W_ << "\n"
       << "                  dD:" << dD_ << "\n"
       << "                  dW:" << dW_ << "\n"
//       << "                Used:" << UtilMeter->Used() << "\n"
//       << "              Wasted:" << UtilMeter->Wasted() << "\n"
//       << "         Utilization:" << UtilMeter->Utilization() << "\n"
       << "\n"
       ;
    return ss.Str();
}

TString TSharePlanner::PrintInfo(bool status, bool band, bool model, bool stats, const TArtParams& art) const
{
    TStringStream ss;
    ss << Endl;
    if (status)
        ss << PrintStatus();
    if (band || model)
        ss << PrintTree(band, model, art);
    if (stats)
        ss << PrintStats();
    return ss.Str();
}

void TSharePlanner::GetStats(TSharePlannerStats& stats) const
{
    stats = Stats;
}

}
