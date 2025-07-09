#pragma once

#include <cmath>
#include <ydb/library/planner/share/apply.h>
#include "recursive.h"

namespace NScheduling {

enum class TDepType { Left = 0, Top, AtNode, Bottom, Right, Other };

template <class TCtx>
struct TDePoint {
    TCtx* Ctx;
    double p;
    TDepType Type;

    void Move(double& h, double& lambda, const TDePoint* prev) const
    {
        if (prev) {
            h += lambda * (prev->p - p); // Integrate density
        }
        switch (Type) {
        case TDepType::Right:  lambda += Ctx->lambda; break;
        case TDepType::Left:   lambda -= Ctx->lambda; break;
        case TDepType::Bottom: break;
        case TDepType::Top:    h += Ctx->x; break;
        default: break; // other types must be processed separately
        }
    }

    bool operator<(const TDePoint& o) const
    {
        if (p == o.p) {
            return Type > o.Type;
        } else {
            return p > o.p;
        }
    }
};

class TDeContext : public IContext {
public:
    SCHEDULING_DEFINE_VISITABLE(IContext);

    ui64 ModelCycle = 0;   // Number of times that Run() passed with this context

    // Context for child role
    TShareNode* Node;
    FWeight w;             // weight
    TDimless ix = 0;       // instant utilization
    TDimless x = 0;        // average utilization
    TDimless u = 0;        // usage
    TDimless iu = 0;       // instant usage
    TDimless wl = 0;       // x/w (water-level)
    TDimless iwl = 0;      // ix/w (instant water-level)
    TLength p = 0;         // normalized proficit
    TLength pr = 0;        // dense interval rightmost point
    TLength pl = 0;        // dense interval leftmost point
    TLength pf = 0;        // normalized proficit relative to floating origin
    double lambda = 0;     // density [1/metre]
    TDimless h = 0;        // h-function value
    TForce s = 0;          // dynamic share

    // Context for parent role
    TEnergy Ec = 0;        // sum of E() of children
    TEnergy dDc = 0;       // sum of dD() of children
    TForce sigma = 0;      // used fraction of band (0; 1]
    TForce isigma = 0;     // instant used fraction of band (0; 1]
    TLength p0 = 0;        // floating origin
    TLength ip0 = 0;       // instant floating origin

    // Context for insensitive puller
    struct {
        ui64 Cycle = ui64(-1);
        TEnergy Dlast = 0; // D was on last pull
        TDimless stretch = 0;
        TDimless retardness = 0;
        TForce s0R = 0;
    } Pull;

    explicit TDeContext(IModel* model, TShareNode* node)
        : IContext(model)
        , Node(node)
        , w(node? node->w0(): 1.0)
    {}

    void Update(TDeContext gctx, double avgLength)
    {
        // Update utilization
        ix = Node->dD() / gctx.dDc;
        if (avgLength > 0) {
            TLength ddg = gctx.dDc / gs;
            double alpha = std::pow(2.0, -ddg / avgLength);
            x = alpha * x + (1-alpha) * ix;
        } else {
            x = ix;
        }

        // Update normalized proficit
        p = Node->p(gctx.Ec);
    }

    void Normalize(double Xsum, double iXsum)
    {
        if (Xsum > 0) {
            // Moving averaging formulas should always give Xsum == 1.0 exactly
            // Reasons for normalize are:
            // 1) fix floating-point arithmetic errors
            // 2) on new group start actual sum of x of all children is zero
            //    and this should be fixed somehow
            // 3) when account leaves group Xsum would be not equal to 1.0
            x /= Xsum;
            wl = x / w;
        }
        if (iXsum > 0) {
            ix /= iXsum;
            iwl = ix / w;
        }
    }

    void ComputeUsage(double& xsum, double& wsum, double& ucur, double& wlcur, bool instant)
    {
        // EXPLANATION FOR FORMULA
        // If we assume:
        //   (1) all "vectors" below are sorted by water-level wl[k] = x[k] / w[k]
        //   (2) u[k] >= u[k-1] -- u should monotonically increase with k
        //   (3) u[k] must be bounded by 1 (when xp[k] == 0)
        //   (4) u[k] must be a linear function of wl[k]
        //
        // One can proove that:
        //                     wl[k] - wl[k-1]
        //    u[k] = u[k-1] + ----------------- (1 - u[k-1])
        //                     WL[k] - wl[k-1]
        //
        //  , where WL[k] = sum(i=k..n, x[i]) / sum(i=k..n, w[i])
        //          WL[k] is max possible value for wl[k] (due to sort by wl[k])
        //
        // Or equivalently (adapted and used below for computation):
        //                           xm[k]
        //  (*) u[k] = u[k-1] + --------------- (1 - u[k-1])
        //                       xp[k] + xm[k]
        //
        //  , where xp[k] = x[k] - wl[k-1] * w[k] -- water in k-th bucket lacking to become max possible WL[k]
        //          xm[k] = WL[k] * w[k] - x[k]   -- water in k-th bucket above min possible wl[k-1]
        //

        Y_ABORT_UNLESS(wsum > 0);
        double xx = (instant? ix: x);
        double xn = xx - wlcur * w;
        double xp = w * (xsum/wsum) - xx;
        if (xn + xp > 0) {
            ucur += xn / (xn+xp) * (1-ucur);
        }
        wlcur = (instant? iwl: wl);
        xsum -= xx;
        wsum -= w;
        (instant? iu: u) = ucur;
    }

    template <class TCtx>
    static void PushPointsImpl(TCtx* t, TVector<TDePoint<TCtx>>& points)
    {
        if (t->pr > t->pl) {
            points.push_back(TDePoint<TCtx>{t, t->pr, TDepType::Right});
            points.push_back(TDePoint<TCtx>{t, t->p, TDepType::AtNode});
            points.push_back(TDePoint<TCtx>{t, t->pl, TDepType::Left});
        } else {
            points.push_back(TDePoint<TCtx>{t, t->p, TDepType::Bottom});
            points.push_back(TDePoint<TCtx>{t, t->p, TDepType::AtNode});
            points.push_back(TDePoint<TCtx>{t, t->p, TDepType::Top});
        }
    }

    void PushPoints(TVector<TDePoint<TDeContext>>& points)
    {
        PushPointsImpl(this, points);
    }

    void PushPoints(TVector<TDePoint<const TDeContext>>& points) const
    {
        PushPointsImpl(this, points);
    }

    FWeight GetWeight() const override
    {
        return w;
    }

    double CalcGlobalRealShare() const
    {
        if (TShareGroup* group = Node->GetParent())
            if (TDeContext* pctx = group->CtxAs<TDeContext>())
                return ix * pctx->CalcGlobalRealShare();
        return 1.0;
    }

    double CalcGlobalAvgRealShare() const
    {
        if (TShareGroup* group = Node->GetParent())
            if (TDeContext* pctx = group->CtxAs<TDeContext>())
                return x * pctx->CalcGlobalAvgRealShare();
        return 1.0;
    }

    double CalcGlobalDynamicShare() const
    {
        if (TShareGroup* group = Node->GetParent())
            if (TDeContext* pctx = group->CtxAs<TDeContext>())
                return s * pctx->CalcGlobalDynamicShare();
        return 1.0;
    }

    void FillSensors(TShareNodeSensors& sensors) const override
    {
        sensors.SetFloatingLag(pf);
        sensors.SetRealShare(CalcGlobalRealShare());
        sensors.SetAvgRealShare(CalcGlobalAvgRealShare());
        sensors.SetDynamicShare(CalcGlobalDynamicShare());
        sensors.SetGrpRealShare(ix);
        sensors.SetGrpAvgRealShare(x);
        sensors.SetGrpDynamicShare(s);
        sensors.SetUsage(u);
        sensors.SetInstantUsage(iu);
        sensors.SetTariff(sigma);
        sensors.SetInstantTariff(isigma);
        sensors.SetFloatingOrigin(p0);
        sensors.SetInstantOrigin(ip0);
        sensors.SetBoost(h);
        sensors.SetPullStretch(Pull.stretch);
        sensors.SetRetardness(Pull.retardness);
        sensors.SetRetardShare(Pull.s0R);
    }
};

class TDensityModel : public TRecursiveModel<TDeContext> {
private:
    TLength DenseLength;
    TLength AveragingLength;
    TVector<TDeContext*> Ctxs;
    TVector<TDePoint<TCtx>> Points;
    double Xsum = 0;
    double iXsum = 0;
    double Wsum = 0;
    double Wsum_new = 0;
public:
    explicit TDensityModel(TSharePlanner* planner)
        : TRecursiveModel(planner)
        , DenseLength(planner->Cfg().GetDenseLength())
        , AveragingLength(planner->Cfg().GetAveragingLength())
    {}
    void OnAttach(TShareNode*) override {}
    void OnDetach(TShareNode*) override {}
    void OnAccount(TShareAccount* account, TCtx& ctx, TCtx& pctx) override
    {
        pctx.Ec += account->E();
        pctx.dDc += account->dD();
        ctx.ModelCycle++;
    }
    void OnDescend(TShareGroup* group, TCtx& gctx, TCtx& pctx) override
    {
        pctx.Ec += group->E();
        pctx.dDc += group->dD();
        gctx.Ec = gctx.dDc = 0;
        gctx.ModelCycle++;
    }
    void OnAscend(TShareGroup* group, TCtx& gctx, TCtx&) override
    {
        if (ProcessNodes(group, gctx)) {
            ProcessUtilization();
            ProcessUsage(gctx);
            ProcessLag(gctx);
            ProcessWeight();
            ProcessSensors(gctx);
        }
    }
private:
    bool ProcessNodes(TShareGroup* group, TCtx& gctx)
    {
        if (gctx.dDc == 0)
            // There was no work done since last model run, so dynamic weights
            // must remain the same
            return false;
        Ctxs.clear();
        Xsum = 0;
        iXsum = 0;
        Wsum = 0;
        ApplyTo<TShareNode>(group, [=, this] (TShareNode* node) {
            TCtx& ctx = node->Ctx<TCtx>();
            ctx.Update(gctx, AveragingLength);
            Xsum += ctx.x;
            iXsum += ctx.ix;
            Wsum += ctx.w;
            Ctxs.push_back(&ctx);
        });
        return Xsum > 0 && iXsum > 0;
    }

    void ProcessUtilization()
    {
        // Renormalize utilization
        double xsum = 0;
        double ixsum = 0;
        for (TDeContext* ctx : Ctxs) {
            ctx->Normalize(Xsum, iXsum);
            xsum += ctx->x;
            ixsum += ctx->ix;
        }
        Xsum = xsum;
        iXsum = ixsum;
    }

    void ProcessUsageImpl(TForce& sigma, TLength& p0, bool instant)
    {
        // Ctx are assumed to be sorted by water-level (instant or average)
        double ucur = 0;
        double wlcur = 0;
        double xsum = (instant? iXsum: Xsum);
        double wsum = Wsum;
        sigma = 0;
        TEnergy p0sigma = 0;
        for (TDeContext* ctx : Ctxs) {
            ctx->ComputeUsage(xsum, wsum, ucur, wlcur, instant);
            TForce sigma_i = ucur * ctx->Node->s0();
            sigma += sigma_i;
            p0sigma += sigma_i * ctx->p;
        }
        p0 = p0sigma / sigma;
    }

    void ProcessUsage(TDeContext& gctx)
    {
        Sort(Ctxs, [] (const TDeContext* x, const TDeContext* y) { return x->iwl < y->iwl; });
        ProcessUsageImpl(gctx.isigma, gctx.ip0, true);
        Sort(Ctxs, [] (const TDeContext* x, const TDeContext* y) { return x->wl < y->wl; });
        ProcessUsageImpl(gctx.sigma, gctx.p0, false);
    }

    void ProcessLag(TDeContext& gctx)
    {
        Points.clear();
        // We should not multiply by sigma iff sigma is used as tariff, but let's not comlicate and
        // not multiply by sigma any ways, so you'd better use tarification by sigma, otherwise
        // density model is NOT insensitive to absent users
        //double dp = DenseLength * gs / gctx.sigma; // TODO[serxa]: also multiply by efficiency
        double dp = DenseLength;
        for (TDeContext* ctx : Ctxs) {
            ctx->pr = ctx->p + Max(0.0, Min(dp, gctx.p0 - ctx->p));
            ctx->pl = ctx->pr - dp;
            ctx->lambda = dp > 0.0? ctx->x / dp: 0.0;
            ctx->PushPoints(Points);
        }
        Sort(Points);
        double h = 0.0;
        double lambda = 0.0;
        TDePoint<TDeContext>* prev = nullptr;
        for (TDePoint<TDeContext>& cur : Points) {
            cur.Move(h, lambda, prev);
            if (cur.Type == TDepType::AtNode) {
                cur.Ctx->h = h;
            }
            prev = &cur;
        }
    }

    void ProcessWeight()
    {
        Wsum_new = 0.0;
        for (TDeContext* ctx : Ctxs) {
            TShareNode& node = *ctx->Node;
            double w = node.w0() + (node.wmax() - node.w0()) * ctx->h;
            ctx->w = Max(node.w0(), Min(node.wmax(), w));
            Wsum_new +=ctx->w;
        }
    }

    void ProcessSensors(TDeContext& gctx)
    {
        for (TDeContext* ctx : Ctxs) {
            ctx->s = ctx->w / Wsum_new;
            ctx->pf = ctx->p - gctx.p0;
        }
    }
};

}
