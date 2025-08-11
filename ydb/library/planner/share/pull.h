#pragma once

#include <util/generic/algorithm.h>
#include "apply.h"
#include "node_visitor.h"
#include "shareplanner.h"
#include <ydb/library/planner/share/models/density.h>

namespace NScheduling {

// It pulls retarded users in group up to their left edge immediately
class TStrictPuller : public ISimpleNodeVisitor {
private:
    struct TNodeInfo {
        TShareNode* Node;
        TLength Key;

        explicit TNodeInfo(TShareNode& n)
            : Node(&n)
            , Key(double(n.D() + n.S() + n.V()) / n.s0()) // e[i] + v[i]
        {}

        bool operator<(const TNodeInfo& o) const
        {
            return Key > o.Key; // Greater is used to build min-heap
        }
    };

    TShareGroup* Group = nullptr;
    TVector<TNodeInfo> Infos;
    TForce s0R = 0; // sum(s0[i] for i in retarded nodes)
    TEnergy E_ = 0; // sum(E[i] for i in Group)
    TEnergy dS = 0;
public:
    void Pull(TShareGroup* group)
    {
        StartGroup(group);
        group->AcceptInChildren(this);
        FinishGroup();
    }
private:
    void StartGroup(TShareGroup* group)
    {
        Group = group;
        Infos.clear();
        s0R = 0;
        E_ = 0;
        dS = 0;
    }

    void Visit(TShareNode* node) override
    {
        Infos.push_back(TNodeInfo(*node));
        E_ += node->E();
    }

    TEnergy E() const { return E_ + dS; }
    TLength e() const { return E() / gs; }

    void FinishGroup()
    {
        if (Infos.size() <= 1) {
            return; // If there is less than 2 nodes, then pull is not needed
        }
        // Iterate over all nodes in Group sorted by e[i] + v[i]
        MakeHeap(Infos.begin(), Infos.end());
        auto last = Infos.end();
        auto second = Infos.begin() + 1; // avoid pulling first node which otherwise lead to dS=inf (s0R - gs == 0.0)
        for (; last != second; ) {
            TLength de = e() - Infos.front().Key;
            if (de <= 0) {
                break; // Stop if no more retarded nodes
            }

            PopHeap(Infos.begin(), last);
            TNodeInfo& ni = *--last;
            TShareNode& n = *ni.Node;
            s0R += n.s0();
            dS += de * gs * n.s0() / (gs - s0R);
        }

        // Iterate backwards over retarded nodes in Group
        for (; last != Infos.end(); ++last) {
            TNodeInfo& ni = *last;
            TShareNode& n = *ni.Node;
            TEnergy dSi = e() * n.s0() - n.V() - n.E();
            if (dSi > 0) { // Negative value may appear only due to floating-point arithmetic inaccuracy
                n.Spoil(dSi);
            }
        }
    }
};

// It pulls all retarded users in group for the same amount of normalized proficit
// It also calculates left edge relative to the floating origin (which in turn is
// insensitive to absent users)
class TInsensitivePuller {
private:
    TLength Length;
public:
    TInsensitivePuller(TLength length)
        : Length(length)
    {}

    void Pull(TShareGroup* group)
    {
        // This puller is assumed to be used only with density model
        TDeContext* gctx = group->CtxAs<TDeContext>();
        if (!gctx)
            return; // Most probably new group

        // (1) Compute p0 based on u computed in last model run
        //     (Note that gctx->p0 is updated only on model runs, so we cannot use it directly
        //      And it is assumed that iu is not changing very fast, so ctx->iu is good enough)
        // (2) And also compute dD since last pull in the same traverse
        TEnergy dD = 0;
        TForce sigma = 0;
        TEnergy p0sigma = 0;
        TEnergy Ec = group->ComputeEc();
        ApplyTo<TShareNode>(group, [=, &dD, &sigma, &p0sigma] (TShareNode* node) {
            if (TDeContext* ctx = node->CtxAs<TDeContext>()) {
                // Avoid pulling with big dD just after turning ON
                if (ctx->Pull.Cycle == ctx->ModelCycle || ctx->Pull.Cycle == ctx->ModelCycle + 1) {
                    TEnergy dDi = node->D() - ctx->Pull.Dlast;
                    dD += dDi;
                }
                ctx->Pull.Cycle = ctx->ModelCycle + 1;
                ctx->Pull.Dlast = node->D();

                TForce sigma_i = ctx->iu * node->s0();
                sigma += sigma_i;
                p0sigma += sigma_i * node->p(Ec);
            }
        });
        TLength p0 = p0sigma / sigma;

        // Take movement into account
        p0 -= dD / gs;
        TForce s0R_prev = gctx->Pull.s0R; // We do not want solve system of equations, so just use previous value of s0R
        if (s0R_prev > 0.0 && s0R_prev < 1.0) {
            TEnergy dS_estimate = s0R_prev / (1 - s0R_prev) * dD; // See formula explanation below
            p0 -= dS_estimate / gs;
        }

        // Find retards and compute their total share
        TForce s0R = 0;
        ApplyTo<TShareNode>(group, [=, this, &s0R] (TShareNode* node) {
            if (TDeContext* ctx = node->CtxAs<TDeContext>()) {
                ctx->Pull.stretch = Min(2.0, Max(0.0, p0 - node->p(Ec) - node->v() + Length) / Length);
                ctx->Pull.retardness = Min(1.0, ctx->Pull.stretch); // To avoid s0R jumps
                if (ctx->Pull.retardness > 0.0) { // Retard found
                    s0R += node->s0() * ctx->Pull.retardness;
                }
            }
        });
        gctx->Pull.s0R = s0R; // Just for analytics

        if (s0R > 0.0 && s0R < 1.0) {
            // EXPLANATION FOR FORMULA
            // If we assume:
            //   (1) dsi = dd            # we pull accounts for the same distance [metre] as non-retarded accounts
            //   (2) dSi = s0i/s0R * dS  # we pull each retarded account for the same distance
            // We can conclude that:
            //   (1) => dSi / s0i = dD / (1 - s0R)
            //   (2) => dS  / s0R = dD / (1 - s0R)
            //   Therefore: dS = s0R / (1 - s0R) * dD
            TEnergy dS = s0R / (1 - s0R) * dD;

            // Pull retards
            ApplyTo<TShareNode>(group, [=] (TShareNode* node) {
                if (TDeContext* ctx = node->CtxAs<TDeContext>()) {
                    if (ctx->Pull.retardness > 0.0) {
                        TEnergy dSi = node->s0() * ctx->Pull.stretch / s0R * dS;
                        if (dSi > 0) { // Just to avoid floating-point arithmetic errors
                            node->Spoil(dSi);
                        }
                    }
                }
            });
        }
    }
};

// It recursively runs given visitor in each group in tree
template <class TPuller>
class TRecursivePuller : public INodeVisitor
{
private:
    TPuller Puller;
public:
    template <class... TArgs>
    explicit TRecursivePuller(TArgs... args)
        : Puller(args...)
    {}

    void Visit(TShareAccount*) override
    {
        // Do nothing for accounts
    }

    void Visit(TShareGroup* node) override
    {
        node->AcceptInChildren(this);
        Puller.Pull(node);
    }
};

}
