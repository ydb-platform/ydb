#pragma once

#include <ydb/library/analytics/data.h>
#include "apply.h"
#include <ydb/library/planner/share/models/density.h>

#define FOREACH_NODE_ACCESSOR(XX, YY) \
    XX(s0) \
    XX(w0) \
    XX(wmax) \
    XX(w) \
    XX(h) \
    XX(D) \
    XX(S) \
    XX(dD) \
    XX(dS) \
    XX(pdD) \
    XX(pdS) \
    XX(E) \
    XX(V) \
    YY(L) \
    YY(O) \
    YY(A) \
    YY(P) \
    XX(dd) \
    XX(ds) \
    XX(pdd) \
    XX(pds) \
    XX(e) \
    XX(v) \
    YY(l) \
    YY(o) \
    YY(a) \
    YY(p) \
    /**/

#define FILL_ROW(n) row[#n] = c->n
#define FILL_ROW_N(n, v) row[n] = c->v

namespace NScheduling { namespace NAnalytics {

using namespace ::NAnalytics;

inline void AddFromCtx(TRow& row, const IContext* ctx)
{
    if (const TDeContext* c = dynamic_cast<const TDeContext*>(ctx)) {
        FILL_ROW(x);
        FILL_ROW(ix);
        FILL_ROW(s);
        FILL_ROW(u);
        FILL_ROW(iu);
        FILL_ROW(wl);
        FILL_ROW(iwl);
        FILL_ROW_N("pa", p); // pa = point of attachment
        FILL_ROW(pr);
        FILL_ROW(pl);
        FILL_ROW(pf);
        FILL_ROW(lambda);
        FILL_ROW_N("hf", h); // h-function
        FILL_ROW(sigma);
        FILL_ROW(isigma);
        FILL_ROW(p0);
        FILL_ROW(ip0);
        FILL_ROW_N("stretch", Pull.stretch);
        FILL_ROW_N("retardness", Pull.retardness);
        FILL_ROW_N("s0R", Pull.s0R);
    }
}

inline TRow FromNode(const TShareNode* node, TEnergy Eg)
{
    TRow row;
    row.Name = node->GetName();
#define XX_MACRO(n) row[#n] = node->n();
#define YY_MACRO(n) row[#n] = node->n(Eg);
    FOREACH_NODE_ACCESSOR(XX_MACRO, YY_MACRO);
#undef XX_MACRO
#undef YY_MACRO
    AddFromCtx(row, node->Ctx());
    return row;
}

inline TTable FromGroup(const TShareGroup* group)
{
    TTable out;
    TEnergy Eg = 0;
    ApplyTo<const TShareNode>(group, [&Eg] (const TShareNode* node) {
        Eg += node->E();
    });
    ApplyTo<const TShareNode>(group, [&out, Eg] (const TShareNode* node) {
        out.push_back(FromNode(node, Eg));
    });
    return out;
}

}}

#undef FILL_ROW
#undef FOREACH_NODE_FIELD
