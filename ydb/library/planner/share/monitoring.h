#pragma once

#include <cmath>
#include <util/system/type_name.h>
#include "shareplanner.h"
#include <ydb/library/planner/share/protos/shareplanner_sensors.pb.h>
#include <ydb/library/planner/share/models/density.h>

namespace NScheduling {

class TNameWidthEvaluator : public IConstNodeVisitor {
private:
    size_t Ident;
    size_t TreeWidth;
    size_t Depth = 0;

    explicit TNameWidthEvaluator(size_t ident, size_t minWidth)
        : Ident(ident)
        , TreeWidth(minWidth)
    {}

    void Visit(const TShareAccount* node) override
    {
        Update(node->GetName().size());
    }

    void Visit(const TShareGroup* node) override
    {
        Update(node->GetName().size());
        Depth++;
        node->AcceptInChildren(this);
        Depth--;
    }

    void Update(size_t width)
    {
        TreeWidth = Max(TreeWidth, Depth * Ident + width);
    }
public:
    static size_t Get(const TShareGroup* root, size_t ident = 1, size_t minWidth = 4 /* = len("Name")*/)
    {
        TNameWidthEvaluator v(ident, minWidth);
        v.Visit(root);
        return v.TreeWidth;
    }
};

#define SP_NAME(x) Sprintf("%-*s ", (int)treeWidth, ((depth>0? TString(depth - 1, '|') + "+": "") + ToString(x)).data()) <<
#define SP_FOREACH_STATUS(XX) \
    XX("V", Node->V()) \
    XX("w0", Node->w0()) \
    XX("wmax", Node->wmax()) \
    XX("s0", Node->s0()) \
    XX("v", Node->v()) \
    XX("w", Node->w()) \
    XX("D", Node->D()) \
    XX("S", Node->S()) \
    XX("l", Node->l(Eg())) \
    XX("e", Node->e()) \
    XX("a", Node->a(Eg())) \
    XX("Ec", Dc + Sc) \
    /**/
#define SP_HEAD(h, e) Sprintf(" %11s", h) <<
#define SP_ELEM(h, e) Sprintf(" %11.2le", double(e)) <<
#define SP_STR(s) SP_HEAD(s, not_used)
#define SP_CTX(n) SP_STR(Sprintf("%s:%.2le", #n, ctx->n).data())

class TCtxStatusPrinter
        : public IVisitorBase
        , public IVisitor<const IContext>
        , public IVisitor<const TDeContext>
{
private:
    TStringStream Ss;
public:
    void Visit(const IContext*) override
    {
        Ss << SP_STR("unknown") "";
    }

    void Visit(const TDeContext* ctx) override
    {
        Ss << SP_STR("density")
              SP_CTX(x)
              SP_CTX(s)
              SP_CTX(u)
              SP_CTX(wl)
              SP_CTX(p)
              SP_CTX(pr)
              SP_CTX(pl)
              SP_CTX(pf)
              SP_CTX(lambda)
              SP_CTX(h)
              SP_CTX(sigma)
              SP_CTX(p0)
              SP_CTX(isigma)
              SP_CTX(ip0)
              "";
    }

    TString Str() const
    {
        return Ss.Str();
    }

    static TString Get(const TShareNode* node)
    {
        TCtxStatusPrinter v;
        if (const IContext* ctx = node->Ctx()) {
            ctx->Accept(&v);
        } else {
            v.Ss << SP_STR("-") "";
        }
        return v.Str();
    }
};

class TStatusPrinter : public IConstNodeVisitor {
private:
    class TSums : public IConstSimpleNodeVisitor {
    public:
        const TShareNode* Node;
        TSums* PSums;
        TEnergy Dc = 0, Sc = 0; // Sums over alll children

        TSums(const TShareAccount* node, TSums* psums)
            : Node(node)
            , PSums(psums)
        {}


        TSums(const TShareGroup* node, TSums* psums)
            : Node(node)
            , PSums(psums)
        {
            node->AcceptInChildren(this);
        }

        void Visit(const TShareNode* node) override
        {
            Dc += node->D(); Sc += node->S();
        }

        void Print(IOutputStream& os, size_t depth, size_t treeWidth) const
        {
            os << SP_NAME(Node->GetName()) SP_FOREACH_STATUS(SP_ELEM) TCtxStatusPrinter::Get(Node) << "\n";
        }

        TEnergy Eg() const
        {
            return PSums? PSums->Dc + PSums->Sc: 0;
        }
    };

    TStringStream Ss;
    size_t TreeWidth;
    size_t Depth = 0;
    TSums* PSums = nullptr;

    explicit TStatusPrinter(size_t treeWidth)
        : TreeWidth(treeWidth)
    {}

    void Visit(const TShareAccount* node) override
    {
        TSums sums(node, PSums);
        sums.Print(Ss, Depth, TreeWidth);
    }

    void Visit(const TShareGroup* node) override
    {
        // Stack
        TSums* StackedSums = PSums;

        // Recurse
        TSums sums(node, PSums);
        PSums = &sums;
        Depth++;
        node->AcceptInChildren(this);
        Depth--;
        PSums = StackedSums;

        // Print
        sums.Print(Ss, Depth, TreeWidth);
    }

    TString Str() const
    {
        return Ss.Str();
    }

public:
    static TString Print(const TSharePlanner* planner)
    {
        TStringStream ss;
        size_t treeWidth = TNameWidthEvaluator::Get(planner->GetRoot());
        size_t depth = 0; // just for macro to work
        ss << SP_NAME("Name") SP_FOREACH_STATUS(SP_HEAD) SP_STR("model") "\n";
        TStatusPrinter v(treeWidth);
        v.Visit(planner->GetRoot());
        ss << v.Str() << "\n";
        return ss.Str();
    }
};

struct TAsciiArt {
    size_t TreeWidth;
    size_t Depth;
    const TShareGroup* Group;
    TArtParams Art;

    int Height = 32;
    int Width = 36;
    int Length = 4 * Width;
    double ScaleFactor = 1.0;
    double Sigma = 1.0;

    TAsciiArt(size_t treeWidth, size_t depth, const TShareGroup* parent, const TArtParams& art)
        : TreeWidth(treeWidth)
        , Depth(depth)
        , Group(parent)
        , Art(art)
    {
        if (const TDeContext* gctx = dynamic_cast<const TDeContext*>(Group->Ctx())) {
            Sigma = gctx->sigma;
        }
        ScaleFactor = (Art.SigmaStretch? Sigma: 1.0) / Art.Scale;
    }

    TLength Xinv(int x) const
    {
        return double(x) / ScaleFactor / Width * gs * Group->GetTotalVolume();
    }

    int Xcut(TLength x) const
    {
        return Max<i64>(0, Min<i64>(Length, X(x)));
    }

    int X(TLength x) const
    {
        return llrint(x * ScaleFactor * Width * gs / Group->GetTotalVolume());
    }

    TForce Yinv(int y) const
    {
        return double(y) / Height * gs;
    }

    int Ycut(TForce s) const
    {
        return Max<i64>(0, Min<i64>(Height, Y(s)));
    }

    int Y(TForce s) const
    {
        return llrint(s * Height / gs);
    }
};

class TBandPrinter : public IConstSimpleNodeVisitor, public TAsciiArt {
private:
    TStringStream Ss;
    TString Prefix;
    TVector<const TShareNode*> Nodes;
    TEnergy Eg = 0;
    FWeight wg = 0;
public:
    TBandPrinter(size_t treeWidth, size_t depth, const TShareGroup* group, const TArtParams& art)
        : TAsciiArt(treeWidth, depth, group, art)
        , Prefix(TString(2 * Depth, ' ') + "|" + TString(TreeWidth + 1, ' '))
    {
        Height = 0; // Manual height
    }

    void Visit(const TShareNode* node) override
    {
        Nodes.push_back(node);
        Eg += node->E();
        wg += node->w();
        Height += 5;
    }

    TString Str()
    {
        TForce x_offset = (Eg - 2 * Group->GetTotalVolume()) / gs;
        for (const TShareNode* node : Nodes) {
            TString status = node->GetStatus();
            Ss << Sprintf("%-*s  ", (int)TreeWidth, (TString(2 * Depth, ' ') + "+-" + ToString(node->GetName())).data())
               << TString(9, '*')
               << Sprintf(" Ac:%-9ld De:%-9ld Re:%-9ld Ot:%-9ld *** Done:%-9ld w0:%-7.2le w:%-7.2le ",
                          node->GetStats().Activations, node->GetStats().Deactivations,
                          node->GetStats().Retardations, node->GetStats().Overtakes,
                          node->GetStats().NDone, node->w0(), node->w())
               << TString(Length - 109 - status.size(), '*')
               << Sprintf(" %s ***", status.data())
               << Endl;
            // Ensure li <= oi <= ei to be able to draw even corrupted or transitional state
            int li = Xcut(node->l(Eg) - x_offset);
            int oi = Max(li, Xcut(node->o(Eg) - x_offset));
            int e  = Max(oi, Xcut(Eg/gs - x_offset));
            int ei = Xcut(node->e() - x_offset);
            //                        ei
            //                        | (4 possible cases)
            //      .-----------+-----+-----+-----------.
            //      |           |           |           |
            //    ei_l    li  ei_lo   oi  ei_oe   e   ei_e
            // -----+-----+-----+-----+-----+-----+-----+------
            // ..... ===== ::::: >>>>> OOOOO ----- XXXXX
            // ..... ===== ::::: >>>>> OOOOO ----- XXXXX
            // -----+-----+-----+-----+-----+-----+-----+-----> x
            //
            int ei_l  = Min(ei, li);
            int ei_lo = Max(li, Min(ei, oi));
            int ei_oe = Max(oi, Min(ei, e));
            int ei_e  = Max(ei, e);
            int h = Max(1, Ycut(node->s0()));
            for (int i = 0; i < h; i++) {
                Ss << Prefix
                   << TString(ei_l         , '.')
                   << TString(li    - ei_l , '=')
                   << TString(ei_lo - li   , ':')
                   << TString(oi    - ei_lo, '>')
                   << TString(ei_oe - oi   , 'O')
                   << TString(e     - ei_oe, '-')
                   << TString(ei_e  - e    , 'X')
                   << Endl;
            }
        }
        Ss << Sprintf("%-*s  ", (int)TreeWidth, (TString(2 * Depth, ' ') + ToString(Group->GetName())).data())
           << TString(61, '*')
           << Sprintf(" gw0:%-7.2le gw:%-7.2le ", Group->GetTotalWeight(), wg)
           << TString(Length - 87, '*')
           << Endl;
        return Ss.Str();
    }

    static TString Print(size_t treeWidth, size_t depth, const TShareGroup* group, const TArtParams& art)
    {
        TBandPrinter v(treeWidth, depth, group, art);
        group->AcceptInChildren(&v);
        return v.Str();
    }
};

class TDensityPlotter
        : public IConstSimpleNodeVisitor
        , public IVisitor<const TDeContext>
        , public IVisitor<const IContext>
        , public TAsciiArt {
private:
    TStringStream Ss;
    TStringStream Err;
    TString Prefix;
    TVector<TDePoint<const TDeContext>> Points;
    TVector<char> Pixels;
    TLength Xoffset = 0;
public:
    TDensityPlotter(size_t treeWidth, size_t depth, const TShareGroup* group, const TArtParams& art)
        : TAsciiArt(treeWidth, depth, group, art)
        , Prefix(TString(2 * Depth + TreeWidth + 1, ' '))
    {}

    void Visit(const IContext* ctx) override
    {
        Err << "unknown context type: " << TypeName(*ctx) << "\n";
    }

    void Visit(const TDeContext* ctx) override
    {
        ctx->PushPoints(Points);
    }

    void Visit(const TShareNode* node) override
    {
        if (const IContext* ctx = node->Ctx()) {
            ctx->Accept(this);
        }
    }

    TString Str()
    {
        if (Points.empty())
            Err << "no points";
        if (!Err.Empty())
            return "TDensityPlotter: error: " + Err.Str() + "\n";
        Xoffset = (- 2 * Group->GetTotalVolume()) / gs;
        AddLine();
        MakePlot();
        PrintPlot();
        return Ss.Str();
    }

    void AddLine()
    {
        for (int x = 0; x <= Length; x++) {
            double p = Xinv(x) + Xoffset;
            Points.push_back(TDePoint<const TDeContext>{nullptr, p, TDepType::Other});
        }
    }

    void MakePlot()
    {
        static const char Signs[] = "+^1.+-";
        Pixels.resize((Length+1) * (Height+1), ' ');
        double h = 0.0;
        double lambda = 0.0;
        TDePoint<const TDeContext>* prev = nullptr;
        Sort(Points);
        for (TDePoint<const TDeContext>& cur : Points) {
            cur.Move(h, lambda, prev);
            prev = &cur;
            int x = X(cur.p - Xoffset);
            int y = Y((1-h) * gs);
            char pc = GetPixel(x, y);
            char c = Signs[int(cur.Type)];
            if (pc == ' ' || pc == '-' || c == '1') {
                if (c == '1') {
                    if (pc >= '1' && pc <= '8') {
                        c = pc + 1;
                    }
                    if (pc == '9' || pc == '*') {
                        c = '*';
                    }
                }
                SetPixel(x, y, c);
            }
        }
        TLength p0 = 0;
        if (const TDeContext* gctx = dynamic_cast<const TDeContext*>(Group->Ctx())) {
            p0 = gctx->p0;
        }
        int xp0 = X(p0 - Xoffset);
        for (int y = 0; y <= Height; y++) {
            char pc = GetPixel(xp0, y);
            if (pc == ' ') {
                SetPixel(xp0, y, ':');
            }
        }
    }

    char SigmaPixel(int y)
    {
        return (1.0 - Yinv(y)) > Sigma? ' ': 'X';
    }

    void PrintPlot()
    {
        for (int y = 0; y <= Height; y++) {
            Ss << Prefix
               << SigmaPixel(y)
               << TString(Pixels.begin() + y*(Length+1), Pixels.begin() + (y+1)*(Length+1)) << Endl;
        }
        Ss << Sprintf("%-*s ", (int)TreeWidth, (TString(2 * Depth, ' ') + ToString(Group->GetName())).data())
           << 's' << TString(Length + 1, '*') << Endl;
    }

    void SetPixel(int x, int y, char c)
    {
        if (x >= 0 && y >= 0 && x <= Length && y <= Height) {
            Pixels[y * (Length+1) + x] = c;
        }
    }

    char GetPixel(int x, int y)
    {
        if (x >= 0 && y >= 0 && x <= Length && y <= Height) {
            return Pixels[y * (Length+1) + x];
        } else {
            return 0;
        }
    }

    static TString Print(size_t treeWidth, size_t depth, const TShareGroup* group, const TArtParams& art)
    {
        TDensityPlotter v(treeWidth, depth, group, art);
        group->AcceptInChildren(&v);
        return v.Str();
    }
};

class TTreePrinter : public IConstNodeVisitor {
private:
    const TSharePlanner* Planner;
    TStringStream Ss;
    size_t TreeWidth;
    size_t Depth = 0;
    bool Band;
    bool Model;
    TArtParams Art;

    explicit TTreePrinter(const TSharePlanner* planner, size_t treeWidth, bool band, bool model, const TArtParams& art)
        : Planner(planner)
        , TreeWidth(treeWidth)
        , Band(band)
        , Model(model)
        , Art(art)
    {}

    void Visit(const TShareAccount*) override
    {
        // Do nothing for accounts
    }

    void Visit(const TShareGroup* node) override
    {
        Depth++;
        node->AcceptInChildren(this);
        Depth--;
        if (Band) {
            Ss << TBandPrinter::Print(TreeWidth, Depth, node, Art);
        }
        if (Model) {
            switch (Planner->Cfg().GetModel()) {
            case PM_STATIC:
                break;
            case PM_MAX:
                break;
            case PM_DENSITY:
                Ss << TDensityPlotter::Print(TreeWidth, Depth, node, Art);
                break;
            default: break;
            }
        }
        Ss << Endl;
    }

    TString Str() const
    {
        return Ss.Str();
    }

public:
    static TString Print(const TSharePlanner* planner, bool band, bool model, const TArtParams& art)
    {
        TTreePrinter v(planner, TNameWidthEvaluator::Get(planner->GetRoot(), 2), band, model, art);
        v.Visit(planner->GetRoot());
        return v.Str();
    }
};

}
