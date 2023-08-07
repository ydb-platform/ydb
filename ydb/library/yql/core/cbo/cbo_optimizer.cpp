#include "cbo_optimizer.h"

#include <array>

#include <util/string/builder.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <library/cpp/disjoint_sets/disjoint_sets.h>

namespace NYql {

namespace {

std::array<TStringBuf,6> Prefixes = {
    "",
    " ",
    "  ",
    "   ",
    "    ",
    "     "
};

TStringBuf Prefix(int level) {
    return level < (int)Prefixes.size()
        ? Prefixes[level]
        : Prefixes.back();
}

void PrettyPrintVar(TStringBuilder& b, const IOptimizer::TInput* input, IOptimizer::TVarId varId) {
    const auto& [relno, varno] = varId;
    auto varName = input
        ? input->Rels[relno-1].TargetVars[varno-1].Name
        : '\0';
    if (!varName) {
        b << "(" << relno << "," << varno << ")";
    } else {
        b << varName;
    }
}

void PrettyPrintNode(int level, TStringBuilder& b, const IOptimizer::TOutput& output, int id) {
    TStringBuf prefix = Prefix(level);
    const auto& node = output.Nodes[id];
    switch (node.Mode) {
        case IOptimizer::EJoinType::Unknown: b << prefix <<  " Node\n"; break;
        case IOptimizer::EJoinType::Inner: b << prefix <<  " Inner Join\n"; break;
        default: b << prefix <<  " Unknown\n"; break;
    }
    switch (node.Strategy) {
        case IOptimizer::EJoinStrategy::Hash: b << prefix <<  " Hash Strategy\n"; break;
        case IOptimizer::EJoinStrategy::Loop: b << prefix <<  " Loop Strategy\n"; break;
        default: break;
    }
    if (!node.Rels.empty())
    {
        b << prefix << " Rels: [";
        for (int i = 0; i < (int)node.Rels.size()-1; i++) {
            b << node.Rels[i] << ",";
        }
        b << node.Rels.back();
        b << "]\n";
    }

    {
        auto isEmpty = [](IOptimizer::TVarId id) -> bool {
            auto& [a, b] = id;
            return a<=0 || b<=0;
        };

        if (!isEmpty(node.LeftVar) && !isEmpty(node.RightVar)) {
            b << prefix << " Op: ";
            PrettyPrintVar(b, output.Input, node.LeftVar);
            b << " = ";
            PrettyPrintVar(b, output.Input, node.RightVar);
            b << "\n";
        }
    }

    if (node.Outer != -1) {
        b << prefix << " {\n";
        PrettyPrintNode(level+1, b, output, node.Outer);
        b << prefix << " }\n";
    }
    if (node.Inner != -1) {
        b << prefix << " {\n";
        PrettyPrintNode(level+1, b, output, node.Inner);
        b << prefix << " }\n";
    }
}

void PrettyPrintRel(TStringBuilder& b, const IOptimizer::TInput* input, const auto& relId) {
    const auto& rel = input->Rels[relId - 1];
    b << "{";
    b << "rows: " << rel.Rows << ",";
    b << "cost: " << rel.TotalCost << ",";
    b << "vars: [";
    for (ui32 i = 0; i < rel.TargetVars.size(); i++) {
        PrettyPrintVar(b, input, {relId, i + 1});
        if (i != rel.TargetVars.size() - 1) {
            b << ", ";
        }
    }
    b << "]";
    b << "}";
}

} // namespace

TString IOptimizer::TOutput::ToString() const {
    TStringBuilder b;
    b << "{\n";
    PrettyPrintNode(0, b, *this, 0);
    b << "}\n";
    return b;
}

TString IOptimizer::TInput::ToString() const {
    TStringBuilder b;
    b << "Rels: [";
    for (ui32 i = 0; i < Rels.size(); ++i) {
        PrettyPrintRel(b, this, i + 1);
        if (i != Rels.size() - 1) {
            b << ",\n";
        }
    }
    b << "]\n";
    b << "EqClasses: [";
    for (ui32 i = 0; i < EqClasses.size(); ++i) {
        b << "[";
        for (ui32 j = 0; j < EqClasses[i].Vars.size(); ++j) {
            PrettyPrintVar(b, this, EqClasses[i].Vars[j]);
            if (j != EqClasses[i].Vars.size() - 1) {
                b << ",";
            }
        }
        b << "]";
        if (i != EqClasses.size() - 1) {
            b << ",";
        }
    }
    b << "]\n";
    return b;
}

void IOptimizer::TInput::Normalize() {
    using TId = TDisjointSets::TElement;

    THashMap<TVarId, TId> var2id;
    std::vector<TVarId> id2var;
    TId curId = 1;

    for (auto& eq : EqClasses) {
        for (auto& v : eq.Vars) {
            auto& id = var2id[v];
            if (id == 0) {
                id = curId;
                id2var.resize(curId + 1);
                id2var[curId] = v;
                ++curId;
            }
        }
    }

    TDisjointSets u(curId + 1);
    for (auto& eq : EqClasses) {
        Y_VERIFY(!eq.Vars.empty());

        ui32 i = 0;
        TId first = var2id[eq.Vars[i++]];
        for (; i < eq.Vars.size(); ++i) {
            TId id = var2id[eq.Vars[i]];
            u.UnionSets(first, id);
        }
    }

    THashMap<TId, THashSet<TId>> eqClasses;
    for (auto& eq : EqClasses) {
        for (auto& var : eq.Vars) {
            auto id = var2id[var];
            auto canonicalId = u.CanonicSetElement(id);
            eqClasses[canonicalId].emplace(id);
        }
    }
    EqClasses.clear();
    for (auto& [_, ids]: eqClasses) {
        TEq eqClass;
        eqClass.Vars.reserve(ids.size());
        for (auto id : ids) {
            eqClass.Vars.emplace_back(id2var[id]);
        }
        std::sort(eqClass.Vars.begin(), eqClass.Vars.end());
        EqClasses.emplace_back(std::move(eqClass));
    }
}

} // namespace NYql
