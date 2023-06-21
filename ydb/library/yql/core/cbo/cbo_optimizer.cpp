#include "cbo_optimizer.h"

#include <array>

#include <util/string/builder.h>

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

void PrettyPrintVar(TStringBuilder& b, const IOptimizer::TOutput& output, IOptimizer::TVarId varId) {
    const auto& [relno, varno] = varId;
    auto varName = output.Input
        ? output.Input->Rels[relno-1].TargetVars[varno-1].Name
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
            PrettyPrintVar(b, output, node.LeftVar);
            b << " = ";
            PrettyPrintVar(b, output, node.RightVar);
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

} // namespace

TString IOptimizer::TOutput::ToString() const {
    TStringBuilder b;
    b << "{\n";
    PrettyPrintNode(0, b, *this, 0);
    b << "}\n";
    return b;
}

} // namespace NYql
