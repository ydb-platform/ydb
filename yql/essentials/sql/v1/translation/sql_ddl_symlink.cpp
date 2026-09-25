#include "sql_ddl_symlink.h"

namespace NSQLTranslationV1 {

bool TSymlinkTranslation::SymlinkPath(const TRule_symlink_path& node, TDeferredAtom& result) {
    switch (node.Alt_case()) {
        case TRule_symlink_path::kAltSymlinkPath1:
            result = TDeferredAtom(Context().Pos(), Id(node.GetAlt_symlink_path1().GetRule_an_id_or_type1(), *this));
            break;
        case TRule_symlink_path::kAltSymlinkPath2: {
            TString bindName;
            if (!NamedNodeImpl(node.GetAlt_symlink_path2().GetRule_bind_parameter1(), bindName, *this)) {
                return false;
            }
            auto named = GetNamedNode(bindName);
            if (!named) {
                return false;
            }
            MakeTableFromExpression(Context().Pos(), Context(), named, result);
            break;
        }
        case TRule_symlink_path::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }

    return !result.Empty();
}

bool TSymlinkTranslation::SymlinkRef(const TRule_symlink_ref& node, TSymlinkRef& result) {
    result = {};
    result.Service = Context().Scoped->CurrService;
    result.Cluster = Context().Scoped->CurrCluster;
    if (node.HasBlock1()) {
        if (!ClusterExpr(node.GetBlock1().GetRule_cluster_expr1(), /*allowWildcard=*/false, result.Service, result.Cluster)) {
            return false;
        }
    }

    if (!SymlinkPath(node.GetRule_symlink_path2(), result.Path)) {
        return false;
    }
    if (result.Cluster.Empty()) {
        Error() << "No cluster name given and no default cluster is selected";
        return false;
    }

    return true;
}

TNodePtr TSymlinkTranslation::Build(const TRule_create_symlink_stmt& node) {
    Ctx_.BodyPart();
    TSymlinkRef link;
    if (!SymlinkRef(node.GetRule_symlink_ref4(), link)) {
        return {};
    }

    TDeferredAtom target;
    if (!SymlinkPath(node.GetRule_symlink_path6(), target)) {
        return {};
    }

    return BuildCreateSymlink(Ctx_.Pos(), link, target, node.HasBlock3(), Ctx_.Scoped);
}

TNodePtr TSymlinkTranslation::Build(const TRule_drop_symlink_stmt& node) {
    Ctx_.BodyPart();
    TSymlinkRef link;
    if (!SymlinkRef(node.GetRule_symlink_ref4(), link)) {
        return {};
    }

    return BuildDropSymlink(Ctx_.Pos(), link, node.HasBlock3(), Ctx_.Scoped);
}

} // namespace NSQLTranslationV1
