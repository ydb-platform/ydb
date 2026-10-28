#pragma once

#include "ddl_symlink.h"
#include "sql_translation.h"

namespace NSQLTranslationV1 {

class TSymlinkTranslation final: public TSqlTranslation {
public:
    TSymlinkTranslation(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TNodePtr Build(const TRule_create_symlink_stmt& node);
    TNodePtr Build(const TRule_drop_symlink_stmt& node);

private:
    bool SymlinkPath(const TRule_symlink_path& node, TDeferredAtom& result);
    bool SymlinkRef(const TRule_symlink_ref& node, TSymlinkRef& result);
};

} // namespace NSQLTranslationV1
