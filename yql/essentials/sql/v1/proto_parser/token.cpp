#include "token.h"

#include "parse_tree.h"

namespace NSQLTranslationV1 {

TToken Beginning(const TRule_select_stmt& rule) {
    const auto& parenthesis = rule.GetRule_select_stmt_intersect1()
                                  .GetRule_select_kind_parenthesis1();
    return Beginning(Unpack(parenthesis).GetRule_select_kind1());
}

TToken Beginning(const TRule_select_kind& rule) {
    const auto& block = rule.GetBlock2();
    switch (block.GetAltCase()) {
        case NSQLv1Generated::TRule_select_kind_TBlock2::kAlt1:
            return block.GetAlt1().GetRule_process_core1().GetToken1();
        case NSQLv1Generated::TRule_select_kind_TBlock2::kAlt2:
            return block.GetAlt2().GetRule_reduce_core1().GetToken1();
        case NSQLv1Generated::TRule_select_kind_TBlock2::kAlt3:
            return block.GetAlt3().GetRule_select_core1().GetToken2();
        case NSQLv1Generated::TRule_select_kind_TBlock2::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

} // namespace NSQLTranslationV1
