#include "token.h"

#include "parse_tree.h"

namespace NSQLTranslationV1 {

TToken Beginning(const TRule_select_stmt& rule) {
    const auto& parenthesis = rule.GetRule_select_stmt_intersect1()
                                  .GetRule_select_kind_parenthesis1();
    const auto& kind = Unpack(parenthesis).GetRule_select_kind1();
    switch (kind.GetBlock2().GetAltCase()) {
        case NSQLv1Generated::TRule_select_kind_TBlock2::kAlt1: {
            const auto& alt = kind.GetBlock2().GetAlt1();
            return alt.GetRule_process_core1().GetToken1();
        }
        case NSQLv1Generated::TRule_select_kind_TBlock2::kAlt2: {
            const auto& alt = kind.GetBlock2().GetAlt2();
            return alt.GetRule_reduce_core1().GetToken1();
        }
        case NSQLv1Generated::TRule_select_kind_TBlock2::kAlt3: {
            const auto& alt = kind.GetBlock2().GetAlt3();
            return alt.GetRule_select_core1().GetToken2();
        }
        case NSQLv1Generated::TRule_select_kind_TBlock2::ALT_NOT_SET:
            Y_UNREACHABLE();
    }
}

} // namespace NSQLTranslationV1
