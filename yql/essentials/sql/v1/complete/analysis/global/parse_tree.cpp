#include "parse_tree.h"

#include <util/system/yassert.h>
#include <util/generic/maybe.h>

namespace NSQLComplete {

    TMaybe<std::string> GetId(SQLv1::Bind_parameterContext* ctx) {
        if (auto* x = ctx->an_id_or_type()) {
            return x->getText();
        } else if (auto* x = ctx->TOKEN_TRUE()) {
            return x->getText();
        } else if (auto* x = ctx->TOKEN_FALSE()) {
            return x->getText();
        } else {
            return Nothing();
        }
    }

} // namespace NSQLComplete
