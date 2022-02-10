#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NYsonPull {
    namespace NDetail {
        namespace NImpl {
            inline void apply_args(TStringBuilder&) {
            }

            template <typename T, typename... Args>
            inline void apply_args(TStringBuilder& builder, T&& arg, Args&&... args) {
                apply_args(builder << arg, std::forward<Args>(args)...);
            }
        }

        template <typename... Args>
        TString format_string(Args&&... args) {
            TStringBuilder builder;
            NImpl::apply_args(builder, std::forward<Args>(args)...);
            return TString(std::move(builder));
        }
    }
}
