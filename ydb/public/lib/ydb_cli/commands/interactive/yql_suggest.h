#pragma once

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <util/generic/fwd.h>

namespace NYdb {
    namespace NConsoleClient {
        using Completions = replxx::Replxx::completions_t;

        class YQLSuggestionEngine final {
        public:
            YQLSuggestionEngine() = default;

        public:
            Completions Suggest(TStringBuf queryUtf8);
        };
    } // namespace NConsoleClient
} // namespace NYdb
