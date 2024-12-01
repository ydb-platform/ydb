#pragma once

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <util/generic/fwd.h>

#include <unordered_set>

namespace NYdb {
    namespace NConsoleClient {
        using Completions = replxx::Replxx::completions_t;

        class YQLSuggestionEngine final {
        public:
            YQLSuggestionEngine();

        public:
            Completions Suggest(TStringBuf queryUtf8);

        private:
            std::unordered_set<size_t> CppIgnoredTokens;
            std::unordered_set<size_t> AnsiIgnoredTokens;
        };
    } // namespace NConsoleClient
} // namespace NYdb
