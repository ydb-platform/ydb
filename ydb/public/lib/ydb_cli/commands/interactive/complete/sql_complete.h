#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    struct TCompletionInput {
        TStringBuf Text;
        size_t CursorPosition = Text.length();
    };

    // std::string is used to prevent copying into replxx api
    struct TCompletionContext {
        TVector<std::string> Keywords;
    };

    class ISqlCompletionEngine {
    public:
        using TPtr = THolder<ISqlCompletionEngine>;

        virtual TCompletionContext Complete(TCompletionInput input) = 0;
        virtual ~ISqlCompletionEngine() = default;
    };

    ISqlCompletionEngine::TPtr MakeSqlCompletionEngine();

} // namespace NSQLComplete
