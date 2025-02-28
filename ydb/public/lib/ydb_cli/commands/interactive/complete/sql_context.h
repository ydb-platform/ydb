#pragma once

#include "sql_complete.h"

#include <util/generic/string.h>

namespace NSQLComplete {

    struct TCompletionContext {
        TVector<TString> Keywords;
    };

    class ISqlContextInference {
    public:
        using TPtr = THolder<ISqlContextInference>;

        virtual TCompletionContext Analyze(TCompletionInput input) = 0;
        virtual ~ISqlContextInference() = default;
    };

    ISqlContextInference::TPtr MakeSqlContextInference();

} // namespace NSQLComplete
