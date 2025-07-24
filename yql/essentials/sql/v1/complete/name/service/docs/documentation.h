#pragma once

#include <util/generic/ptr.h>
#include <util/generic/maybe.h>

namespace NSQLComplete {

    class IDocumentation: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<IDocumentation>;

        virtual TMaybe<TString> Lookup(TStringBuf name) const = 0;
    };

    using TDocByNormalizedNameMap = THashMap<TString, TString>;

    IDocumentation::TPtr MakeStaticDocumentation(TDocByNormalizedNameMap docs);

    IDocumentation::TPtr MakeReservedDocumentation(
        IDocumentation::TPtr primary, IDocumentation::TPtr fallback);

} // namespace NSQLComplete
