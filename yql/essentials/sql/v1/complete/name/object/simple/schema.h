#pragma once

#include <yql/essentials/sql/v1/complete/name/object/schema.h>

namespace NSQLComplete {

    struct TSplittedPath {
        TStringBuf Path;
        TStringBuf NameHint;
    };

    class ISimpleSchema: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<ISimpleSchema>;

        virtual ~ISimpleSchema() = default;
        virtual TSplittedPath Split(TStringBuf path) const = 0;
        virtual NThreading::TFuture<TVector<TFolderEntry>> List(TString folder) const = 0;
    };

    ISchema::TPtr MakeSimpleSchema(ISimpleSchema::TPtr simple);

} // namespace NSQLComplete
