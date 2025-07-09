#pragma once

#include <yql/essentials/sql/v1/complete/name/object/schema.h>

namespace NSQLComplete {

    struct TSplittedPath {
        TStringBuf Path;
        TStringBuf NameHint;
    };

    struct TTableDetails {
        TVector<TString> Columns;
    };

    class ISimpleSchema: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<ISimpleSchema>;

        ~ISimpleSchema() override = default;

        virtual TSplittedPath Split(TStringBuf path) const = 0;

        // TODO(YQL-19747): Deprecated, use List(cluster, folder) instead.
        virtual NThreading::TFuture<TVector<TFolderEntry>> List(TString folder) const;

        virtual NThreading::TFuture<TVector<TFolderEntry>>
        List(TString cluster, TString folder) const;

        virtual NThreading::TFuture<TMaybe<TTableDetails>>
        DescribeTable(const TString& cluster, const TString& path) const;
    };

    ISchema::TPtr MakeSimpleSchema(ISimpleSchema::TPtr simple);

} // namespace NSQLComplete
