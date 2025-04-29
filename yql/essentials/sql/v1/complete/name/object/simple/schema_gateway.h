#pragma once

#include <yql/essentials/sql/v1/complete/name/object/schema_gateway.h>

namespace NSQLComplete {

    struct TSplittedPath {
        TStringBuf Path;
        TStringBuf NameHint;
    };

    class ISimpleSchemaGateway: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<ISimpleSchemaGateway>;

        virtual ~ISimpleSchemaGateway() = default;
        virtual TSplittedPath Split(TStringBuf path) const = 0;
        virtual NThreading::TFuture<TVector<TFolderEntry>> List(TString folder) const = 0;
    };

    ISchemaGateway::TPtr MakeSimpleSchemaGateway(ISimpleSchemaGateway::TPtr simple);

} // namespace NSQLComplete
