#pragma once

#include "ypath_detail.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Helper for organizing several YPath services into a map-like object.
/*!
 *  This helper is designed for a small number of YPath services with well-known names.
 *  To handle arbitrary size maps of similar objects see |TVirtualMapBase|.
 */
class TStaticServiceDispatcher
    : public virtual TYPathServiceBase
    , public virtual TSupportsList
{
protected:
    //! Register #service under #key.
    /*! Note that services are instantiated for each request invocation, thus #serviceFactory
     *  must be thread-safe enough.
     *
     *  Also, be careful not to introduce circular reference, i.e. #serviceFactory must
     *  not hold MakeStrong(this). It is guaranteed that this remains valid during #serviceFactory
     *  execution, so binding to a raw this pointer is OK.
     *
     *  Finally, note that IYPathService returned from #serviceFactory _may_ outlive this.
     */
    void RegisterService(TStringBuf key, TCallback<IYPathServicePtr()> serviceFactory);

private:
    THashMap<TString, TCallback<IYPathServicePtr()>> Services_;

    void ListSelf(
        TReqList* /*request*/,
        TRspList* response,
        const TCtxListPtr& context) override;

    TResolveResult ResolveRecursive(
        const TYPath& path,
        const IYPathServiceContextPtr& context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
