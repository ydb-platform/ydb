#include "static_service_dispatcher.h"

#include "exception_helpers.h"
#include "fluent.h"

#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

void TStaticServiceDispatcher::RegisterService(
    TStringBuf key,
    TCallback<IYPathServicePtr()> serviceFactory)
{
    bool inserted = Services_.emplace(key, std::move(serviceFactory)).second;
    YT_ASSERT(inserted);
}

void TStaticServiceDispatcher::ListSelf(
    TReqList* /*request*/,
    TRspList* response,
    const TCtxListPtr& context)
{
    context->SetRequestInfo();

    auto result = BuildYsonStringFluently()
        .DoListFor(
            Services_,
            [&] (auto fluentList, const auto& pair) {
                fluentList
                    .Item().Value(pair.first);
            });

    response->set_value(result.ToString());
    context->Reply();
}

TStaticServiceDispatcher::TResolveResult TStaticServiceDispatcher::ResolveRecursive(
    const NYPath::TYPath& path,
    const IYPathServiceContextPtr& context)
{
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);
    auto key = tokenizer.GetLiteralValue();

    auto it = Services_.find(key);

    if (it == Services_.end()) {
        const auto& method = context->GetMethod();
        if (method == "Exists") {
            return TResolveResultHere{"/" + path};
        }
        ThrowNoSuchChildKey(key);
    }

    return TResolveResultThere{it->second(), NYPath::TYPath(tokenizer.GetSuffix())};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
