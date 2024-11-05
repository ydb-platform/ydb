#include "origin_attributes.h"

#include <yt/yt/core/ytree/attributes.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

TOriginAttributes ExtractFromDictionary(const NYTree::IAttributeDictionaryPtr& attributes)
{
    using TFunctor = TOriginAttributes(*)(const NYTree::IAttributeDictionaryPtr&);

    if (auto strong = NGlobal::GetErasedVariable(ExtractFromDictionaryTag)) {
        return strong->AsConcrete<TFunctor>()(attributes);
    }

    return ExtractFromDictionaryDefault(attributes);
}

////////////////////////////////////////////////////////////////////////////////

TOriginAttributes ExtractFromDictionaryDefault(const NYTree::IAttributeDictionaryPtr& attributes)
{
    TOriginAttributes result;
    if (!attributes) {
        return result;
    }

    static const TString HostKey("host");
    result.HostHolder = TSharedRef::FromString(attributes->GetAndRemove<TString>(HostKey, TString()));
    result.Host = result.HostHolder.empty() ? TStringBuf() : TStringBuf(result.HostHolder.Begin(), result.HostHolder.End());

    static const TString DatetimeKey("datetime");
    result.Datetime = attributes->GetAndRemove<TInstant>(DatetimeKey, TInstant());

    static const TString PidKey("pid");
    result.Pid = attributes->GetAndRemove<TProcessId>(PidKey, 0);

    static const TString TidKey("tid");
    result.Tid = attributes->GetAndRemove<NThreading::TThreadId>(TidKey, NThreading::InvalidThreadId);

    static const TString ThreadNameKey("thread");
    result.ThreadName = attributes->GetAndRemove<TString>(ThreadNameKey, TString());

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
