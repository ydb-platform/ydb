#include "signature.h"

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const std::string& TSignature::Payload() const
{
    return Payload_;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TSignature& signature, IYsonConsumer* consumer)
{
    consumer->OnBeginMap();
    BuildYsonMapFragmentFluently(consumer)
        .Item("header").Value(signature.Header_.ToString())
        .Item("payload").Value(signature.Payload_)
        .Item("signature").Value(TStringBuf(
            reinterpret_cast<const char*>(signature.Signature_.data()),
            signature.Signature_.size()));
    consumer->OnEndMap();
}

////////////////////////////////////////////////////////////////////////////////

void Deserialize(TSignature& signature, INodePtr node)
{
    auto mapNode = node->AsMap();
    signature.Header_ = TYsonString(mapNode->GetChildValueOrThrow<TString>("header"));
    signature.Payload_ = mapNode->GetChildValueOrThrow<std::string>("payload");

    auto signatureString = mapNode->GetChildValueOrThrow<std::string>("signature");
    auto signatureBytes = std::as_bytes(std::span(signatureString));
    signature.Signature_.resize(signatureBytes.size());

    std::copy(signatureBytes.begin(), signatureBytes.end(), signature.Signature_.begin());
}

void Deserialize(TSignature& signature, TYsonPullParserCursor* cursor)
{
    Deserialize(signature, ExtractTo<INodePtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
