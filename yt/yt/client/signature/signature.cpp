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
        .Item("header").Value((signature.Header_ ? signature.Header_.ToString() : ""))
        .Item("payload").Value(signature.Payload_)
        .Item("signature").Value(signature.Signature_);
    consumer->OnEndMap();
}

void ToProto(NProto::TSignature* protoSignature, const TSignature& signature)
{
    auto headerBuf = signature.Header_.AsStringBuf();
    protoSignature->set_header(headerBuf.Data(), headerBuf.Size());
    protoSignature->set_payload(signature.Payload_);
    protoSignature->set_signature(signature.Signature_);
}

void ToProto(NProto::TSignature* protoSignature, const TSignaturePtr& signature)
{
    ToProto(protoSignature, *signature);
}

////////////////////////////////////////////////////////////////////////////////

void Deserialize(TSignature& signature, INodePtr node)
{
    auto mapNode = node->AsMap();
    signature.Header_ = TYsonString(mapNode->GetChildValueOrThrow<TString>("header"));
    signature.Payload_ = mapNode->GetChildValueOrThrow<std::string>("payload");
    signature.Signature_ = mapNode->GetChildValueOrThrow<std::string>("signature");
}

void Deserialize(TSignature& signature, TYsonPullParserCursor* cursor)
{
    Deserialize(signature, ExtractTo<INodePtr>(cursor));
}

void FromProto(TSignature* signature, const NProto::TSignature& protoSignature)
{
    signature->Header_ = TYsonString(protoSignature.header());
    signature->Payload_ = protoSignature.payload();
    signature->Signature_ = protoSignature.signature();
}

void FromProto(TSignaturePtr* signature, const NProto::TSignature& protoSignature)
{
    *signature = New<TSignature>();
    FromProto(signature->Get(), protoSignature);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
