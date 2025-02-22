#pragma once

#include "public.h"

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/public.h>

#include <vector>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TSignature final
{
public:
    // NB(pavook) only needed for Deserialize internals.

    //! Constructs an empty TSignature.
    TSignature() = default;

    [[nodiscard]] const NYson::TYsonString& Payload() const;

private:
    NYson::TYsonString Header_;
    NYson::TYsonString Payload_;
    std::vector<std::byte> Signature_;

    friend struct ISignatureGenerator;
    friend struct TDummySignatureGenerator;
    friend struct TAlwaysThrowingSignatureGenerator;
    friend class TSignatureGenerator;

    friend struct ISignatureValidator;
    friend struct TDummySignatureValidator;
    friend struct TAlwaysThrowingSignatureValidator;
    friend class TSignatureValidator;

    friend void Serialize(const TSignature& signature, NYson::IYsonConsumer* consumer);
    friend void Deserialize(TSignature& signature, NYTree::INodePtr node);
    friend void Deserialize(TSignature& signature, NYson::TYsonPullParserCursor* cursor);
};

DEFINE_REFCOUNTED_TYPE(TSignature)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
