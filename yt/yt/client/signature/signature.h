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

    [[nodiscard]] const std::string& Payload() const;

private:
    NYson::TYsonString Header_;
    std::string Payload_;
    std::string Signature_;

    friend struct ISignatureGenerator;
    friend class TSignatureGenerator;

    friend struct ISignatureValidator;
    friend class TSignatureValidator;

    friend void Serialize(const TSignature& signature, NYson::IYsonConsumer* consumer);
    friend void Deserialize(TSignature& signature, NYTree::INodePtr node);
    friend void Deserialize(TSignature& signature, NYson::TYsonPullParserCursor* cursor);
};

DEFINE_REFCOUNTED_TYPE(TSignature)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
