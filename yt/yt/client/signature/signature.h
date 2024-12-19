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
    [[nodiscard]] const NYson::TYsonString& Payload() const;

private:
    NYson::TYsonString Header_;
    NYson::TYsonString Payload_;
    std::vector<std::byte> Signature_;

    friend class TSignatureGenerator;
    friend class TSignatureValidator;

    friend void Serialize(const TSignature& signature, NYson::IYsonConsumer* consumer);
    friend void Deserialize(TSignature& signature, NYTree::INodePtr node);
    friend void Deserialize(TSignature& signature, NYson::TYsonPullParserCursor* cursor);
};

DEFINE_REFCOUNTED_TYPE(TSignature)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
