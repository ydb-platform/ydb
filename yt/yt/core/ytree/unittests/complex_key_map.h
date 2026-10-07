#pragma once

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

struct TTestComplexKey
{
    std::string First;
    std::string Second;

    static TTestComplexKey FromString(TStringBuf string);
    std::string ToString() const;

    bool operator==(const TTestComplexKey& other) const;
    bool operator<(const TTestComplexKey& other) const;
};

void Serialize(const TTestComplexKey& value, NYson::IYsonConsumer* consumer);
void Deserialize(TTestComplexKey& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
