#pragma once

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

struct TTestComplexKey
{
    TString First;
    TString Second;

    static TTestComplexKey FromString(TStringBuf string);
    TString ToString() const;

    bool operator==(const TTestComplexKey& other) const;
    bool operator<(const TTestComplexKey& other) const;
};

void Serialize(const TTestComplexKey& value, NYson::IYsonConsumer* consumer);
void Deserialize(TTestComplexKey& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
