#include "complex_key_map.h"

#include <util/string/split.h>

#include <util/string/cast.h>
#include <util/stream/output.h>

namespace NYT::NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TTestComplexKey TTestComplexKey::FromString(TStringBuf string)
{
    TTestComplexKey key;
    Split(string, '/', key.First, key.Second);
    return key;
}

TString TTestComplexKey::ToString() const
{
    return Format("%v/%v", First, Second);
}

bool TTestComplexKey::operator==(const TTestComplexKey& other) const
{
    return std::tie(First, Second) == std::tie(other.First, other.Second);
}

bool TTestComplexKey::operator<(const TTestComplexKey& other) const
{
    return std::tie(First, Second) < std::tie(other.First, other.Second);
}

void Serialize(const TTestComplexKey& value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(ToString(value));
}

void Deserialize(TTestComplexKey& value, INodePtr node)
{
    if (node->GetType() == ENodeType::String) {
        value = FromString<TTestComplexKey>(node->AsString()->GetValue());
    } else {
        THROW_ERROR_EXCEPTION("Cannot parse TTestComplexKey value from %Qlv",
            node->GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

////////////////////////////////////////////////////////////////////////////////

template <>
NYT::NYTree::TTestComplexKey FromStringImpl<NYT::NYTree::TTestComplexKey, char>(const char* data, size_t size)
{
    return NYT::NYTree::TTestComplexKey::FromString(TStringBuf(data, size));
}

template <>
bool TryFromStringImpl<NYT::NYTree::TTestComplexKey, char>(const char* data, size_t size, NYT::NYTree::TTestComplexKey& value)
{
    try {
        value = NYT::NYTree::TTestComplexKey::FromString(TStringBuf(data, size));
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NYT::NYTree::TTestComplexKey>(IOutputStream& out, const NYT::NYTree::TTestComplexKey& value) {
    out << value.ToString();
}

////////////////////////////////////////////////////////////////////////////////
