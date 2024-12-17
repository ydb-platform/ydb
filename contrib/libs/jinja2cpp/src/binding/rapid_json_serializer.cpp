#include "rapid_json_serializer.h"

#include "../value_visitors.h"

#include <rapidjson/prettywriter.h>

namespace jinja2
{
namespace rapidjson_serializer
{
namespace
{
struct JsonInserter : visitors::BaseVisitor<rapidjson::Value>
{
    using BaseVisitor::operator();

    explicit JsonInserter(rapidjson::Document::AllocatorType& allocator)
        : m_allocator(allocator)
    {
    }

    rapidjson::Value operator()(const ListAdapter& list) const
    {
        rapidjson::Value listValue(rapidjson::kArrayType);

        for (auto& v : list)
        {
            listValue.PushBack(Apply<JsonInserter>(v, m_allocator), m_allocator);
        }
        return listValue;
    }

    rapidjson::Value operator()(const MapAdapter& map) const
    {
        rapidjson::Value mapNode(rapidjson::kObjectType);

        const auto& keys = map.GetKeys();
        for (auto& k : keys)
        {
            mapNode.AddMember(rapidjson::Value(k.c_str(), m_allocator), Apply<JsonInserter>(map.GetValueByName(k), m_allocator), m_allocator);
        }

        return mapNode;
    }

    rapidjson::Value operator()(const KeyValuePair& kwPair) const
    {
        rapidjson::Value pairNode(rapidjson::kObjectType);
        pairNode.AddMember(rapidjson::Value(kwPair.key.c_str(), m_allocator), Apply<JsonInserter>(kwPair.value, m_allocator), m_allocator);

        return pairNode;
    }

    rapidjson::Value operator()(const std::string& str) const { return rapidjson::Value(str.c_str(), m_allocator); }

    rapidjson::Value operator()(const std::string_view& str) const
    {
        return rapidjson::Value(str.data(), static_cast<rapidjson::SizeType>(str.size()), m_allocator);
    }

    rapidjson::Value operator()(const std::wstring& str) const
    {
        auto s = ConvertString<std::string>(str);
        return rapidjson::Value(s.c_str(), m_allocator);
    }

    rapidjson::Value operator()(const std::wstring_view& str) const
    {
        auto s = ConvertString<std::string>(str);
        return rapidjson::Value(s.c_str(), m_allocator);
    }

    rapidjson::Value operator()(bool val) const { return rapidjson::Value(val); }

    rapidjson::Value operator()(EmptyValue) const { return rapidjson::Value(); }

    rapidjson::Value operator()(const Callable&) const { return rapidjson::Value("<callable>"); }

    rapidjson::Value operator()(double val) const { return rapidjson::Value(val); }

    rapidjson::Value operator()(int64_t val) const { return rapidjson::Value(val); }

    rapidjson::Document::AllocatorType& m_allocator;
};
} // namespace

DocumentWrapper::DocumentWrapper()
    : m_document(std::make_shared<rapidjson::Document>())
{
}

ValueWrapper DocumentWrapper::CreateValue(const InternalValue& value) const
{
    auto v = Apply<JsonInserter>(value, m_document->GetAllocator());
    return ValueWrapper(std::move(v), m_document);
}

ValueWrapper::ValueWrapper(rapidjson::Value&& value, std::shared_ptr<rapidjson::Document> document)
    : m_value(std::move(value))
    , m_document(document)
{
}

std::string ValueWrapper::AsString(const uint8_t indent) const
{
    using Writer = rapidjson::Writer<rapidjson::StringBuffer, rapidjson::Document::EncodingType, rapidjson::UTF8<>>;
    using PrettyWriter = rapidjson::PrettyWriter<rapidjson::StringBuffer, rapidjson::Document::EncodingType, rapidjson::UTF8<>>;

    rapidjson::StringBuffer buffer;
    if (indent == 0)
    {
        Writer writer(buffer);
        m_value.Accept(writer);
    }
    else
    {
        PrettyWriter writer(buffer);
        writer.SetIndent(' ', indent);
        writer.SetFormatOptions(rapidjson::kFormatSingleLineArray);
        m_value.Accept(writer);
    }

    return buffer.GetString();
}

} // namespace rapidjson_serializer
} // namespace jinja2
