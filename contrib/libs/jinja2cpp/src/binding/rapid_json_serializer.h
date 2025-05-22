#ifndef JINJA2CPP_SRC_RAPID_JSON_SERIALIZER_H
#define JINJA2CPP_SRC_RAPID_JSON_SERIALIZER_H

#include "../internal_value.h"

#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include <memory>

namespace jinja2
{
namespace rapidjson_serializer
{

class ValueWrapper
{
    friend class DocumentWrapper;

public:
    ValueWrapper(ValueWrapper&&) = default;
    ValueWrapper& operator=(ValueWrapper&&) = default;

    std::string AsString(uint8_t indent = 0) const;

private:
    ValueWrapper(rapidjson::Value&& value, std::shared_ptr<rapidjson::Document> document);

    rapidjson::Value m_value;
    std::shared_ptr<rapidjson::Document> m_document;
};

class DocumentWrapper
{
public:
    DocumentWrapper();

    DocumentWrapper(DocumentWrapper&&) = default;
    DocumentWrapper& operator=(DocumentWrapper&&) = default;

    ValueWrapper CreateValue(const InternalValue& value) const;

private:
    std::shared_ptr<rapidjson::Document> m_document;
};

} // namespace rapidjson_serializer
} // namespace jinja2

#endif // JINJA2CPP_SRC_RAPID_JSON_SERIALIZER_H
