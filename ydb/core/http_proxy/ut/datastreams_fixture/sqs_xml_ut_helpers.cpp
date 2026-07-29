#include "sqs_xml_ut_helpers.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/xml/document/xml-document.h>

#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace {

    void AppendParam(TString& result, const TString& key, const TString& value) {
        if (!result.empty()) {
            result.append('&');
        }
        result.append(CGIEscapeRet(key));
        result.append('=');
        result.append(CGIEscapeRet(value));
    }

    void EncodeAttributes(TString& result, TStringBuf prefix, const NJson::TJsonValue& attributes) {
        if (!attributes.IsMap()) {
            return;
        }
        int i = 1;
        for (const auto& [name, value] : attributes.GetMapSafe()) {
            AppendParam(result, TStringBuilder() << prefix << '.' << i << ".Name", name);
            AppendParam(result, TStringBuilder() << prefix << '.' << i << ".Value", value.GetStringRobust());
            ++i;
        }
    }

    void EncodeAttributeNames(TString& result, const NJson::TJsonValue& attributeNames) {
        if (attributeNames.IsString()) {
            AppendParam(result, "AttributeName.1", attributeNames.GetStringSafe());
            return;
        }
        if (!attributeNames.IsArray()) {
            return;
        }
        int i = 1;
        for (const auto& name : attributeNames.GetArraySafe()) {
            AppendParam(result, TStringBuilder() << "AttributeName." << i, name.GetStringRobust());
            ++i;
        }
    }

    void EncodeTags(TString& result, const NJson::TJsonValue& tags) {
        if (!tags.IsMap()) {
            return;
        }
        int i = 1;
        for (const auto& [key, value] : tags.GetMapSafe()) {
            AppendParam(result, TStringBuilder() << "Tag." << i << ".Key", key);
            AppendParam(result, TStringBuilder() << "Tag." << i << ".Value", value.GetStringRobust());
            ++i;
        }
    }

    void EncodeTagKeys(TString& result, const NJson::TJsonValue& tagKeys) {
        if (!tagKeys.IsArray()) {
            return;
        }
        int i = 1;
        for (const auto& key : tagKeys.GetArraySafe()) {
            AppendParam(result, TStringBuilder() << "TagKey." << i, key.GetStringRobust());
            ++i;
        }
    }

    void EncodeMessageAttributes(TString& result, TStringBuf prefix, const NJson::TJsonValue& messageAttributes) {
        if (!messageAttributes.IsMap()) {
            return;
        }
        int i = 1;
        for (const auto& [name, attr] : messageAttributes.GetMapSafe()) {
            const TString attrPrefix = TStringBuilder() << prefix << '.' << i;
            AppendParam(result, attrPrefix + ".Name", name);
            if (attr.IsMap()) {
                for (const auto& [key, val] : attr.GetMapSafe()) {
                    AppendParam(result, TStringBuilder() << attrPrefix << ".Value." << key, val.GetStringRobust());
                }
            }
            ++i;
        }
    }

    void EncodeBatchEntries(TString& result, TStringBuf action, const NJson::TJsonValue& entries) {
        if (!entries.IsArray()) {
            return;
        }
        const TString entryPrefix = TStringBuilder() << action << "RequestEntry";
        int i = 1;
        for (const auto& entry : entries.GetArraySafe()) {
            if (!entry.IsMap()) {
                continue;
            }
            const auto& entryMap = entry.GetMapSafe();
            static constexpr TStringBuf scalarFields[] = {
                "Id",
                "MessageBody",
                "MessageGroupId",
                "MessageDeduplicationId",
                "DelaySeconds",
                "ReceiptHandle",
                "VisibilityTimeout",
            };
            for (const auto field : scalarFields) {
                if (const auto* value = entryMap.FindPtr(field)) {
                    if (value->IsString() || value->IsUInteger() || value->IsInteger()) {
                        AppendParam(
                            result,
                            TStringBuilder() << entryPrefix << '.' << i << '.' << field,
                            value->GetStringRobust()
                        );
                    }
                }
            }
            if (const auto* messageAttributes = entryMap.FindPtr("MessageAttributes")) {
                EncodeMessageAttributes(
                    result,
                    TStringBuilder() << entryPrefix << '.' << i << ".MessageAttribute",
                    *messageAttributes
                );
            }
            ++i;
        }
    }

    NJson::TJsonValue XmlElementToJson(const NXml::TConstNode& node);

    NJson::TJsonValue XmlScalarElementToJson(const NXml::TConstNode& node) {
        NXml::TConstNode firstChild = node.FirstChild();
        if (firstChild.IsNull()) {
            return "";
        }
        if (firstChild.IsText() && firstChild.NextSibling().IsNull()) {
            return node.Value<TString>("");
        }
        return XmlElementToJson(node);
    }

    NJson::TJsonValue XmlElementToJson(const NXml::TConstNode& node) {
        NJson::TJsonValue result(NJson::JSON_MAP);
        NJson::TJsonMap attributes;
        NJson::TJsonValue messageAttributes(NJson::JSON_ARRAY);
        bool hasAttributes = false;

        for (NXml::TConstNode child = node.FirstChild(); !child.IsNull(); child = child.NextSibling()) {
            if (!child.IsElementNode()) {
                continue;
            }
            const TString name = child.Name();
            if (name == "Attribute") {
                const TString attrName = child.Node("Name", true).Value<TString>("");
                const TString attrValue = child.Node("Value", true).Value<TString>("");
                if (!attrName.empty()) {
                    attributes[attrName] = attrValue;
                    hasAttributes = true;
                }
                continue;
            }
            if (name == "MessageAttribute") {
                NJson::TJsonMap attr;
                attr["Name"] = child.Node("Name", true).Value<TString>("");
                auto valueNode = child.Node("Value", true);
                if (!valueNode.IsNull()) {
                    attr["Value"] = XmlElementToJson(valueNode);
                }
                messageAttributes.AppendValue(std::move(attr));
                continue;
            }
            result[name] = XmlScalarElementToJson(child);
        }

        if (hasAttributes) {
            result["Attributes"] = std::move(attributes);
        }
        if (!messageAttributes.GetArraySafe().empty()) {
            result["MessageAttributes"] = std::move(messageAttributes);
        }
        return result;
    }

    NJson::TJsonValue CollectQueueUrls(const NXml::TConstNode& parent) {
        NJson::TJsonValue queueUrls(NJson::JSON_ARRAY);
        for (NXml::TConstNode child = parent.FirstChild(); !child.IsNull(); child = child.NextSibling()) {
            if (child.IsElementNode() && child.Name() == "QueueUrl") {
                queueUrls.AppendValue(child.Value<TString>());
            }
        }
        return queueUrls;
    }

    NJson::TJsonValue CollectSameNamedChildren(const NXml::TConstNode& parent, TStringBuf name) {
        NJson::TJsonValue array(NJson::JSON_ARRAY);
        for (NXml::TConstNode child = parent.FirstChild(); !child.IsNull(); child = child.NextSibling()) {
            if (!child.IsElementNode() || child.Name() != name) {
                continue;
            }
            if (name == "QueueUrl") {
                array.AppendValue(child.Value<TString>());
            } else if (name == "Attribute") {
                const TString attrName = child.Node("Name", true).Value<TString>("");
                const TString attrValue = child.Node("Value", true).Value<TString>("");
                if (!attrName.empty()) {
                    NJson::TJsonMap attr;
                    attr["Name"] = attrName;
                    attr["Value"] = attrValue;
                    array.AppendValue(std::move(attr));
                }
            } else if (name == "SendMessageBatchResultEntry"
                    || name == "DeleteMessageBatchResultEntry"
                    || name == "ChangeMessageVisibilityBatchResultEntry") {
                array.AppendValue(XmlElementToJson(child));
            } else if (name == "BatchResultErrorEntry") {
                array.AppendValue(XmlElementToJson(child));
            } else if (name == "Message") {
                array.AppendValue(XmlElementToJson(child));
            } else if (name == "Tag") {
                const TString key = child.Node("Key", true).Value<TString>("");
                const TString value = child.Node("Value", true).Value<TString>("");
                if (!key.empty()) {
                    NJson::TJsonMap tag;
                    tag["Key"] = key;
                    tag["Value"] = value;
                    array.AppendValue(std::move(tag));
                }
            } else {
                array.AppendValue(child.Value<TString>());
            }
        }
        return array;
    }

    void FlattenResultNode(NJson::TJsonMap& result, const NXml::TConstNode& resultNode) {
        THashSet<TString> arrayElementNames = {
            "Attribute",
            "Message",
            "SendMessageBatchResultEntry",
            "DeleteMessageBatchResultEntry",
            "ChangeMessageVisibilityBatchResultEntry",
            "BatchResultErrorEntry",
            "Tag",
        };

        THashMap<TString, ui32> childNameCounts;
        for (NXml::TConstNode child = resultNode.FirstChild(); !child.IsNull(); child = child.NextSibling()) {
            if (!child.IsElementNode()) {
                continue;
            }
            childNameCounts[child.Name()] += 1;
        }

        const bool isListQueuesResult = resultNode.Name() == "ListQueuesResult";
        if (isListQueuesResult) {
            result["QueueUrls"] = CollectQueueUrls(resultNode);
        }

        for (const auto& [name, count] : childNameCounts) {
            if (name == "Attribute") {
                NJson::TJsonMap attrs;
                for (NXml::TConstNode child = resultNode.FirstChild(); !child.IsNull(); child = child.NextSibling()) {
                    if (child.Name() == "Attribute") {
                        attrs[child.Node("Name").Value<TString>()] = child.Node("Value").Value<TString>();
                    }
                }
                result["Attributes"] = attrs;
                continue;
            }
            if (name == "Tag") {
                NJson::TJsonMap tags;
                for (NXml::TConstNode child = resultNode.FirstChild(); !child.IsNull(); child = child.NextSibling()) {
                    if (child.Name() == "Tag") {
                        tags[child.Node("Key").Value<TString>()] = child.Node("Value").Value<TString>();
                    }
                }
                result["Tags"] = tags;
                continue;
            }
            if (name == "QueueUrl") {
                if (isListQueuesResult) {
                    continue;
                }
                if (count > 1) {
                    result["QueueUrls"] = CollectSameNamedChildren(resultNode, name);
                } else {
                    result["QueueUrl"] = resultNode.FirstChild("QueueUrl").Value<TString>();
                }
                continue;
            }
            if (arrayElementNames.contains(name) || count > 1) {
                auto collected = CollectSameNamedChildren(resultNode, name);
                if (name == "Message") {
                    result["Messages"] = collected;
                } else if (name == "SendMessageBatchResultEntry"
                        || name == "DeleteMessageBatchResultEntry"
                        || name == "ChangeMessageVisibilityBatchResultEntry") {
                    result["Successful"] = collected;
                } else if (name == "BatchResultErrorEntry") {
                    result["Failed"] = collected;
                } else if (name == "QueueUrl") {
                    result["QueueUrls"] = collected;
                } else {
                    result[name] = collected;
                }
                continue;
            }

            auto child = resultNode.FirstChild(name);
            NXml::TConstNode grandChild = child.FirstChild();
            if (!grandChild.IsNull() && grandChild.IsElementNode()) {
                result[name] = XmlElementToJson(child);
            } else {
                result[name] = child.Value<TString>("");
            }
        }
    }

} // namespace

TString EncodeSqsXmlRequest(TStringBuf action, const NJson::TJsonMap& request) {
    TString result;
    AppendParam(result, "Action", TString{action});
    for (const auto& [key, value] : request.GetMapSafe()) {
        if (key == "Entries") {
            EncodeBatchEntries(result, action, value);
        } else if (key == "Attributes") {
            EncodeAttributes(result, "Attribute", value);
        } else if (key == "AttributeNames") {
            EncodeAttributeNames(result, value);
        } else if (key == "Tags") {
            EncodeTags(result, value);
        } else if (key == "TagKeys") {
            EncodeTagKeys(result, value);
        } else if (key == "MessageAttributes") {
            EncodeMessageAttributes(result, "MessageAttribute", value);
        } else {
            AppendParam(result, key, value.GetStringRobust());
        }
    }
    return result;
}

NJson::TJsonMap ParseSqsXmlResponse(const TString& body) {
    NXml::TDocument doc(body, NXml::TDocument::String);
    const NXml::TConstNode root = doc.Root();

    NJson::TJsonMap result;
    if (root.Name() == "ErrorResponse") {
        result["__type"] = root.Node("Error/Code").Value<TString>();
        result["message"] = root.Node("Error/Message").Value<TString>();
        return result;
    }

    for (NXml::TConstNode child = root.FirstChild(); !child.IsNull(); child = child.NextSibling()) {
        if (child.Name().EndsWith("Result")) {
            FlattenResultNode(result, child);
        } else if (child.Name() == "ResponseMetadata") {
            result["RequestId"] = child.Node("RequestId", true).Value<TString>("");
        }
    }
    return result;
}
