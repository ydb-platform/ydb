#pragma once

#include "attributes_md5.h"

#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/protos/sqs.pb.h>
#include <ydb/public/api/protos/draft/ymq.pb.h>

#include <util/generic/string.h>

#include <unordered_map>

namespace NKikimr::NSQS {

inline bool DeserializeUserAttributes(Ydb::Ymq::V1::Message& message, std::unordered_multimap<TString, TString>& attributes) {
    if (auto it = attributes.find(TString{NPQ::MESSAGE_ATTRIBUTE_ATTRIBUTES}); it != attributes.end()) {
        NKikimr::NSQS::TMessageAttributes messageAttributes;
        if (messageAttributes.ParseFromString(it->second)) {
            message.set_m_d_5_of_message_attributes(NSQS::CalcMD5OfMessageAttributes(messageAttributes.attributes()));

            auto* mma = message.mutable_message_attributes();
            for (auto&& attribute : messageAttributes.attributes()) {
                Ydb::Ymq::V1::MessageAttribute value;
                if (attribute.has_binaryvalue()) {
                    value.set_binary_value(std::move(attribute.binaryvalue()));
                } else if (attribute.has_stringvalue()) {
                    value.set_string_value(std::move(attribute.stringvalue()));
                } else {
                    continue;
                }
                value.set_data_type(std::move(attribute.datatype()));
                mma->emplace(std::move(attribute.name()), std::move(value));
            }

            return true;
        } else {
            return false;
        }
    }

    return true;
}

// Ydb::Ymq::V1::Message or Ydb::Ymq::V1::SendMessageRequest
inline std::pair<std::unordered_multimap<TString, TString>, TString> SerializeUserAttributes(const auto& message) {
    std::unordered_multimap<TString, TString> attributes;
    TString md5;

    if (message.message_attributes_size()) {
        NKikimr::NSQS::TMessageAttributes messageAttributes;
        for (const auto& [attrName, attrValue] : message.message_attributes()) {
            auto* dstAttribute = messageAttributes.add_attributes();
            dstAttribute->SetName(attrName);
            if (const auto& value = attrValue.string_value()) {
                dstAttribute->SetStringValue(value);
            } else if (const auto& value = attrValue.binary_value()) {
                dstAttribute->SetBinaryValue(value);
            }
            dstAttribute->SetDataType(attrValue.data_type());
        }

        TString serialized;
        bool res = messageAttributes.SerializeToString(&serialized);
        Y_ABORT_UNLESS(res);

        attributes.emplace(TString{NPQ::MESSAGE_ATTRIBUTE_ATTRIBUTES}, std::move(serialized));
        md5 = NSQS::CalcMD5OfMessageAttributes(messageAttributes.attributes());
    }

    return {std::move(attributes), std::move(md5)};
}

} // namespace NKikimr::NSQS
