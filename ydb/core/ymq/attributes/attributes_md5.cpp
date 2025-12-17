#include "attributes_md5.h"

#include <library/cpp/digest/md5/md5.h>

#include <util/network/init.h>

#include <cstdint>
#include <vector>

//
// About attributes MD5 calculation.
// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-attributes.html#sqs-attributes-md5-message-digest-calculation
//

namespace NKikimr::NSQS {

static const std::uint8_t STRING_TRANSPORT_TYPE_CODE = 1;
static const std::uint8_t BINARY_TRANSPORT_TYPE_CODE = 2;

static std::uint32_t ToBigEndian(std::uint32_t x) {
    return htonl(x);
}

static void Update(MD5& md5, const TMessageAttribute& attr) {
    const std::uint32_t nameLen = ToBigEndian(static_cast<std::uint32_t>(attr.GetName().size()));
    const std::uint32_t dataTypeLen = ToBigEndian(static_cast<std::uint32_t>(attr.GetDataType().size()));
    md5
        .Update(&nameLen, sizeof(nameLen)).Update(attr.GetName())
        .Update(&dataTypeLen, sizeof(dataTypeLen)).Update(attr.GetDataType());

    if (attr.HasStringValue()) {
        const std::uint32_t valueLen = ToBigEndian(static_cast<std::uint32_t>(attr.GetStringValue().size()));
        md5
            .Update(&STRING_TRANSPORT_TYPE_CODE, sizeof(std::uint8_t))
            .Update(&valueLen, sizeof(valueLen)).Update(attr.GetStringValue());
    } else {
        const std::uint32_t valueLen = ToBigEndian(static_cast<std::uint32_t>(attr.GetBinaryValue().size()));
        md5
            .Update(&BINARY_TRANSPORT_TYPE_CODE, sizeof(std::uint8_t))
            .Update(&valueLen, sizeof(valueLen)).Update(attr.GetBinaryValue());
    }
}

TString CalcMD5OfMessageAttributes(const google::protobuf::RepeatedPtrField<TMessageAttribute>& attributes) {
    std::vector<const TMessageAttribute*> sortedAttrs(attributes.size());
    for (size_t i = 0; i < sortedAttrs.size(); ++i) {
        sortedAttrs[i] = &attributes.Get(i);
    }
    std::sort(
              sortedAttrs.begin(),
              sortedAttrs.end(),
              [](const TMessageAttribute* a1, const TMessageAttribute* a2) {
                  return a1->GetName() < a2->GetName();
              }
    );
    MD5 md5;
    for (const auto* attr : sortedAttrs) {
        Update(md5, *attr);
    }
    char res[33] = {'\0'};
    md5.End(res);
    return res;
}

} // namespace NKikimr::NSQS
