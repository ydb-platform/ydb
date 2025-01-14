#pragma once
#include <library/cpp/json/json_writer.h>
#include <ydb/core/ymq/proto/records.pb.h>

#include <util/generic/strbuf.h>

namespace NKikimr::NSQS {

// Validation of deduplication id, group id, receive request attempt id
bool IsAlphaNumAndPunctuation(TStringBuf str);

// https://docs.aws.amazon.com/en_us/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-attributes.html
bool ValidateMessageAttributeName(TStringBuf str, bool& hasYandexPrefix, bool allowYandexPrefix = false);

bool ValidateQueueNameOrUserName(TStringBuf name);

// Validation function for message body or string message attributes
bool ValidateMessageBody(TStringBuf body, TString& errorDescription);

TString TagsToJson(NJson::TJsonMap tags);

class TTagValidator {
public:
    TTagValidator(const TMaybe<NJson::TJsonMap>& currentTags, const NJson::TJsonMap& newTags);
    bool Validate() const;
    TString GetJson() const;
    TString GetError() const;
private:
    void PrepareJson();
    bool ValidateString(const TString& str, const bool key);
    TString Error;
    TString Json;
private:
    const TMaybe<NJson::TJsonMap>& CurrentTags;
    const NJson::TJsonMap& NewTags;
};

TString EncodeReceiptHandle(const TReceipt& receipt);
TReceipt DecodeReceiptHandle(const TString& receipt);

} // namespace NKikimr::NSQS
