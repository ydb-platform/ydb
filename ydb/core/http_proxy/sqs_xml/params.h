#pragma once

//#include <ydb/core/protos/sqs.pb.h>

#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimr::NHttpProxy::NSQS {

struct TParameters {
    TMaybe<TString> Action;
    TMaybe<TString> Clear;
    TMaybe<ui64> DelaySeconds;
    TMaybe<TString> FolderId;
    TMaybe<TString> Id;
    TMaybe<ui32> MaxNumberOfMessages;
    TMaybe<TString> MessageBody;
    TMaybe<TString> MessageDeduplicationId;
    TMaybe<TString> MessageGroupId;
    TMaybe<TString> Path;
    TMaybe<TString> QueueName;
    TMaybe<TString> QueueNamePrefix;
    TMaybe<TString> QueueUrl;
    TMaybe<TString> ReceiptHandle;
    TMaybe<TString> ReceiveRequestAttemptId;
    TMaybe<TString> Subject;
    TMaybe<TString> UserName;
    TMaybe<TString> UserNamePrefix;
    TMaybe<TString> Version;
    TMaybe<ui64> VisibilityTimeout;
    TMaybe<ui64> WaitTimeSeconds;
    TMaybe<ui64> CreateTimestampSeconds;
    TMaybe<TString> CustomQueueName;


    TMap<int, TString> AttributeNames;
    // <name, value>
    TMap<int, std::pair<TString, TString>> Attributes;
    // <name, dataType, value>
    TMap<int, std::tuple<TString, TString, TString>> MessageAttributes;
    TMap<int, TParameters> BatchEntries;
    // <key, value>
    TMap<int, std::pair<TString, TString>> Tags;
    TMap<int, TString> TagKeys;
};

class TParametersParser {
public:
     TParametersParser(TParameters* params);
    ~TParametersParser();

    // Throws TSQSException
    void Append(const TString& name, const TStringBuf value);

private:
    TParameters* const Params_;
    TParameters* CurrentParams_;
    int Id_;
    int Num_;
};

} // namespace NKikimr::NHttpProxy::NSQS
