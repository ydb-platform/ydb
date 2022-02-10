#pragma once

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NPersQueue {

TString GetDC(const TString& topic);

TString GetRealTopic(const TString& topic);

TString BuildFullTopicName(const TString& topicPath, const TString& topicDC);

TString GetProducer(const TString& topic);
TString GetAccount(const TString& topic);
TString GetTopicPath(const TString& topic);

TString NormalizePath(const TString& path);

bool CorrectName(const TString& topic);

TString ConvertNewTopicName(const TString& topic);

TString ConvertNewConsumerName(const TString& consumer);
TString ConvertNewProducerName(const TString& consumer);


TString ConvertOldTopicName(const TString& topic);
TString ConvertOldProducerName(const TString& producer);
TString ConvertOldConsumerName(const TString& consumer);


} // namespace NPersQueue
