#pragma once

#include <string>

namespace NPersQueue {
inline namespace Dev {

std::string GetDC(const std::string& topic);

std::string GetRealTopic(const std::string& topic);

std::string BuildFullTopicName(const std::string& topicPath, const std::string& topicDC);

std::string GetProducer(const std::string& topic);
std::string GetAccount(const std::string& topic);
std::string GetTopicPath(const std::string& topic);

std::string NormalizePath(const std::string& path);

bool CorrectName(const std::string& topic);

std::string ConvertNewTopicName(const std::string& topic);

std::string ConvertNewConsumerName(const std::string& consumer);
std::string ConvertNewProducerName(const std::string& consumer);


std::string ConvertOldTopicName(const std::string& topic);
std::string ConvertOldProducerName(const std::string& producer);
std::string ConvertOldConsumerName(const std::string& consumer);


}
}
