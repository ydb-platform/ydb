#include "topic_parser.h"

#include <util/string/builder.h>

#include <util/folder/path.h>

namespace NPersQueue {
inline namespace Dev {

bool CorrectName(const std::string& topic) {
    if (!std::string_view{topic}.starts_with("rt3."))
        return false;
    auto pos = topic.find("--");
    if (pos == std::string::npos || pos == 4) //dc is empty
        return false;
    pos += 2; //skip "--"
    if (pos == topic.size()) // no real topic
        return false;
    auto pos2 = topic.find("--", pos);
    if (pos2 == std::string::npos)
        return true;
    if (pos2 == pos || pos2 + 2 == topic.size()) //producer or topic is empty
        return false;
    return true;
}

std::string GetDC(const std::string& topic) {
    if (!CorrectName(topic))
        return "unknown";
    auto pos = topic.find("--");
    Y_ABORT_UNLESS(pos != std::string::npos);
    Y_ABORT_UNLESS(pos > 4); //length of "rt3."
    auto res = topic.substr(4, pos - 4);
    return res;
}

std::string GetRealTopic(const std::string& topic) {
    if (!CorrectName(topic))
        return topic;
    auto pos = topic.find("--");
    Y_ABORT_UNLESS(pos != std::string::npos);
    Y_ABORT_UNLESS(topic.size() > pos + 2);
    return topic.substr(pos + 2);
}

std::string GetTopicPath(const std::string& topic) {
    return ConvertOldTopicName(GetRealTopic(topic));
}

std::string GetAccount(const std::string& topic) {
    auto res = GetTopicPath(topic);
    return res.substr(0, res.find("/"));
}

std::string GetProducer(const std::string& topic) {
    if (!CorrectName(topic))
        return "unknown";
    auto res = GetRealTopic(topic);
    return res.substr(0, res.find("--"));
}

std::string ConvertNewTopicName(const std::string& topic) {
    std::string t = NormalizePath(topic);
    auto pos = t.rfind("/");
    if (pos == std::string::npos)
        return t;
    TStringBuilder res;
    for (ui32 i = 0; i < pos; ++i) {
        if (t[i] == '/') res << '@';
        else res << t[i];
    }
    res << "--";
    res << t.substr(pos + 1);
    return res;
}


std::string ConvertOldTopicName(const std::string& topic) {
    auto pos = topic.rfind("--");
    if (pos == std::string::npos)
        return topic;
    TStringBuilder res;
    for (ui32 i = 0; i < pos; ++i) {
        if (topic[i] == '@') res << '/';
        else res << topic[i];
    }
    res << "/";
    res << topic.substr(pos + 2);
    return res;
}

std::string BuildFullTopicName(const std::string& topicPath, const std::string& topicDC) {
    return "rt3." + topicDC + "--" + ConvertNewTopicName(topicPath);
}

std::string ConvertOldProducerName(const std::string& producer) {
    TStringBuilder res;
    for (ui32 i = 0; i < producer.size(); ++i) {
        if (producer[i] == '@') res << "/";
        else res << producer[i];
    }
    return res;
}


std::string NormalizePath(const std::string& path) {
    size_t st = 0;
    size_t end = path.size();
    if (std::string_view{path}.starts_with("/")) st = 1;
    if (std::string_view{path}.ends_with("/") && end > st) end--;
    return path.substr(st, end - st);
}


std::string ConvertNewConsumerName(const std::string& consumer) {
    TStringBuilder res;
    ui32 pos = 0;
    std::string c = NormalizePath(consumer);
    if (std::string_view{c}.starts_with("shared/"))
        pos = 7;
    for (ui32 i = pos; i < c.size(); ++i) {
        if (c[i] == '/') res  << "@";
        else res << c[i];
    }
    return res;
}

std::string ConvertNewProducerName(const std::string& producer) {
    TStringBuilder res;
    for (ui32 i = 0; i < producer.size(); ++i) {
        if (producer[i] == '/') res  << "@";
        else res << producer[i];
    }
    return res;
}


std::string ConvertOldConsumerName(const std::string& consumer) {
    TStringBuilder res;
    bool shared = true;
    for (ui32 i = 0; i < consumer.size(); ++i) {
        if (consumer[i] == '@') {
            res << "/";
            shared = false;
        } else {
            res << consumer[i];
        }
    }
    if (shared)
        return TStringBuilder() << "shared/" << res;
    return res;
}


}
}
