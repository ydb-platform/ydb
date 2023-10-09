#include "topic_parser.h"

#include <util/folder/path.h>

namespace NPersQueue {

bool CorrectName(const TString& topic) {
    if (!topic.StartsWith("rt3."))
        return false;
    auto pos = topic.find("--");
    if (pos == TString::npos || pos == 4) //dc is empty
        return false;
    pos += 2; //skip "--"
    if (pos == topic.size()) // no real topic
        return false;
    auto pos2 = topic.find("--", pos);
    if (pos2 == TString::npos)
        return true;
    if (pos2 == pos || pos2 + 2 == topic.size()) //producer or topic is empty
        return false;
    return true;
}

TString GetDC(const TString& topic) {
    if (!CorrectName(topic))
        return "unknown";
    auto pos = topic.find("--");
    Y_ABORT_UNLESS(pos != TString::npos);
    Y_ABORT_UNLESS(pos > 4); //length of "rt3."
    auto res = topic.substr(4, pos - 4);
    return res;
}

TString GetRealTopic(const TString& topic) {
    if (!CorrectName(topic))
        return topic;
    auto pos = topic.find("--");
    Y_ABORT_UNLESS(pos != TString::npos);
    Y_ABORT_UNLESS(topic.size() > pos + 2);
    return topic.substr(pos + 2);
}

TString GetTopicPath(const TString& topic) {
    return ConvertOldTopicName(GetRealTopic(topic));
}

TString GetAccount(const TString& topic) {
    auto res = GetTopicPath(topic);
    return res.substr(0, res.find("/"));
}

TString GetProducer(const TString& topic) {
    if (!CorrectName(topic))
        return "unknown";
    auto res = GetRealTopic(topic);
    return res.substr(0, res.find("--"));
}

TString ConvertNewTopicName(const TString& topic) {
    TString t = NormalizePath(topic);
    auto pos = t.rfind("/");
    if (pos == TString::npos)
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


TString ConvertOldTopicName(const TString& topic) {
    auto pos = topic.rfind("--");
    if (pos == TString::npos)
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

TString BuildFullTopicName(const TString& topicPath, const TString& topicDC) {
    return "rt3." + topicDC + "--" + ConvertNewTopicName(topicPath);
}

TString ConvertOldProducerName(const TString& producer) {
    TStringBuilder res;
    for (ui32 i = 0; i < producer.size(); ++i) {
        if (producer[i] == '@') res << "/";
        else res << producer[i];
    }
    return res;
}


TString NormalizePath(const TString& path) {
    size_t st = 0;
    size_t end = path.size();
    if (path.StartsWith("/")) st = 1;
    if (path.EndsWith("/") && end > st) end--;
    return path.substr(st, end - st);
}


TString ConvertNewConsumerName(const TString& consumer) {
    TStringBuilder res;
    ui32 pos = 0;
    TString c = NormalizePath(consumer);
    if (c.StartsWith("shared/"))
        pos = 7;
    for (ui32 i = pos; i < c.size(); ++i) {
        if (c[i] == '/') res  << "@";
        else res << c[i];
    }
    return res;
}

TString ConvertNewProducerName(const TString& producer) {
    TStringBuilder res;
    for (ui32 i = 0; i < producer.size(); ++i) {
        if (producer[i] == '/') res  << "@";
        else res << producer[i];
    }
    return res;
}


TString ConvertOldConsumerName(const TString& consumer) {
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


} // namespace NPersQueue
