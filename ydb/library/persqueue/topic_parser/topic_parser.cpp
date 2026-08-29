#include "topic_parser.h"

#include <ydb/core/base/appdata.h>

#include <util/folder/path.h>
#include <util/string/builder.h>

namespace NPersQueue {

namespace {
    TString FullPath(const TMaybe<TString>& database, const TString& path) {
        if (database.Defined() && !path.StartsWith(*database) && !path.Contains('\0')) {
            try {
                return (TFsPath(*database) / path).GetPath();
            } catch(...) {
                return path;
            }
        } else {
            return path;
        }
    }
}

TString StripLeadSlash(const TString& path) {
    if (!path.StartsWith("/")) {
        return path;
    } else {
        return path.substr(1);
    }
}

TString NormalizeFullPath(const TString& fullPath) {
    if (!fullPath.empty() && !fullPath.StartsWith("/")) {
        return TString("/") + fullPath;
    } else {
        return fullPath;
    }
}

TString GetFullTopicPath(const TMaybe<TString>& database, const TString& topicPath) {
    return FullPath(database, topicPath);
}

TString ConvertNewConsumerName(const TString& consumer, const NKikimrPQ::TPQConfig& pqConfig) {
    if (pqConfig.GetTopicsAreFirstClassCitizen()) {
        return consumer;
    } else {
        return ConvertNewConsumerName(consumer);
    }
}

TString ConvertNewConsumerName(const TString& consumer, const NActors::TActorContext& ctx) {
    return ConvertNewConsumerName(consumer, NKikimr::AppData(ctx)->PQConfig);
}

TString ConvertOldConsumerName(const TString& consumer, const NKikimrPQ::TPQConfig& pqConfig) {
    if (pqConfig.GetTopicsAreFirstClassCitizen()) {
        return consumer;
    } else {
        return ConvertOldConsumerName(consumer);
    }
}

TString ConvertOldConsumerName(const TString& consumer, const NActors::TActorContext& ctx) {
    return ConvertOldConsumerName(consumer, NKikimr::AppData(ctx)->PQConfig);
}


TString MakeConsumerPath(const TString& consumer) {
    TStringBuilder res;
    res.reserve(consumer.size());
    for (ui32 i = 0; i < consumer.size(); ++i) {
        if (consumer[i] == '@') {
            res << "/";
        } else {
            res << consumer[i];
        }
    }
    if (res.find("/") == TString::npos) {
        return TStringBuilder() << "shared/" << res;
    }
    return res;
}

} // namespace NPersQueue
