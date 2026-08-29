#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/string/builder.h>
#include <ydb/core/base/path.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <ydb/public/sdk/cpp/src/library/persqueue/topic_parser_public/topic_parser.h>


namespace NPersQueue {

TString GetFullTopicPath(const TMaybe<TString>& database, const TString& topicPath);
TString ConvertNewConsumerName(const TString& consumer, const NKikimrPQ::TPQConfig& pqConfig);
TString ConvertNewConsumerName(const TString& consumer, const NActors::TActorContext& ctx);
TString ConvertOldConsumerName(const TString& consumer, const NKikimrPQ::TPQConfig& pqConfig);
TString ConvertOldConsumerName(const TString& consumer, const NActors::TActorContext& ctx);
TString MakeConsumerPath(const TString& consumer);

TString NormalizeFullPath(const TString& fullPath);
TString StripLeadSlash(const TString& path);

} // namespace NPersQueue
