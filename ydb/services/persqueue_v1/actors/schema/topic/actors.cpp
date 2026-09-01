#include "actors.h"

namespace NKikimr::NGRpcProxy::V1::NTopic {

namespace {

TString ResolveDlqPath(TStringBuf path, const TMaybe<TString>& database) {
    return path.StartsWith("sqs://")
        ? TString(path)
        : NGRpcService::ResolvePathToDatabase(database, path);
}

void ResolveConsumerPaths(Ydb::Topic::Consumer& consumer, const TMaybe<TString>& database) {
    if (!consumer.has_shared_consumer_type()) {
        return;
    }

    auto* type = consumer.mutable_shared_consumer_type();
    if (!type->has_dead_letter_policy()) {
        return;
    }

    auto* policy = type->mutable_dead_letter_policy();
    if (policy->has_move_action()) {
        auto* action = policy->mutable_move_action();
        action->set_dead_letter_queue(ResolveDlqPath(action->dead_letter_queue(), database));
    }
}

void ResolveConsumerPaths(Ydb::Topic::AlterConsumer& consumer, const TMaybe<TString>& database) {
    if (!consumer.has_alter_shared_consumer_type()) {
        return;
    }

    auto* type = consumer.mutable_alter_shared_consumer_type();
    if (!type->has_alter_dead_letter_policy()) {
        return;
    }

    auto* policy = type->mutable_alter_dead_letter_policy();
    if (policy->has_alter_move_action() && policy->alter_move_action().has_set_dead_letter_queue()) {
        auto* action = policy->mutable_alter_move_action();
        action->set_set_dead_letter_queue(ResolveDlqPath(action->set_dead_letter_queue(), database));
    } else if (policy->has_set_move_action()) {
        auto* action = policy->mutable_set_move_action();
        action->set_dead_letter_queue(ResolveDlqPath(action->dead_letter_queue(), database));
    }
}

} // namespace

void ResolveTopicRequestPaths(Ydb::Topic::CreateTopicRequest& request, const TMaybe<TString>& database) {
    request.set_path(NGRpcService::ResolvePathToDatabase(database, request.path()));
    for (auto& consumer : *request.mutable_consumers()) {
        ResolveConsumerPaths(consumer, database);
    }
}

void ResolveTopicRequestPaths(Ydb::Topic::AlterTopicRequest& request, const TMaybe<TString>& database) {
    request.set_path(NGRpcService::ResolvePathToDatabase(database, request.path()));
    for (auto& consumer : *request.mutable_add_consumers()) {
        ResolveConsumerPaths(consumer, database);
    }
    for (auto& consumer : *request.mutable_alter_consumers()) {
        ResolveConsumerPaths(consumer, database);
    }
}

} // namespace NKikimr::NGRpcProxy::V1::NTopic
