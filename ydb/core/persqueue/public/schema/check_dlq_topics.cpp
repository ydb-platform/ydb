#include "check_dlq_topics.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/containers/absl/flat_hash_set.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/join.h>

#include <algorithm>
#include <optional>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PQ_SCHEMA

#define LOG_PREFIX NActors::TlsActivationContext->AsActorContext().SelfID

namespace NKikimr::NPQ::NSchema {

namespace {

constexpr TStringBuf SqsDlqPrefix = "sqs://";

TString GetSchemeDlqTopicPath(const NKikimrPQ::TPQTabletConfig_TConsumer& consumer) {
    if (consumer.GetType() != NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP) {
        return {};
    }
    if (consumer.GetDeadLetterPolicy() != NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE
        || !consumer.GetDeadLetterPolicyEnabled())
    {
        return {};
    }

    const auto& dlq = consumer.GetDeadLetterQueue();
    if (dlq.empty() || dlq.StartsWith(SqsDlqPrefix)) {
        return {};
    }
    return dlq;
}

TString NormalizeDlqTopicPath(const TString& dlq, const TString& database, bool topicsAreFirstClassCitizen) {
    if (topicsAreFirstClassCitizen) {
        return dlq.StartsWith('/')
            ? CanonizePath(dlq)
            : ResolvePathToDatabase(CanonizePath(database), dlq);
    }
    return NormalizePath(CanonizePath(database), CanonizePath(dlq));
}

bool IsCdcDlqTarget(const NDescriber::TTopicInfo& info) {
    if (info.CdcStream) {
        return true;
    }
    return info.Self && info.Self->Info.GetPathSubType() == NKikimrSchemeOp::EPathSubTypeStreamImpl;
}

TString AccessDeniedMessage(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TString& path) {
    const TString sid = userToken ? TString(userToken->GetUserSID()) : TString("anonymous");
    return TStringBuilder()
        << "Access denied for " << sid
        << " on path " << path
        << " with any of access rights AlterSchema or UpdateRow";
}

class TCheckDlqTopicsActor : public TActorBootstrapped<TCheckDlqTopicsActor> {
public:
    TCheckDlqTopicsActor(
        const TActorId& parent,
        const TString& databasePath,
        absl::flat_hash_set<TString>&& dlqPaths,
        const TCheckDlqTopicsSettings& settings
    )
        : Parent(parent)
        , DatabasePath(databasePath)
        , DlqPaths(std::move(dlqPaths))
        , Settings(settings)
    {
    }

    void Bootstrap() {
        Become(&TCheckDlqTopicsActor::StateWork);

        if (DlqPaths.empty()) {
            return ReplyAndDie(Ydb::StatusIds::SUCCESS, {});
        }

        YDB_LOG_DEBUG("Check DLQ topics",
            {"logPrefix", LOG_PREFIX},
            {"database", DatabasePath},
            {"paths", JoinRange(", ", DlqPaths.begin(), DlqPaths.end())});

        Register(NDescriber::CreateDescriberActor(
            SelfId(),
            DatabasePath,
            std::move(DlqPaths),
            NDescriber::TDescribeSettings{
                .UserToken = Settings.UserToken,
                .AccessRights = NDescriber::TAccessRights(
                    NACLib::EAccessRights::AlterSchema,
                    NACLib::EAccessRights::UpdateRow
                ),
                .ForceSyncVersion = true,
            }
        ));
    }

    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
        TVector<TString> paths;
        paths.reserve(ev->Get()->Topics.size());
        for (const auto& [path, _] : ev->Get()->Topics) {
            paths.push_back(path);
        }
        std::sort(paths.begin(), paths.end());

        for (const auto& path : paths) {
            const auto& info = ev->Get()->Topics.at(path);
            if (auto error = MapStatus(path, info); error) {
                YDB_LOG_DEBUG("DLQ check failed",
                    {"logPrefix", LOG_PREFIX},
                    {"path", path},
                    {"status", error->first},
                    {"errorMessage", error->second});
                return ReplyAndDie(error->first, std::move(error->second));
            }
        }
        return ReplyAndDie(Ydb::StatusIds::SUCCESS, {});
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NDescriber::TEvDescribeTopicsResponse, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    using TStatusAndMessage = std::pair<Ydb::StatusIds::StatusCode, TString>;

    std::optional<TStatusAndMessage> MapStatus(const TString& path, const NDescriber::TTopicInfo& info) const {
        switch (info.Status) {
            case NDescriber::EStatus::SUCCESS:
                if (IsCdcDlqTarget(info)) {
                    return TStatusAndMessage{
                        Ydb::StatusIds::BAD_REQUEST,
                        TStringBuilder() << "CDC stream cannot be used as a dead letter queue: " << path
                    };
                }
                return std::nullopt;
            case NDescriber::EStatus::NOT_TOPIC:
                return TStatusAndMessage{
                    Ydb::StatusIds::BAD_REQUEST,
                    TStringBuilder() << "Dead letter queue path must be a topic, got " << path
                };
            case NDescriber::EStatus::NOT_FOUND:
                return TStatusAndMessage{
                    Ydb::StatusIds::SCHEME_ERROR,
                    TStringBuilder() << "Path `" << path << "` does not exist"
                };
            case NDescriber::EStatus::UNAUTHORIZED:
            case NDescriber::EStatus::UNAUTHORIZED_WITH_DESCRIBE_ACCESS:
                return TStatusAndMessage{
                    Ydb::StatusIds::UNAUTHORIZED,
                    AccessDeniedMessage(Settings.UserToken, path)
                };
            case NDescriber::EStatus::BAD_REQUEST:
                return TStatusAndMessage{
                    Ydb::StatusIds::BAD_REQUEST,
                    NDescriber::Description(path, info.Status)
                };
            case NDescriber::EStatus::UNKNOWN_ERROR:
                return TStatusAndMessage{
                    Ydb::StatusIds::INTERNAL_ERROR,
                    NDescriber::Description(path, info.Status)
                };
        }
        return TStatusAndMessage{
            Ydb::StatusIds::INTERNAL_ERROR,
            TStringBuilder() << "Unexpected describer status for path " << path
        };
    }

    void ReplyAndDie(Ydb::StatusIds::StatusCode status, TString&& errorMessage) {
        Send(Parent, new TEvCheckDlqTopicsResponse(status, std::move(errorMessage)));
        PassAway();
    }

private:
    const TActorId Parent;
    const TString DatabasePath;
    absl::flat_hash_set<TString> DlqPaths;
    const TCheckDlqTopicsSettings Settings;
};

} // namespace

absl::flat_hash_set<TString> CollectDlqTopicPaths(
    const NKikimrPQ::TPQTabletConfig& config,
    const TString& database,
    bool topicsAreFirstClassCitizen
) {
    absl::flat_hash_set<TString> result;
    for (const auto& consumer : config.GetConsumers()) {
        const auto dlq = GetSchemeDlqTopicPath(consumer);
        if (!dlq.empty()) {
            result.insert(NormalizeDlqTopicPath(dlq, database, topicsAreFirstClassCitizen));
        }
    }
    return result;
}

absl::flat_hash_set<TString> CollectNewDlqTopicPaths(
    const NKikimrPQ::TPQTabletConfig& newConfig,
    const NKikimrPQ::TPQTabletConfig& oldConfig,
    const TString& database,
    bool topicsAreFirstClassCitizen
) {
    auto result = CollectDlqTopicPaths(newConfig, database, topicsAreFirstClassCitizen);
    for (const auto& path : CollectDlqTopicPaths(oldConfig, database, topicsAreFirstClassCitizen)) {
        result.erase(path);
    }
    return result;
}

IActor* CreateCheckDlqTopicsActorIfNeeded(
    const TActorId& parent,
    const TString& databasePath,
    const NKikimrPQ::TPQTabletConfig& newConfig,
    const NKikimrPQ::TPQTabletConfig& oldConfig,
    const TCheckDlqTopicsSettings& settings
) {
    auto dlqPaths = CollectNewDlqTopicPaths(
        newConfig, oldConfig, databasePath, settings.TopicsAreFirstClassCitizen);
    if (dlqPaths.empty()) {
        return nullptr;
    }
    return new TCheckDlqTopicsActor(parent, databasePath, std::move(dlqPaths), settings);
}

} // namespace NKikimr::NPQ::NSchema
