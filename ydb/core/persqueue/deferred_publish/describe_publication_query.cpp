#include "describe_publication_query.h"

#include "events.h"
#include "query_utils.h"
#include "tables_creator.h"

#include <ydb/library/query_actor/query_actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

namespace {

class TDescribePublicationQuery : public TQueryBase {
public:
    TDescribePublicationQuery(
        const NActors::TActorId& replyTo,
        const TString& database,
        ui64 intPublicationId,
        const TString& callerSid)
        : TQueryBase(NKikimrServices::PERSQUEUE, {}, database, /* isSystemUser */ true)
        , ReplyTo(replyTo)
        , IntPublicationId(intPublicationId)
        , CallerSid(callerSid)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            -- TDescribePublicationQuery::OnRunQuery
            DECLARE $int_publication_id AS Uint64;

            SELECT
                p.`ext_publication_id` AS ext_publication_id,
                p.`writer_identity` AS writer_identity,
                p.`created_at` AS created_at,
                p.`created_by` AS created_by,
                d.`path` AS destination_path
            FROM `)" << PublicationsTablePath() << R"(` AS p
            LEFT JOIN `)" << DestinationsTablePath() << R"(` AS d
                ON p.`int_publication_id` = d.`int_publication_id`
            WHERE p.`int_publication_id` = $int_publication_id;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$int_publication_id")
                .Uint64(IntPublicationId)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        if (ResultSets.empty() || ResultSets[0].RowsCount() == 0) {
            Finish(Ydb::StatusIds::NOT_FOUND, "Publication not found");
            return;
        }

        TDescribePublicationData publication;
        THashSet<TString> seenPaths;

        NYdb::TResultSetParser parser(ResultSets[0]);
        while (parser.TryNextRow()) {
            if (publication.ExtPublicationId.empty()) {
                publication.ExtPublicationId = parser.ColumnParser("ext_publication_id").GetUtf8();
                if (const auto writerIdentity = parser.ColumnParser("writer_identity").GetOptionalUtf8()) {
                    publication.WriterIdentity = TString(*writerIdentity);
                }
                publication.CreatedAt = parser.ColumnParser("created_at").GetTimestamp();
                if (const auto createdBy = parser.ColumnParser("created_by").GetOptionalUtf8()) {
                    publication.CreatedBy = TString(*createdBy);
                }
            }

            const auto destinationPath = parser.ColumnParser("destination_path").GetOptionalUtf8();
            if (!destinationPath) {
                continue;
            }

            const TString path(*destinationPath);
            if (seenPaths.contains(path)) {
                continue;
            }
            seenPaths.insert(path);

            TDescribeDestination destination;
            destination.TopicPath = path;
            publication.Destinations.push_back(std::move(destination));
        }

        Publication = std::move(publication);
        if (!IsPublicationOwnedByCaller(CallerSid, Publication->CreatedBy)) {
            Publication = Nothing();
            Finish(Ydb::StatusIds::UNAUTHORIZED, "Access denied");
            return;
        }
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (IsRegistryTableMissing(status)) {
            status = Ydb::StatusIds::NOT_FOUND;
            issues.Clear();
            issues.AddIssue("Publication not found");
            Publication = Nothing();
        }

        Send(Owner, [&] {
            auto* event = new TEvDescribePublicationResponse;
            event->Status = status;
            event->Issues = std::move(issues);
            event->Publication = Publication;
            return event;
        }());
    }

private:
    const NActors::TActorId ReplyTo;
    const ui64 IntPublicationId;
    const TString CallerSid;
    TMaybe<TDescribePublicationData> Publication;
};

} // namespace

NActors::IActor* CreateDescribePublicationQueryActor(
    const NActors::TActorId& replyTo,
    const TString& database,
    ui64 intPublicationId,
    const TString& callerSid)
{
    return new TDescribePublicationQuery(replyTo, database, intPublicationId, callerSid);
}

} // namespace NKikimr::NPQ::NDeferredPublish
