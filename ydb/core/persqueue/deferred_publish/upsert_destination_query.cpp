#include "upsert_destination_query.h"

#include "events.h"
#include "query_utils.h"
#include "tables_creator.h"

#include <ydb/library/query_actor/query_actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

namespace {

class TUpsertDestinationQuery : public TQueryBase {
public:
    TUpsertDestinationQuery(
        const NActors::TActorId& replyTo,
        const TString& database,
        ui64 intPublicationId,
        const TString& path,
        const TString& destinationBlob)
        : TQueryBase(NKikimrServices::PERSQUEUE, {}, database, /* isSystemUser */ true)
        , ReplyTo(replyTo)
        , IntPublicationId(intPublicationId)
        , Path(path)
        , DestinationBlob(destinationBlob)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            -- TUpsertDestinationQuery::OnRunQuery
            DECLARE $int_publication_id AS Uint64;
            DECLARE $path AS Text;
            DECLARE $destination_blob AS String;

            UPSERT INTO `)" << DestinationsTablePath() << R"(` (
                `int_publication_id`,
                `path`,
                `destination_blob`
            )
            SELECT
                $int_publication_id,
                $path,
                $destination_blob
            FROM `)" << PublicationsTablePath() << R"(`
            WHERE `int_publication_id` = $int_publication_id
            RETURNING `int_publication_id`;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$int_publication_id")
                .Uint64(IntPublicationId)
                .Build()
            .AddParam("$path")
                .Utf8(Path)
                .Build()
            .AddParam("$destination_blob")
                .String(DestinationBlob)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        if (ResultSets.empty() || ResultSets[0].RowsCount() == 0) {
            Finish(Ydb::StatusIds::NOT_FOUND, "Publication not found");
            return;
        }
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (IsRegistryTableMissing(status)) {
            status = Ydb::StatusIds::NOT_FOUND;
            issues.Clear();
            issues.AddIssue("Publication not found");
        }

        Send(Owner, [&] {
            auto* event = new TEvUpsertDestinationResponse;
            event->Status = status;
            event->Issues = std::move(issues);
            return event;
        }());
    }

private:
    const NActors::TActorId ReplyTo;
    const ui64 IntPublicationId;
    const TString Path;
    const TString DestinationBlob;
};

} // namespace

NActors::IActor* CreateUpsertDestinationQueryActor(
    const NActors::TActorId& replyTo,
    const TString& database,
    ui64 intPublicationId,
    const TString& path,
    const TString& destinationBlob)
{
    return new TUpsertDestinationQuery(replyTo, database, intPublicationId, path, destinationBlob);
}

} // namespace NKikimr::NPQ::NDeferredPublish
