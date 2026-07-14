#include "get_destination_blob_query.h"

#include "events.h"
#include "query_utils.h"
#include "tables_creator.h"

#include <ydb/library/query_actor/query_actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

namespace {

class TGetDestinationBlobQuery : public TQueryBase {
public:
    TGetDestinationBlobQuery(
        const NActors::TActorId& replyTo,
        const TString& database,
        ui64 intPublicationId,
        const TString& path)
        : TQueryBase(NKikimrServices::PERSQUEUE, {}, database, /* isSystemUser */ true)
        , ReplyTo(replyTo)
        , IntPublicationId(intPublicationId)
        , Path(path)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            -- TGetDestinationBlobQuery::OnRunQuery
            DECLARE $int_publication_id AS Uint64;
            DECLARE $path AS Text;

            SELECT
                `destination_blob` AS destination_blob
            FROM `)" << DestinationsTablePath() << R"(`
            WHERE `int_publication_id` = $int_publication_id
              AND `path` = $path;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$int_publication_id")
                .Uint64(IntPublicationId)
                .Build()
            .AddParam("$path")
                .Utf8(Path)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        if (ResultSets.empty() || ResultSets[0].RowsCount() == 0) {
            Finish();
            return;
        }

        NYdb::TResultSetParser parser(ResultSets[0]);
        if (parser.TryNextRow()) {
            DestinationBlob = parser.ColumnParser("destination_blob").GetString();
        }
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (IsRegistryTableMissing(status)) {
            status = Ydb::StatusIds::NOT_FOUND;
            issues.Clear();
            issues.AddIssue("Publication not found");
            DestinationBlob = Nothing();
        }

        Send(Owner, [&] {
            auto* event = new TEvGetDestinationBlobResponse;
            event->Status = status;
            event->Issues = std::move(issues);
            event->DestinationBlob = DestinationBlob;
            return event;
        }());
    }

private:
    const NActors::TActorId ReplyTo;
    const ui64 IntPublicationId;
    const TString Path;
    TMaybe<TString> DestinationBlob;
};

} // namespace

NActors::IActor* CreateGetDestinationBlobQueryActor(
    const NActors::TActorId& replyTo,
    const TString& database,
    ui64 intPublicationId,
    const TString& path)
{
    return new TGetDestinationBlobQuery(replyTo, database, intPublicationId, path);
}

} // namespace NKikimr::NPQ::NDeferredPublish
