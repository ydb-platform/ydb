#include "list_destinations_query.h"

#include "events.h"
#include "query_utils.h"
#include "tables_creator.h"

#include <ydb/library/query_actor/query_actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

namespace {

class TListDestinationsQuery : public TQueryBase {
public:
    TListDestinationsQuery(
        const NActors::TActorId& replyTo,
        const TString& database,
        ui64 intPublicationId)
        : TQueryBase(NKikimrServices::PERSQUEUE, {}, database, /* isSystemUser */ true)
        , ReplyTo(replyTo)
        , IntPublicationId(intPublicationId)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            -- TListDestinationsQuery::OnRunQuery
            DECLARE $int_publication_id AS Uint64;

            SELECT
                p.`ext_publication_id` AS ext_publication_id,
                d.`path` AS destination_path,
                d.`destination_blob` AS destination_blob
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

        TListDestinationsData data;
        NYdb::TResultSetParser parser(ResultSets[0]);
        while (parser.TryNextRow()) {
            if (data.ExtPublicationId.empty()) {
                data.ExtPublicationId = parser.ColumnParser("ext_publication_id").GetUtf8();
            }

            const auto destinationPath = parser.ColumnParser("destination_path").GetOptionalUtf8();
            if (!destinationPath) {
                continue;
            }

            const auto destinationBlob = parser.ColumnParser("destination_blob").GetOptionalString();
            if (!destinationBlob) {
                continue;
            }

            TDestinationRow row;
            row.Path = TString(*destinationPath);
            row.DestinationBlob = TString(*destinationBlob);
            data.Destinations.push_back(std::move(row));
        }

        ListDestinationsData = std::move(data);
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (IsRegistryTableMissing(status)) {
            status = Ydb::StatusIds::NOT_FOUND;
            issues.Clear();
            issues.AddIssue("Publication not found");
            ListDestinationsData = Nothing();
        }

        Send(Owner, [&] {
            auto* event = new TEvListDestinationsResponse;
            event->Status = status;
            event->Issues = std::move(issues);
            event->Data = ListDestinationsData;
            return event;
        }());
    }

private:
    const NActors::TActorId ReplyTo;
    const ui64 IntPublicationId;
    TMaybe<TListDestinationsData> ListDestinationsData;
};

} // namespace

NActors::IActor* CreateListDestinationsQueryActor(
    const NActors::TActorId& replyTo,
    const TString& database,
    ui64 intPublicationId)
{
    return new TListDestinationsQuery(replyTo, database, intPublicationId);
}

} // namespace NKikimr::NPQ::NDeferredPublish
