#include "list_publications_query.h"

#include "events.h"
#include "query_utils.h"
#include "tables_creator.h"

#include <ydb/library/query_actor/query_actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

namespace {

class TListPublicationsQuery : public TQueryBase {
public:
    TListPublicationsQuery(
        const NActors::TActorId& replyTo,
        const TString& database,
        const TMaybe<TString>& writerIdentityFilter)
        : TQueryBase(NKikimrServices::PERSQUEUE, {}, database, /* isSystemUser */ true)
        , ReplyTo(replyTo)
        , WriterIdentityFilter(writerIdentityFilter)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            -- TListPublicationsQuery::OnRunQuery
            DECLARE $writer_identity AS Optional<Text>;

            SELECT
                `int_publication_id`,
                `ext_publication_id`,
                `writer_identity`
            FROM `)" << PublicationsTablePath() << R"(`
            WHERE ($writer_identity IS NULL OR `writer_identity` = $writer_identity)
            ORDER BY `int_publication_id`;
        )";

        std::optional<std::string> writerIdentity;
        if (WriterIdentityFilter) {
            writerIdentity = *WriterIdentityFilter;
        }

        NYdb::TParamsBuilder params;
        params
            .AddParam("$writer_identity")
                .OptionalUtf8(writerIdentity)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        Publications.clear();
        if (!ResultSets.empty()) {
            NYdb::TResultSetParser parser(ResultSets[0]);
            while (parser.TryNextRow()) {
                TPublicationSummary summary;
                summary.IntPublicationId = parser.ColumnParser("int_publication_id").GetUint64();
                summary.ExtPublicationId = parser.ColumnParser("ext_publication_id").GetUtf8();
                summary.WriterIdentity = Nothing();
                if (const auto writerIdentity = parser.ColumnParser("writer_identity").GetOptionalUtf8()) {
                    summary.WriterIdentity = TString(*writerIdentity);
                }
                Publications.push_back(std::move(summary));
            }
        }
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (IsRegistryTableMissing(status)) {
            status = Ydb::StatusIds::SUCCESS;
            issues.Clear();
            Publications.clear();
        }

        Send(Owner, [&] {
            auto* event = new TEvListPublicationsResponse;
            event->Status = status;
            event->Issues = std::move(issues);
            event->Publications = std::move(Publications);
            return event;
        }());
    }

private:
    const NActors::TActorId ReplyTo;
    const TMaybe<TString> WriterIdentityFilter;
    TVector<TPublicationSummary> Publications;
};

} // namespace

NActors::IActor* CreateListPublicationsQueryActor(
    const NActors::TActorId& replyTo,
    const TString& database,
    const TMaybe<TString>& writerIdentityFilter)
{
    return new TListPublicationsQuery(replyTo, database, writerIdentityFilter);
}

} // namespace NKikimr::NPQ::NDeferredPublish
