#include "tables_creator.h"

#include <ydb/core/base/path.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/library/table_creator/table_creator.h>

namespace NKikimr::NPQ::NDeferredPublish {

namespace {

using namespace NTableCreator;

class TDeferredPublishTablesCreator : public TMultiTableCreator {
public:
    explicit TDeferredPublishTablesCreator(const TString& database)
        : TBase({
            CreatePublicationsTableCreator(database),
            CreateDestinationsTableCreator(database),
        })
        , Database(database)
    {}

    void OnTablesCreated(bool success, NYql::TIssues issues) override {
        auto* event = new TEvTablesCreationFinished;
        event->Database = Database;
        event->Success = success;
        event->Issues = std::move(issues);
        Send(Owner, event);
    }

private:
    using TBase = TMultiTableCreator;

    static NKikimrSchemeOp::TColumnDescription StringColumn(const TString& name, bool notNull) {
        auto column = Col(name, NScheme::NTypeIds::Text);
        column.SetNotNull(notNull);
        return column;
    }

    static NActors::IActor* CreatePublicationsTableCreator(const TString& database) {
        return CreateTableCreator(
            PublicationsTablePathComponents(),
            {
                IntPublicationIdColumn(),
                StringColumn("ext_publication_id", true),
                StringColumn("writer_identity", false),
                [] {
                    auto column = Col("created_at", NScheme::NTypeIds::Timestamp);
                    column.SetNotNull(true);
                    return column;
                }(),
                StringColumn("created_by", false),
            },
            { "int_publication_id" },
            NKikimrServices::PERSQUEUE,
            Nothing(),
            database,
            /* isSystemUser */ true,
            AutoPartitioningByLoadPolicy(),
            Nothing(),
            { ExtPublicationIdUniqueIndex() },
            { IntPublicationIdSequence() }
        );
    }

    static NActors::IActor* CreateDestinationsTableCreator(const TString& database) {
        return CreateTableCreator(
            DestinationsTablePathComponents(),
            {
                Col("int_publication_id", NScheme::NTypeIds::Uint64),
                StringColumn("path", true),
                [] {
                    auto column = Col("destination_blob", NScheme::NTypeIds::String);
                    column.SetNotNull(true);
                    return column;
                }(),
            },
            { "int_publication_id", "path" },
            NKikimrServices::PERSQUEUE,
            Nothing(),
            database,
            /* isSystemUser */ true,
            AutoPartitioningByLoadPolicy()
        );
    }

    const TString Database;
};

} // namespace

TVector<TString> PublicationsTablePathComponents() {
    return { ".metadata", TString(PublicationsTableName) };
}

TVector<TString> DestinationsTablePathComponents() {
    return { ".metadata", TString(DestinationsTableName) };
}

TString PublicationsTablePath() {
    return JoinPath(PublicationsTablePathComponents());
}

TString DestinationsTablePath() {
    return JoinPath(DestinationsTablePathComponents());
}

NKikimrSchemeOp::TColumnDescription IntPublicationIdColumn() {
    NKikimrSchemeOp::TColumnDescription column;
    column.SetName("int_publication_id");
    column.SetType(NScheme::TypeName(NScheme::NTypeIds::Uint64));
    column.SetNotNull(true);
    column.SetDefaultFromSequence(TString(IntPublicationIdSequenceName));
    return column;
}

NKikimrSchemeOp::TSequenceDescription IntPublicationIdSequence() {
    NKikimrSchemeOp::TSequenceDescription sequence;
    sequence.SetName(TString(IntPublicationIdSequenceName));
    return sequence;
}

NKikimrSchemeOp::TIndexDescription ExtPublicationIdUniqueIndex() {
    NKikimrSchemeOp::TIndexDescription index;
    index.SetName(TString(ExtPublicationIdIndexName));
    index.AddKeyColumnNames("ext_publication_id");
    index.SetType(NKikimrSchemeOp::EIndexTypeGlobalUnique);
    index.SetState(NKikimrSchemeOp::EIndexStateReady);
    return index;
}

NActors::IActor* CreateDeferredPublishTablesCreator(const TString& database) {
    return new TDeferredPublishTablesCreator(database);
}

} // namespace NKikimr::NPQ::NDeferredPublish
