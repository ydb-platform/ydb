#pragma once

#include "events.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

constexpr TStringBuf PublicationsTableName = "topic_deferred_publications";
constexpr TStringBuf DestinationsTableName = "topic_deferred_publication_destinations";
constexpr TStringBuf ExtPublicationIdIndexName = "idx_ext_publication_id";
constexpr TStringBuf IntPublicationIdSequenceName = "int_publication_id";

TVector<TString> PublicationsTablePathComponents();
TVector<TString> DestinationsTablePathComponents();
TString PublicationsTablePath();
TString DestinationsTablePath();

NKikimrSchemeOp::TColumnDescription IntPublicationIdColumn();
NKikimrSchemeOp::TSequenceDescription IntPublicationIdSequence();
NKikimrSchemeOp::TIndexDescription ExtPublicationIdUniqueIndex();

NActors::IActor* CreateDeferredPublishTablesCreator(const TString& database);

} // namespace NKikimr::NPQ::NDeferredPublish
