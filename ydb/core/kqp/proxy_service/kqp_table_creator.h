#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NKqp {

NActors::IActor* CreateTableCreator(TVector<TString> pathComponents, TVector<NKikimrSchemeOp::TColumnDescription> columns, TVector<TString> keyColumns,
                  TMaybe<NKikimrSchemeOp::TTTLSettings> ttlSettings = Nothing());

} // namespace NKikimr::NKqp