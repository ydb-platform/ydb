#pragma once

#include "core_ydb.h"

using TYdbcLocation = TYdbLocation;

TString SnakeToCamelCase(TString name);
TString SnakeToCCamelCase(TString name);
TString CamelToSnakeCase(TString name);

void InitYdbc(NActors::TActorSystem& actorSystem, const NActors::TActorId& httpProxyId);
TString TrimAtAs(const TString& owner);

TString EscapeStreamName(const TString& name);
TString UnescapeStreamName(const TString& name);
