#pragma once

#include "kafka_events.h"

namespace NKafka {

TVector<std::pair<TString, TString>> BuildLabels(const NKafka::TContext::TPtr context, const TString& method, const TString& topic, const TString& name, const TString& errorCode);

TVector<std::pair<TString, TString>> BuildGroupLabels(const NKafka::TContext::TPtr context, const TString& groupId, const TString& name);

TActorId MakeKafkaMetricsServiceID();

} // namespace NKafka
