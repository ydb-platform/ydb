#pragma once

#include "collector.h"

namespace NKikimr::NOlap::NStats::NScan {

void AddMetric(NColumnShardStats::TNumericObservation& target, const NColumnShardStats::TNumericObservation& value);
void AddMetric(NColumnShardStats::TBooleanObservation& target, const NColumnShardStats::TBooleanObservation& value);
void NormalizeCall(NColumnShardStats::TOperation& operation);
void AddMeasurements(NColumnShardStats::TOperation& target, const NColumnShardStats::TOperation& value);
NColumnShardStats::TOperation Definition(const NColumnShardStats::TOperation& operation);
NColumnShardStats::TOperation MeasurementPrototype(const NColumnShardStats::TOperation& definition);
void AddRead(google::protobuf::RepeatedPtrField<NColumnShardStats::TDataSource>& sources, const NColumnShardStats::TDataSource& read);

}   // namespace NKikimr::NOlap::NStats::NScan
