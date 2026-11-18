#include "metrics.h"

#include <google/protobuf/descriptor.h>
#include <util/system/yassert.h>

#include <map>
#include <vector>

namespace NKikimr::NOlap::NStats::NScan {
namespace {

void AddCoverage(NColumnShardStats::TMeasurementCoverage& target, const NColumnShardStats::TMeasurementCoverage& value) {
    if (target.HasMeasured() && value.HasMeasured()) {
        target.SetMeasured(target.GetMeasured() + value.GetMeasured());
    } else {
        target.ClearMeasured();
    }
    if (target.HasApplicable() && value.HasApplicable()) {
        target.SetApplicable(target.GetApplicable() + value.GetApplicable());
    } else {
        target.ClearApplicable();
    }
    if (value.GetMissingReason() != NColumnShardStats::MISSING_NONE &&
        (target.GetMissingReason() == NColumnShardStats::MISSING_NONE || value.GetMissingReason() == NColumnShardStats::OBSERVATION_LOST)) {
        target.SetMissingReason(value.GetMissingReason());
    }
}

template <class TObservation>
void AddApplicability(TObservation& target, const TObservation& value) {
    if (!target.HasApplicability()) {
        target.SetApplicability(value.GetApplicability());
    } else if (target.GetApplicability() != value.GetApplicability()) {
        target.SetApplicability(NColumnShardStats::APPLICABILITY_UNKNOWN);
    }
}

using TMetricPath = std::vector<const google::protobuf::FieldDescriptor*>;

const std::vector<TMetricPath>& MetricPaths(const google::protobuf::Descriptor* descriptor) {
    static const auto paths = [] {
        std::map<const google::protobuf::Descriptor*, std::vector<TMetricPath>> result;
        const auto collect = [&](const auto& self, const google::protobuf::Descriptor* message, TMetricPath& path,
                                 std::vector<TMetricPath>& destination) -> void {
            for (int i = 0; i != message->field_count(); ++i) {
                const auto* field = message->field(i);
                if (field->is_repeated() || field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
                    continue;
                }
                path.push_back(field);
                if (field->message_type() == NColumnShardStats::TNumericObservation::descriptor() ||
                    field->message_type() == NColumnShardStats::TBooleanObservation::descriptor()) {
                    destination.push_back(path);
                } else if (field->message_type() != NColumnShardStats::TOperation::descriptor()) {
                    self(self, field->message_type(), path, destination);
                }
                path.pop_back();
            }
        };
        const auto* operation = NColumnShardStats::TOperation::descriptor();
        for (int i = 0; i != operation->field_count(); ++i) {
            const auto* field = operation->field(i);
            if (!field->is_repeated() && field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE &&
                field->message_type() != operation) {
                auto [position, inserted] = result.try_emplace(field->message_type());
                if (inserted) {
                    TMetricPath path;
                    collect(collect, field->message_type(), path, position->second);
                }
            }
        }
        return result;
    }();
    return paths.at(descriptor);
}

template <class TNumber, class TBoolean>
void VisitMetricMessage(google::protobuf::Message& target, const google::protobuf::Message& value, TNumber& number, TBoolean& boolean) {
    for (const auto& path : MetricPaths(value.GetDescriptor())) {
        auto* destination = &target;
        const auto* source = &value;
        for (size_t i = 0; i + 1 != path.size(); ++i) {
            source = &source->GetReflection()->GetMessage(*source, path[i]);
            destination = destination->GetReflection()->MutableMessage(destination, path[i]);
        }
        const auto* field = path.back();
        const auto& incoming = source->GetReflection()->GetMessage(*source, field);
        auto* metric = destination->GetReflection()->MutableMessage(destination, field);
        if (field->message_type() == NColumnShardStats::TNumericObservation::descriptor()) {
            number(static_cast<NColumnShardStats::TNumericObservation&>(*metric),
                static_cast<const NColumnShardStats::TNumericObservation&>(incoming));
        } else {
            boolean(static_cast<NColumnShardStats::TBooleanObservation&>(*metric),
                static_cast<const NColumnShardStats::TBooleanObservation&>(incoming));
        }
    }
}

template <class TNumber, class TBoolean>
void VisitMetrics(NColumnShardStats::TOperation& target, const NColumnShardStats::TOperation& value, TNumber&& number, TBoolean&& boolean) {
    VisitMetricMessage(*target.MutableStatistics(), value.GetStatistics(), number, boolean);
    if (value.details_case() != NColumnShardStats::TOperation::DETAILS_NOT_SET) {
        const auto* field = value.GetDescriptor()->FindFieldByNumber(value.details_case());
        VisitMetricMessage(
            *target.GetReflection()->MutableMessage(&target, field), value.GetReflection()->GetMessage(value, field), number, boolean);
    }
}

void Normalize(NColumnShardStats::TNumericObservation& value) {
    if (value.HasValue()) {
        Y_ABORT_UNLESS(value.GetApplicability() != NColumnShardStats::NOT_APPLICABLE, "value for inapplicable metric");
        value.SetApplicability(NColumnShardStats::APPLICABLE);
    }
    auto* coverage = value.MutableCoverage();
    if (!coverage->HasMeasured()) {
        coverage->SetMeasured(value.HasValue() ? 1 : 0);
    }
    if (!coverage->HasApplicable() && value.GetApplicability() != NColumnShardStats::APPLICABILITY_UNKNOWN) {
        coverage->SetApplicable(value.GetApplicability() == NColumnShardStats::APPLICABLE ? 1 : 0);
    }
    if (!coverage->HasMissingReason()) {
        coverage->SetMissingReason(value.HasValue() || value.GetApplicability() == NColumnShardStats::NOT_APPLICABLE
                                       ? NColumnShardStats::MISSING_NONE
                                       : NColumnShardStats::NOT_REPORTED);
    }
}

void Normalize(NColumnShardStats::TBooleanObservation& value) {
    const bool measured = value.HasTrueCount() || value.HasFalseCount();
    if (measured) {
        Y_ABORT_UNLESS(value.GetApplicability() != NColumnShardStats::NOT_APPLICABLE, "value for inapplicable metric");
        value.SetApplicability(NColumnShardStats::APPLICABLE);
    }
    auto* coverage = value.MutableCoverage();
    if (!coverage->HasMeasured()) {
        coverage->SetMeasured(measured ? value.GetTrueCount() + value.GetFalseCount() : 0);
    }
    if (!coverage->HasApplicable() && value.GetApplicability() != NColumnShardStats::APPLICABILITY_UNKNOWN) {
        coverage->SetApplicable(value.GetApplicability() == NColumnShardStats::APPLICABLE ? 1 : 0);
    }
    if (!coverage->HasMissingReason()) {
        coverage->SetMissingReason(measured || value.GetApplicability() == NColumnShardStats::NOT_APPLICABLE ? NColumnShardStats::MISSING_NONE
                                                                                                             : NColumnShardStats::NOT_REPORTED);
    }
}

bool SameSource(const NColumnShardStats::TDataSource& left, const NColumnShardStats::TDataSource& right) {
    return left.GetStorageId() == right.GetStorageId() && left.GetKind() == right.GetKind() &&
           left.GetReadBytes().GetBoundary() == right.GetReadBytes().GetBoundary() &&
           left.GetReadBytes().GetRepresentation() == right.GetReadBytes().GetRepresentation();
}

}   // namespace

void AddMetric(NColumnShardStats::TNumericObservation& target, const NColumnShardStats::TNumericObservation& value) {
    if (!value.HasApplicability() && !value.HasValue() && !value.HasCoverage()) {
        return;
    }
    if (!target.HasCoverage() && !target.HasValue()) {
        target = value;
        return;
    }
    AddApplicability(target, value);
    if (value.HasValue()) {
        target.SetValue(target.GetValue() + value.GetValue());
    }
    AddCoverage(*target.MutableCoverage(), value.GetCoverage());
}

void AddMetric(NColumnShardStats::TBooleanObservation& target, const NColumnShardStats::TBooleanObservation& value) {
    if (!value.HasApplicability() && !value.HasTrueCount() && !value.HasFalseCount() && !value.HasCoverage()) {
        return;
    }
    if (!target.HasCoverage() && !target.HasTrueCount() && !target.HasFalseCount()) {
        target = value;
        return;
    }
    AddApplicability(target, value);
    if (value.HasTrueCount()) {
        target.SetTrueCount(target.GetTrueCount() + value.GetTrueCount());
    }
    if (value.HasFalseCount()) {
        target.SetFalseCount(target.GetFalseCount() + value.GetFalseCount());
    }
    AddCoverage(*target.MutableCoverage(), value.GetCoverage());
}

void NormalizeCall(NColumnShardStats::TOperation& operation) {
    VisitMetrics(
        operation, operation,
        [](auto& value, const auto&) {
            Normalize(value);
        },
        [](auto& value, const auto&) {
            Normalize(value);
        });
}

void AddMeasurements(NColumnShardStats::TOperation& target, const NColumnShardStats::TOperation& value) {
    VisitMetrics(
        target, value,
        [](auto& destination, const auto& source) {
            AddMetric(destination, source);
        },
        [](auto& destination, const auto& source) {
            AddMetric(destination, source);
        });
}

NColumnShardStats::TOperation Definition(const NColumnShardStats::TOperation& operation) {
    auto result = MeasurementPrototype(operation);
    if (operation.HasKey()) {
        result.SetKey(operation.GetKey());
    }
    if (operation.HasName()) {
        result.SetName(operation.GetName());
    }
    const auto* reflection = operation.GetReflection();
    if (operation.details_case() != NColumnShardStats::TOperation::DETAILS_NOT_SET) {
        const auto* field = operation.GetDescriptor()->FindFieldByNumber(operation.details_case());
        result.GetReflection()->MutableMessage(&result, field)->CopyFrom(reflection->GetMessage(operation, field));
    }
    result.GetReflection()->MutableUnknownFields(&result)->MergeFrom(reflection->GetUnknownFields(operation));
    VisitMetrics(
        result, result,
        [](auto& value, const auto&) {
            value.ClearValue();
            value.ClearCoverage();
        },
        [](auto& value, const auto&) {
            value.ClearTrueCount();
            value.ClearFalseCount();
            value.ClearCoverage();
        });
    return result;
}

NColumnShardStats::TOperation MeasurementPrototype(const NColumnShardStats::TOperation& definition) {
    NColumnShardStats::TOperation result;
    if (definition.details_case() != NColumnShardStats::TOperation::DETAILS_NOT_SET) {
        const auto* field = definition.GetDescriptor()->FindFieldByNumber(definition.details_case());
        result.GetReflection()->MutableMessage(&result, field);
    }
    const auto copyApplicability = [](auto& target, const auto& source) {
        if (source.HasApplicability()) {
            target.SetApplicability(source.GetApplicability());
        }
    };
    VisitMetrics(result, definition, copyApplicability, copyApplicability);
    if (definition.GetStatistics().HasInputRepresentation()) {
        result.MutableStatistics()->SetInputRepresentation(definition.GetStatistics().GetInputRepresentation());
    }
    if (definition.GetStatistics().HasOutputRepresentation()) {
        result.MutableStatistics()->SetOutputRepresentation(definition.GetStatistics().GetOutputRepresentation());
    }
    return result;
}

void AddRead(google::protobuf::RepeatedPtrField<NColumnShardStats::TDataSource>& sources, const NColumnShardStats::TDataSource& read) {
    Y_ABORT_UNLESS(!read.GetStorageId().empty() && read.HasKind() && read.GetReadBytes().GetBoundary() != NColumnShardStats::BOUNDARY_UNKNOWN &&
                       read.GetReadBytes().GetRepresentation() != NColumnShardStats::REPRESENTATION_UNKNOWN, "invalid data source");
    for (auto& source : sources) {
        if (SameSource(source, read)) {
            source.SetReads(source.GetReads() + read.GetReads());
            if (read.GetReadBytes().HasValue()) {
                source.MutableReadBytes()->SetValue(source.GetReadBytes().GetValue() + read.GetReadBytes().GetValue());
            }
            AddCoverage(*source.MutableVolumeCoverage(), read.GetVolumeCoverage());
            return;
        }
    }
    *sources.Add() = read;
}
}   // namespace NKikimr::NOlap::NStats::NScan
