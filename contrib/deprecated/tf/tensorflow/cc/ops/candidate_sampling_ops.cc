// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/candidate_sampling_ops.h"

namespace tensorflow {
namespace ops {

AllCandidateSampler::AllCandidateSampler(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input true_classes,
                                         int64 num_true, int64 num_sampled,
                                         bool unique, const
                                         AllCandidateSampler::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _true_classes = ::tensorflow::ops::AsNodeOut(scope, true_classes);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AllCandidateSampler");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AllCandidateSampler")
                     .Input(_true_classes)
                     .Attr("num_true", num_true)
                     .Attr("num_sampled", num_sampled)
                     .Attr("unique", unique)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->sampled_candidates = Output(ret, _outputs_range["sampled_candidates"].first);
  this->true_expected_count = Output(ret, _outputs_range["true_expected_count"].first);
  this->sampled_expected_count = Output(ret, _outputs_range["sampled_expected_count"].first);
}

AllCandidateSampler::AllCandidateSampler(const ::tensorflow::Scope& scope,
                                         ::tensorflow::Input true_classes,
                                         int64 num_true, int64 num_sampled,
                                         bool unique)
  : AllCandidateSampler(scope, true_classes, num_true, num_sampled, unique, AllCandidateSampler::Attrs()) {}

ComputeAccidentalHits::ComputeAccidentalHits(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input true_classes,
                                             ::tensorflow::Input
                                             sampled_candidates, int64
                                             num_true, const
                                             ComputeAccidentalHits::Attrs&
                                             attrs) {
  if (!scope.ok()) return;
  auto _true_classes = ::tensorflow::ops::AsNodeOut(scope, true_classes);
  if (!scope.ok()) return;
  auto _sampled_candidates = ::tensorflow::ops::AsNodeOut(scope, sampled_candidates);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("ComputeAccidentalHits");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "ComputeAccidentalHits")
                     .Input(_true_classes)
                     .Input(_sampled_candidates)
                     .Attr("num_true", num_true)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->indices = Output(ret, _outputs_range["indices"].first);
  this->ids = Output(ret, _outputs_range["ids"].first);
  this->weights = Output(ret, _outputs_range["weights"].first);
}

ComputeAccidentalHits::ComputeAccidentalHits(const ::tensorflow::Scope& scope,
                                             ::tensorflow::Input true_classes,
                                             ::tensorflow::Input
                                             sampled_candidates, int64
                                             num_true)
  : ComputeAccidentalHits(scope, true_classes, sampled_candidates, num_true, ComputeAccidentalHits::Attrs()) {}

FixedUnigramCandidateSampler::FixedUnigramCandidateSampler(const
                                                           ::tensorflow::Scope&
                                                           scope,
                                                           ::tensorflow::Input
                                                           true_classes, int64
                                                           num_true, int64
                                                           num_sampled, bool
                                                           unique, int64
                                                           range_max, const
                                                           FixedUnigramCandidateSampler::Attrs&
                                                           attrs) {
  if (!scope.ok()) return;
  auto _true_classes = ::tensorflow::ops::AsNodeOut(scope, true_classes);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("FixedUnigramCandidateSampler");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "FixedUnigramCandidateSampler")
                     .Input(_true_classes)
                     .Attr("num_true", num_true)
                     .Attr("num_sampled", num_sampled)
                     .Attr("unique", unique)
                     .Attr("range_max", range_max)
                     .Attr("vocab_file", attrs.vocab_file_)
                     .Attr("distortion", attrs.distortion_)
                     .Attr("num_reserved_ids", attrs.num_reserved_ids_)
                     .Attr("num_shards", attrs.num_shards_)
                     .Attr("shard", attrs.shard_)
                     .Attr("unigrams", attrs.unigrams_)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->sampled_candidates = Output(ret, _outputs_range["sampled_candidates"].first);
  this->true_expected_count = Output(ret, _outputs_range["true_expected_count"].first);
  this->sampled_expected_count = Output(ret, _outputs_range["sampled_expected_count"].first);
}

FixedUnigramCandidateSampler::FixedUnigramCandidateSampler(const
                                                           ::tensorflow::Scope&
                                                           scope,
                                                           ::tensorflow::Input
                                                           true_classes, int64
                                                           num_true, int64
                                                           num_sampled, bool
                                                           unique, int64
                                                           range_max)
  : FixedUnigramCandidateSampler(scope, true_classes, num_true, num_sampled, unique, range_max, FixedUnigramCandidateSampler::Attrs()) {}

LearnedUnigramCandidateSampler::LearnedUnigramCandidateSampler(const
                                                               ::tensorflow::Scope&
                                                               scope,
                                                               ::tensorflow::Input
                                                               true_classes,
                                                               int64 num_true,
                                                               int64
                                                               num_sampled,
                                                               bool unique,
                                                               int64 range_max,
                                                               const
                                                               LearnedUnigramCandidateSampler::Attrs&
                                                               attrs) {
  if (!scope.ok()) return;
  auto _true_classes = ::tensorflow::ops::AsNodeOut(scope, true_classes);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LearnedUnigramCandidateSampler");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LearnedUnigramCandidateSampler")
                     .Input(_true_classes)
                     .Attr("num_true", num_true)
                     .Attr("num_sampled", num_sampled)
                     .Attr("unique", unique)
                     .Attr("range_max", range_max)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->sampled_candidates = Output(ret, _outputs_range["sampled_candidates"].first);
  this->true_expected_count = Output(ret, _outputs_range["true_expected_count"].first);
  this->sampled_expected_count = Output(ret, _outputs_range["sampled_expected_count"].first);
}

LearnedUnigramCandidateSampler::LearnedUnigramCandidateSampler(const
                                                               ::tensorflow::Scope&
                                                               scope,
                                                               ::tensorflow::Input
                                                               true_classes,
                                                               int64 num_true,
                                                               int64
                                                               num_sampled,
                                                               bool unique,
                                                               int64 range_max)
  : LearnedUnigramCandidateSampler(scope, true_classes, num_true, num_sampled, unique, range_max, LearnedUnigramCandidateSampler::Attrs()) {}

LogUniformCandidateSampler::LogUniformCandidateSampler(const
                                                       ::tensorflow::Scope&
                                                       scope,
                                                       ::tensorflow::Input
                                                       true_classes, int64
                                                       num_true, int64
                                                       num_sampled, bool
                                                       unique, int64 range_max,
                                                       const
                                                       LogUniformCandidateSampler::Attrs&
                                                       attrs) {
  if (!scope.ok()) return;
  auto _true_classes = ::tensorflow::ops::AsNodeOut(scope, true_classes);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("LogUniformCandidateSampler");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "LogUniformCandidateSampler")
                     .Input(_true_classes)
                     .Attr("num_true", num_true)
                     .Attr("num_sampled", num_sampled)
                     .Attr("unique", unique)
                     .Attr("range_max", range_max)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->sampled_candidates = Output(ret, _outputs_range["sampled_candidates"].first);
  this->true_expected_count = Output(ret, _outputs_range["true_expected_count"].first);
  this->sampled_expected_count = Output(ret, _outputs_range["sampled_expected_count"].first);
}

LogUniformCandidateSampler::LogUniformCandidateSampler(const
                                                       ::tensorflow::Scope&
                                                       scope,
                                                       ::tensorflow::Input
                                                       true_classes, int64
                                                       num_true, int64
                                                       num_sampled, bool
                                                       unique, int64 range_max)
  : LogUniformCandidateSampler(scope, true_classes, num_true, num_sampled, unique, range_max, LogUniformCandidateSampler::Attrs()) {}

UniformCandidateSampler::UniformCandidateSampler(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 true_classes, int64 num_true,
                                                 int64 num_sampled, bool
                                                 unique, int64 range_max, const
                                                 UniformCandidateSampler::Attrs&
                                                 attrs) {
  if (!scope.ok()) return;
  auto _true_classes = ::tensorflow::ops::AsNodeOut(scope, true_classes);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("UniformCandidateSampler");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "UniformCandidateSampler")
                     .Input(_true_classes)
                     .Attr("num_true", num_true)
                     .Attr("num_sampled", num_sampled)
                     .Attr("unique", unique)
                     .Attr("range_max", range_max)
                     .Attr("seed", attrs.seed_)
                     .Attr("seed2", attrs.seed2_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  ::tensorflow::NameRangeMap _outputs_range;
  ::tensorflow::Status _status_ = ::tensorflow::NameRangesForNode(*ret, ret->op_def(), nullptr, &_outputs_range);
  if (!_status_.ok()) {
    scope.UpdateStatus(_status_);
    return;
  }

  this->sampled_candidates = Output(ret, _outputs_range["sampled_candidates"].first);
  this->true_expected_count = Output(ret, _outputs_range["true_expected_count"].first);
  this->sampled_expected_count = Output(ret, _outputs_range["sampled_expected_count"].first);
}

UniformCandidateSampler::UniformCandidateSampler(const ::tensorflow::Scope&
                                                 scope, ::tensorflow::Input
                                                 true_classes, int64 num_true,
                                                 int64 num_sampled, bool
                                                 unique, int64 range_max)
  : UniformCandidateSampler(scope, true_classes, num_true, num_sampled, unique, range_max, UniformCandidateSampler::Attrs()) {}

/// @}

}  // namespace ops
}  // namespace tensorflow
