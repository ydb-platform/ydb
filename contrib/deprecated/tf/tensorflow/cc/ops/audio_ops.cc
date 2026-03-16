// This file is MACHINE GENERATED! Do not edit.


#include "tensorflow/cc/ops/const_op.h"
#include "tensorflow/cc/ops/audio_ops.h"

namespace tensorflow {
namespace ops {

AudioSpectrogram::AudioSpectrogram(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input input, int64
                                   window_size, int64 stride, const
                                   AudioSpectrogram::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _input = ::tensorflow::ops::AsNodeOut(scope, input);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("AudioSpectrogram");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "AudioSpectrogram")
                     .Input(_input)
                     .Attr("window_size", window_size)
                     .Attr("stride", stride)
                     .Attr("magnitude_squared", attrs.magnitude_squared_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->spectrogram = Output(ret, 0);
}

AudioSpectrogram::AudioSpectrogram(const ::tensorflow::Scope& scope,
                                   ::tensorflow::Input input, int64
                                   window_size, int64 stride)
  : AudioSpectrogram(scope, input, window_size, stride, AudioSpectrogram::Attrs()) {}

DecodeWav::DecodeWav(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     contents, const DecodeWav::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _contents = ::tensorflow::ops::AsNodeOut(scope, contents);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("DecodeWav");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "DecodeWav")
                     .Input(_contents)
                     .Attr("desired_channels", attrs.desired_channels_)
                     .Attr("desired_samples", attrs.desired_samples_)
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

  this->audio = Output(ret, _outputs_range["audio"].first);
  this->sample_rate = Output(ret, _outputs_range["sample_rate"].first);
}

DecodeWav::DecodeWav(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     contents)
  : DecodeWav(scope, contents, DecodeWav::Attrs()) {}

EncodeWav::EncodeWav(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     audio, ::tensorflow::Input sample_rate) {
  if (!scope.ok()) return;
  auto _audio = ::tensorflow::ops::AsNodeOut(scope, audio);
  if (!scope.ok()) return;
  auto _sample_rate = ::tensorflow::ops::AsNodeOut(scope, sample_rate);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("EncodeWav");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "EncodeWav")
                     .Input(_audio)
                     .Input(_sample_rate)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->contents = Output(ret, 0);
}

Mfcc::Mfcc(const ::tensorflow::Scope& scope, ::tensorflow::Input spectrogram,
           ::tensorflow::Input sample_rate, const Mfcc::Attrs& attrs) {
  if (!scope.ok()) return;
  auto _spectrogram = ::tensorflow::ops::AsNodeOut(scope, spectrogram);
  if (!scope.ok()) return;
  auto _sample_rate = ::tensorflow::ops::AsNodeOut(scope, sample_rate);
  if (!scope.ok()) return;
  ::tensorflow::Node* ret;
  const auto unique_name = scope.GetUniqueNameForOp("Mfcc");
  auto builder = ::tensorflow::NodeBuilder(unique_name, "Mfcc")
                     .Input(_spectrogram)
                     .Input(_sample_rate)
                     .Attr("upper_frequency_limit", attrs.upper_frequency_limit_)
                     .Attr("lower_frequency_limit", attrs.lower_frequency_limit_)
                     .Attr("filterbank_channel_count", attrs.filterbank_channel_count_)
                     .Attr("dct_coefficient_count", attrs.dct_coefficient_count_)
  ;
  scope.UpdateBuilder(&builder);
  scope.UpdateStatus(builder.Finalize(scope.graph(), &ret));
  if (!scope.ok()) return;
  scope.UpdateStatus(scope.DoShapeInference(ret));
  this->output = Output(ret, 0);
}

Mfcc::Mfcc(const ::tensorflow::Scope& scope, ::tensorflow::Input spectrogram,
           ::tensorflow::Input sample_rate)
  : Mfcc(scope, spectrogram, sample_rate, Mfcc::Attrs()) {}

/// @}

}  // namespace ops
}  // namespace tensorflow
