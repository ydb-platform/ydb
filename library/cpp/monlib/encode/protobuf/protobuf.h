#pragma once

#include <library/cpp/monlib/encode/encoder.h>

#include <library/cpp/monlib/encode/protobuf/protos/samples.pb.h>

namespace NMonitoring {
    namespace NProto {
        class TSingleSamplesList;
        class TMultiSamplesList;
    }

    IMetricEncoderPtr EncoderProtobuf(NProto::TSingleSamplesList* samples);
    IMetricEncoderPtr EncoderProtobuf(NProto::TMultiSamplesList* samples);

}
