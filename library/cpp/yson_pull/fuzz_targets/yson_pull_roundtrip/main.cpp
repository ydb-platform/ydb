#include <library/cpp/yson_pull/input.h>
#include <library/cpp/yson_pull/output.h>
#include <library/cpp/yson_pull/reader.h>
#include <library/cpp/yson_pull/writer.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/stream/str.h>

namespace {

template <typename TWriterFactory>
void RoundTripWithWriter(TStringBuf input, NYsonPull::EStreamType mode, TWriterFactory&& writerFactory) {
    TString output;
    {
        NYsonPull::TReader reader(NYsonPull::NInput::FromMemory(input), mode);
        auto writer = writerFactory(&output, mode);
        for (;;) {
            const auto& event = reader.NextEvent();
            (void)reader.LastEvent();
            writer.Event(event);
            if (event.Type() == NYsonPull::EEventType::EndStream) {
                break;
            }
        }
    }

    {
        NYsonPull::TReader reader(NYsonPull::NInput::FromMemory(output), mode);
        for (;;) {
            const auto& event = reader.NextEvent();
            (void)reader.LastEvent();
            if (event.Type() == NYsonPull::EEventType::EndStream) {
                break;
            }
        }
    }

    {
        TStringInput inputStream(output);
        NYsonPull::TReader reader(
            NYsonPull::NInput::FromInputStream(&inputStream, 1 + (output.size() % 32)),
            mode);
        for (;;) {
            const auto& event = reader.NextEvent();
            if (event.Type() == NYsonPull::EEventType::EndStream) {
                break;
            }
        }
    }
}

void FuzzPullRoundTrip(TStringBuf input) {
    for (auto mode : {
             NYsonPull::EStreamType::Node,
             NYsonPull::EStreamType::ListFragment,
             NYsonPull::EStreamType::MapFragment,
         })
    {
        try {
            RoundTripWithWriter(input, mode, [] (TString* output, NYsonPull::EStreamType streamType) {
                return NYsonPull::MakeTextWriter(NYsonPull::NOutput::FromString(output), streamType);
            });
        } catch (...) {
        }

        try {
            RoundTripWithWriter(input, mode, [] (TString* output, NYsonPull::EStreamType streamType) {
                return NYsonPull::MakePrettyTextWriter(NYsonPull::NOutput::FromString(output), streamType);
            });
        } catch (...) {
        }

        try {
            RoundTripWithWriter(input, mode, [] (TString* output, NYsonPull::EStreamType streamType) {
                return NYsonPull::MakeBinaryWriter(NYsonPull::NOutput::FromString(output), streamType);
            });
        } catch (...) {
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    try {
        FuzzedDataProvider fdp(data, size);
        const TString input = fdp.ConsumeRemainingBytesAsString();
        FuzzPullRoundTrip(input);
    } catch (...) {
    }

    return 0;
}
