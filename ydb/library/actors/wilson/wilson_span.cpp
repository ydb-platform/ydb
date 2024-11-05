#include "wilson_span.h"
#include "wilson_uploader.h"
#include <util/system/backtrace.h>
#include <google/protobuf/text_format.h>

namespace NWilson {

    using namespace NActors;

    void SerializeValue(TAttributeValue value, NCommonProto::AnyValue *pb) {
        switch (value.index()) {
            case 0:
                pb->set_string_value(std::get<0>(std::move(value)));
                break;

            case 1:
                pb->set_bool_value(std::get<1>(value));
                break;

            case 2:
                pb->set_int_value(std::get<2>(value));
                break;

            case 3:
                pb->set_double_value(std::get<3>(std::move(value)));
                break;

            case 4: {
                auto *array = pb->mutable_array_value();
                for (auto&& item : std::get<4>(std::move(value))) {
                    SerializeValue(std::move(item), array->add_values());
                }
                break;
            }

            case 5: {
                auto *kv = pb->mutable_kvlist_value();
                for (auto&& [key, value] : std::get<5>(std::move(value))) {
                    SerializeKeyValue(std::move(key), std::move(value), kv->add_values());
                }
                break;
            }

            case 6:
                pb->set_bytes_value(std::get<6>(std::move(value)));
                break;
        }
    }

    void SerializeKeyValue(TString key, TAttributeValue value, NCommonProto::KeyValue *pb) {
        pb->set_key(std::move(key));
        SerializeValue(std::move(value), pb->mutable_value());
    }

    TSpan& TSpan::Link(const TTraceId& traceId) {
        return Link(traceId, {});
    }

    void TSpan::Send() {
        if (Data->ActorSystem) {
            Data->ActorSystem->Send(new IEventHandle(MakeWilsonUploaderId(), {}, new TEvWilson(&Data->Span)));
        }
        Data->Sent = true;
    }

    TSpan& TSpan::operator=(TSpan&& other) {
        if (this != &other) {
            if (Y_UNLIKELY(*this)) {
                TStringStream err;
                err << "TSpan instance incorrectly overwritten at:\n";
                FormatBackTrace(&err);
                EndError(std::move(err.Str()));
            }
            Data = std::exchange(other.Data, nullptr);
        }
        return *this;
    }

    const TSpan TSpan::Empty;

} // NWilson
