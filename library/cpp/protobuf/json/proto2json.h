#pragma once

#include "config.h"
#include "json_output.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/message.h>

#include <util/generic/fwd.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>

#include <functional>

namespace NJson {
    class TJsonValue;
    class TJsonWriter;
}

class IOutputStream;
class TStringStream;

namespace NProtobufJson {
    void Proto2Json(const NProtoBuf::Message& proto, IJsonOutput& jsonOutput,
                    const TProto2JsonConfig& config = TProto2JsonConfig(), bool closeMap = true);

    void Proto2Json(const NProtoBuf::Message& proto, NJson::TJsonWriter& writer,
                    const TProto2JsonConfig& config = TProto2JsonConfig());

    /// @throw yexception
    void Proto2Json(const NProtoBuf::Message& proto, NJson::TJsonValue& json,
                    const TProto2JsonConfig& config = TProto2JsonConfig());

    /// @throw yexception
    void Proto2Json(const NProtoBuf::Message& proto, IOutputStream& out,
                    const TProto2JsonConfig& config);
    // Generated code shortcut
    template <class T>
    inline void Proto2Json(const T& proto, IOutputStream& out) {
        out << proto.AsJSON();
    }

    // TStringStream deserves a special overload as its operator TString() would cause ambiguity
    /// @throw yexception
    void Proto2Json(const NProtoBuf::Message& proto, TStringStream& out,
                    const TProto2JsonConfig& config);
    // Generated code shortcut
    template <class T>
    inline void Proto2Json(const T& proto, TStringStream& out) {
        out << proto.AsJSON();
    }

    /// @throw yexception
    void Proto2Json(const NProtoBuf::Message& proto, TString& str,
                    const TProto2JsonConfig& config);
    // Generated code shortcut
    template <class T>
    inline void Proto2Json(const T& proto, TString& str) {
        str.clear();
        TStringOutput out(str);
        out << proto.AsJSON();
    }

    /// @throw yexception
    TString Proto2Json(const NProtoBuf::Message& proto,
                       const TProto2JsonConfig& config);
    // Returns incorrect result if proto contains another NProtoBuf::Message
    // Generated code shortcut
    template <class T>
    inline TString Proto2Json(const T& proto) {
        TString result;
        Proto2Json(proto, result);
        return result;
    }

}
