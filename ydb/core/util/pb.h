#pragma once
#include "defs.h"
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/text_format.h>
#include <util/stream/file.h>
#include <util/generic/array_ref.h>
#include <util/system/type_name.h>

namespace NKikimr {

template<typename T>
bool ParsePBFromFile(const TString &path, T *pb, bool allowUnknown = false) {
    TAutoPtr<TMappedFileInput> fileInput(new TMappedFileInput(path));
    const TString content = fileInput->ReadAll();
    if (!allowUnknown)
        return ::google::protobuf::TextFormat::ParseFromString(content, pb);
    
    ::google::protobuf::TextFormat::Parser parser;
    parser.AllowUnknownField(true);
    return parser.ParseFromString(content, pb);
}

template<typename T>
bool ParseBinPBFromFile(const TString &path, T *pb) {
    TAutoPtr<TMappedFileInput> fileInput(new TMappedFileInput(path));
    const TString content = fileInput->ReadAll();
    const bool ok = pb->ParseFromString(content);
    return ok;
}

// Deserialize persisted protobuf without checking size limit (size should have checked before saving)
template <class TProto>
bool ParseFromStringNoSizeLimit(TProto& proto, TArrayRef<const char> str) {
    google::protobuf::io::CodedInputStream input(reinterpret_cast<const ui8*>(str.data()), str.size());
    input.SetTotalBytesLimit(str.size());
    return proto.ParseFromCodedStream(&input) && input.ConsumedEntireMessage();
}

template<typename TProto>
struct TProtoBox : public TProto {
    TProtoBox(TArrayRef<const char> plain) {
        bool ok = ParseFromStringNoSizeLimit(*this, plain);
        Y_ABORT_UNLESS(ok, "Cannot parse proto %s", TypeName<TProto>().c_str());
    }
};

// Deserialized and merge persisted protobuf without checking size limit
template <class TProto>
bool MergeFromStringNoSizeLimit(TProto& proto, TArrayRef<const char> str) {
    google::protobuf::io::CodedInputStream input(reinterpret_cast<const ui8*>(str.data()), str.size());
    input.SetTotalBytesLimit(str.size());
    return proto.MergeFromCodedStream(&input) && input.ConsumedEntireMessage();
}

inline TString SingleLineProto(const NProtoBuf::Message& message) {
    NProtoBuf::TextFormat::Printer p;
    p.SetSingleLineMode(true);
    TString res;
    const bool success = p.PrintToString(message, &res);
    Y_ABORT_UNLESS(success);
    return res;
}

}
