#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/core/driver_lib/cli_config_base/config_base.h>
#include <util/string/builder.h>

namespace NKikimr {

namespace NDriverClient {

using namespace NYdb::NConsoleClient;

class TClientCommandBase : public TClientCommand {
public:
    TClientCommandBase(
        const TString& name,
        const std::initializer_list<TString>& aliases = std::initializer_list<TString>(),
        const TString& description = TString()
    )
        : TClientCommand(name, aliases, description)
    {}

    static bool IsProtobuf(const TString& string) {
        return string.empty() || string.find('{') != TString::npos;
    }

    static bool IsMiniKQL(const TString& string) {
        return string.empty() || string.find('(') != TString::npos;
    }

    template <typename ProtobufType>
    static TAutoPtr<ProtobufType> GetProtobuf(const TString& string) {
        TAutoPtr<ProtobufType> pb(new ProtobufType);
        if (IsProtobuf(string)) {
            Y_ABORT_UNLESS(::google::protobuf::TextFormat::ParseFromString(string, pb.Get()));
        } else {
            Y_ABORT_UNLESS(ParsePBFromFile(string, pb.Get()));
        }
        return pb;
    }

    template <typename ProtobufType>
    static void ParseProtobuf(ProtobufType* pb, const TString& string) {
        if (IsProtobuf(string)) {
            Y_ABORT_UNLESS(::google::protobuf::TextFormat::ParseFromString(string, pb));
        } else {
            Y_ABORT_UNLESS(ParsePBFromFile(string, pb));
        }
    }

    static TString GetMiniKQL(const TString& string) {
        if (IsMiniKQL(string)) {
            return string;
        } else {
            return TUnbufferedFileInput(string).ReadAll();
        }
    }

    template <typename ProtobufType>
    static TString GetString(const ProtobufType& protobuf) {
        TString string;
        if (!::google::protobuf::TextFormat::PrintToString(protobuf, &string)) {
            ythrow TWithBackTrace<yexception>() << "Error printing protobuf to string!";
        }
        return string;
    }
};

}
}
