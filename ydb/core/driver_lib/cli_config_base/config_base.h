#pragma once

#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/public/lib/deprecated/client/msgbus_client.h>
#include <ydb/public/lib/deprecated/client/grpc_client.h>
#include <ydb/core/util/pb.h>
#include <ydb/core/protos/blobstorage_vdisk_config.pb.h>
#include <ydb/core/protos/drivemodel.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/netclassifier.pb.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/key.pb.h>
#include <ydb/core/fq/libs/config/protos/fq_config.pb.h>

#include <google/protobuf/text_format.h>

#include <util/datetime/base.h>

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/generic/variant.h>

#include <util/stream/file.h>
#include <util/stream/format.h>

namespace NKikimr {

/// last_getopt parse handler for time durations.
/// Canonical usage:
///     NLastGetopt::TOpt& option = ...;
///     TDuration duration;
///     ...
///     option.StoreMappedResultT<TStringBuf>(&duration, &ParseDuration);
TDuration ParseDuration(const TStringBuf& str);

// all modes use common argument format
const TString ArgFormatDescription();

struct TCommandConfig {
    NGRpcProxy::TGRpcClientConfig ClientConfig;

    enum class EServerType {
        MessageBus,
        GRpc,
    };

    struct TServerEndpoint {
        EServerType ServerType;
        TString Address;
        TMaybe<bool> EnableSsl = Nothing();
    };

    static TServerEndpoint ParseServerAddress(const TString& address);
};

extern TCommandConfig CommandConfig;

} // namespace NKikimr


#define MODE_GEN_HELP(enumName, commandName, help, ...) \
    helpStream << "  " << RightPad(commandName, 20) << " - " << help << "\n";

#define APPEND_ENUM(name, command, ...) \
    if (!first) commandsList << ", "; commandsList << command; first = false;

#define MODE_GEN_COMMAND(enumName, commandName, ...) \
    if (mode == defaultValue && stricmp(arg, commandName) == 0) mode = enumName;

#define CLI_MODES_IMPL(MODES_ENUM, DEFAULT_VALUE, MODES_MAP)\
\
enum MODES_ENUM {\
    DEFAULT_VALUE,\
    MODES_MAP(ENUM_VALUE_GEN_NO_VALUE)\
};\
\
template<typename TMode> struct TCliCommands;\
\
template<> struct TCliCommands<MODES_ENUM> {\
    MODES_ENUM Mode;\
\
    TCliCommands(): Mode( (DEFAULT_VALUE) ) {}\
\
    static TString CommandsCsv() {\
        TStringStream commandsList;\
        bool first = true;\
        MODES_MAP(APPEND_ENUM);\
        return commandsList.Str();\
    }\
\
    static TString CommandsDescription(const TString& toolName) {\
        TStringStream helpStream;\
        helpStream << "[global options...] <command> [command options...] [mbus] [messagebus options...]";\
        helpStream << "\n\nAvailable commands are:\n";\
        MODES_MAP(MODE_GEN_HELP);\
        helpStream << "\nSee '" << toolName << " <command> --help' for mode options\n";\
        return helpStream.Str();\
    } \
\
    static MODES_ENUM ParseCommand(const char* arg) {\
        static constexpr auto defaultValue = (DEFAULT_VALUE);\
        MODES_ENUM mode = defaultValue;\
        MODES_MAP(MODE_GEN_COMMAND);\
        return mode;\
    }\
};
