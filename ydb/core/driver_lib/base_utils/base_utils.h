#pragma once

#include <ydb/public/lib/deprecated/client/msgbus_client.h>
#include <ydb/core/driver_lib/cli_config_base/config_base.h>

#include <ydb/core/protos/config.pb.h>

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>
#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/system/hostname.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/string_utils/parse_size/parse_size.h>
#include <library/cpp/svnversion/svnversion.h>

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_defs.h>

namespace NKikimr {

struct TCmdFormatConfig {
    TString Path;
    NSize::TSize DiskSize;
    NSize::TSize ChunkSize;
    NSize::TSize SectorSize;
    ui64 Guid;
    NPDisk::TMainKey MainKey;
    TString TextMessage;

    TCmdFormatConfig();

    void Parse(int argc, char **argv);
};

struct TCmdFormatInfoConfig {
    TString Path;
    TVector<NPDisk::TKey> MainKeyTmp; // required for .AppendTo()
    NPDisk::TMainKey MainKey;
    bool IsVerbose;

    TCmdFormatInfoConfig();

    void Parse(int argc, char **argv);
};

struct TCmdNodeByHostConfig {
    TString NamingFile;
    TString Hostname;
    i32 Port;

    TCmdNodeByHostConfig();

    void Parse(int argc, char **argv);
};

struct TCmdFormatUtilConfig {
    TString FormatFile;
    ui32 NodeId;

    TCmdFormatUtilConfig ();

    void Parse(int argc, char **argv);
};

}
