#pragma once

#include "datashard_user_table.h"

namespace NKikimrClient {
    class TPersQueuePartitionRequest_TCmdWrite;
}

namespace NKikimr::NDataShard {

class TChangeRecord;

// Short CDC payloads stay raw. A zstd frame is several bytes, and even level -1
// still scans the whole input, so rows under ~200 bytes cost more CPU than they
// save. That covers a key plus a few small columns, which is most OLTP traffic.
inline constexpr size_t CdcZstdMinPayloadBytes = 200;

// Fast zstd strategy used when the stream does not set a level. 0 means unset.
// An explicit level on the stream overrides this.
inline constexpr int DefaultCdcZstdCompressionLevel = -1;

class IChangeRecordSerializer {
protected:
    using TCmdWrite = NKikimrClient::TPersQueuePartitionRequest_TCmdWrite;

public:
    virtual ~IChangeRecordSerializer() = default;
    virtual void Serialize(TCmdWrite& cmd, const TChangeRecord& record) = 0;
    virtual TString DebugString(const TChangeRecord& record) = 0;
};

struct TChangeRecordSerializerOpts {
    TUserTable::TCdcStream::EFormat StreamFormat = NKikimrSchemeOp::ECdcStreamFormatInvalid;
    TUserTable::TCdcStream::EMode StreamMode = NKikimrSchemeOp::ECdcStreamModeInvalid;
    TString AwsRegion;
    bool VirtualTimestamps = false;
    ui64 ShardId = 0;
    bool Debug = false;
    bool UserSIDs = false;
    bool TraceIds = false;
    TUserTable::TCdcStream::ECompression Compression = NKikimrSchemeOp::ECdcStreamCompressionNone;
    i32 CompressionLevel = 0;

    static TChangeRecordSerializerOpts DebugOpts();
};

IChangeRecordSerializer* CreateChangeRecordSerializer(const TChangeRecordSerializerOpts& opts);
IChangeRecordSerializer* CreateChangeRecordDebugSerializer();

}
