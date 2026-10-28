#pragma once

#include "datashard_user_table.h"

namespace NKikimrClient {
    class TPersQueuePartitionRequest_TCmdWrite;
}

namespace NKikimr::NDataShard {

class TChangeRecord;

class IChangeRecordSerializer {
protected:
    using TCmdWrite = NKikimrClient::TPersQueuePartitionRequest_TCmdWrite;

public:
    virtual ~IChangeRecordSerializer() = default;
    virtual void Serialize(TCmdWrite& cmd, const TChangeRecord& record) = 0;
    virtual TString DebugString(const TChangeRecord& record) = 0;
};

// A data row shorter than this stays raw even when the stream asks for zstd.
// Heartbeats are always raw. A longer row is stored with zstd when the
// compressed bytes are shorter than the row.
inline constexpr size_t CdcZstdMinPayloadBytes = 512;

struct TChangeRecordSerializerOpts {
    TUserTable::TCdcStream::EFormat StreamFormat = NKikimrSchemeOp::ECdcStreamFormatInvalid;
    TUserTable::TCdcStream::EMode StreamMode = NKikimrSchemeOp::ECdcStreamModeInvalid;
    TString AwsRegion;
    bool VirtualTimestamps = false;
    ui64 ShardId = 0;
    bool Debug = false;
    bool UserSIDs = false;
    bool TraceIds = false;
    TUserTable::TCdcStream::ECompression Compression = NKikimrSchemeOp::ECdcStreamCompressionUnspecified;

    static TChangeRecordSerializerOpts DebugOpts();
};

IChangeRecordSerializer* CreateChangeRecordSerializer(const TChangeRecordSerializerOpts& opts);
IChangeRecordSerializer* CreateChangeRecordDebugSerializer();

}
