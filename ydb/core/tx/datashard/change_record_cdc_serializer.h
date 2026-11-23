#pragma once

#include "datashard_user_table.h"

#include <util/generic/ptr.h>

namespace NKikimrClient {
    class TPersQueuePartitionRequest_TCmdWrite;
}

namespace NKikimr::NChangeExchange {
    class IChangeRecord;
}

namespace NKikimr::NDataShard {

// Fast zstd strategy used when a stream does not set a level. 0 means unset.
inline constexpr int DefaultCdcZstdCompressionLevel = -1;

// Same value as TDataShardConfig::CdcZstdMinPayloadBytes. Used when the
// process has no actor app data, such as a serializer unit test.
// A non-zero DataShardControls.CdcZstdMinPayloadBytes immediate control
// overrides the startup config on the next record.
inline constexpr ui64 DefaultCdcZstdMinPayloadBytes = 200;

// One zstd context for every serializer of a single CDC sender.
// TCdcChangeSenderPartition actors run on the main sender mailbox, so
// Compress is never called from two threads. The context itself is created
// on the first payload that is actually compressed.
class TCdcZstdContext : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TCdcZstdContext>;

    TCdcZstdContext();
    ~TCdcZstdContext() override;

    size_t Compress(void* dst, size_t dstCapacity, const void* src, size_t srcSize, int level) const;

private:
    struct TState;
    THolder<TState> State;
};

class TChangeRecord;

class IChangeRecordSerializer {
protected:
    using TCmdWrite = NKikimrClient::TPersQueuePartitionRequest_TCmdWrite;

public:
    virtual ~IChangeRecordSerializer() = default;
    virtual void Serialize(TCmdWrite& cmd, const TChangeRecord& record) = 0;
    virtual TString DebugString(const TChangeRecord& record) = 0;

    void Serialize(TCmdWrite& cmd, const NChangeExchange::IChangeRecord& record);
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
    // Empty means this serializer owns a private context. The CDC sender sets
    // one shared context for all partition serializers of the stream.
    TCdcZstdContext::TPtr ZstdContext;

    static TChangeRecordSerializerOpts DebugOpts();
};

IChangeRecordSerializer* CreateChangeRecordSerializer(const TChangeRecordSerializerOpts& opts);
IChangeRecordSerializer* CreateChangeRecordDebugSerializer();

}
