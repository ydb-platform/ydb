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

struct TChangeRecordSerializerOpts {
    TUserTable::TCdcStream::EFormat StreamFormat = NKikimrSchemeOp::ECdcStreamFormatInvalid;
    TUserTable::TCdcStream::EMode StreamMode = NKikimrSchemeOp::ECdcStreamModeInvalid;
    TString AwsRegion;
    bool VirtualTimestamps = false;
    ui64 ShardId = 0;
    bool Debug = false;

    static TChangeRecordSerializerOpts DebugOpts();
};

IChangeRecordSerializer* CreateChangeRecordSerializer(const TChangeRecordSerializerOpts& opts);
IChangeRecordSerializer* CreateChangeRecordDebugSerializer();

}
