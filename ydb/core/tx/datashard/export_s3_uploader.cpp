#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_s3_base_uploader.h"

namespace NKikimr {
namespace NDataShard {

class TS3Uploader: public TS3UploaderBase<TS3Uploader> {
protected:
    bool NeedToResolveProxy() const override {
        return false;
    }

    void ResolveProxy() override {
        Y_FAIL("unreachable");
    }

public:
    using TS3UploaderBase::TS3UploaderBase;

}; // TS3Uploader

IActor* TS3Export::CreateUploader(
        const TActorId& dataShard,
        ui64 txId,
        const TTableColumns& columns,
        const TTask& task) const
{
    auto scheme = (task.GetShardNum() == 0)
        ? GenYdbScheme(columns, task.GetTable())
        : Nothing();

    return new TS3Uploader(dataShard, txId, task, std::move(scheme));
}

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
