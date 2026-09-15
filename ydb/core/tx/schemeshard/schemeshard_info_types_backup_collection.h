#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/ptr.h>

namespace NKikimr {
namespace NSchemeShard {

struct TBackupCollectionInfo : TSimpleRefCount<TBackupCollectionInfo> {
    using TPtr = TIntrusivePtr<TBackupCollectionInfo>;

    static TPtr New() {
        return new TBackupCollectionInfo();
    }

    static TPtr Create(const NKikimrSchemeOp::TBackupCollectionDescription& desc) {
        TPtr result = New();

        result->Description = desc;

        return result;
    }

    ui64 AlterVersion = 0;
    NKikimrSchemeOp::TBackupCollectionDescription Description;
};

}
}
