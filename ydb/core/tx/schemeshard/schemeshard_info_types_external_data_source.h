#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimr {
namespace NSchemeShard {

struct TExternalDataSourceInfo: TSimpleRefCount<TExternalDataSourceInfo> {
    using TPtr = TIntrusivePtr<TExternalDataSourceInfo>;

    ui64 AlterVersion = 0;
    TString SourceType;
    TString Location;
    TString Installation;
    NKikimrSchemeOp::TAuth Auth;
    NKikimrSchemeOp::TExternalTableReferences ExternalTableReferences;
    NKikimrSchemeOp::TExternalDataSourceProperties Properties;

    void FillProto(NKikimrSchemeOp::TExternalDataSourceDescription& proto, bool withReferences = true) const {
        proto.SetVersion(AlterVersion);
        proto.SetSourceType(SourceType);
        proto.SetLocation(Location);
        proto.SetInstallation(Installation);
        proto.MutableAuth()->CopyFrom(Auth);
        proto.MutableProperties()->CopyFrom(Properties);
        if (withReferences) {
            proto.MutableReferences()->CopyFrom(ExternalTableReferences);
        }
    }
};

}
}
