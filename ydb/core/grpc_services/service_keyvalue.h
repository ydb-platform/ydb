#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

    class IRequestOpCtx;
    class IFacilityProvider;

    void DoCreateVolumeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoDropVolumeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoAlterVolumeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoDescribeVolumeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoListLocalPartitionsKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

    void DoAcquireLockKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoExecuteTransactionKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoReadKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoReadRangeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoListRangeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoGetStorageChannelStatusKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

} // NKikimr::NGRpcService
