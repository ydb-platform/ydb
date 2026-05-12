#pragma once

#include <memory>

namespace NKikimr::NGRpcService {

    class IRequestOpCtx;
    class IRequestNoOpCtx;
    class IFacilityProvider;

    struct TKeyValueRequestSettings {
        bool UseCustomSerialization = false;
    };

    void DoCreateVolumeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoDropVolumeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoAlterVolumeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoDescribeVolumeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoListLocalPartitionsKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

    void DoAcquireLockKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoExecuteTransactionKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoReadKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoReadKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&, const TKeyValueRequestSettings& settings);
    void DoReadRangeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoReadRangeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&, const TKeyValueRequestSettings& settings);
    void DoListRangeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
    void DoGetStorageChannelStatusKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);

    void DoAcquireLockKeyValueV2(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&);
    void DoExecuteTransactionKeyValueV2(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&);
    void DoReadKeyValueV2(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&);
    void DoReadKeyValueV2(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&, const TKeyValueRequestSettings& settings);
    void DoReadRangeKeyValueV2(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&);
    void DoReadRangeKeyValueV2(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&, const TKeyValueRequestSettings& settings);
    void DoListRangeKeyValueV2(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&);
    void DoGetStorageChannelStatusKeyValueV2(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&);

} // NKikimr::NGRpcService
