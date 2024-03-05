#include "common.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/simple_builder/array.h>
#include <ydb/core/formats/arrow/simple_builder/batch.h>
#include <ydb/core/formats/arrow/simple_builder/filler.h>
#include <ydb/core/tx/columnshard/blobs_action/storages_manager/manager.h>
#include <ydb/core/tx/columnshard/blobs_action/tier/storage.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/tiering/tier/object.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <arrow/api.h>
#include <library/cpp/testing/unittest/registar.h>

#include <memory>
#include <vector>

namespace NKikimr {

namespace NTier = NOlap::NBlobOperations::NTier;
using TFakeExternalStorage = NWrappers::NExternalStorage::TFakeExternalStorage;

std::unique_ptr<NEvents::TDataEvents::TEvWrite> PrepareEvWrite(std::shared_ptr<arrow::RecordBatch> batch,
                                                               const ui64 txId, const ui64 tableId, const ui64 ownerId,
                                                               const ui64 schemaVersion,
                                                               const std::vector<ui32> columnsIds) {
    TString blobData = NArrow::SerializeBatchNoCompression(batch);

    auto evWrite = std::make_unique<NEvents::TDataEvents::TEvWrite>(txId, NKikimrDataEvents::TEvWrite::MODE_PREPARE);
    const ui64 payloadIndex =
        NEvWrite::TPayloadWriter<NEvents::TDataEvents::TEvWrite>(*evWrite).AddDataToPayload(std::move(blobData));
    evWrite->AddOperation(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE, {ownerId, tableId, schemaVersion},
                          columnsIds, payloadIndex, NKikimrDataEvents::FORMAT_ARROW);

    return evWrite;
}

std::shared_ptr<NTier::TOperator> PrepareInsertOp(const TActorId& sender, const ui64 tableId) {
    const auto storageId = "some storageId";
    const TActorIdentity tabletActorID(sender);

    auto sharedBlobsManager = std::make_shared<NOlap::NDataSharing::TSharedBlobsManager>(NOlap::TTabletId{tableId});

    NKikimrSchemeOp::TStorageTierConfig cfgProto;
    cfgProto.SetName("some_name");

    ::NKikimrSchemeOp::TS3Settings s3_settings;
    s3_settings.set_secretkey(Singleton<TFakeExternalStorage>()->GetSecretKey());
    s3_settings.set_endpoint("fake");

    *cfgProto.MutableObjectStorage() = s3_settings;
    NColumnShard::NTiers::TTierConfig cfg("tier_name", cfgProto);

    auto* tierManager = new NColumnShard::NTiers::TManager(tableId, tabletActorID, cfg);

    tierManager->Start(nullptr);

    return std::make_shared<NTier::TOperator>(storageId, tabletActorID, tierManager,
                                              sharedBlobsManager->GetStorageManagerGuarantee(storageId));
}

}   // namespace NKikimr