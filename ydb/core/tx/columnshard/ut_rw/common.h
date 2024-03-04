#pragma once

#include <memory>
#include <vector>

#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/formats/arrow/simple_builder/array.h>
#include <ydb/core/formats/arrow/simple_builder/filler.h>
#include <ydb/core/formats/arrow/simple_builder/batch.h>
#include <ydb/core/wrappers/fake_storage.h>
#include <ydb/core/tx/columnshard/blobs_action/tier/storage.h>
#include <ydb/core/tx/columnshard/blobs_action/storages_manager/manager.h>
#include <ydb/core/tx/tiering/tier/object.h>

#include <arrow/api.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

std::unique_ptr<NEvents::TDataEvents::TEvWrite> PrepareEvWrite(std::shared_ptr<arrow::RecordBatch> batch, const ui64 txId, const ui64 tableId, const ui64 ownerId, const ui64 schemaVersion, const std::vector<ui32> columnsIds);

std::shared_ptr<NOlap::NBlobOperations::NTier::TOperator> PrepareInsertOp(const TActorId& sender, const ui64 tableId);

} // namespace NKikimr