#include "helper.h"
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/storage.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/wrappers/fake_storage_config.h>
#include <ydb/core/wrappers/fake_storage.h>
#include <ydb/core/tx/columnshard/blobs_action/tier/storage.h>

namespace NKikimr::NArrow::NTest {

NKikimrSchemeOp::TOlapColumnDescription TTestColumn::CreateColumn(const ui32 id) const {
    NKikimrSchemeOp::TOlapColumnDescription col;
    col.SetId(id);
    col.SetName(Name);
    if (StorageId) {
        col.SetStorageId(StorageId);
    }
    auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(Type, "");
    col.SetTypeId(columnType.TypeId);
    if (columnType.TypeInfo) {
        *col.MutableTypeInfo() = *columnType.TypeInfo;
    }
    return col;
}

std::vector<std::pair<TString, NKikimr::NScheme::TTypeInfo>> TTestColumn::ConvertToPairs(const std::vector<TTestColumn>& columns) {
    std::vector<std::pair<TString, NScheme::TTypeInfo>> result;
    for (auto&& i : columns) {
        result.emplace_back(std::make_pair(i.GetName(), i.GetType()));
    }
    return result;
}

std::vector<NKikimr::NArrow::NTest::TTestColumn> TTestColumn::BuildFromPairs(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns) {
    std::vector<TTestColumn> result;
    for (auto&& i : columns) {
        result.emplace_back(i.first, i.second);
    }
    return result;
}

THashMap<TString, NKikimr::NScheme::TTypeInfo> TTestColumn::ConvertToHash(const std::vector<TTestColumn>& columns) {
    THashMap<TString, NScheme::TTypeInfo> result;
    for (auto&& i : columns) {
        result.emplace(i.GetName(), i.GetType());
    }
    return result;
}

std::vector<NKikimr::NArrow::NTest::TTestColumn> TTestColumn::CropSchema(const std::vector<TTestColumn>& input, const ui32 size) {
    AFL_VERIFY(input.size() >= size);
    return std::vector<TTestColumn>(input.begin(), input.begin() + size);
}

}

namespace NKikimr::NArrow {

std::vector<std::shared_ptr<arrow::Field>> MakeArrowFields(const std::vector<NTest::TTestColumn>& columns, const std::set<std::string>& notNullColumns /*= {}*/) {
    return MakeArrowFields(NTest::TTestColumn::ConvertToPairs(columns), notNullColumns);
}

std::shared_ptr<arrow::Schema> MakeArrowSchema(const std::vector<NTest::TTestColumn>& columns, const std::set<std::string>& notNullColumns /*= {}*/) {
    return MakeArrowSchema(NTest::TTestColumn::ConvertToPairs(columns), notNullColumns);
}

}

namespace NKikimr::NOlap {

std::shared_ptr<NKikimr::NOlap::IBlobsStorageOperator> TTestStoragesManager::DoBuildOperator(const TString& storageId) {
    if (storageId == TBase::DefaultStorageId) {
        return std::make_shared<NOlap::NBlobOperations::NBlobStorage::TOperator>(storageId, NActors::TActorId(), TabletInfo,
            1, SharedBlobsManager->GetStorageManagerGuarantee(TBase::DefaultStorageId));
    } else if (storageId == TBase::MemoryStorageId) {
        Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->SetSecretKey("fakeSecret");
        return std::make_shared<NOlap::NBlobOperations::NTier::TOperator>(storageId, NActors::TActorId(), std::make_shared<NWrappers::NExternalStorage::TFakeExternalStorageConfig>("fakeBucket", "fakeSecret"),
            SharedBlobsManager->GetStorageManagerGuarantee(storageId));
    } else {
        return nullptr;
    }
}

}