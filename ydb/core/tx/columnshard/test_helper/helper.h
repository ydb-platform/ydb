#pragma once
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimrSchemeOp {
class TOlapColumnDescription;
}

namespace NKikimr::NOlap {

class TTestStoragesManager: public NOlap::IStoragesManager {
private:
    using TBase = NOlap::IStoragesManager;
    TIntrusivePtr<TTabletStorageInfo> TabletInfo = new TTabletStorageInfo();
    std::shared_ptr<NOlap::NDataSharing::TSharedBlobsManager> SharedBlobsManager = std::make_shared<NOlap::NDataSharing::TSharedBlobsManager>(NOlap::TTabletId(0));
protected:
    virtual bool DoLoadIdempotency(NTable::TDatabase& /*database*/) override {
        return true;
    }

    virtual std::shared_ptr<NOlap::IBlobsStorageOperator> DoBuildOperator(const TString& storageId) override;
    virtual const std::shared_ptr<NDataSharing::TSharedBlobsManager>& DoGetSharedBlobsManager() const override {
        return SharedBlobsManager;
    }
public:
};


}

namespace NKikimr::NArrow::NTest {

class TTestColumn {
private:
    YDB_ACCESSOR_DEF(TString, Name);
    YDB_ACCESSOR_DEF(NScheme::TTypeInfo, Type);
    YDB_ACCESSOR_DEF(TString, StorageId);
public:
    explicit TTestColumn(const TString& name, const NScheme::TTypeInfo& type)
        : Name(name)
        , Type(type) {

    }

    NKikimrSchemeOp::TOlapColumnDescription CreateColumn(const ui32 id) const;
    static std::vector<std::pair<TString, NScheme::TTypeInfo>> ConvertToPairs(const std::vector<TTestColumn>& columns);
    static THashMap<TString, NScheme::TTypeInfo> ConvertToHash(const std::vector<TTestColumn>& columns);
    static std::vector<TTestColumn> BuildFromPairs(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns);
    static std::vector<TTestColumn> CropSchema(const std::vector<TTestColumn>& input, const ui32 size);
};

}

namespace NKikimr::NArrow {

std::vector<std::shared_ptr<arrow::Field>> MakeArrowFields(const std::vector<NTest::TTestColumn>& columns, const std::set<std::string>& notNullColumns = {});
std::shared_ptr<arrow::Schema> MakeArrowSchema(const std::vector<NTest::TTestColumn>& columns, const std::set<std::string>& notNullColumns = {});

}
