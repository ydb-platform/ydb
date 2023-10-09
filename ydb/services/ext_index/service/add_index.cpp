#include "add_index.h"
#include <ydb/core/tx/tx_proxy/upload_rows.h>
#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NCSIndex {

namespace {
class TCellsWriter: public NArrow::IRowWriter {
private:
    std::shared_ptr<NTxProxy::TUploadRows> Rows = std::make_shared<NTxProxy::TUploadRows>();
    const std::vector<ui64>& Hashes;
    ui32 Index = 0;
    const ui32 PKColumnsCount;
public:
    TCellsWriter(const ui32 rowsCount, const ui32 pkColumnsCount, const std::vector<ui64>& hashes)
        : Hashes(hashes)
        , PKColumnsCount(pkColumnsCount)
    {
        Rows->reserve(rowsCount);
    }

    std::shared_ptr<NTxProxy::TUploadRows> GetRows() const {
        return Rows;
    }

    virtual void AddRow(const TConstArrayRef<TCell>& cells) override {
        TVector<TCell> key;
        key.reserve(1 + PKColumnsCount);
        key.emplace_back(TCell::Make(Hashes[Index]));
        Y_ABORT_UNLESS(PKColumnsCount == cells.size());
        for (auto&& c : cells) {
            key.emplace_back(c);
        }
        TSerializedCellVec serializedKey(key);
        Rows->emplace_back(std::move(serializedKey), "");
        ++Index;
    }
};
}

void TIndexUpsertActor::Bootstrap() {
    std::shared_ptr<NTxProxy::TUploadTypes> types = std::make_shared<NTxProxy::TUploadTypes>();
    std::vector<std::pair<TString, NScheme::TTypeInfo>> yqlTypes;

    std::vector<std::shared_ptr<arrow::Array>> pkColumns;
    for (auto&& i : PKFields) {
        auto f = Data->schema()->GetFieldByName(i);
        if (!f) {
            ExternalController->OnIndexUpsertionFailed("incorrect field for pk");
            PassAway();
            return;
        }
        pkColumns.emplace_back(Data->GetColumnByName(i));
        auto ydbType = NMetadata::NInternal::TYDBType::ConvertArrowToYDB(f->type()->id());
        if (!ydbType) {
            ExternalController->OnIndexUpsertionFailed("incorrect arrow type for ydb field");
            PassAway();
            return;
        }
        types->emplace_back(i, NMetadata::NInternal::TYDBType::Primitive(*ydbType));
    }

    const std::vector<ui64> hashes = IndexInfo.GetExtractor()->ExtractIndex(Data);
    if (hashes.size() != (size_t)Data->num_rows()) {
        ExternalController->OnIndexUpsertionFailed("inconsistency hashes: " + ::ToString(hashes.size()) + " != " + ::ToString(Data->num_rows()));
        PassAway();
        return;
    }
    TCellsWriter writer(Data->num_rows(), pkColumns.size(), hashes);
    {
        auto yqlTypes = NMetadata::NInternal::TYDBType::ConvertYDBToYQL(*types);
        if (!yqlTypes) {
            ExternalController->OnIndexUpsertionFailed("cannot convert types ydb -> yql");
            PassAway();
            return;
        }
        NArrow::TArrowToYdbConverter ydbConverter(*yqlTypes, writer);
        TString errorMessage;
        if (!ydbConverter.Process(*Data, errorMessage)) {
            ExternalController->OnIndexUpsertionFailed(errorMessage);
            PassAway();
            return;
        }
    }
    for (auto&& i : *types) {
        i.first = "pk_" + i.first;
    }
    types->insert(types->begin(), std::make_pair("index_hash", NMetadata::NInternal::TYDBType::Primitive(Ydb::Type::UINT64)));
    auto actor = NTxProxy::CreateUploadRowsInternal(SelfId(), IndexTablePath, types, writer.GetRows());
    Become(&TIndexUpsertActor::StateMain);
    Register(actor);
}

void TIndexUpsertActor::Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev) {
    auto g = PassAwayGuard();
    if (ev->Get()->Status == Ydb::StatusIds::SUCCESS) {
        ExternalController->OnIndexUpserted();
    } else {
        ExternalController->OnIndexUpsertionFailed("incorrect response code:" + ev->Get()->Issues.ToString());
    }
}

}
