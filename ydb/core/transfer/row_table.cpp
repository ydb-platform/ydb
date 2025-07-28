#include "table_kind_state.h"
#include "uploader.h"

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

namespace NKikimr::NReplication::NTransfer {

using TData = TVector<std::pair<TSerializedCellVec, TString>>;

class TRowTableState : public ITableKindState {
public:
    TRowTableState(
        const TActorId& selfId,
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate>& result
    )
        : ITableKindState(selfId, result)
    {
        Path = JoinPath(result->ResultSet.front().Path);
    }

    NKqp::IDataBatcherPtr CreateDataBatcher() override {
        return NKqp::CreateRowDataBatcher(GetScheme()->ColumnsMetadata, GetScheme()->WriteIndex, nullptr, GetScheme()->ReadIndex);
    }

    bool Flush() override {
        if (Batchers.empty() || !BatchSize()) {
            return false;
        }

        std::unordered_map<TString, std::shared_ptr<TData>> tableData;

        for (auto& [tablePath, batcher] : Batchers)  {
            NKqp::IDataBatchPtr batch = batcher->Build();

            auto data = reinterpret_pointer_cast<TOwnedCellVecBatch>(batch->ExtractBatch());
            Y_VERIFY(data);

            auto d = std::make_shared<TData>();
            for (auto r : *data) {
                TVector<TCell> key;
                TVector<TCell> value;

                for (size_t i = 0; i < r.size(); ++i) {
                    auto& column = GetScheme()->TableColumns[i];
                    if (column.KeyColumn) {
                        key.push_back(r[i]);
                    } else {
                        value.push_back(r[i]);
                    }
                }

                TSerializedCellVec serializedKey(key);
                TString serializedValue = TSerializedCellVec::Serialize(value);

                d->emplace_back(serializedKey, serializedValue);
            }

            tableData[tablePath] = d;
        }

        TActivationContext::AsActorContext().RegisterWithSameMailbox(
            new TTableUploader(SelfId, GetScheme(), std::move(tableData))
        );

        Batchers.clear();

        return true;
    }

private:
    TString Path;
};

std::unique_ptr<ITableKindState> CreateRowTableState(const TActorId& selfId, TAutoPtr<NSchemeCache::TSchemeCacheNavigate>& result) {
    return std::make_unique<TRowTableState>(selfId, result);
}

template<>
IActor* TTableUploader<TData>::CreateUploaderInternal(const TString& tablePath, const std::shared_ptr<TData>& data, ui64 cookie) {
    return NTxProxy::CreateUploadRowsInternal(SelfId(), tablePath, Scheme->Types, data, NTxProxy::EUploadRowsMode::Normal, false, false, cookie);
}

}
