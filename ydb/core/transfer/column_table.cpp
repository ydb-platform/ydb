#include "table_kind_state.h"
#include "uploader.h"

#include <ydb/core/tx/tx_proxy/upload_columns.h>

namespace NKikimr::NReplication::NTransfer {

class TColumnTableState : public ITableKindState {
public:
    TColumnTableState(
        const TActorId& selfId,
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate>& result
    )
        : ITableKindState(selfId, result)
    {
        Path = JoinPath(result->ResultSet.front().Path);
    }

    NKqp::IDataBatcherPtr CreateDataBatcher() override {
        return NKqp::CreateColumnDataBatcher(Scheme->ColumnsMetadata, Scheme->WriteIndex, nullptr, GetScheme()->ReadIndex);
    }

    bool Flush() override {
        if (Batchers.empty() || !BatchSize()) {
            return false;
        }

        std::unordered_map<TString, std::shared_ptr<arrow::RecordBatch>> tableData;

        for (auto& [tablePath, batcher] : Batchers)  {
            NKqp::IDataBatchPtr batch = batcher->Build();
            tableData[tablePath] = reinterpret_pointer_cast<arrow::RecordBatch>(batch->ExtractBatch());
        }

        UploaderActorId = TActivationContext::AsActorContext().RegisterWithSameMailbox(
            new TTableUploader(SelfId, GetScheme(), std::move(tableData))
        );

        Batchers.clear();

        return true;
    }

private:
    TString Path;
};

std::unique_ptr<ITableKindState> CreateColumnTableState(const TActorId& selfId, TAutoPtr<NSchemeCache::TSchemeCacheNavigate>& result) {
    return std::make_unique<TColumnTableState>(selfId, result);
}

template<>
IActor* TTableUploader<arrow::RecordBatch>::CreateUploaderInternal(const TString& tablePath, const std::shared_ptr<arrow::RecordBatch>& data, ui64 cookie) {
    return NTxProxy::CreateUploadColumnsInternal(SelfId(), tablePath, Scheme->Types, data, cookie);
}

}
