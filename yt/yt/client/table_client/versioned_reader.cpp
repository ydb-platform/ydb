#include "versioned_reader.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

namespace NYT::NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

class TEmptyVersionedReader
    : public IVersionedReader
{
public:
    explicit TEmptyVersionedReader(int rowCount)
        : RowCount_(rowCount)
    { }

    TFuture<void> Open() override
    {
        return VoidFuture;
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (RowCount_ == 0) {
            return nullptr;
        }

        std::vector<TVersionedRow> rows;
        int rowCount = std::min<i64>(options.MaxRowsPerRead, RowCount_);
        rows.reserve(rowCount);
        for (int index = 0; index < rowCount; ++index) {
            rows.push_back(TVersionedRow());
        }

        RowCount_ -= rowCount;

        return CreateBatchFromVersionedRows(MakeSharedRange(std::move(rows)));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return VoidFuture;
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return NChunkClient::NProto::TDataStatistics();
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return NChunkClient::TCodecStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return true;
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return std::vector<TChunkId>();
    }

private:
    int RowCount_;
};

DEFINE_REFCOUNTED_TYPE(TEmptyVersionedReader)

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateEmptyVersionedReader(int rowCount)
{
    return New<TEmptyVersionedReader>(rowCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
