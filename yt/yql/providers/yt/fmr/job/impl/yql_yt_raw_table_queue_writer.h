#include <yt/yql/providers/yt/fmr/utils/yql_yt_raw_table_queue.h>

namespace NYql::NFmr {

struct TFmrRawTableQueueWriterSettings {
    ui64 ChunkSize = 1024 * 1024;
};

class TFmrRawTableQueueWriter: public NYT::TRawTableWriter {
public:
    // enableTableIndexMarking should be false whenever the whole job only ever reads a single
    // section (the common case): a lone section always decodes fine against the decoder's
    // default TableIndex_ (0), so marking would be pure overhead with no correctness benefit.
    TFmrRawTableQueueWriter(
        TFmrRawTableQueue::TPtr rawTableQueue,
        ui32 tableIndex = 0,
        bool enableTableIndexMarking = false,
        const TFmrRawTableQueueWriterSettings& settings = TFmrRawTableQueueWriterSettings()
    );

    void NotifyRowEnd() override;

    // Changes which table_index gets written before subsequent rows. Call this between
    // ParseRecords() calls when a single writer instance relays rows from several distinct
    // physical inputs (e.g. several original tables bundled into one partition).
    void SetTableIndex(ui32 tableIndex);

protected:
    void DoWrite(const void* buf, size_t len) override;

    void DoFlush() override;

private:
    TFmrRawTableQueue::TPtr RawTableQueue_;
    ui32 TableIndex_;
    bool EnableTableIndexMarking_;

    TBuffer BlockContent_;
    // Chunks flushed into the shared union queue may interleave with chunks from other
    // tables' writers, so every chunk must be self-describing: mark it with the active
    // table index whenever a fresh chunk starts, or whenever SetTableIndex() changes it
    // mid-chunk. Only relevant when EnableTableIndexMarking_ is set.
    bool NeedsTableIndexMarker_ = true;

    TFmrRawTableQueueWriterSettings Settings_ = {};
};

} // namespace NYql::NFmr
