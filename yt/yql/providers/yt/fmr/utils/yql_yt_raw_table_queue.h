#pragma once

#include <util/generic/queue.h>
#include <util/system/condvar.h>
#include <yt/cpp/mapreduce/interface/io.h>

// TODO - проверить весь код, писал не я

namespace NYql::NFmr {

struct TFmrRawTableQueueSettings {
    ui64 MaxInflightBytes = 128 * 1024 * 1024;
};

class TFmrRawTableQueue: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TFmrRawTableQueue>;

    virtual ~TFmrRawTableQueue() = default;

    TFmrRawTableQueue(ui64 inputStreamsNum, const TFmrRawTableQueueSettings& settings = TFmrRawTableQueueSettings());

    void AddRow(TBuffer&& rowContent);

    TMaybe<TBuffer> PopRow();

    void NotifyInputFinished(ui64 input_id);

    void SetException(const TString& exceptionMessage);

private:
    bool IsFinished() const;

    TQueue<TBuffer> RowsQueue_;
    ui64 CurInflightBytes_ = 0;
    const ui64 InputStreamsNum_;
    TMutex QueueMutex_ = TMutex();
    TCondVar QueueInflightBytesCondVar_;
    TCondVar QueueSizeCondVar_;

    TMaybe<TString> ExceptionMessage_ = Nothing();
    std::unordered_set<ui64> FinishedInputs_;
    TFmrRawTableQueueSettings Settings_;
};

} // namespace NYql::NFmr
