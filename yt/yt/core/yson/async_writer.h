#pragma once

#include "public.h"
#include "async_consumer.h"
#include "writer.h"
#include "string.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

class TAsyncYsonWriter final
    : public IAsyncYsonConsumer
    , private TNonCopyable
{
public:
    //! Represents a YSON segment obtained either from a sequence of contiguous
    //! synchronous OnXXX, or an asynchronous OnRaw(TFuture<TYsonString>) call.
    //! Second element of a pair indicates whether a segment must be extended by
    //! a semicolon.
    using TSegment = std::pair<TYsonString, bool>;
    using TAsyncSegments = std::vector<TFuture<TSegment>>;

    explicit TAsyncYsonWriter(EYsonType type = EYsonType::Node);

    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;
    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;
    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;
    void OnBeginAttributes() override;
    void OnEndAttributes() override;
    void OnRaw(TStringBuf yson, EYsonType type) override;
    void OnRaw(TFuture<TYsonString> asyncStr) override;

    TFuture<TYsonString> Finish();
    const TAsyncSegments& GetSegments() const;

    i64 GetTotalWrittenSize() const;

private:
    const EYsonType Type_;

    TStringStream Stream_;
    TBufferedBinaryYsonWriter SyncWriter_;

    TAsyncSegments AsyncSegments_;
    std::shared_ptr<std::atomic<i64>> FlushedSize_;

    void FlushCurrentSegment();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

