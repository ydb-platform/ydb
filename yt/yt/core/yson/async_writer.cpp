#include "async_writer.h"
#include "detail.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

TAsyncYsonWriter::TAsyncYsonWriter(EYsonType type)
    : Type_(type)
    , SyncWriter_(&Stream_, type)
    , FlushedSize_(std::make_shared<std::atomic<i64>>(0))
{ }

void TAsyncYsonWriter::OnStringScalar(TStringBuf value)
{
    SyncWriter_.OnStringScalar(value);
}

void TAsyncYsonWriter::OnInt64Scalar(i64 value)
{
    SyncWriter_.OnInt64Scalar(value);
}

void TAsyncYsonWriter::OnUint64Scalar(ui64 value)
{
    SyncWriter_.OnUint64Scalar(value);
}

void TAsyncYsonWriter::OnDoubleScalar(double value)
{
    SyncWriter_.OnDoubleScalar(value);
}

void TAsyncYsonWriter::OnBooleanScalar(bool value)
{
    SyncWriter_.OnBooleanScalar(value);
}

void TAsyncYsonWriter::OnEntity()
{
    SyncWriter_.OnEntity();
}

void TAsyncYsonWriter::OnBeginList()
{
    SyncWriter_.OnBeginList();
}

void TAsyncYsonWriter::OnListItem()
{
    SyncWriter_.OnListItem();
}

void TAsyncYsonWriter::OnEndList()
{
    SyncWriter_.OnEndList();
}

void TAsyncYsonWriter::OnBeginMap()
{
    SyncWriter_.OnBeginMap();
}

void TAsyncYsonWriter::OnKeyedItem(TStringBuf key)
{
    SyncWriter_.OnKeyedItem(key);
}

void TAsyncYsonWriter::OnEndMap()
{
    SyncWriter_.OnEndMap();
}

void TAsyncYsonWriter::OnBeginAttributes()
{
    SyncWriter_.OnBeginAttributes();
}

void TAsyncYsonWriter::OnEndAttributes()
{
    SyncWriter_.OnEndAttributes();
}

void TAsyncYsonWriter::OnRaw(TStringBuf yson, EYsonType type)
{
    SyncWriter_.OnRaw(yson, type);
}

void TAsyncYsonWriter::OnRaw(TFuture<TYsonString> asyncStr)
{
    FlushCurrentSegment();
    AsyncSegments_.push_back(asyncStr.Apply(
        BIND([topLevel = SyncWriter_.GetDepth() == 0, type = Type_, flushedSize = FlushedSize_] (const TYsonString& ysonStr) {
            flushedSize->fetch_add(ysonStr.AsStringBuf().size(), std::memory_order::relaxed);
            return TSegment{
                ysonStr,
                ysonStr.GetType() == EYsonType::Node && (!topLevel || type != EYsonType::Node),
            };
        })));
}

i64 TAsyncYsonWriter::GetTotalWrittenSize() const
{
    return FlushedSize_->load(std::memory_order::relaxed) + SyncWriter_.GetTotalWrittenSize();
}

TFuture<TYsonString> TAsyncYsonWriter::Finish()
{
    FlushCurrentSegment();

    auto callback = BIND([type = Type_] (std::vector<TSegment>&& segments) {
        size_t length = 0;
        for (const auto& [ysonStr, trailingSeparator] : segments) {
            length += ysonStr.AsStringBuf().length();
            if (trailingSeparator) {
                length += 1;
            }
        }

        TString result;
        result.reserve(length);
        for (const auto& [ysonStr, trailingSeparator] : segments) {
            result.append(ysonStr.AsStringBuf());
            if (trailingSeparator) {
                result.append(NDetail::ItemSeparatorSymbol);
            }
        }

        return TYsonString(result, type);
    });

    return AllSucceeded(AsyncSegments_).ApplyUnique(std::move(callback));
}

const TAsyncYsonWriter::TAsyncSegments& TAsyncYsonWriter::GetSegments() const
{
    return AsyncSegments_;
}

void TAsyncYsonWriter::FlushCurrentSegment()
{
    SyncWriter_.Flush();
    if (!Stream_.Str().empty()) {
        FlushedSize_->fetch_add(Stream_.Str().length());
        AsyncSegments_.push_back(MakeFuture(TSegment{Stream_.Str(), false}));
        Stream_.Str().clear();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
