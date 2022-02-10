#include "reader.h"
#include <library/cpp/yson_pull/detail/reader.h>

using namespace NYsonPull;

TReader::TReader(
    THolder<NInput::IStream> stream,
    EStreamType mode)
    : Stream_{std::move(stream)}
    , Impl_{MakeHolder<NDetail::reader_impl>(*Stream_, mode)} {
}

TReader::TReader(TReader&& other) noexcept
    : Stream_{std::move(other.Stream_)}
    , Impl_{std::move(other.Impl_)} {
}

TReader::~TReader() {
}

const TEvent& TReader::NextEvent() {
    return Impl_->next_event();
}

const TEvent& TReader::LastEvent() const noexcept {
    return Impl_->last_event();
}
