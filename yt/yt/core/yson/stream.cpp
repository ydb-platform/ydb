#include "stream.h"
#include "parser.h"

#include <yt/yt/core/concurrency/async_stream.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/yson/parser.h>

namespace NYT::NYson {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const size_t ParseBufferSize = 1 << 16;

////////////////////////////////////////////////////////////////////////////////

TYsonInput::TYsonInput(IInputStream* stream, EYsonType type)
    : AsyncStream_(nullptr)
    , Stream_(stream)
    , Type_(type)
{ }

TYsonInput::TYsonInput(
    const NConcurrency::IAsyncZeroCopyInputStreamPtr& asyncStream,
    EYsonType type)
    : AsyncStream_(asyncStream)
    , Stream_(nullptr)
    , Type_(type)
{ }

////////////////////////////////////////////////////////////////////////////////

TYsonOutput::TYsonOutput(IOutputStream* stream, EYsonType type)
    : Stream_(stream)
    , Type_(type)
{ }

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonInput& input, IYsonConsumer* consumer)
{
    ParseYson(input, consumer);
}

void ParseYson(
    const TYsonInput& input,
    IYsonConsumer* consumer,
    bool enableLinePositionInfo)
{
    TYsonParser parser(consumer, input.GetType(), {.EnableLinePositionInfo = enableLinePositionInfo});
    if (input.GetStream()) {
        char buffer[ParseBufferSize];
        while (true) {
            size_t bytesRead = input.GetStream()->Read(buffer, ParseBufferSize);
            if (bytesRead == 0) {
                break;
            }
            parser.Read(TStringBuf(buffer, bytesRead));
        }
    } else {
        while (true) {
            auto buffer = WaitFor(input.AsyncStream()->Read())
                .ValueOrThrow();

            if (buffer.Empty()) {
                break;
            }

            parser.Read(TStringBuf(buffer.Begin(), buffer.Size()));
        }
    }

    parser.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
