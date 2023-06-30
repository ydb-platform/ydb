#include "table_output.h"

namespace NYT::NTableClient {

using namespace NFormats;

////////////////////////////////////////////////////////////////////////////////

TTableOutput::TTableOutput(std::unique_ptr<IParser> parser)
    : Parser_(std::move(parser))
{ }

TTableOutput::~TTableOutput() = default;

void TTableOutput::DoWrite(const void* buf, size_t len)
{
    YT_VERIFY(ParserValid_);
    try {
        Parser_->Read(TStringBuf(static_cast<const char*>(buf), len));
    } catch (const std::exception& ex) {
        ParserValid_ = false;
        throw;
    }
}

void TTableOutput::DoFinish()
{
    if (ParserValid_) {
        // Dump everything into consumer.
        Parser_->Finish();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
