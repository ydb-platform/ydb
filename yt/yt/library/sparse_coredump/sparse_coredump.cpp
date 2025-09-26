#include "sparse_coredump.h"

#include <yt/yt/core/concurrency/scheduler.h>

#include <util/generic/buffer.h>
#include <util/generic/size_literals.h>

namespace NYT::NCoreDump {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

// Do not change the value of SparseCoreDumpPageSize.
// The value of SPARSE_CORE_DUMP_PAGE_SIZE from download_core_dump.py is to be the same.
constexpr auto SparseCoreDumpPageSize = 64_KB;

} // namespace

////////////////////////////////////////////////////////////////////////////////

TFileSparseCoreDumpWriter::TFileSparseCoreDumpWriter(TFile* outputFile)
    : OutputFile_(outputFile)
{ }

TFileSparseCoreDumpWriter::~TFileSparseCoreDumpWriter()
{
    OutputFile_->Resize(FileOffset_);
}

void TFileSparseCoreDumpWriter::OnRegularBlock(TSharedRef block)
{
    OutputFile_->Pwrite(block.begin(), block.size(), FileOffset_);
    FileOffset_ += block.size();
}

void TFileSparseCoreDumpWriter::OnZeroBlock(i64 length)
{
    FileOffset_ += length;
}

////////////////////////////////////////////////////////////////////////////////

const TSharedRef TStreamSparseCoreDumpWriter::ZeroBlockHeader = TSharedRef::FromString("0");
const TSharedRef TStreamSparseCoreDumpWriter::RegularBlockHeader = TSharedRef::FromString("1");

TStreamSparseCoreDumpWriter::TStreamSparseCoreDumpWriter(
    NYT::NConcurrency::IAsyncOutputStreamPtr outputStream,
    TDuration writeTimeout)
    : OutputStream_(CreateZeroCopyAdapter(outputStream))
    , WriteTimeout_(writeTimeout)
{ }

void TStreamSparseCoreDumpWriter::OnRegularBlock(TSharedRef block)
{
    WaitFor(OutputStream_->Write(RegularBlockHeader).WithTimeout(WriteTimeout_))
        .ThrowOnError();
    WaitFor(OutputStream_->Write(block).WithTimeout(WriteTimeout_))
        .ThrowOnError();
}

void TStreamSparseCoreDumpWriter::OnZeroBlock(i64 length)
{
    TBuffer page(SparseCoreDumpPageSize);
    for (size_t index = 0; index < sizeof(i64) / sizeof(char); ++index) {
        page.Append(length & 255);
        length >>= 8;
    }
    while (page.size() < SparseCoreDumpPageSize) {
        page.Append('\0');
    }

    WaitFor(OutputStream_->Write(ZeroBlockHeader).WithTimeout(WriteTimeout_))
        .ThrowOnError();
    WaitFor(OutputStream_->Write(TSharedRef(page.data(), page.size(), MakeSharedRangeHolder(MakeStrong(this)))).WithTimeout(WriteTimeout_))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

i64 SparsifyCoreDump(
    NConcurrency::IAsyncInputStreamPtr coreDumpStream,
    TIntrusivePtr<ISparseCoreDumpConsumer> consumer,
    TDuration readTimeout)
{
    auto stream = CreateZeroCopyAdapter(coreDumpStream, SparseCoreDumpPageSize);
    enum EState { DEFAULT, ZERO_BLOCK };
    EState state = EState::DEFAULT;

    i64 bytesWritten = 0;
    i64 zeroBlockLength = 0;

    while (true) {
        auto page = WaitFor(stream->Read().WithTimeout(readTimeout))
            .ValueOrThrow();
        if (!page) {
            break;
        }

        bytesWritten += page.size();

        bool isZeroPage = true;
        for (auto symbol : page) {
            if (symbol) {
                isZeroPage = false;
                break;
            }
        }

        if (isZeroPage) {
            zeroBlockLength += page.size();
            state = EState::ZERO_BLOCK;
        } else {
            if (state == EState::ZERO_BLOCK) {
                consumer->OnZeroBlock(zeroBlockLength);
                zeroBlockLength = 0;
            }
            consumer->OnRegularBlock(page);
            state = EState::DEFAULT;
        }
    }

    if (state == EState::ZERO_BLOCK) {
        consumer->OnZeroBlock(zeroBlockLength);
    }

    return bytesWritten;
}

i64 WriteSparseCoreDump(IInputStream* in, TFile* out)
{
    auto asyncStream = NConcurrency::CreateAsyncAdapter(in);
    auto writer = New<TFileSparseCoreDumpWriter>(out);
    return SparsifyCoreDump(asyncStream, writer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCoreDump
