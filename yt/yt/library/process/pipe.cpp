#include "pipe.h"
#include "private.h"
#include "io_dispatcher.h"

#include <yt/yt/core/net/connection.h>

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/fs.h>

#include <sys/types.h>
#include <sys/stat.h>

namespace NYT::NPipes {

using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = PipesLogger;

////////////////////////////////////////////////////////////////////////////////

TNamedPipe::TNamedPipe(const TString& path, std::optional<int> capacity, bool owning)
    : Path_(path)
    , Capacity_(capacity)
    , Owning_(owning)
{ }

TNamedPipe::~TNamedPipe()
{
    if (!Owning_) {
        return;
    }

    if (unlink(Path_.c_str()) == -1) {
        YT_LOG_INFO(TError::FromSystem(), "Failed to unlink pipe %v", Path_);
    }
}

TNamedPipePtr TNamedPipe::Create(const TString& path, int permissions, std::optional<int> capacity)
{
    auto pipe = New<TNamedPipe>(path, capacity, /*owning*/ true);
    pipe->Open(permissions);
    YT_LOG_DEBUG("Named pipe created (Path: %v, Permissions: %v)", path, permissions);
    return pipe;
}

TNamedPipePtr TNamedPipe::FromPath(const TString& path)
{
    return New<TNamedPipe>(path, /*capacity*/ std::nullopt, /*owning*/ false);
}

void TNamedPipe::Open(int permissions)
{
    if (mkfifo(Path_.c_str(), permissions) == -1) {
        THROW_ERROR_EXCEPTION("Failed to create named pipe %v", Path_)
            << TError::FromSystem();
    }
}

IConnectionReaderPtr TNamedPipe::CreateAsyncReader()
{
    YT_VERIFY(!Path_.empty());
    return CreateInputConnectionFromPath(Path_, TIODispatcher::Get()->GetPoller(), MakeStrong(this));
}

IConnectionWriterPtr TNamedPipe::CreateAsyncWriter(bool useDeliveryFence)
{
    YT_VERIFY(!Path_.empty());
    return CreateOutputConnectionFromPath(Path_, TIODispatcher::Get()->GetPoller(), MakeStrong(this), Capacity_, useDeliveryFence);
}

TString TNamedPipe::GetPath() const
{
    return Path_;
}

////////////////////////////////////////////////////////////////////////////////

TNamedPipeConfigPtr TNamedPipeConfig::Create(TString path, int fd, bool write)
{
    auto result = New<TNamedPipeConfig>();
    result->Path = std::move(path);
    result->FD = fd;
    result->Write = write;

    return result;
}

void TNamedPipeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default();

    registrar.Parameter("fd", &TThis::FD)
        .Default(0);

    registrar.Parameter("write", &TThis::Write)
        .Default(false);
}

DEFINE_REFCOUNTED_TYPE(TNamedPipeConfig)

////////////////////////////////////////////////////////////////////////////////

TPipe::TPipe()
{ }

TPipe::TPipe(TPipe&& pipe)
{
    Init(std::move(pipe));
}

TPipe::TPipe(int fd[2])
    : ReadFD_(fd[0])
    , WriteFD_(fd[1])
{ }

void TPipe::Init(TPipe&& other)
{
    ReadFD_ = other.ReadFD_;
    WriteFD_ = other.WriteFD_;
    other.ReadFD_ = InvalidFD;
    other.WriteFD_ = InvalidFD;
}

TPipe::~TPipe()
{
    if (ReadFD_ != InvalidFD) {
        YT_VERIFY(TryClose(ReadFD_, false));
    }

    if (WriteFD_ != InvalidFD) {
        YT_VERIFY(TryClose(WriteFD_, false));
    }
}

void TPipe::operator=(TPipe&& other)
{
    if (this == &other) {
        return;
    }

    Init(std::move(other));
}

IConnectionWriterPtr TPipe::CreateAsyncWriter()
{
    YT_VERIFY(WriteFD_ != InvalidFD);
    SafeMakeNonblocking(WriteFD_);
    return CreateConnectionFromFD(ReleaseWriteFD(), {}, {}, TIODispatcher::Get()->GetPoller());
}

IConnectionReaderPtr TPipe::CreateAsyncReader()
{
    YT_VERIFY(ReadFD_ != InvalidFD);
    SafeMakeNonblocking(ReadFD_);
    return CreateConnectionFromFD(ReleaseReadFD(), {}, {}, TIODispatcher::Get()->GetPoller());
}

int TPipe::ReleaseReadFD()
{
    YT_VERIFY(ReadFD_ != InvalidFD);
    auto fd = ReadFD_;
    ReadFD_ = InvalidFD;
    return fd;
}

int TPipe::ReleaseWriteFD()
{
    YT_VERIFY(WriteFD_ != InvalidFD);
    auto fd = WriteFD_;
    WriteFD_ = InvalidFD;
    return fd;
}

int TPipe::GetReadFD() const
{
    YT_VERIFY(ReadFD_ != InvalidFD);
    return ReadFD_;
}

int TPipe::GetWriteFD() const
{
    YT_VERIFY(WriteFD_ != InvalidFD);
    return WriteFD_;
}

void TPipe::CloseReadFD()
{
    if (ReadFD_ == InvalidFD) {
        return;
    }
    auto fd = ReadFD_;
    ReadFD_ = InvalidFD;
    SafeClose(fd, false);
}

void TPipe::CloseWriteFD()
{
    if (WriteFD_ == InvalidFD) {
        return;
    }
    auto fd = WriteFD_;
    WriteFD_ = InvalidFD;
    SafeClose(fd, false);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TPipe& pipe, TStringBuf spec)
{
    // TODO(arkady-e1ppa): We format pipe twice
    // (pipe itself and its serialization)
    // This is probably redundant.
    // Check if it is later and remove
    // the second step.
    FormatValue(
        builder,
        Format(
            "{ReadFD: %v, WriteFD: %v}",
            pipe.GetReadFD(),
            pipe.GetWriteFD()),
        spec);
}

////////////////////////////////////////////////////////////////////////////////

TPipeFactory::TPipeFactory(int minFD)
    : MinFD_(minFD)
{ }

TPipeFactory::~TPipeFactory()
{
    for (int fd : ReservedFDs_) {
        YT_VERIFY(TryClose(fd, false));
    }
}

TPipe TPipeFactory::Create()
{
    while (true) {
        int fd[2];
        SafePipe(fd);
        if (fd[0] >= MinFD_ && fd[1] >= MinFD_) {
            TPipe pipe(fd);
            return pipe;
        } else {
            ReservedFDs_.push_back(fd[0]);
            ReservedFDs_.push_back(fd[1]);
        }
    }
}

void TPipeFactory::Clear()
{
    for (int& fd : ReservedFDs_) {
        YT_VERIFY(TryClose(fd, false));
        fd = TPipe::InvalidFD;
    }
    ReservedFDs_.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
