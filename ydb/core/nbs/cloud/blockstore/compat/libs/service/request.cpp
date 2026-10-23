#include "request.h"

#include <util/string/cast.h>

namespace NCloud::NBlockStore {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

TWriteBlocksLocalRequest::TWriteBlocksLocalRequest(
    TWriteBlocksLocalRequest&& other) noexcept
    : TWriteBlocksRequest(std::move(other))
    , Sglist(std::move(other.Sglist))
    , BlocksCount(std::exchange(other.BlocksCount, 0))
    , OwnsSglist(std::exchange(other.OwnsSglist, false))
{}

TWriteBlocksLocalRequest::TWriteBlocksLocalRequest(
    const TWriteBlocksLocalRequest& source,
    TDependentTag)
{
    Y_ABORT_UNLESS(source.GetDescriptor()->field_count() == 8);

    // Copy all protobuf fields except Blocks (field 4)
    MutableHeaders()->CopyFrom(source.GetHeaders());
    SetDiskId(source.GetDiskId());
    SetStartIndex(source.GetStartIndex());
    SetFlags(source.GetFlags());
    SetSessionId(source.GetSessionId());
    SetBlockSize(source.GetBlockSize());
    MutableChecksums()->CopyFrom(source.GetChecksums());

    // Copy non-protobuf fields
    Sglist = source.Sglist;
    BlocksCount = source.BlocksCount;
}

TWriteBlocksLocalRequest& TWriteBlocksLocalRequest::operator=(
    TWriteBlocksLocalRequest&& other) noexcept
{
    CloseOwnedSglist();

    Sglist = std::move(other.Sglist);
    BlocksCount = std::exchange(other.BlocksCount, 0);
    OwnsSglist = std::exchange(other.OwnsSglist, false);
    TWriteBlocksRequest::operator=(std::move(other));
    return *this;
}

TWriteBlocksLocalRequest::~TWriteBlocksLocalRequest()
{
    CloseOwnedSglist();
}

void TWriteBlocksLocalRequest::TakeDataOwnership()
{
    if (OwnsSglist || Sglist.Empty()) {
        return;
    }

    auto g = Sglist.Acquire();
    if (!g) {
        return;
    }

    MutableBlocks()->Clear();
    const auto& sgList = g.Get();
    TSgList newSgList;
    newSgList.reserve(sgList.size());
    for (const auto& block: sgList) {
        auto& buffer = *MutableBlocks()->AddBuffers();
        buffer.ReserveAndResize(block.Size());
        memcpy(buffer.begin(), block.Data(), block.Size());
        newSgList.emplace_back(buffer.data(), buffer.size());
    }
    Sglist = TGuardedSgList(std::move(newSgList));
    OwnsSglist = true;
}

void TWriteBlocksLocalRequest::CloseOwnedSglist()
{
    if (OwnsSglist) {
        Sglist.Close();
        OwnsSglist = false;
    }
}

TWriteBlocksLocalRequest CopyRequest(const TWriteBlocksLocalRequest& request)
{
    return TWriteBlocksLocalRequest(
        request,
        TWriteBlocksLocalRequest::TDependentTag{});
}

TWriteBlocksRequest CopyRequest(const TWriteBlocksRequest& request)
{
    return request;
}

TZeroBlocksRequest CopyRequest(const TZeroBlocksRequest& request)
{
    return request;
}

}   // namespace NProto

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)    #name,

static const TString RequestNames[] = {
    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)
};

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

const TString& GetBlockStoreRequestName(EBlockStoreRequest request)
{
    if (request < EBlockStoreRequest::MAX) {
        return RequestNames[(int)request];
    }

    static const TString Unknown = "unknown";
    return Unknown;
}

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                     \
    template <>                                                  \
    TString GetBlockStoreRequestName<NProto::T##name##Request>() \
    {                                                            \
        return #name;                                            \
    }

BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

TString GetSysRequestName(ESysRequestType requestType)
{
    return ToString(requestType);
}

TStringBuf GetPrivateRequestName(EPrivateRequestType requestType)
{
    switch (requestType) {
        case EPrivateRequestType::DescribeBlocks: return "DescribeBlocks";
        default: return "unknown";
    }
}

}   // namespace NCloud::NBlockStore

template <>
void Out<NCloud::NBlockStore::NProto::TReadBlocksLocalResponse>(
    IOutputStream& out,
    const NCloud::NBlockStore::NProto::TReadBlocksLocalResponse& value)
{
    out << value.ShortDebugString();

    out << " FailedInfo->FailedRanges: [";
    for (const auto& blob: value.FailInfo.FailedRanges) {
        out << blob << ", ";
    }
    out << "]";
}
