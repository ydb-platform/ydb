#include "merge.h"

#include "state.h"

#include "data_ptr.h"

#include <library/cpp/xdelta3/xdelta_codec/codec.h>
#include <library/cpp/xdelta3/state/hash.h>

#include <util/digest/murmur.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NXdeltaAggregateColumn {

    static ui8* AllocateFromContext(XDeltaContext* context, size_t size)
    {
        if (!context) {
            return reinterpret_cast<ui8*>(malloc(size));
        }
        return reinterpret_cast<ui8*>(context->allocate(context->opaque, size));
    }

    bool EncodeHeaderTo(const TStateHeader& header, ui8* data, size_t size, size_t& resultSize);

    bool EncodeErrorHeader(XDeltaContext* context, NProtoBuf::Arena& arena, TStateHeader::EErrorCode error, TSpan* result)
    {
        result->Offset = result->Size = 0;

        auto header = NProtoBuf::Arena::CreateMessage<NXdeltaAggregateColumn::TStateHeader>(&arena);
        header->set_error_code(error);
        auto headerSize = SizeOfHeader(*header);
        auto data = TDataPtr(AllocateFromContext(context, headerSize), TDeleter(context));
        if (EncodeHeaderTo(*header, data.get(), headerSize, result->Size)) {
            result->Offset = 0;
            result->Data = data.release();
            return true;
        }
        return false;
    }

    bool EncodeState(XDeltaContext* context, NProtoBuf::Arena& arena, const TState& state, TSpan* result)
    {
        auto headerSize = SizeOfHeader(state.Header());
        result->Size = headerSize + state.PayloadSize();
        auto data = TDataPtr(AllocateFromContext(context, result->Size), TDeleter(context));
        size_t written = 0;
        if (EncodeHeaderTo(state.Header(), data.get(), result->Size, written)) {
            if (state.PayloadSize() && state.PayloadData()) {
                memcpy(data.get() + headerSize, state.PayloadData(), state.PayloadSize());
            }
            result->Data = data.release();
            return true;
        }
        return EncodeErrorHeader(context, arena, TStateHeader::PROTOBUF_ERROR, result);
    }

    // NOTE: empty (data_size == 0) patch means nothing changed.
    // empty valid patch and will be ignored unless will raise error

    bool IsBadEmptyPatch(const TState& empty)
    {
        return 0u == empty.PayloadSize() && empty.Header().base_hash() != empty.Header().state_hash();
    }

    bool MergePatches(XDeltaContext* context, NProtoBuf::Arena& arena, const TState& lhs, const TState& rhs, TSpan* result)
    {
        if (lhs.Header().state_hash() != rhs.Header().base_hash()) {
            return EncodeErrorHeader(context, arena, TStateHeader::MERGE_PATCHES_ERROR, result);
        }

        if (IsBadEmptyPatch(lhs) || IsBadEmptyPatch(rhs)) {
            return EncodeErrorHeader(context, arena, TStateHeader::MERGE_PATCHES_ERROR, result);
        }

        if (0u == lhs.PayloadSize()) {
            return EncodeState(context, arena, rhs, result);
        }

        if (0u == rhs.PayloadSize()) {
            return EncodeState(context, arena, lhs, result);
        }

        auto merged = NProtoBuf::Arena::CreateMessage<TStateHeader>(&arena);
        merged->set_type(TStateHeader::PATCH);
        merged->set_base_hash(lhs.Header().base_hash());
        merged->set_state_hash(rhs.Header().state_hash());
        merged->set_state_size(rhs.Header().state_size());

        size_t patchSize = 0;
        auto maxMergedHeaderSize = SizeOfHeader(*merged) + sizeof(patchSize); // estimation should be valid: sizeof(ui64 patchSize) covers possible sizeOfHeaderSize growth (+1)
                                                                                  // as well as ui32 data_size which is unset at this point
        auto patch = TDataPtr(MergePatches(
            context,
            maxMergedHeaderSize,
            lhs.PayloadData(),
            lhs.PayloadSize(),
            rhs.PayloadData(),
            rhs.PayloadSize(),
            &patchSize),
            TDeleter(context));
        if (!patch) {
            return EncodeErrorHeader(context, arena, TStateHeader::MERGE_PATCHES_ERROR, result);
        }

        merged->set_data_size(patchSize);

        auto mergedHeaderSize = SizeOfHeader(*merged);
        Y_ENSURE(maxMergedHeaderSize >= mergedHeaderSize);
        auto offset = maxMergedHeaderSize - mergedHeaderSize;

        size_t headerSize = 0;
        if (!EncodeHeaderTo(*merged, patch.get() + offset, mergedHeaderSize, headerSize)) {
            return EncodeErrorHeader(context, arena, TStateHeader::PROTOBUF_ERROR, result);
        }

        result->Size = mergedHeaderSize + patchSize;
        result->Offset = offset;
        result->Data = patch.release();
        return true;
    }

    bool ApplyPatch(XDeltaContext* context, NProtoBuf::Arena& arena, const TState& base, const TState& patch, TSpan* result)
    {
        auto baseHash = base.CalcHash();
        if (baseHash != patch.Header().base_hash()) {
            return EncodeErrorHeader(context, arena, TStateHeader::BASE_HASH_ERROR, result);
        }

        if (patch.Header().data_size() == 0) {
            if (patch.Header().state_size() == base.Header().data_size()) {
                if (patch.Header().state_hash() == baseHash) {
                    return EncodeState(context, arena, base, result);
                }
                return EncodeErrorHeader(context, arena, TStateHeader::STATE_HASH_ERROR, result);
            }
            return EncodeErrorHeader(context, arena, TStateHeader::STATE_SIZE_ERROR, result);
        }

        size_t stateSize = 0;

        auto merged = NProtoBuf::Arena::CreateMessage<TStateHeader>(&arena);
        merged->set_type(TStateHeader::BASE);

        auto maxHeaderSize = SizeOfHeader(*merged) + sizeof(stateSize);

        auto state = TDataPtr(ApplyPatch(
            context,
            maxHeaderSize,
            base.PayloadData(),
            base.PayloadSize(),
            patch.PayloadData(),
            patch.PayloadSize(),
            patch.Header().state_size(),
            &stateSize),
            TDeleter(context));
        if (!state) {
            return EncodeErrorHeader(context, arena, TStateHeader::APPLY_PATCH_ERROR, result);
        }

        if (stateSize != patch.Header().state_size()) {
            return EncodeErrorHeader(context, arena, TStateHeader::STATE_SIZE_ERROR, result);
        }

        auto stateHash = CalcHash(state.get() + maxHeaderSize, stateSize);
        if (stateHash != patch.Header().state_hash()) {
            return EncodeErrorHeader(context, arena, TStateHeader::STATE_HASH_ERROR, result);
        }

        merged->set_data_size(stateSize);

        auto mergedHeaderSize = SizeOfHeader(*merged);
        auto offset = maxHeaderSize - mergedHeaderSize;
        size_t headerSize = 0;
        if (!EncodeHeaderTo(*merged, state.get() + offset, mergedHeaderSize, headerSize)) {
            return EncodeErrorHeader(context, arena, TStateHeader::PROTOBUF_ERROR, result);
        }

        result->Size = mergedHeaderSize + stateSize;
        result->Offset = offset;
        result->Data = state.release();
        return true;
    }

    int MergeStates(XDeltaContext* context, const ui8* lhsData, size_t lhsSize, const ui8* rhsData, size_t rhsSize, TSpan* result)
    {
        using namespace NXdeltaAggregateColumn;
        using namespace NProtoBuf::io;

        result->Data = nullptr;
        result->Size = 0;
        result->Offset = 0;

        NProtoBuf::ArenaOptions options;
        options.initial_block_size = ArenaMaxSize;   
        auto buffer = TDataPtr(AllocateFromContext(context, options.initial_block_size), TDeleter(context));
        options.initial_block = reinterpret_cast<char*>(buffer.get());
        NProtoBuf::Arena arena(options);

        TState rhs(arena, rhsData, rhsSize);

        if (rhs.Header().has_error_code()) {
            return EncodeErrorHeader(context, arena, rhs.Error(), result);
        }

        if (rhs.Type() == TStateHeader::BASE) {
            result->Data = rhsData;
            result->Size = rhsSize;
            return true;
        }

        TState lhs(arena, lhsData, lhsSize);
        if (lhs.Header().has_error_code()) {
            return EncodeErrorHeader(context, arena, lhs.Error(), result);
        }

        if (lhs.Type() == TStateHeader::PATCH && rhs.Type() == TStateHeader::PATCH) {
            return MergePatches(context, arena, lhs, rhs, result);
        } else if (lhs.Type() == TStateHeader::BASE && rhs.Type() == TStateHeader::PATCH) {
            return ApplyPatch(context, arena, lhs, rhs, result);
        }
        return EncodeErrorHeader(context, arena, TStateHeader::YT_MERGE_ERROR, result);
    }
}
