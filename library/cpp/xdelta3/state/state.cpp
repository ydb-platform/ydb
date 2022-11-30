#include "state.h"

#include <library/cpp/xdelta3/state/hash.h>
#include <library/cpp/xdelta3/xdelta_codec/codec.h>

#include <util/stream/null.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NXdeltaAggregateColumn {
    size_t SizeOfHeaderSize(size_t headerSize)
    {
        using namespace NProtoBuf::io;

        ui32 dummy = 0;
        auto data = reinterpret_cast<ui8*>(&dummy);
        return CodedOutputStream::WriteVarint32ToArray(headerSize, data) - data;
    }

    size_t SizeOfHeader(const TStateHeader& header)
    {
        // length of header + calculated length
        auto headerSize = header.ByteSize();
        return SizeOfHeaderSize(headerSize) + headerSize;
    }

    TStateHeader* ParseHeader(NProtoBuf::Arena& arena, const ui8* data, size_t size)
    {
        using namespace NProtoBuf::io;

        auto header = NProtoBuf::Arena::CreateMessage<TStateHeader>(&arena);
        if (nullptr == data || 0 == size) {
            header->set_error_code(TStateHeader::HEADER_PARSE_ERROR);
            return header;
        }

        ui32 headerSize = 0;
        CodedInputStream in(data, size);
        if (in.ReadVarint32(&headerSize)) {
            auto sizeofHeaderSize = in.CurrentPosition();
            if (size - sizeofHeaderSize < headerSize) {
                header->set_error_code(TStateHeader::HEADER_PARSE_ERROR);
                return header;
            }

            if (!header->ParseFromArray(data + sizeofHeaderSize, headerSize)) {
                header->Clear();
                header->set_error_code(TStateHeader::HEADER_PARSE_ERROR);
            }
        } else {
            header->set_error_code(TStateHeader::HEADER_PARSE_ERROR);
        }

        return header;
    }

    bool EncodeHeaderTo(const TStateHeader& header, ui8* data, size_t size, size_t& resultSize)
    {
        using namespace NProtoBuf::io;

        resultSize = 0;
        auto headerSize = header.ByteSize();
        auto sizeOfHeaderSize = SizeOfHeaderSize(headerSize);
        if (header.SerializeToArray(data + sizeOfHeaderSize, size - sizeOfHeaderSize)) {
            sizeOfHeaderSize = CodedOutputStream::WriteVarint32ToArray(headerSize, data) - data;
            resultSize = sizeOfHeaderSize + headerSize;
            return true;
        }
        return false;
    }

    TStateHeader::EErrorCode CheckProto(const TStateHeader* header, size_t dataSize)
    {
        auto hasRequiredFields = false;
        if (header->type() == TStateHeader::BASE) {
            hasRequiredFields = header->has_data_size();
        } else if (header->type() == TStateHeader::PATCH) {
            hasRequiredFields = header->has_base_hash() &&
                header->has_state_hash() &&
                header->has_state_size() &&
                header->has_data_size();
        } else {
            hasRequiredFields = header->has_error_code();
        }
        if (!hasRequiredFields) {
            return TStateHeader::MISSING_REQUIRED_FIELD_ERROR;
        }

        auto payloadSizeOk = header->data_size() <= dataSize - SizeOfHeader(*header);
        if (!payloadSizeOk) {
            return TStateHeader::WRONG_DATA_SIZE;
        }
        return TStateHeader::NO_ERROR;
    }

    TState::TState(NProtoBuf::Arena& arena, const ui8* data, size_t size)
    {
        if (nullptr == data || 0 == size) {
            return;
        }

        HeaderPtr = ParseHeader(arena, data, size);
        if (HeaderPtr->has_error_code() && HeaderPtr->error_code() == TStateHeader::HEADER_PARSE_ERROR) {
            return;
        }

        auto errorCode = CheckProto(HeaderPtr, size);
        if (errorCode != TStateHeader::NO_ERROR) {
            HeaderPtr->Clear();
            HeaderPtr->set_error_code(errorCode);
            return;
        }

        if (HeaderPtr->type() != TStateHeader::NONE_TYPE) {
            if (HeaderPtr->data_size()) {
                Data = data + SizeOfHeader(*HeaderPtr);
            }
        }
    }

    ui32 TState::CalcHash() const
    {
        if (nullptr != PayloadData() && 0 != PayloadSize()) {
            return NXdeltaAggregateColumn::CalcHash(PayloadData(), PayloadSize());
        }
        return 0;
    }

    size_t TState::PayloadSize() const
    {
        return HeaderPtr->data_size();
    }

    TStateHeader::EErrorCode TState::Error() const
    {
        return HeaderPtr->has_error_code()
            ? HeaderPtr->error_code()
            : TStateHeader::NO_ERROR;
    }

    TStateHeader::EType TState::Type() const
    {
        return HeaderPtr->type();
    }
}
