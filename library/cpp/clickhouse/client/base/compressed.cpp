#include "compressed.h"
#include "wire_format.h"

#include <util/generic/yexception.h>

#include <contrib/libs/lz4/lz4.h>
#include <contrib/restricted/cityhash-1.0.2/city.h>

#define DBMS_MAX_COMPRESSED_SIZE 0x40000000ULL // 1GB

namespace NClickHouse {
    TCompressedInput::TCompressedInput(TCodedInputStream* input)
        : Input_(input)
    {
    }

    TCompressedInput::~TCompressedInput() {
        if (!Mem_.Exhausted()) {
            Y_ABORT("some data was not read");
        }
    }

    size_t TCompressedInput::DoNext(const void** ptr, size_t len) {
        if (Mem_.Exhausted()) {
            if (!Decompress()) {
                return 0;
            }
        }

        return Mem_.Next(ptr, len);
    }

    bool TCompressedInput::Decompress() {
        CityHash_v1_0_2::uint128 hash;
        ui32 compressed = 0;
        ui32 original = 0;
        ui8 method = 0;

        if (!TWireFormat::ReadFixed(Input_, &hash)) {
            return false;
        }
        if (!TWireFormat::ReadFixed(Input_, &method)) {
            return false;
        }

        if (method != 0x82) {
            ythrow yexception() << "unsupported compression method "
                                << int(method);
        } else {
            if (!TWireFormat::ReadFixed(Input_, &compressed)) {
                return false;
            }
            if (!TWireFormat::ReadFixed(Input_, &original)) {
                return false;
            }

            if (compressed > DBMS_MAX_COMPRESSED_SIZE) {
                ythrow yexception() << "compressed data too big";
            }

            TTempBuf tmp(compressed);

            // Заполнить заголовок сжатых данных.
            tmp.Append(&method, sizeof(method));
            tmp.Append(&compressed, sizeof(compressed));
            tmp.Append(&original, sizeof(original));

            if (!TWireFormat::ReadBytes(Input_, tmp.Data() + 9, compressed - 9)) {
                return false;
            } else {
                if (hash != CityHash_v1_0_2::CityHash128(tmp.Data(), compressed)) {
                    ythrow yexception() << "data was corrupted";
                }
            }

            Data_ = TTempBuf(original);

            if (LZ4_decompress_fast(tmp.Data() + 9, Data_.Data(), original) < 0) {
                ythrow yexception() << "can't decompress data";
            } else {
                Mem_.Reset(Data_.Data(), original);
            }
        }

        return true;
    }

}
