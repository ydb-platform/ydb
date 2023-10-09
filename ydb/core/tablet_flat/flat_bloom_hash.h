#pragma once

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <util/digest/murmur.h>

namespace NKikimr {
namespace NTable {
namespace NBloom {

    class TPrefix : TNonCopyable {
    public:
        explicit TPrefix(TArrayRef<const TCell> row)
        {
            ui32 size = 0;

            for (const TCell &cell : row) {
                /* ui32 is the size of header */
                Offsets.push_back(size += sizeof(ui32) + cell.Size());
            }

            Buffer.reserve(size);

            for (const TCell &cell : row) {
                ui32 sz = cell.IsNull() ? Max<ui32>() : cell.Size();
                Buffer.insert(Buffer.end(), (const char*)&sz, ((const char*)&sz) + sizeof(sz));
                Buffer.insert(Buffer.end(), cell.Data(), cell.Data() + cell.Size());
            }
        }

        inline TStringBuf Get(ui32 len) const noexcept
        {
            Y_ABORT_UNLESS(len > 0 && len <= Offsets.size());

            return { Buffer.data(), Buffer.data() + Offsets[len - 1] };
        }

    private:
        TSmallVec<char> Buffer;
        TSmallVec<ui32> Offsets;
    };

    struct THashRoot {
        ui64 Value;
    };

    class THash {
    public:
        explicit THash(THashRoot root)
        {
            // https://github.com/google/leveldb/blob/master/util/bloom.cc#L54
            Hash = root.Value;
            Delta = (Hash >> 33) | (Hash << 31);
        }

        explicit THash(TArrayRef<const char> row)
            : THash(Root(row))
        {
        }

        ui64 Next() noexcept
        {
            return std::exchange(Hash, Hash + Delta);
        }

        static THashRoot Root(TArrayRef<const char> row) noexcept
        {
            return THashRoot{ MurmurHash<ui64>(row.data(), row.size(), 0x4b7db4c869874dd1ull) };
        }

    private:
        ui64 Hash = 0;
        ui64 Delta = 0;
    };

}
}
}
