#pragma once

#include "flat_table_part.h"
#include <util/stream/output.h>

namespace NKikimr {
namespace NTable {

    class TDump {
    public:
        using IOut = IOutputStream;
        using TReg = NScheme::TTypeRegistry;

        TDump(IOut &out, IPages *env, const TReg *reg);
        ~TDump();

        void Part(const TPart&, ui32 depth = 10) noexcept;
        void Dump(const NPage::TFrames&, const char *tag) noexcept;
        void Dump(const NPage::TExtBlobs&) noexcept;
        void Dump(const NPage::TBloom&) noexcept;
        void Index(const TPart&, ui32 depth = 10) noexcept;
        void DataPage(const TPart&, ui32 page) noexcept;
        void TName(ui32 num) noexcept;
        void DumpKey(const TPartScheme&) noexcept;

    private:
        IOutputStream &Out;
        IPages * const Env = nullptr;
        const TReg * const Reg = nullptr;
        TSmallVec<TCell> Key;
    };

}
}
