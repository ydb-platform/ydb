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

    private:
        void Frames(const NPage::TFrames&, const char *tag) noexcept;
        void Blobs(const NPage::TExtBlobs&) noexcept;
        void Bloom(const NPage::TBloom&) noexcept;
        void Index(const TPart&, ui32 depth = 10) noexcept;
        void BTreeIndex(const TPart&) noexcept;
        void DataPage(const TPart&, ui32 page) noexcept;
        void TName(ui32 num) noexcept;
        void Key(TCellsRef cells, const TPartScheme&) noexcept;
        void BTreeIndexNode(const TPart &part, NPage::TBtreeIndexNode::TChild meta, ui32 level = 0) noexcept;

        IOutputStream &Out;
        IPages * const Env = nullptr;
        const TReg * const Reg = nullptr;
    };

}
}
