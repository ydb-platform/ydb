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

        void Part(const TPart&, ui32 depth = 10);

    private:
        void Frames(const NPage::TFrames&, const char *tag);
        void Blobs(const NPage::TExtBlobs&);
        void Bloom(const NPage::TBloom&);
        void Index(const TPart&, ui32 depth = 10);
        void BTreeIndex(const TPart&);
        void DataPage(const TPart&, NPage::TPageLocation location);
        void TName(ui32 num);
        void Key(TCellsRef cells, const TPartScheme&);
        void BTreeIndexNode(const TPart &part, NPage::TPageLocation loc, const TString &parent, ui32 level, ui32 totalLevels);

        IOutputStream &Out;
        IPages * const Env = nullptr;
        const TReg * const Reg = nullptr;
    };

}
}
