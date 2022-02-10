#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>

namespace NKikimr {
    namespace NSyncLog {

        ////////////////////////////////////////////////////////////////////////////
        // TWriteParts
        // It consists of a references to pages we want to write to a chunk
        ////////////////////////////////////////////////////////////////////////////
        class TWriteParts : public NPDisk::TEvChunkWrite::IParts {
        public:
            using NPDisk::TEvChunkWrite::IParts::TDataRef;

            TWriteParts(ui32 pageSize)
                : PageSize(pageSize)
            {}

            virtual TDataRef operator[] (ui32 i) const { return Refs[i]; }
            virtual ui32 Size() const { return Refs.size(); }

            void Push(TSyncLogPageSnap pageSnap) {
                Pages.push_back(pageSnap);
            }

            void GenRefs() {
                Refs.clear();
                Refs.reserve(Pages.size() * PartsForPage);
                for (const auto& p : Pages) {
                    Refs.push_back(p.GetFirstRaw());
                    Refs.push_back(p.GetSecondRaw());
                    if (const TDataRef& ref = p.GetThirdRaw(PageSize); ref.second) {
                        Refs.push_back(ref);
                    }
                }
            }

            const TVector<TSyncLogPageSnap> &GetSnapPages() const {
                return Pages;
            }

            void Clear() {
                Pages.clear();
            }

            void Reserve(ui32 n) {
                Pages.reserve(n);
            }

            static const ui32 PartsForPage = 3;

        private:
            const ui32 PageSize;
            TVector<TSyncLogPageSnap> Pages;
            std::vector<TDataRef> Refs;
        };

    } // NSyncLog
} // NKikimr
