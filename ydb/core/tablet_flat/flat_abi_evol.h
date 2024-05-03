#pragma once

#include <util/system/types.h>

namespace NKikimr {
namespace NTable {

    enum class ECompatibility: ui32 {

        /* Evolution of API/ABI changes of executor binary logs, protocols
            and table internal data units. Each alteration of these entities
            that requires to keep legacy code for compatability for unknown
            time period should be recorded in this log with Head increment.

            Each drop of legacy code recorded in this log have to lead to
            raising Tail value below which is the lowest supported evolution.

                Tail    least supported version for blobs reads,
                Head    least supported version for produced blobs,
                Edge    the evolution edge, upper supported version.

            get = [ Tail, Edge ] - can be parsed and interpreted correctly,
            put = [ Head, Edge ] - write backward compatibility version span.
        */

        Tail = 1,
        Head = 17,
        Edge = 28,
    };

    /* Ev | Desc                                                   | Gone
     ---------------------------------------------------------------------
        2   All bundles proto was moved to usage of TLargeGlobId units
     ---------------------------------------------------------------------
        3   Support of NPage v2 labels with additional metadata
            Page readers adopted to work with relative offsets
     ---------------------------------------------------------------------
        4   EPage::DataPage compression support via NPage v2 labels
     ---------------------------------------------------------------------
        5   TPart columns limited to -Min<ui16>, or to 32767 cols
     ---------------------------------------------------------------------
        6   Complete support of ELargeObj::Extern blobs references
     ---------------------------------------------------------------------
        7   Extend NPage::TBlobs with channel mask (version 1)
     ---------------------------------------------------------------------
        8   ECellOp::{Null,Empty,Reset} cannot have values in redo log
     ---------------------------------------------------------------------
        9   Evolution number storages changed from ui64 to ui32
     ---------------------------------------------------------------------
       10   Read suppport of new REDO log chunks + log ABI markers
            Key items and updates in REDO log limited to Max<ui16>
     ---------------------------------------------------------------------
       11   Complete support of annex in redo log (exnternal blobs)
     ---------------------------------------------------------------------
       12   Write redo log in new binary format with generic chunks
            Use only TLargeGlobId in bundles proto (TPageCollectionProtoHelper serializer)
     ---------------------------------------------------------------------
       13   Read/write support for TPart page collection root meta data
            Scheme page now has TLabel'ed version EPage::Schem2
            +part metainfo: ABI labels, rows and ERowOp::Erase stats
     ---------------------------------------------------------------------
       14   Per-part by key bloom filter support (EPage:ByKey v0)
     ---------------------------------------------------------------------
       15   Complete support ELargeObj::Outer, values packed in page collection
     ---------------------------------------------------------------------
       16   Read and write final key in EPage::FlatIndex
     ---------------------------------------------------------------------
       17   Fixed incomplete read support of rooted page collections
     ---------------------------------------------------------------------
       18   Durable db change serial numbers with EvBegin v1
            Stamp field in EvFlush is not used anymore
     ---------------------------------------------------------------------
       19   Generation and serialization of part slices
     ---------------------------------------------------------------------
       20   Support for partial bundle compaction
     ---------------------------------------------------------------------
       21   Fix of diverged TxStamp filed in EvBegin, KIKIMR-5323
     ---------------------------------------------------------------------
       22   Fix of unaligned array placement in NPage::TFrames buf
     ---------------------------------------------------------------------
       23   Start producing all page collections only with TLabel-ed pages
            Resuse unused channels mask in EPage::Blobs for stats
     ---------------------------------------------------------------------
       24   Support large NLabel variant in pages and blobs readers
            Start write at least small variant of NLabel everywhere
     ---------------------------------------------------------------------
       25   Back to evolution 24 and legacy Tiny + Small labels
     ---------------------------------------------------------------------
       26   Parts with column groups
     ---------------------------------------------------------------------
       27   Parts with versioned rows and historic indexes
     ---------------------------------------------------------------------
       28   Parts with uncommitted delta rows
     */

}
}
