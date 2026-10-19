#pragma once

// NKikimr::NPDisk::YdbDefaultPDiskSequence (ydb/core/blobstorage/crypto/default.h)
// is defined by the program: its main.cpp, or ydb/library/keys for ydbd -- ya
// links the whole program. Force-included into the libraries that refer to it
// (see //build/gn/config:ydb_default_pdisk_sequence_weak): a weak reference
// lets the shared library link with --no-undefined, the loader resolves it to
// the program's definition.
__asm__(".weak _ZN7NKikimr6NPDisk23YdbDefaultPDiskSequenceE");
