#pragma once

// It is recommended to support all reasonal value combinations via this enum,
// to let Load() function argument be of EDataLoadMode type, not just int type

enum EDataLoadMode {
    DLM_READ = 0,
    DLM_MMAP_PRC = 1,      // precharge
    DLM_MMAP = 2,          // w/o precharge
    DLM_MMAP_AUTO_PRC = 3, // precharge automatically (same as DLM_MMAP unless specifically supported)
    DLM_LD_TYPE_MASK = 15,
    DLM_EXACT_SIZE = 16, // fail if input file is larger than what header says

    DLM_READ_ESZ = DLM_READ | DLM_EXACT_SIZE,
    DLM_MMAP_PRC_ESZ = DLM_MMAP_PRC | DLM_EXACT_SIZE,
    DLM_MMAP_ESZ = DLM_MMAP | DLM_EXACT_SIZE,
    DLM_MMAP_APRC_ESZ = DLM_MMAP_AUTO_PRC | DLM_EXACT_SIZE,

    DLM_DEFAULT = DLM_MMAP_PRC_ESZ,
};
