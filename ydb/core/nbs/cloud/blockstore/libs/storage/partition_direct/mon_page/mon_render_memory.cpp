#include "mon_render_memory.h"

#include "mon_model.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

void RenderMemory(IOutputStream& str, const TMonPageData& data)
{
    TDirtyMapStats dirtyMapStats;
    for (const auto& dbg: data.Dbgs) {
        dirtyMapStats.Aggregate(dbg.DirtyMapStats);
    }

    HTML (str) {
        if (data.FastPathServiceInfo) {
            size_t totalReservedSize = 0;
            size_t totalUsedSize = 0;
            size_t totalCount = 0;
            TAG (TH3) {
                str << "Arena allocator";
            }
            TABLE_CLASS ("table table-condensed") {
                TABLEHEAD () {
                    TABLER () {
                        TABLEH () {
                            str << "Slot size";
                        }
                        TABLEH () {
                            str << "Arena size";
                        }
                        TABLEH () {
                            str << "Reserved";
                        }
                        TABLEH () {
                            str << "Used";
                        }
                        TABLEH () {
                            str << "Max used";
                        }
                        TABLEH () {
                            str << "Count";
                        }
                    }
                }
                TABLEBODY () {
                    for (const auto& usage:
                         data.FastPathServiceInfo->ArenaMemoryUsage.Slots)
                    {
                        totalReservedSize += usage.ReservedSize;
                        totalUsedSize += usage.UsedSize;
                        totalCount += usage.Count;
                        TABLER () {
                            TABLED () {
                                str << FormatByteSize(usage.SlotSize);
                            }
                            TABLED () {
                                str << FormatByteSize(usage.ArenaSize);
                            }
                            TABLED () {
                                str << FormatByteSize(usage.ReservedSize);
                            }
                            TABLED () {
                                str << FormatByteSize(usage.UsedSize);
                            }
                            TABLED () {
                                str << FormatByteSize(usage.MaxUsedSize);
                            }
                            TABLED () {
                                str << usage.Count;
                            }
                        }
                    }
                    TABLER () {
                        TABLED () {
                            str << "Total";
                        }
                        TABLED () {
                            str << "-";
                        }
                        TABLED () {
                            str << FormatByteSize(totalReservedSize);
                        }
                        TABLED () {
                            str << FormatByteSize(totalUsedSize);
                        }
                        TABLED () {
                            str << "-";
                        }
                        TABLED () {
                            str << totalCount;
                        }
                    }
                }
            }

            TAG (TH3) {
                str << "Arena allocator pool summary (one pool per DBG)";
            }
            TABLE_CLASS ("table table-condensed") {
                TABLEHEAD () {
                    TABLER () {
                        TABLEH () {
                            str << "Slot size";
                        }
                        TABLEH () {
                            str << "Reserved";
                        }
                        TABLEH () {
                            str << "Used";
                        }
                        TABLEH () {
                            str << "Max used";
                        }
                        TABLEH () {
                            str << "Count";
                        }
                    }
                }
                TABLEBODY () {
                    size_t totalReservedSize = 0;
                    size_t totalUsedSize = 0;
                    size_t totalMaxUsedSize = 0;
                    size_t totalCount = 0;
                    for (const auto& usage:
                         data.FastPathServiceInfo->ArenaMemoryUsage.PoolSlots)
                    {
                        totalReservedSize += usage.ReservedSize;
                        totalUsedSize += usage.UsedSize;
                        totalMaxUsedSize += usage.MaxUsedSize;
                        totalCount += usage.Count;
                        TABLER () {
                            TABLED () {
                                str << FormatByteSize(usage.SlotSize);
                            }
                            TABLED () {
                                str << FormatByteSize(usage.ReservedSize);
                            }
                            TABLED () {
                                str << FormatByteSize(usage.UsedSize);
                            }
                            TABLED () {
                                str << FormatByteSize(usage.MaxUsedSize);
                            }
                            TABLED () {
                                str << usage.Count;
                            }
                        }
                    }
                    TABLER () {
                        TABLED () {
                            str << "Total";
                        }
                        TABLED () {
                            str << FormatByteSize(totalReservedSize);
                        }
                        TABLED () {
                            str << FormatByteSize(totalUsedSize);
                        }
                        TABLED () {
                            str << FormatByteSize(totalMaxUsedSize);
                        }
                        TABLED () {
                            str << totalCount;
                        }
                    }
                }
            }
        }

        TAG (TH3) {
            str << "Memory usage by DBG";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "DBG";
                    }
                    TABLEH () {
                        str << "Reserved";
                    }
                    TABLEH () {
                        str << "Used";
                    }
                    TABLEH () {
                        str << "Count";
                    }
                }
            }
            TABLEBODY () {
                TArenaPoolStats totalMemoryStats;
                for (const auto& dbg: data.Dbgs) {
                    totalMemoryStats.Aggregate(dbg.MemoryStats);
                    TABLER () {
                        TABLED () {
                            str << "<a href='?TabletID="
                                << data.TabletInfo.TabletId
                                << "&page=dbg&dbg=" << dbg.Index << "'>#"
                                << dbg.Index << "</a>";
                        }
                        TABLED () {
                            str << FormatByteSize(dbg.MemoryStats.ReservedSize);
                        }
                        TABLED () {
                            str << FormatByteSize(dbg.MemoryStats.UsedSize);
                        }
                        TABLED () {
                            str << dbg.MemoryStats.AllocationCount;
                        }
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "Total";
                    }
                    TABLED () {
                        str << FormatByteSize(totalMemoryStats.ReservedSize);
                    }
                    TABLED () {
                        str << FormatByteSize(totalMemoryStats.UsedSize);
                    }
                    TABLED () {
                        str << totalMemoryStats.AllocationCount;
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
