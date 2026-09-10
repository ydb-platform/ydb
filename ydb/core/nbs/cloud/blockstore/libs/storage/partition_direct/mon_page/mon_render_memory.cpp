#include "mon_render_memory.h"

#include "mon_model.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

void RenderMemory(IOutputStream& str, const TMonPageData& data)
{
    size_t totalAllocatedMemorySize = 0;
    size_t totalUsedMemorySize = 0;

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
                        str << "Used";
                    }
                    TABLEH () {
                        str << "Allocated";
                    }
                }
            }
            TABLEBODY () {
                for (const auto& dbg: data.Dbgs) {
                    totalAllocatedMemorySize += dbg.AllocatedMemorySize;
                    totalUsedMemorySize += dbg.UsedMemorySize;
                    TABLER () {
                        TABLED () {
                            str << "<a href='?TabletID="
                                << data.TabletInfo.TabletId
                                << "&page=dbg&dbg=" << dbg.Index << "'>#"
                                << dbg.Index << "</a>";
                        }
                        TABLED () {
                            str << FormatByteSize(dbg.UsedMemorySize);
                        }
                        TABLED () {
                            str << FormatByteSize(dbg.AllocatedMemorySize);
                        }
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "Total";
                    }
                    TABLED () {
                        str << FormatByteSize(totalUsedMemorySize);
                    }
                    TABLED () {
                        str << FormatByteSize(totalAllocatedMemorySize);
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
