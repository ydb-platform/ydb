#include "mon_render_deleted_ddisks.h"

#include "mon_model.h"
#include "mon_util.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/datetime/base.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

const char* StatusName(
    NYdb::NBS::PartitionDirect::NProto::EDeletedDDiskStatus status)
{
    using namespace NYdb::NBS::PartitionDirect::NProto;
    switch (status) {
        case DELETED_DDISK_STATUS_REGISTERED:
            return "Registered";
        case DELETED_DDISK_STATUS_IN_PROGRESS:
            return "In progress";
        case DELETED_DDISK_STATUS_EXECUTED:
            return "Executed";
        default:
            return "Unknown";
    }
    return "Unknown";
}

}   // namespace

void RenderDeletedDDisks(IOutputStream& str, const TMonPageData& data)
{
    constexpr size_t RecordsPerPage = 200;
    const size_t recordCount = data.DeletedDDiskRecords.size();
    const size_t pageCount =
        recordCount / RecordsPerPage + (recordCount % RecordsPerPage != 0);
    const size_t page = pageCount
        ? Min(data.DeletedDDiskPage, pageCount - 1)
        : 0;
    const size_t firstRecord = page * RecordsPerPage;
    const size_t endRecord = Min(firstRecord + RecordsPerPage, recordCount);

    HTML (str) {
        TAG (TH3) {
            str << "Deleted DDisks (" << recordCount << ")";
        }
        if (pageCount > 1) {
            str << "<p>Page " << page + 1 << " of " << pageCount << " ";
            if (page > 0) {
                str << "<a href='?TabletID=" << data.TabletInfo.TabletId
                    << "&page=deletedddisks&ddisk_page=" << page - 1
                    << "'>Previous</a> ";
            }
            if (page + 1 < pageCount) {
                str << "<a href='?TabletID=" << data.TabletInfo.TabletId
                    << "&page=deletedddisks&ddisk_page=" << page + 1
                    << "'>Next</a>";
            }
            str << "</p>";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "Record ID";
                    }
                    TABLEH () {
                        str << "VChunk";
                    }
                    TABLEH () {
                        str << "Tablet generation";
                    }
                    TABLEH () {
                        str << "Status";
                    }
                    TABLEH () {
                        str << "Processing tablet generation";
                    }
                    TABLEH () {
                        str << "Timestamp (UTC)";
                    }
                    TABLEH () {
                        str << "DDisk ID";
                    }
                }
            }
            TABLEBODY () {
                for (size_t i = firstRecord; i < endRecord; ++i) {
                    const auto& record = data.DeletedDDiskRecords[i];
                    TABLER () {
                        TABLED () {
                            str << record.GetRecordId();
                        }
                        TABLED () {
                            str << "<a href='?TabletID="
                                << data.TabletInfo.TabletId
                                << "&page=vchunk&vchunk="
                                << record.GetVChunkIndex() << "'>"
                                << record.GetVChunkIndex() << "</a>";
                        }
                        TABLED () {
                            str << record.GetTabletGeneration();
                        }
                        TABLED () {
                            str << StatusName(record.GetStatus());
                        }
                        TABLED () {
                            if (record.GetProcessingTabletGeneration()) {
                                str << record.GetProcessingTabletGeneration();
                            } else {
                                str << "-";
                            }
                        }
                        TABLED () {
                            str << TInstant::MicroSeconds(record.GetTimestampUs())
                                       .ToStringUpToSeconds();
                        }
                        TABLED () {
                            RenderDDiskLink(
                                str,
                                NKikimr::NBsController::TDDiskId(
                                    record.GetDDiskId()));
                        }
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
