#include "mon_util.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <ydb/core/base/services/blobstorage_service_id.h>

#include <library/cpp/resource/resource.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/string/subst.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

void AddResource(IOutputStream& str, TStringBuf tag, TStringBuf resourceName)
{
    TString content;
    if (!NResource::FindExact(resourceName, &content)) {
        str << "<!-- resource " << resourceName << " not found -->";
        return;
    }
    str << "<" << tag << ">" << content << "</" << tag << ">";
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

void AddScript(IOutputStream& str, TStringBuf resourceName)
{
    AddResource(str, "script", resourceName);
}

void AddStyle(IOutputStream& str, TStringBuf resourceName)
{
    AddResource(str, "style", resourceName);
}

TString HtmlEscape(TStringBuf in)
{
    TString escaped(in);
    SubstGlobal(escaped, "&", "&amp;");
    SubstGlobal(escaped, "<", "&lt;");
    SubstGlobal(escaped, ">", "&gt;");
    SubstGlobal(escaped, "\"", "&quot;");
    return escaped;
}

const char* PageParam(EMonPage page)
{
    switch (page) {
        case EMonPage::Overview:
            return "overview";
        case EMonPage::Dbg:
            return "dbg";
        case EMonPage::Chaos:
            return "chaos";
        case EMonPage::LocalDb:
            return "localdb";
        case EMonPage::VChunk:
            return "vchunk";
        case EMonPage::VChunkCounters:
            return "vchunkcounters";
        case EMonPage::Latency:
            return "latency";
        case EMonPage::Memory:
            return "memory";
    }
    return "overview";
}

const char* PageTitle(EMonPage page)
{
    switch (page) {
        case EMonPage::Overview:
            return "Overview";
        case EMonPage::Dbg:
            return "DBGs";
        case EMonPage::Chaos:
            return "Chaos";
        case EMonPage::LocalDb:
            return "Local DB";
        case EMonPage::VChunk:
            return "VChunk";
        case EMonPage::VChunkCounters:
            return "VChunk counters";
        case EMonPage::Latency:
            return "Latency";
        case EMonPage::Memory:
            return "Memory";
    }
    return "";
}

TString MakeDDiskMonPageUrl(const NKikimr::NBsController::TDDiskId& ddiskId)
{
    return TStringBuilder()
           << "/node/" << ddiskId.NodeId
           << Sprintf(
                  "/actors/ddisks/ddisk_p%09" PRIu32 "_s%09" PRIu32,
                  ddiskId.PDiskId,
                  ddiskId.DDiskSlotId);
}

void RenderDDiskLink(
    IOutputStream& str,
    const NKikimr::NBsController::TDDiskId& ddiskId)
{
    str << "<a href='" << MakeDDiskMonPageUrl(ddiskId) << "'>"
        << HtmlEscape(ddiskId.ToString()) << "</a>";
}

TString MakePBufferMonPageUrl(const NKikimr::NBsController::TDDiskId& pbufferId)
{
    const auto serviceId = NKikimr::MakeBlobStoragePersistentBufferId(
        pbufferId.NodeId,
        pbufferId.PDiskId,
        pbufferId.DDiskSlotId);
    return TStringBuilder()
           << "/node/" << pbufferId.NodeId << "/actors/persistent_buffer?pb="
           << CGIEscapeRet(serviceId.ToString());
}

void RenderPBufferLink(
    IOutputStream& str,
    const NKikimr::NBsController::TDDiskId& pbufferId)
{
    str << "<a href='" << MakePBufferMonPageUrl(pbufferId) << "'>"
        << HtmlEscape(pbufferId.ToString()) << "</a>";
}

TString HealthRollup(const TMap<EHostHealth, size_t>& counts)
{
    TStringBuilder result;
    for (const auto& [health, count]: counts) {
        if (!result.empty()) {
            result << " / ";
        }
        result << count << " " << ToString(health);
    }
    return result.empty() ? TString("-") : TString(result);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
