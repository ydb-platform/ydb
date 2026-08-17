#include "flat_executor.h"

#include <ydb/core/base/appdata.h>

#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <util/stream/hex.h>
#include <util/string/escape.h>
#include <library/cpp/html/pcdata/pcdata.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

namespace {

static constexpr TDuration DbMonRequestTimeout = TDuration::Seconds(60);
static constexpr TStringBuf DbMonRequestDeadlineHeader = "x-ydb-monitoring-deadline-us";

TInstant GetDbMonRequestDeadline(const NMon::TEvRemoteHttpInfo::TPtr& event) {
    const TString deadlineUs = event->Get()->GetHeader(DbMonRequestDeadlineHeader);
    const ui64 deadlineValue = FromStringWithDefault<ui64>(deadlineUs);
    if (deadlineValue) {
        return TInstant::MicroSeconds(deadlineValue);
    }
    return TAppData::TimeProvider->Now() + DbMonRequestTimeout;
}

}

class TExecutor::TTxExecutorDbMon : public TTransactionBase<TExecutor> {
public:
    NMon::TEvRemoteHttpInfo::TPtr Event;

    TTxExecutorDbMon(NMon::TEvRemoteHttpInfo::TPtr& event, TSelf *executor, TInstant deadline)
        : TBase(executor)
        , Event(event)
        , Deadline(deadline)
    {}

private:
    TInstant Deadline;
    ssize_t RowsScanned = 0;
    ui64 TotalDataSteps = 0;
    std::optional<TSerializedCellVec> LastSeenKey;
    TVector<TString> RenderedRows;

    // We check for timeout every TimeoutCheckRows rows, so that we don't check too often and don't check too rarely.
    static constexpr ui64 TimeoutCheckRows = 256;
    static constexpr ui64 OffsetScanPrechargeBytes = 16 * 1024 * 1024;

private:
    bool IsTimedOut() const {
        return TAppData::TimeProvider->Now() >= Deadline;
    }

    static TVector<TRawTypeValue> MakeRawKey(const NTable::TScheme::TTableInfo& tableInfo, TConstArrayRef<TCell> cells) {
        TVector<TRawTypeValue> key;
        key.reserve(cells.size());

        auto itColumn = tableInfo.KeyColumns.begin();
        for (const auto& cell : cells) {
            Y_ENSURE(itColumn != tableInfo.KeyColumns.end());
            const NTable::TColumn& columnInfo = tableInfo.Columns.find(*itColumn)->second;
            if (cell.IsNull()) {
                key.emplace_back();
            } else {
                key.emplace_back(cell.Data(), cell.Size(), columnInfo.PType.GetTypeId());
            }
            ++itColumn;
        }

        return key;
    }

public:
    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        if (IsTimedOut()) {
            return true;
        }

        TStringStream str;
        {
            const auto &scheme = txc.DB.GetScheme();
            TCgiParameters cgi(Event->Get()->Cgi());
            if (cgi.Has("TableID")) {
                ui32 tableId = FromStringWithDefault<ui32>(cgi.Get("TableID"));
                const auto *tableInfo = scheme.GetTableInfo(tableId);
                if (tableInfo != nullptr) {
                    str << "<script>$('.container').toggleClass('container container-fluid').css('padding-left','5%').css('padding-right','5%');</script>";
                    str << "<h3>" << tableInfo->Id << ":" << tableInfo->Name << "</h3>";
                    str << "<script>";
                    str << R"___(
                           function go() {
                             var location = 'db?';
                             var iargs = window.location.search.substring(1).split('&');
                             var oargs = [];
                             iargs.forEach(function(item){
                               var params = item.split('=');
                               if (params[0] == 'TabletID' || params[0] == 'TableID') {
                                 oargs.push(item);
                               }
                             });
                             location += oargs.join('&');
                             var keys = [];
                             $('#go-form').find('input[type=text]').each(function(){if($(this).val().length>0)keys.push($(this).val());});
                             location += "&Key=" + keys.join(',');
                             if ($('#exact-checkbox').prop('checked')) {
                               location += "&Lookup=Exact";
                             }
                             window.location.href = location;
                           }
                           )___";
                    str << "</script>";
                    str << "<form id='go-form' style='padding-bottom:10px'>";
                    for (auto id : tableInfo->KeyColumns) {
                        str << "<input type='text' id='key" << id << "' style='margin-right:3px'>";
                    }
                    str << R"___(
                           <input type='button' value='Go' onclick='go();' style='margin-right:3px'>
                           <input type='checkbox' id='exact-checkbox' checked='true'>
                           <label for='exact-checkbox' style='padding-left:5px'>Exact</label>
                           </form>
                           )___";
                    TVector<ui32> columns;
                    for (const auto& pr : tableInfo->Columns) {
                        columns.push_back(pr.first);
                    }
                    Sort(columns);
                    TVector<TRawTypeValue> key;
                    TList<TBuffer> vals;
                    if (cgi.Has("Key")) {
                        auto keys = SplitString(cgi.Get("Key"), ",");
                        auto itColumn = tableInfo->KeyColumns.begin();
                        for (const auto& val : keys) {
                            if (itColumn == tableInfo->KeyColumns.end()) {
                                break;
                            }
                            const auto& columnInfo = tableInfo->Columns.find(*itColumn)->second;
                            auto type = columnInfo.PType.GetTypeId();
                            switch (type) {
                            case NScheme::NTypeIds::Uint32:
                            {
                                ui32 v = FromStringWithDefault<ui32>(val);
                                vals.emplace_back();
                                TBuffer& buf = vals.back();
                                buf.Assign(reinterpret_cast<const char*>(&v), sizeof(v));
                                key.emplace_back(buf.Data(), buf.Size(), type);
                                break;
                            }
                            case NScheme::NTypeIds::Uint64:
                            {
                                ui64 v = FromStringWithDefault<ui64>(val);
                                vals.emplace_back();
                                TBuffer& buf = vals.back();
                                buf.Assign(reinterpret_cast<const char*>(&v), sizeof(v));
                                key.emplace_back(buf.Data(), buf.Size(), type);
                                break;
                            }
                            case NScheme::NTypeIds::String:
                            case NScheme::NTypeIds::Utf8:
                            {
                                vals.emplace_back();
                                TBuffer& buf = vals.back();
                                buf.Assign(val.data(), val.size());
                                key.emplace_back(buf.Data(), buf.Size(), type);
                                break;
                            }
                            default:
                                key.emplace_back();
                                break;
                            }
                            ++itColumn;
                        }
                        while (itColumn != tableInfo->KeyColumns.end()) {
                            key.emplace_back();
                            ++itColumn;
                        }
                    }
                    auto lookup = NTable::ELookup::GreaterOrEqualThan;
                    if (cgi.Get("Lookup") == "Exact") {
                        lookup = NTable::ELookup::ExactMatch;
                    }
                    const bool disableOffsetScanResume = cgi.Has("DisableOffsetScanResume");
                    const bool disableOffsetScanPrecharge = cgi.Has("DisableOffsetScanPrecharge");
                    if (disableOffsetScanResume) {
                        LastSeenKey.reset();
                        RowsScanned = 0;
                        RenderedRows.clear();
                    }
                    const bool canResumeByKey = lookup != NTable::ELookup::ExactMatch && !tableInfo->KeyColumns.empty();
                    if (LastSeenKey && (LastSeenKey->GetCells().size() != tableInfo->KeyColumns.size())) {
                        // LastSeenKey is captured from iterator GetKey() after a successful step.
                        // It must always be a full primary key for the same table.
                        // Better stop here in this case.
                        Y_TABLET_ERROR("Cannot resume offset scan because key size changed: "
                            << LastSeenKey->GetCells().size() << " vs " << tableInfo->KeyColumns.size());
                    }
                    if (LastSeenKey && !canResumeByKey) {
                        LastSeenKey.reset();
                        RowsScanned = 0;
                        RenderedRows.clear();
                    }
                    if (LastSeenKey && canResumeByKey) {
                        key = MakeRawKey(*tableInfo, LastSeenKey->GetCells());
                        lookup = NTable::ELookup::GreaterThan;
                    }
                    auto result = txc.DB.Iterate(tableId, key, columns, lookup);
                    str << "<table class='table table-sortable'>";
                    str << "<thead>";
                    str << "<tr>";
                    for (ui32 column : columns) {
                        const auto &columnInfo = tableInfo->Columns.find(column)->second;
                        str << "<th>" 
                            << column << ":" << NScheme::TypeName(columnInfo.PType, columnInfo.PTypeMod) 
                            << " " << columnInfo.Name
                        << "</th>";
                    }
                    str << "</tr>";
                    str << "</thead>";
                    str << "<tbody>";
                    ssize_t rowOffset = FromStringWithDefault<ssize_t>(cgi.Get("RowsOffset"), 0);
                    rowOffset = Max<ssize_t>(rowOffset, 0);
                    ssize_t rowLimit = FromStringWithDefault<ssize_t>(cgi.Get("MaxRows"), 1000);
                    rowLimit = Max<ssize_t>(rowLimit, 1);
                    ssize_t stringlimit = FromStringWithDefault<ssize_t>(cgi.Get("MaxString"), 1024);
                    stringlimit = Max<ssize_t>(stringlimit, 1);
                    const ssize_t rowsEnd = rowLimit > Max<ssize_t>() - rowOffset
                        ? Max<ssize_t>()
                        : rowOffset + rowLimit;
                    ssize_t rowCount = RowsScanned;

                    auto rememberIteratorKey = [&] {
                        const auto lastKey = result->GetKey().Cells();
                        if (lastKey) {
                            LastSeenKey.emplace(lastKey);
                        }
                    };

                    auto doPrecharge = [&] {
                        if (disableOffsetScanPrecharge) {
                            return;
                        }

                        TVector<TRawTypeValue> prechargeKey;
                        const auto lastKey = result->GetKey().Cells();
                        if (lastKey) {
                            prechargeKey = MakeRawKey(*tableInfo, lastKey);
                        } else if (LastSeenKey) {
                            prechargeKey = MakeRawKey(*tableInfo, LastSeenKey->GetCells());
                        } else {
                            prechargeKey = key;
                        }

                        txc.DB.Precharge(tableId, prechargeKey, {}, columns, 0, 0, OffsetScanPrechargeBytes);
                    };

                    while (rowCount < rowsEnd && result->Next(NTable::ENext::Data) == NTable::EReady::Data) {
                        ++TotalDataSteps;
                        ++RowsScanned;
                        rowCount = RowsScanned;
                        if (TotalDataSteps % TimeoutCheckRows == 0 && IsTimedOut()) {
                            return true;
                        }
                        if (rowCount > rowOffset) {
                            TStringStream rowStr;
                            {
                                auto& row = rowStr;
                                row << "<tr>";
                                TDbTupleRef tuple = result->GetValues();
                                for (size_t i = 0; i < columns.size(); ++i) {
                                    const void *data = tuple.Columns[i].Data();
                                    ui32 size = tuple.Columns[i].Size();
                                    row << "<td>";
                                    const auto& columnInfo = tableInfo->Columns.find(columns[i])->second;
                                    if (columnInfo.IsSensitive) {
                                        // Hide sensitive column value
                                        row << "<i>&lt;HIDDEN VALUE&gt;</i>";
                                    } else if (data == nullptr) {
                                        row << "<i>&lt;null&gt;</i>";
                                    } else {
                                        switch(tuple.Types[i].GetTypeId()) {
                                        case NScheme::NTypeIds::Int8:
                                            row << *(i8*)data;
                                            break;
                                        case NScheme::NTypeIds::Int16:
                                            row << *(i16*)data;
                                            break;
                                        case NScheme::NTypeIds::Uint16:
                                            row << *(ui16*)data;
                                            break;
                                        case NScheme::NTypeIds::Int32:
                                            row << *(i32*)data;
                                            break;
                                        case NScheme::NTypeIds::Uint32:
                                            row << *(ui32*)data;
                                            break;
                                        case NScheme::NTypeIds::Int64:
                                            row << *(i64*)data;
                                            break;
                                        case NScheme::NTypeIds::Uint64:
                                            row << *(ui64*)data;
                                            break;
                                        case NScheme::NTypeIds::Byte:
                                            row << (ui32)*(ui8*)data;
                                            break;
                                        case NScheme::NTypeIds::Bool:
                                            row << *(bool*)data;
                                            break;
                                        case NScheme::NTypeIds::Double:
                                            row << *(double*)data;
                                            break;
                                        case NScheme::NTypeIds::Float:
                                            row << *(float*)data;
                                            break;
                                        case NScheme::NTypeIds::Date:
                                            row << *(ui16*)data;
                                            break;
                                        case NScheme::NTypeIds::Datetime:
                                            row << *(ui32*)data;
                                            break;
                                        case NScheme::NTypeIds::Timestamp:
                                            row << *(ui64*)data;
                                            break;
                                        case NScheme::NTypeIds::Interval:
                                            row << *(i64*)data;
                                            break;
                                        case NScheme::NTypeIds::Date32:
                                            row << *(i32*)data;
                                            break;
                                        case NScheme::NTypeIds::Datetime64:
                                        case NScheme::NTypeIds::Timestamp64:
                                        case NScheme::NTypeIds::Interval64:
                                            row << *(i64*)data;
                                            break;
                                        case NScheme::NTypeIds::PairUi64Ui64:
                                            row << "(" << ((std::pair<ui64,ui64>*)data)->first << "," << ((std::pair<ui64,ui64>*)data)->second << ")";
                                            break;
                                        case NScheme::NTypeIds::String:
                                        case NScheme::NTypeIds::String4k:
                                        case NScheme::NTypeIds::String2m:
                                            row << EncodeHtmlPcdata(EscapeC(TStringBuf(static_cast<const char*>(data), Min(size, (ui32)stringlimit))));
                                            break;
                                        case NScheme::NTypeIds::ActorId:
                                            row << *(TActorId*)data;
                                            break;
                                        case NScheme::NTypeIds::Utf8:
                                        case NScheme::NTypeIds::Json:
                                            row << EncodeHtmlPcdata(TStringBuf((const char*)data, size));
                                            break;
                                        case NScheme::NTypeIds::JsonDocument: {
                                            const auto json = NBinaryJson::SerializeToJson(TStringBuf((const char*)data, size));
                                            row << "(JsonDocument) " << EncodeHtmlPcdata(json);
                                            break;
                                        }
                                        case NScheme::NTypeIds::DyNumber: {
                                            const auto number = NDyNumber::DyNumberToString(TStringBuf((const char*)data, size));
                                            row << "(DyNumber) " << number;
                                            break;
                                        }
                                        case NScheme::NTypeIds::Decimal: {
                                            tuple.Types[i].GetDecimalType().CellValueToStream(tuple.Columns[i].AsValue<std::pair<ui64, i64>>(), row);
                                            break;
                                        }
                                        case NScheme::NTypeIds::Pg: {
                                            auto convert = NPg::PgNativeTextFromNativeBinary(tuple.Columns[i].AsBuf(), tuple.Types[i].GetPgTypeDesc());
                                            row << EncodeHtmlPcdata(!convert.Error ? convert.Str : *convert.Error);
                                            break;
                                        }
                                        default:
                                            row << "<i>unknown type " << NScheme::TypeName(tuple.Types[i]) << "</i>";
                                            break;
                                        }
                                    }
                                    row << "</td>";
                                }
                                row << "</tr>";
                            }
                            RenderedRows.push_back(rowStr.Str());
                        }
                    }

                    rememberIteratorKey();

                    if (IsTimedOut()) {
                        return true;
                    }

                    if (result->Last() == NTable::EReady::Page) {
                        doPrecharge();
                        return false;
                    }

                    // More rows?
                    const auto moreRowsReady = result->Next(NTable::ENext::Data);
                    if (IsTimedOut()) {
                        return true;
                    }
                    if (moreRowsReady == NTable::EReady::Page) {
                        rememberIteratorKey();
                        doPrecharge();
                        return false;
                    }

                    auto fnPrintLink = [this, &str, tableId, &cgi] (ssize_t offset, ssize_t limit, TString caption) {
                        str << "<a href='db?TabletID=" << Self->TabletId()
                            << "&TableID=" << tableId;
                        if (cgi.Has("Key")) {
                            str << "&Key=" << cgi.Get("Key");
                        }
                        if (cgi.Has("Lookup")) {
                            str << "&Lookup=" << cgi.Get("Lookup");
                        }
                        str << "&RowsOffset=" << offset
                            << "&MaxRows=" << limit
                            << "'>" << caption << "</a>";
                    };

                    for (const auto& row : RenderedRows) {
                        str << row;
                    }
                    str << "</tbody>";
                    str << "</table>";

                    // Prev rows?
                    if (rowOffset > 0) {
                        ssize_t off = Max<ssize_t>(0, rowOffset - rowLimit);
                        ssize_t lim = Min<ssize_t>(rowOffset, rowLimit);
                        fnPrintLink(off, lim, Sprintf("Prev %" PRISZT " rows", lim));
                        str << "<br>";
                    }

                    if (moreRowsReady != NTable::EReady::Gone) {
                        fnPrintLink(rowCount, rowLimit, Sprintf("Next %" PRISZT " rows", rowLimit));
                        str << "<br>";
                    }

                    fnPrintLink(0, 1000000000, "All");
                    if (cgi.Has("TraceOffsetScan")) {
                        str << "<!-- DbMonOffsetScanDataSteps=" << TotalDataSteps << " -->";
                    }
                }
            }
        }
        if (IsTimedOut()) {
            return true;
        }
        ctx.Send(Event->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        return true;
    }

    void Complete(const TActorContext&) override {}
};

void TExecutor::RenderHtmlDb(NMon::TEvRemoteHttpInfo::TPtr &ev, const TActorContext &ctx) const {
    const_cast<TExecutor*>(this)->Execute(new TTxExecutorDbMon(ev, const_cast<TExecutor*>(this), GetDbMonRequestDeadline(ev)), ctx);
}


}
}
