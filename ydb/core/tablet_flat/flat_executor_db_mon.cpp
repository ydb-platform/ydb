#include "flat_executor.h"

#include <ydb/library/binary_json/read.h>
#include <ydb/library/dynumber/dynumber.h>

#include <util/stream/hex.h>
#include <util/string/escape.h>
#include <library/cpp/html/pcdata/pcdata.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

class TExecutor::TTxExecutorDbMon : public TTransactionBase<TExecutor> {
public:
    NMon::TEvRemoteHttpInfo::TPtr Event;

    TTxExecutorDbMon(NMon::TEvRemoteHttpInfo::TPtr& event, TSelf *executor)
        : TBase(executor)
        , Event(event)
    {}

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
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
                                key.emplace_back(buf.Data(), buf.Size(), NScheme::TTypeInfo(type));
                                break;
                            }
                            case NScheme::NTypeIds::Uint64:
                            {
                                ui64 v = FromStringWithDefault<ui64>(val);
                                vals.emplace_back();
                                TBuffer& buf = vals.back();
                                buf.Assign(reinterpret_cast<const char*>(&v), sizeof(v));
                                key.emplace_back(buf.Data(), buf.Size(), NScheme::TTypeInfo(type));
                                break;
                            }
                            case NScheme::NTypeIds::String:
                            case NScheme::NTypeIds::Utf8:
                            {
                                vals.emplace_back();
                                TBuffer& buf = vals.back();
                                buf.Assign(val.data(), val.size());
                                key.emplace_back(buf.Data(), buf.Size(), NScheme::TTypeInfo(type));
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
                    auto result = txc.DB.Iterate(tableId, key, columns, lookup);
                    str << "<table class='table table-sortable'>";
                    str << "<thead>";
                    str << "<tr>";
                    for (ui32 column : columns) {
                        const auto &columnInfo = tableInfo->Columns.find(column)->second;
                        str << "<th>" << column << ":" << columnInfo.Name << "</th>";
                    }
                    str << "</tr>";
                    str << "</thead>";
                    str << "<tbody>";
                    ssize_t rowOffset = FromStringWithDefault<ssize_t>(cgi.Get("RowsOffset"), 0);
                    rowOffset = Max<ssize_t>(rowOffset, 0);
                    ssize_t rowLimit = FromStringWithDefault<ssize_t>(cgi.Get("MaxRows"), 1000);
                    rowLimit = Max<ssize_t>(rowLimit, 1);
                    ssize_t rowCount = 0;
                    while (result->Next(NTable::ENext::Data) == NTable::EReady::Data && rowCount < rowOffset + rowLimit) {
                            ++rowCount;
                            if (rowCount > rowOffset) {
                                str << "<tr>";
                                TDbTupleRef tuple = result->GetValues();
                                for (size_t i = 0; i < columns.size(); ++i) {
                                    const void *data = tuple.Columns[i].Data();
                                    ui32 size = tuple.Columns[i].Size();
                                    str << "<td>";
                                    if (data == nullptr) {
                                        str << "<i>&lt;null&gt;</i>";
                                    } else {
                                        switch(tuple.Types[i].GetTypeId()) {
                                        case NScheme::NTypeIds::Int8:
                                            str << *(i8*)data;
                                            break;
                                        case NScheme::NTypeIds::Int16:
                                            str << *(i16*)data;
                                            break;
                                        case NScheme::NTypeIds::Uint16:
                                            str << *(ui16*)data;
                                            break;
                                        case NScheme::NTypeIds::Int32:
                                            str << *(i32*)data;
                                            break;
                                        case NScheme::NTypeIds::Uint32:
                                            str << *(ui32*)data;
                                            break;
                                        case NScheme::NTypeIds::Int64:
                                            str << *(i64*)data;
                                            break;
                                        case NScheme::NTypeIds::Uint64:
                                            str << *(ui64*)data;
                                            break;
                                        case NScheme::NTypeIds::Byte:
                                            str << (ui32)*(ui8*)data;
                                            break;
                                        case NScheme::NTypeIds::Bool:
                                            str << *(bool*)data;
                                            break;
                                        case NScheme::NTypeIds::Double:
                                            str << *(double*)data;
                                            break;
                                        case NScheme::NTypeIds::Float:
                                            str << *(float*)data;
                                            break;
                                        case NScheme::NTypeIds::Date:
                                            str << *(ui16*)data;
                                            break;
                                        case NScheme::NTypeIds::Datetime:
                                            str << *(ui32*)data;
                                            break;
                                        case NScheme::NTypeIds::Timestamp:
                                            str << *(ui64*)data;
                                            break;
                                        case NScheme::NTypeIds::Interval:
                                            str << *(i64*)data;
                                            break;
                                        case NScheme::NTypeIds::Date32:
                                            str << *(i32*)data;
                                            break;
                                        case NScheme::NTypeIds::Datetime64:
                                        case NScheme::NTypeIds::Timestamp64:
                                        case NScheme::NTypeIds::Interval64:
                                            str << *(i64*)data;
                                            break;
                                        case NScheme::NTypeIds::PairUi64Ui64:
                                            str << "(" << ((std::pair<ui64,ui64>*)data)->first << "," << ((std::pair<ui64,ui64>*)data)->second << ")";
                                            break;
                                        case NScheme::NTypeIds::String:
                                        case NScheme::NTypeIds::String4k:
                                        case NScheme::NTypeIds::String2m:
                                            str << EncodeHtmlPcdata(EscapeC(TStringBuf(static_cast<const char*>(data), Min(size, (ui32)1024))));
                                            break;
                                        case NScheme::NTypeIds::ActorId:
                                            str << *(TActorId*)data;
                                            break;
                                        case NScheme::NTypeIds::Utf8:
                                        case NScheme::NTypeIds::Json:
                                            str << EncodeHtmlPcdata(TStringBuf((const char*)data, size));
                                            break;
                                        case NScheme::NTypeIds::JsonDocument: {
                                            const auto json = NBinaryJson::SerializeToJson(TStringBuf((const char*)data, size));
                                            str << "(JsonDocument) " << EncodeHtmlPcdata(json);
                                            break;
                                        }
                                        case NScheme::NTypeIds::DyNumber: {
                                            const auto number = NDyNumber::DyNumberToString(TStringBuf((const char*)data, size));
                                            str << "(DyNumber) " << number;
                                            break;
                                        }
                                        case NScheme::NTypeIds::Pg: {
                                            str << "(Pg) " << NPg::PgTypeNameFromTypeDesc(tuple.Types[i].GetTypeDesc());
                                            break;
                                        }
                                        default:
                                            str << "<i>unknown type " << tuple.Types[i].GetTypeId() << "</i>";
                                            break;
                                        }
                                    }
                                    str << "</td>";
                                }
                                str << "</tr>";
                            }
                    }
                    str << "</tbody>";
                    str << "</table>";

                    if (result->Last() == NTable::EReady::Page)
                        return false;

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

                    // Prev rows?
                    if (rowOffset > 0) {
                        ssize_t off = Max<ssize_t>(0, rowOffset - rowLimit);
                        ssize_t lim = Min<ssize_t>(rowOffset, rowLimit);
                        fnPrintLink(off, lim, Sprintf("Prev %" PRISZT " rows", lim));
                        str << "<br>";
                    }

                    // More rows?
                    if (result->Next(NTable::ENext::Data) != NTable::EReady::Gone) {
                        fnPrintLink(rowCount, rowLimit, Sprintf("Next %" PRISZT " rows", rowLimit));
                        str << "<br>";
                    }

                    fnPrintLink(0, 1000000000, "All");
                }
            }
        }
        ctx.Send(Event->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        return true;
    }

    void Complete(const TActorContext&) override {}
};

void TExecutor::RenderHtmlDb(NMon::TEvRemoteHttpInfo::TPtr &ev, const TActorContext &ctx) const {
    const_cast<TExecutor*>(this)->Execute(new TTxExecutorDbMon(ev, const_cast<TExecutor*>(this)), ctx);
}


}
}
