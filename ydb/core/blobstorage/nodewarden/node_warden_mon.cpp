#include "node_warden_impl.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

#include <ydb/library/pdisk_io/file_params.h>
#include <library/cpp/json/json_writer.h>

#include <util/string/split.h>
#include <util/string/strip.h>

using namespace NKikimr;
using namespace NStorage;

void TNodeWarden::Handle(NMon::TEvHttpInfo::TPtr &ev) {
    const TCgiParameters &cgi = ev->Get()->Request.GetParams();

    if (cgi.Get("page") == "distconf") {
        TActivationContext::Send(ev->Forward(DistributedConfigKeeperId));
        return;
    }

    TStringBuf pathInfo = ev->Get()->Request.GetPathInfo();
    TStringStream out;
    std::unique_ptr<NMon::TEvHttpInfoRes> result;
    if (pathInfo.StartsWith("/json")) {
        if (pathInfo.EndsWith("all")) {
        } else if (pathInfo.EndsWith("groups")) {
            std::set<ui32> groupsIds;
            if (cgi.Has("ids")) {
                StringSplitter(cgi.Get("ids")).SplitBySet(" ,;\n").SkipEmpty().Consume([&](TStringBuf token) {
                    i64 groupId = FromStringWithDefault<i64>(StripString(token), -1);
                    if (groupId != -1) {
                        groupsIds.emplace(groupId);
                    }
                });
            }
            RenderJsonGroupInfo(out, groupsIds);
        }
        result = std::make_unique<NMon::TEvHttpInfoRes>(NMonitoring::HTTPOKJSON + out.Str(), 0,
                NMon::IEvHttpInfoRes::EContentType::Custom);
    } else {
        RenderWholePage(out);
        result = std::make_unique<NMon::TEvHttpInfoRes>(out.Str());
    }
    Send(ev->Sender, result.release());
}

void TNodeWarden::RenderJsonGroupInfo(IOutputStream& out, const std::set<ui32>& groupIds) {
    std::set<ui32> allGroups;
    if (groupIds.empty()) {
        allGroups.insert(EjectedGroups.begin(), EjectedGroups.end());
        for (const auto& [groupId, _] : Groups) {
            allGroups.emplace(groupId);
        }
    }

    NJson::TJsonArray array;
    for (auto& groupId : (!groupIds.empty() ? groupIds : allGroups)) {
        NJson::TJsonValue groupInfo;

        groupInfo["GroupId"] = groupId;

        if (EjectedGroups.count(groupId)) {
            groupInfo["Status"] = "ejected";
        } else {
            TGroupRecord& group = Groups[groupId];
            groupInfo["Status"] = group.ProxyId ? "started" : "stopped";

            if (const auto& info = group.Info) {
                groupInfo["Generation"] = info->GroupGeneration;
                groupInfo["ErasureType"] = info->Type.ToString();
                groupInfo["EncryptionMode"] = TStringBuilder() << info->GetEncryptionMode();
                groupInfo["LifeCyclePhase"] = TStringBuilder() << info->GetLifeCyclePhase();

                NJson::TJsonArray vdisks;
                for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                    vdisks.AppendValue(info->GetVDiskId(i).ToString());
                }
                groupInfo["VDisks"] = std::move(vdisks);
            }
        }

        array.AppendValue(std::move(groupInfo));
    }
    NJson::WriteJson(&out, &array);
}

void TNodeWarden::RenderWholePage(IOutputStream& out) {
    HTML (out) {
        out << R"__(
            <style>
                table.oddgray > tbody > tr:nth-child(odd) {
                    background-color: #f0f0f0;
                }
            </style>
            )__";

        TAG(TH2) { out << "NodeWarden on node " << LocalNodeId; }

        TAG(TH3) {
            DIV() {
                out << "<a href=\"?page=distconf\">Distributed Config Keeper</a>";
            }
        }

        TAG(TH3) { out << "StorageConfig"; }
        DIV() {
            TString s;
            NProtoBuf::TextFormat::PrintToString(StorageConfig, &s);
            out << "<pre>" << s << "</pre>";
        }

        TAG(TH3) { out << "Static service set"; }
        DIV() {
            TString s;
            NProtoBuf::TextFormat::PrintToString(StaticServices, &s);
            out << "<pre>" << s << "</pre>";
        }

        TAG(TH3) { out << "Dynamic service set"; }
        DIV() {
            TString s;
            NProtoBuf::TextFormat::PrintToString(DynamicServices, &s);
            out << "<pre>" << s << "</pre>";
        }

        RenderLocalDrives(out);

        TAG(TH3) { out << "PDisks"; }
        TABLE_CLASS("table oddgray") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Id (NodeId, PDiskId)"; }
                    TABLEH() { out << "Path"; }
                    TABLEH() { out << "Guid"; }
                    TABLEH() { out << "Category"; }
                    TABLEH() { out << "Temporary"; }
                    TABLEH() { out << "Pending"; }
                }
            }
            TABLEBODY() {
                for (auto& [key, value] : LocalPDisks) {
                    TString pending;
                    if (const auto it = PDiskByPath.find(value.Record.GetPath()); it != PDiskByPath.end()) {
                        if (it->second.Pending) {
                            pending = TStringBuilder() << "PDiskId# " << it->second.Pending->GetPDiskID();
                        } else {
                            pending = "<none>";
                        }
                    } else {
                        pending = "<path not found>"; // this is strange
                    }

                    TABLER() {
                        TABLED() { out << "[" << key.NodeId << ":" << key.PDiskId << "]"; }
                        TABLED() { out << value.Record.GetPath(); }
                        TABLED() { out << value.Record.GetPDiskGuid(); }
                        TABLED() { out << value.Record.GetPDiskCategory(); }
                        TABLED() { out << value.Temporary; }
                        TABLED() { out << pending; }
                    }
                }
            }
        }
        if (!PDiskRestartInFlight.empty()) {
            DIV() {
                out << "PDiskRestartInFlight# " << FormatList(PDiskRestartInFlight);
            }
        }
        if (!PDisksWaitingToStart.empty()) {
            DIV() {
                out << "PDisksWaitingToStart# " << FormatList(PDisksWaitingToStart);
            }
        }

        TAG(TH3) { out << "VDisks"; }
        TABLE_CLASS("table oddgray") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Location"; }
                    TABLEH() { out << "VDiskId"; }
                    TABLEH() { out << "Running"; }
                    TABLEH() { out << "StoragePoolName"; }
                    TABLEH() { out << "DonorMode"; }
                    TABLEH() { out << "CurrentStatus"; }
                    TABLEH() { out << "ReportedVDiskStatus"; }
                }
            }
            TABLEBODY() {
                for (auto& [key, value] : LocalVDisks) {
                    TABLER() {
                        TABLED() { out << "[" << key.NodeId << ":" << key.PDiskId << ":" << key.VDiskSlotId << "]"; }
                        TABLED() { out << value.GetVDiskId(); }
                        TABLED() { out << (value.RuntimeData ? "true" : "false"); }
                        TABLED() { out << value.Config.GetStoragePoolName(); }
                        TABLED() { out << (value.Config.HasDonorMode() ? "true" : "false"); }
                        TABLED() {
                            out << value.Status;
                        }
                        TABLED() {
                            if (value.ReportedVDiskStatus) {
                                out << *value.ReportedVDiskStatus;
                            } else {
                                out << "(unknown)";
                            }
                        }
                    }
                }
            }
        }

        RenderDSProxies(out);
    }
}

void TNodeWarden::RenderDSProxies(IOutputStream& out) {
    HTML(out) {
        out << R"_(
            <script>
                function loadGroups(status) {
                    $.ajax({
                        url: document.URL + "/json/groups",
                        success: printGroupTable.bind(this, status)
                    });
                }

                function getOrEmpty(val) {
                    return val !== undefined ? val : "(empty)";
                }

                function printGroupTable(status, result) {
                    var tbody = document.getElementById(status + 'DSProxiesTBody');
                    tbody.innerHTML = "";
                    for (var i = 0; i < result.length; ++i) {
                        var html = '';
                        var group = result[i];
                        if (group['Status'] == status) {
                            html += "<td>" + getOrEmpty(group['GroupId']) + "</td>";
                            html += "<td>" + getOrEmpty(group['Generation']) + "</td>";
                            html += "<td>" + getOrEmpty(group['ErasureType']) + "</td>";
                            html += "<td>" + getOrEmpty(group['EncryptionMode']) + "</td>";
                            html += "<td>" + getOrEmpty(group['LifeCyclePhase']) + "</td>";
                            html += "<td>";
                            if (group['VDisks'] !== undefined) {
                                html += group['VDisks'].join('<br>');
                            } else {
                                html += "(empty)";
                            }
                            html += "</td>";
                        }
                        tbody.insertRow(-1).innerHTML = html;
                    }

                    //document.getElementById(status + 'DSProxiesButton').remove();
                }
            </script>
        )_";

        auto createTable = [&](IOutputStream& out, const TString& status, const ui64 rows) {
            out << "<table class='table oddgray'>";
            out << R"_( <thead>
                            <tr>
                                <th>GroupId</th>
                                <th>Generation</th>
                                <th>ErasureType</th>
                                <th>EncryptionMode</th>
                                <th>LifeCyclePhase</th>
                                <th>VDisks</th>
                            </tr>
                        </thead>
            )_";
            out << "<tbody id='" << status << "DSProxiesTBody'><tr><td colspan=6 style='text-align:center'>";
            out << "<button style='margin-top:30px;margin-bottom:30px' onclick='loadGroups(\"" << status << "\");'>";
            out << "Load, approx. rows " << rows;
            out << "</button>";
            out << "</td></tr></tbody>";
            out << "</table>";
        };

        ui32 numStarted = 0, numEjected = EjectedGroups.size();
        for (const auto& [groupId, group] : Groups) {
            numStarted += bool(group.ProxyId);
        }

        TAG(TH3) { out << "Started DSProxies"; }
        createTable(out, "started", numStarted);

        TAG(TH3) { out << "Ejected DSProxies"; }
        createTable(out, "ejected", numEjected);
    }
}

void TNodeWarden::RenderLocalDrives(IOutputStream& out) {
    TVector<NPDisk::TDriveData> onlineLocalDrives = ListLocalDrives();

    HTML(out) {
        TAG(TH3) { out << "LocalDrives"; }
        out << "\n";
        TABLE_CLASS("table oddgray") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "SentToBSC"; }
                    TABLEH() { out << "Online"; }
                    TABLEH() { out << "Path"; }
                    TABLEH() { out << "Serial"; }
                    TABLEH() { out << "DeviceType"; }
                }
            }
            out << "\n";
            TABLEBODY() {
                auto initialIt = WorkingLocalDrives.begin();
                auto onlineIt = onlineLocalDrives.begin();
                while (initialIt != WorkingLocalDrives.end() || onlineIt != onlineLocalDrives.end()) {
                    TABLER() {
                        NPDisk::TDriveData *initialData = nullptr;
                        NPDisk::TDriveData *onlineData = nullptr;
                        if (initialIt == WorkingLocalDrives.end()) {
                            onlineData = &*onlineIt;
                            ++onlineIt;
                        } else if (onlineIt == onlineLocalDrives.end()) {
                            initialData = &*initialIt;
                            ++initialIt;
                        } else {
                            if (initialIt->Path < onlineIt->Path) {
                                initialData = &*initialIt;
                                ++initialIt;
                            } else if (initialIt->Path > onlineIt->Path) {
                                onlineData = &*onlineIt;
                                ++onlineIt;
                            } else {
                                if (initialIt->SerialNumber == onlineIt->SerialNumber) {
                                    initialData = &*initialIt;
                                    ++initialIt;
                                    onlineData = &*onlineIt;
                                    ++onlineIt;
                                } else {
                                    initialData = &*initialIt;
                                    ++initialIt;
                                }
                            }
                        }
                        TABLED() { out << (initialData ? "true" : "<b style='color: red'>false</b>"); }
                        TABLED() { out << (onlineData ? "true" : "<b style='color: red'>false</b>"); }
                        NPDisk::TDriveData *data = initialData ? initialData : onlineData ? onlineData : nullptr;
                        Y_ABORT_UNLESS(data);
                        TABLED() { out << data->Path; }
                        TABLED() { out << data->SerialNumber.Quote(); }
                        TABLED() {
                            out << NPDisk::DeviceTypeStr(data->DeviceType, true);
                            out << (data->IsMock ? "(mock)" : "");
                        }
                    }
                    out << "\n";
                }
            }
        }
    }
}
