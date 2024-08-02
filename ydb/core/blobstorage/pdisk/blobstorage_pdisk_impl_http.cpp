#include "blobstorage_pdisk_impl.h"

#include <ydb/core/blobstorage/base/html.h>
#include <ydb/core/base/feature_flags.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr::NPDisk {

void TPDisk::RenderState(IOutputStream &str, THttpInfo &httpInfo) {
#define GREEN_TEXT(str, text) THtmlLightSignalRenderer(NKikimrWhiteboard::EFlag::Green, text).Output(str)
#define RED_TEXT(str, text) THtmlLightSignalRenderer(NKikimrWhiteboard::EFlag::Red, text).Output(str)
#define YELLOW_TEXT(str, text) THtmlLightSignalRenderer(NKikimrWhiteboard::EFlag::Yellow, text).Output(str)
    HTML(str) {
        TAG(TH4) {str << "Current state";}
        TABLE_CLASS ("table") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() {str << "Component";}
                    TABLEH() {str << "State";}
                    TABLEH() {str << "Brief state";}
                    TABLEH() {str << "Detailed state";}
                }
            }
            TABLEBODY() {
                TABLER() {
                    TABLED() {str << "PDisk";}
                    switch(Mon.PDiskBriefState->Val()) {
                    case TPDiskMon::TPDisk::OK:
                        TABLED() {GREEN_TEXT(str, TPDiskMon::TPDisk::StateToStr(Mon.PDiskState->Val()));}
                        TABLED() {GREEN_TEXT(str, TPDiskMon::TPDisk::BriefStateToStr(Mon.PDiskBriefState->Val()));}
                        break;
                    case TPDiskMon::TPDisk::Booting:
                        TABLED() {YELLOW_TEXT(str, TPDiskMon::TPDisk::StateToStr(Mon.PDiskState->Val()));}
                        TABLED() {YELLOW_TEXT(str, TPDiskMon::TPDisk::BriefStateToStr(Mon.PDiskBriefState->Val()));}
                        break;
                    case TPDiskMon::TPDisk::Error:
                        TABLED() {RED_TEXT(str, TPDiskMon::TPDisk::StateToStr(Mon.PDiskState->Val()));}
                        TABLED() {RED_TEXT(str, TPDiskMon::TPDisk::BriefStateToStr(Mon.PDiskBriefState->Val()));}
                        break;
                    }
                    TABLED() {str << TPDiskMon::TPDisk::DetailedStateToStr(Mon.PDiskDetailedState->Val());}
                }
                TABLER() {
                    TABLED() {str << "Device";}
                    TABLED() {str << httpInfo.DeviceFlagStr;}
                }
                TABLER() {
                    TABLED() {str << "Realtime";}
                    TABLED() {str << httpInfo.RealtimeFlagStr;}
                }
                TABLER() {
                    TABLED() {str << "Worst VDisk";}
                    TABLED() {
                        TOwnerData::EVDiskStatus worstStatus = TOwnerData::VDISK_STATUS_LOGGED;
                        for (const TOwnerData& data : OwnerData) {
                            if (data.VDiskId != TVDiskID::InvalidId && data.Status < worstStatus) {
                                worstStatus = data.Status;
                            }
                        }
                        if (worstStatus < TOwnerData::VDISK_STATUS_LOGGED) {
                            YELLOW_TEXT(str, TOwnerData::RenderStatus(worstStatus));
                        } else {
                            GREEN_TEXT(str, TOwnerData::RenderStatus(worstStatus));
                        }
                    }
                }
                TABLER() {
                    TABLED() {str << "SerialNumber match";}
                    TABLED() {
                        if (!Cfg->ExpectedSerial) {
                            YELLOW_TEXT(str, "Not set");
                        } else if (Cfg->ExpectedSerial != DriveData.SerialNumber) {
                            RED_TEXT(str, "Error");
                        } else {
                            GREEN_TEXT(str, "Ok");
                        }
                    }
                }
            }
        }
        TAG(TH4) {str << "State description"; }
        if (Cfg->SectorMap) {
            PARA() {str << "Note - this is SectorMap device<br>"; }
        }
        if (!Cfg->EnableSectorEncryption) {
            PARA() {str << "Note - PDisk sector enctyption is disabled<br>"; }
        }
        PARA() {str << httpInfo.ErrorStr; }
        TAG(TH4) {str << "Uptime"; }
        PARA() {
            TDuration uptime = TInstant::Now() - CreationTime;
            if (uptime.Days() > 0) {
                str << Sprintf("%2lu days ", uptime.Days());
            }
            str << Sprintf("%02lu:%02lu:%02lu", uptime.Hours() % 24, uptime.Minutes() % 60, uptime.Seconds() % 60);
        }
        // Restart button
        TAG(TH4) {str << "Restart"; }
        DIV() {
            str << R"___(
                <script>
                    function reloadPage(data) {
                        if (data.result) {
                            window.location.replace(window.location.href);
                        } else {
                            alert(data.error);
                        }
                    }

                    function sendRestartRequest() {
                        $.ajax({
                            url: "",
                            data: "restartPDisk=",
                            method: "POST",
                            success: reloadPage
                        });
                    }

                    function sendStopRequest() {
                        $.ajax({
                            url: "",
                            data: "stopPDisk=",
                            method: "POST",
                            success: reloadPage
                        });
                    }
                </script>
            )___";
            str << "<button onclick='sendRestartRequest()' name='restartPDisk' class='btn btn-default' ";
            str << "style='background:LightGray; margin:5px' ";
            str << ">";
            str << "Restart";
            str << "</button>";

            if (Cfg->SectorMap) {
                str << "<button onclick='sendStopRequest()' name='stopPDisk' class='btn btn-default' ";
                str << "style='background:Tomato; margin:5px'>";
                str << "Stop";
                str << "</button>";
            }
        }
        if (Cfg->SectorMap) {
            TAG(TH4) {str << "SectorMap"; }
            PRE() {str << Cfg->SectorMap->ToString();}
        }
        TAG(TH4) { str << "Metadata"; }
        TABLE_CLASS ("table") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() {str << "Parameter";}
                    TABLEH() {str << "Value";}
                }
            }
            auto kv = [&](const auto& key, const auto& value) {
                TABLER() {
                    TABLED() { str << key; }
                    TABLED() { str << value; }
                }
            };
            TABLEBODY() {
                std::visit(TOverloaded{
                    [&](const std::monostate&) {
                        kv("State", "monostate");
                    },
                    [&](const NMeta::TFormatted& x) {
                        kv("State", "Formatted");
                        kv("Slots.size", x.Slots.size());
                        kv("ReadPending.size", x.ReadPending.size());
                        kv("NumReadsInFlight", x.NumReadsInFlight);
                        kv("Parts.size", x.Parts.size());
                    },
                    [&](const NMeta::TUnformatted& x) {
                        kv("State", "Unformatted");
                        kv("Format.has_value", x.Format.has_value());
                    },
                }, Meta.State);
                kv("StoredMetadata", std::visit<TString>(TOverloaded{
                    [](const NMeta::TScanInProgress&) { return "ScanInProgress"; },
                    [](const NMeta::TNoMetadata&) { return "NoMetadata"; },
                    [](const NMeta::TError& e) { return TStringBuilder() << "Error# " << e.Description; },
                    [](const TRcBuf& meta) { return TStringBuilder() << "Metadata Size# " << meta.size(); },
                }, Meta.StoredMetadata));
                kv("Requests.size", Meta.Requests.size());
                kv("WriteInFlight", Meta.WriteInFlight);
                kv("NextSequenceNumber", Meta.NextSequenceNumber);
            }
        }
        TAG(TH4) {str << "Config"; }
        PRE() {str << Cfg->ToString(true);}
        if (Mon.PDiskBriefState->Val() != TPDiskMon::TPDisk::Booting) {
            TAG(TH4) {str << "Drive Data"; }
            PRE() {str << DriveData.ToString(true);}
            TAG(TH4) {str << "Fair Scheduler"; }
            PRE() {str << httpInfo.FairSchedulerStr;}
            TAG(TH4) {str << "Format info"; }
            PRE() {str << Format.ToString(true);}
            TAG(TH4) {str << "Drive model"; }
            PRE() {str << DriveModel.ToString(true);}
            TAG(TH4) {str << "Sys log record"; }
            PRE() {str << SysLogRecord.ToString(true);}
            TAG(TH4) {str << "Logged NONCEs"; }
            PRE() {str << LoggedNonces.ToString(true);}
            TAG(TH4) {str << "Dynamic state"; }
            PRE() {str << DynamicStateToString(true);}
            TAG(TH4) {str << "Last Nonce Jump Log Page Header"; }
            PRE() {str << LastNonceJumpLogPageHeader2.ToString(true);}
            TAG(TH4) {str << "VDisk statuses"; }
            PRE() {
                for (const TOwnerData& data : OwnerData) {
                    if (data.VDiskId != TVDiskID::InvalidId) {
                        str << "VDiskId# " << data.VDiskId.ToStringWOGeneration()
                            << " Status# " << data.GetStringStatus() << Endl;
                    }
                }
            }
        }
    }
#undef GREEN_TEXT
#undef RED_TEXT
#undef YELLOW_TEXT
}

void TPDisk::OutputHtmlOwners(TStringStream &str) {
    ui64 chunksOwned[256];
    memset(chunksOwned, 0, sizeof(chunksOwned));


    size_t size = ChunkState.size();
    for (size_t idx = 0; idx < size; idx++) {
        chunksOwned[ChunkState[idx].OwnerId]++;
    }

    HTML(str) {
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { str << "OwnerId";}
                    TABLEH() { str << "VDiskId"; }
                    TABLEH() { str << "ChunksOwned"; }
                    TABLEH() { str << "CutLogId"; }
                    TABLEH() { str << "WhiteboardProxyId"; }
                    TABLEH() { str << "CurLsnToKeep"; }
                    TABLEH() { str << "AskedFreeUpToLsn"; }
                    TABLEH() { str << "AskedLogChunkToCut"; }
                    TABLEH() { str << "LogChunkCountBeforeCut"; }
                    TABLEH() { str << "FirstNonceToKeep"; }
                    TABLEH() { str << "AskedToCutLogAt"; }
                    TABLEH() { str << "CutLogAt"; }
                    TABLEH() { str << "OperationLog"; }
                }
            }
            TABLEBODY() {
                for (ui32 owner = 0; owner < OwnerData.size(); ++owner) {
                    const TOwnerData &data = OwnerData[owner];
                    if (data.VDiskId != TVDiskID::InvalidId) {
                        TABLER() {
                            TABLED() { str << (ui32) owner;}
                            TABLED() { str << data.VDiskId.ToStringWOGeneration() << "<br/>(" << data.VDiskId.GroupID << ")"; }
                            TABLED() { str << chunksOwned[owner]; }
                            TABLED() { str << data.CutLogId.ToString(); }
                            TABLED() { str << data.WhiteboardProxyId; }
                            TABLED() { str << data.CurrentFirstLsnToKeep; }
                            TABLED() { str << data.AskedFreeUpToLsn; }
                            TABLED() { str << data.AskedLogChunkToCut; }
                            TABLED() { str << data.LogChunkCountBeforeCut; }
                            TABLED() { str << SysLogFirstNoncesToKeep.FirstNonceToKeep[owner]; }
                            TABLED() { str << data.AskedToCutLogAt; }
                            TABLED() {
                                if (data.CutLogAt < data.AskedToCutLogAt) {
                                    str << "<font color=\"red\">";
                                    str << data.CutLogAt;
                                    str << "</font>";
                                } else {
                                    str << data.CutLogAt;
                                }
                            }
                            TABLED() {
                                ui32 logSize = OwnerData[owner].OperationLog.Size();
                                str << "<button type='button' class='btn btn-default' data-toggle='collapse' style='margin:5px' \
                                    data-target='#operationLogCollapse" << owner <<
                                    "'>Show last " << logSize << " operations</button>";

                                str << "<div id='operationLogCollapse" << owner << "' class='collapse'>";
                                for (ui32 i = 0; i < logSize; ++i) {
                                    auto record = OwnerData[owner].OperationLog.BorrowByIdx(i);
                                    str << *record << "<br><br>";
                                    OwnerData[owner].OperationLog.ReturnBorrowedRecord(record);
                                }
                                str << "</div>";
                            }
                        }
                    }
                }
            }
        }
    }
}

void TPDisk::OutputHtmlLogChunksDetails(TStringStream &str) {
    HTML(str) {
        TABLE_CLASS ("table table-condensed") {
            TVector<ui32> activeOwners;
            for (auto& [vdiskId, owner] : VDiskOwners) {
                activeOwners.push_back(owner);
            }
            Sort(activeOwners);
            TABLEHEAD() {
                TABLER() {
                    TABLEH() {str << "#";}
                    TABLEH() {str << "ChunkId";}
                    TABLEH() {str << "IsCommited";}
                    TABLEH() {str << "Nonces";}
                    TABLEH() {str << "Users";}
                    for (ui32 owner : activeOwners) {
                        const TVDiskID &id = OwnerData[owner].VDiskId;
                        if (id == TVDiskID::InvalidId) {
                            TABLEH() {str << "o" << owner << "v--"; }
                        } else {
                            TABLEH() {str << "o" << owner << "v" << id.ToStringWOGeneration() << "<br/>(" << id.GroupID << ")"; }
                        }
                    }
                }
            }
            TABLEBODY() {
                ui32 idx = 0;
                for (auto it = LogChunks.begin(); it != LogChunks.end(); ++it) {
                    ++idx;
                    TABLER() {
                        TABLED() { str << idx;}
                        TABLED() { str << it->ChunkIdx; }
                        TABLED() { str << (ChunkState[it->ChunkIdx].CommitState == TChunkState::DATA_COMMITTED); }
                        TABLED() { str << "[" << it->FirstNonce << ", " << it->LastNonce << "]"; }
                        TABLED() { str << it->CurrentUserCount; }
                        for (ui32 owner : activeOwners) {
                            if (owner < it->OwnerLsnRange.size()) {
                                const TLogChunkInfo::TLsnRange &range = it->OwnerLsnRange[owner];
                                const auto askedFreeUpToLsn = OwnerData[owner].AskedFreeUpToLsn;
                                if (range.IsPresent) {
                                    TABLED() {
                                        str << "<font color=\"" << (range.LastLsn < askedFreeUpToLsn ? "red" : "black") <<"\">"
                                            << "[" << range.FirstLsn << ", " << range.LastLsn << "]"
                                            << "</font>";
                                    }
                                } else {
                                    TABLED() {str << "-";}
                                }
                            } else {
                                TABLED() {str << ".";}
                            }
                        }
                        if (it->CurrentUserCount > activeOwners.size()) {
                            // There are some owners of chunks that are not present in VDiskOwners
                            for (ui64 chunkOwner = 0; chunkOwner < it->OwnerLsnRange.size(); ++chunkOwner) {
                                const TLogChunkInfo::TLsnRange &range = it->OwnerLsnRange[chunkOwner];
                                if (range.IsPresent &&
                                        !std::count(activeOwners.begin(), activeOwners.end(), chunkOwner)) {
                                    TABLED() {
                                        str << "ERROR! ownerId# " << chunkOwner
                                            << " OwnerLsnRange.size()# " << it->OwnerLsnRange.size()
                                            << " [" << range.FirstLsn << ", " << range.LastLsn << "]";
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

void TPDisk::OutputHtmlChunkLockUnlockInfo(TStringStream &str) {
    using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
    bool chunkLockingEnabled = NKikimr::AppData(ActorSystem)->FeatureFlags.GetEnableChunkLocking();

    auto commonParams = [&] (TStringStream &str, TString requestName) {
        for (TEvChunkLock::ELockFrom from : { TEvChunkLock::ELockFrom::LOG, TEvChunkLock::ELockFrom::PERSONAL_QUOTA } ) {
            str << "<input id='" << requestName << "LockFrom_" << TEvChunkLock::ELockFrom_Name(from) <<
                "' name='lockFrom' type='radio' value='" << TEvChunkLock::ELockFrom_Name(from) << "'";
            if (from == TEvChunkLock::ELockFrom::PERSONAL_QUOTA) {
                str << " checked";
            }
            str << "/>";
            str << "<label for='" << requestName << "LockFrom_'" << TEvChunkLock::ELockFrom_Name(from) << "'>" <<
                TEvChunkLock::ELockFrom_Name(from) << "</label>";
        }
        str << "<br>";
        str << "<input id='" << requestName << "ByVDiskId' name='byVDiskId' type='checkbox'/>";
        str << "<label for='" << requestName << "ByVDiskId'>" << "By VDisk Id" << "</label>";
        str << "<br>";
        str << "<input id='" << requestName << "Owner' name='owner' type='text' value='" << 4 << "'/>";
        str << "<label for='" << requestName << "Owner'>" << "Owner Id" << "</label>";
        str << "<br>";
        str << "<input id='" << requestName << "VDiskId' name='vdiskId' type='text' value='" << "[0:_:0:0:0]" << "'/>";
        str << "<label for='" << requestName << "VDiskId'>" << "VDisk Id" << "</label>";
    };


    HTML(str) {
        if (chunkLockingEnabled) {
            str << "<button type='button' class='btn btn-default' data-toggle='collapse' style='margin:5px' \
                data-target='#lockByColorCollapse'> Lock by color </button>";
            str << "<button type='button' class='btn btn-default' data-toggle='collapse' style='margin:5px' \
                data-target='#lockByCountCollapse'> Lock by count </button>";
            str << "<button type='button' class='btn btn-default' data-toggle='collapse' style='margin:5px' \
                data-target='#unlockCollapse'> Unlock </button>";

            str << "<div id='lockByColorCollapse' class='collapse'>";
            str << "<form class='form_horizontal' method='post'>";
            LABEL_CLASS_FOR("control-label", "color") { str << "Color"; }
            for (TColor::E color : { TColor::CYAN, TColor::LIGHT_YELLOW, TColor::YELLOW, TColor::LIGHT_ORANGE,
                    TColor::ORANGE, TColor::RED, TColor::BLACK} ) {
                str << "<input id='inputColor_" << TPDiskSpaceColor_Name(color) << "' name='spaceColor' type='radio' value='"
                    << TPDiskSpaceColor_Name(color) << "'";
                if (color == TColor::CYAN) {
                    str << " checked";
                }
                str << "/>";
                TString textColor = color == TColor::BLACK ? "white" : "black";
                str << "<label for='inputColor_'" << TPDiskSpaceColor_Name(color) << "' style='background:" <<
                    TPDiskSpaceColor_HtmlCode(color) << "; color:" << textColor << ";'>" <<
                    TPDiskSpaceColor_Name(color) << "</label>";
            }

            str << "<br>";
            commonParams(str, "lockByColor");
            str << "<br>";

            str << "<button type='submit' name='chunkLockByColor' class='btn btn-default'\
                style='background:red; margin:5px'>Lock by color</button>";
            str << "</form>";
            str << "</div>";

            str << "<div id='lockByCountCollapse' class='collapse'>";
            str << "<form class='form_horizontal' method='post'>";
            LABEL_CLASS_FOR("control-label", "count") { str << "Count"; }
            str << "<input id='inputByCountCount' name='count' type='text' value='" <<
                ChunkState.size() << "'/>";
            str << "<br>";
            commonParams(str, "lockByCount");
            str << "<br>";
            str << "<button type='submit' name='chunkLockByCount' class='btn btn-default' \
                style='background:red; margin:5px'>Lock by count</button>";
            str << "</form>";
            str << "</div>";

            str << "<div id='unlockCollapse' class='collapse'>";
            str << "<form class='form_horizontal' method='post'>";
            commonParams(str, "unlock");
            str << "<br>";
            str << "<button type='submit' name='chunkUnlock' class='btn btn-default' \
                style='background:green; margin:5px'>Unlock</button>";
            str << "</form>";
            str << "</div>";
        }

        COLLAPSED_BUTTON_CONTENT("chunksStateTable", "Chunks State") {
            str << "<style>";
            str << "#chunksStateTable th, #chunksStateTable td { border: 1px solid #000; padding: 1px; }";
            str << "#chunksStateTable th { position: sticky; top: 0; }";
            str << "#chunksStateTable table { width: 100%; }";
            str << "</style>";
            TABLE_CLASS ("") {
                const size_t columns = 50;
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "Chunk";}
                        for (size_t n = 0; n < columns; ++n) {
                            TABLEH() {str << Sprintf("%02d", (int)n);}
                        }
                    }
                }
                TABLEBODY() {
                    size_t size = ChunkState.size();
                    for (size_t i = 0; i < size; i += columns) {
                        TABLER() {
                            TABLED() {str << (ui32)i;}
                            for (size_t n = 0; n < columns; ++n) {
                                size_t idx = i + n;
                                if (idx < size) {
                                    const TChunkState &chunk = ChunkState[idx];
                                    TABLED() {
                                        str << "<span style='color:";
                                        str << (chunk.CommitState == TChunkState::LOCKED ? "red" : "black");
                                        str << ";'>";
                                        if (chunk.OwnerId == (TOwner) OwnerSystem) {
                                            str << "L";
                                        } else if (chunk.OwnerId == OwnerUnallocated) {
                                            str << ".";
                                        } else if (chunk.OwnerId == OwnerUnallocatedTrimmed) {
                                            str << ",";
                                        } else if (chunk.OwnerId == OwnerLocked) {
                                            str << "X";
                                        } else if (chunk.OwnerId == OwnerMetadata) {
                                            str << 'M';
                                        } else {
                                            str << (ui32)chunk.OwnerId;
                                            if (chunk.CommitState != TChunkState::DATA_COMMITTED && chunk.CommitState != TChunkState::LOCKED) {
                                                str << "-";
                                            }
                                        }
                                        str << "</span>";
                                    }
                                } else {
                                    TABLED() {}
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

void TPDisk::HttpInfo(THttpInfo &httpInfo) {
    TEvHttpInfoResult *reportResult = new TEvHttpInfoResult(httpInfo.EndCustomer);
    if (httpInfo.DoGetSchedule) {
        TStringStream out;
        out << "HTTP/1.1 200 Ok\r\n"
            << "Content-Type: text/html\r\n"
            << "Access-Control-Allow-Origin: *\r\n"
            << "Connection: Close\r\n\r\n";
        TGuard<TMutex> guard(StateMutex);
        ForsetiScheduler.OutputLog(out);
        reportResult->HttpInfoRes = new NMon::TEvHttpInfoRes(out.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom);
        ActorSystem->Send(httpInfo.Sender, reportResult);
    } else {
        TStringStream str = httpInfo.OutputString;
        TGuard<TMutex> guard(StateMutex);
        HTML(str) {
            str << "<style>@media (min-width: 1600px) {.container { width: 1560px;}} </style>"; // Make this page's container wider on big screens.
            DIV_CLASS("row") {
                DIV_CLASS("col-md-7") { RenderState(str, httpInfo); }
                DIV_CLASS("col-md-5") {
                    str << "<button type='button' class='btn btn-default' data-toggle='collapse' style='margin:5px'\
                        data-target='#countersHtml'> Solomon counters </button>";
                    str << "<div id='countersHtml' class='collapse'>";
                    Mon.Counters->OutputHtml(str);
                    str << "</div>";
                }
            }

            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Chunks Keeper";
                }
                DIV_CLASS("panel-body") {
                    Keeper.PrintHTML(str);
                }
            }

            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Owners";
                }
                TAG_CLASS_STYLE(TDiv, "panel-body", "overflow-x: scroll;") {
                    OutputHtmlOwners(str);
                }
            } // Owners

            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Log Chunk Details";
                }
                DIV_CLASS("panel-body") {
                    OutputHtmlLogChunksDetails(str);
                }
            } // Log Chunk Details

            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Chunks";
                }
                DIV_CLASS("panel-body") {
                    OutputHtmlChunkLockUnlockInfo(str);
                }
            } // Chunks

        }
        reportResult->HttpInfoRes = new NMon::TEvHttpInfoRes(str.Str());
        ActorSystem->Send(httpInfo.Sender, reportResult);
    }
}

} // NKikimr::NPDisk
