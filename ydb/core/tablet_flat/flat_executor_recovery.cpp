#include "flat_executor_recovery.h"

#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr::NTabletFlatExecutor::NRecovery {

using NTabletFlatExecutor::TTabletExecutedFlat;

class TRecoveryShard : public TActor<TRecoveryShard>, public TTabletExecutedFlat {
public:
    explicit TRecoveryShard(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateWork)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    {}

    void OnActivateExecutor(const TActorContext &ctx) override {
        SignalTabletActive(ctx);
        CompleteRestore();
    }

    void OnDetach(const TActorContext &) override {
        PassAway();
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &, const TActorContext &) override {
        PassAway();
    }

    void DefaultSignalTabletActive(const TActorContext &) override {}

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRestoreBackup, Handle);
            default:
                HandleDefaultEvents(ev, SelfId());
        }
    }

    void Handle(TEvRestoreBackup::TPtr& ev) {
        if (RestoreState == ERestoreState::NotStarted) {
            StartRestore(ev->Get()->BackupPath, ev->Sender);
        }
    }

    void StartRestore(const TString& backupPath, TActorId subscriber = {}) {
        RestoreState = ERestoreState::InProgress;
        TotalBytes = 1;

        BackupPath = backupPath;
        RestoreSubscriber = subscriber;

        Send(Tablet(), new TEvTablet::TEvCompleteRecoveryBoot);
    }

    void CompleteRestore() {
        RestoreState = ERestoreState::Done;
        RestoredBytes = 1;

        if (RestoreSubscriber) {
            Send(RestoreSubscriber, new TEvRestoreCompleted(true));
        }
    }

    using TRenderer = std::function<void(IOutputStream&)>;

    static void Alert(IOutputStream& str, TStringBuf type, TStringBuf text) {
        HTML(str) {
            DIV_CLASS(TStringBuilder() << "alert alert-" << type) {
                if (type == "warning") {
                    STRONG() {
                        str << "Warning: ";
                    }
                }
                str << text;
            }
        }
    }

    static void Warning(IOutputStream& str, TStringBuf text) {
        Alert(str, "warning", text);
    }
    static void Danger(IOutputStream& str, TStringBuf text) {
        Alert(str, "danger", text);
    }
    static void Info(IOutputStream& str, TStringBuf text) {
        Alert(str, "info", text);
    }
    static void Success(IOutputStream& str, TStringBuf text) {
        Alert(str, "success", text);
    }

    template <typename T>
    static void Header(IOutputStream& str, const T& title) {
        HTML(str) {
            DIV_CLASS("page-header") {
                TAG(TH3) {
                    str << title;
                }
            }
        }
    }

    static void Panel(IOutputStream& str, TRenderer title, TRenderer body) {
        HTML(str) {
            DIV_CLASS("panel panel-default") {
                DIV_CLASS("panel-heading") {
                    H4_CLASS("panel-title") {
                        title(str);
                    }
                }
                body(str);
            }
        }
    }

    static void SimplePanel(IOutputStream& str, const TStringBuf title, TRenderer body) {
        auto titleRenderer = [&title](IOutputStream& str) {
            HTML(str) {
                str << title;
            }
        };

        auto bodyRenderer = [body = std::move(body)](IOutputStream& str) {
            HTML(str) {
                DIV_CLASS("panel-body") {
                    body(str);
                }
            }
        };

        Panel(str, titleRenderer, bodyRenderer);
    }

    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext &ctx) override {
        if (!ev) {
            return true;
        }

        auto cgi = ev->Get()->Cgi();
        if (const auto& path = cgi.Get("restoreBackup")) {
            if (RestoreState == ERestoreState::NotStarted) {
                StartRestore(path);
            }
        }

        TStringStream str;
        HTML(str) {
            SimplePanel(str, "Restore Tablet", [&](IOutputStream& str) {
                HTML(str) {
                    FORM_CLASS("form-horizontal") {
                        DIV_CLASS("form-group") {
                            LABEL_CLASS_FOR("col-sm-2 control-label", "restoreBackup") {
                                str << "Backup Path";
                            }
                            DIV_CLASS("col-sm-10") {
                                str << "<input type='text' id='restoreBackup' name='restoreBackup' class='form-control' "
                                    << "placeholder='/tmp/backup/gen_312' required>";
                            }
                        }

                        DIV_CLASS("form-group") {
                            DIV_CLASS("col-sm-offset-2 col-sm-10") {
                                const char* state = RestoreState != ERestoreState::NotStarted ? "disabled" : "";
                                str << "<button type='submit' name='startRestore' class='btn btn-danger' " << state << ">"
                                    << "Start Restore"
                                << "</button>";
                            }
                        }
                    }

                    if (RestoreState != ERestoreState::NotStarted) {
                        if (RestoreState == ERestoreState::InProgress) {
                            Info(str, TStringBuilder() << "Restoring from " << BackupPath.GetPath().Quote());
                        } else if (RestoreState == ERestoreState::Done) {
                            Success(str, TStringBuilder() << "Restore from " << BackupPath.GetPath().Quote() << " completed successfully");
                        } else if (RestoreState == ERestoreState::Error) {
                            Danger(str, TStringBuilder() << "Restore from " << BackupPath.GetPath().Quote() << " failed: " << Error);
                        }

                        double p = TotalBytes != 0 ? (100.0 * RestoredBytes / TotalBytes) : 0;
                        DIV_CLASS_ID("progress", "restoreProgress") {
                            TAG_CLASS_STYLE(TDiv, "progress-bar progress-bar-infp", TStringBuilder() << "width:" << p << "%;") {
                                str << Sprintf("%.2f%", p);
                            }
                        }

                        TAG_ATTRS(TDiv, {{"id", "restoreDetails"}}) {
                            str << "Restored Bytes: " << RestoredBytes
                                << " / Total Bytes: " << TotalBytes;
                        }
                    }

                    str << R"(
                    <script>
                    $(document).ready(function() {
                        $('button[name="startRestore"]').click(function(e) {
                            e.preventDefault();

                            var btn = this;
                            var form = $(btn.form);

                            if (!confirm('Are you sure you want to start restore? This will override existing data')) {
                                return;
                            }

                            $.ajax({
                                type: "GET",
                                url: window.location.href,
                                data: form.serialize(),
                                success: function(response) {
                                    $('body').html(response);
                                },
                                error: function(xhr, status, error) {
                                    alert('Failed to start restore: ' + error);
                                }
                            });
                        });
                    });
                    </script>
                    )";
                }
            });
        }

        ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        return true;
    }

private:
    TFsPath BackupPath;
    ERestoreState RestoreState = ERestoreState::NotStarted;
    TString Error;

    ui64 RestoredBytes = 0;
    ui64 TotalBytes = 0;

    TActorId RestoreSubscriber; // only for tests
}; // TRecoveryShard

IActor* CreateRecoveryShard(const TActorId &tablet, TTabletStorageInfo *info) {
    return new TRecoveryShard(tablet, info);
}

} //namespace NKikimr::NTabletFlatExecutor::NRecovery
