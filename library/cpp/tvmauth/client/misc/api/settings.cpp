#include "settings.h"

#include <util/datetime/base.h>
#include <util/stream/file.h>
#include <util/system/fs.h>

#include <set>

namespace NTvmAuth::NTvmApi {
    void TClientSettings::CheckPermissions(const TString& dir) {
        const TString name = dir + "/check.tmp";

        try {
            NFs::EnsureExists(dir);

            TFile file(name, CreateAlways | RdWr);

            NFs::Remove(name);
        } catch (const std::exception& e) {
            NFs::Remove(name);
            ythrow TPermissionDenied() << "Permission denied to disk cache directory: " << e.what();
        }
    }

    void TClientSettings::CheckValid() const {
        if (DiskCacheDir) {
            CheckPermissions(DiskCacheDir);
        }

        if (TStringBuf(Secret)) {
            Y_ENSURE_EX(NeedServiceTicketsFetching(),
                        TBrokenTvmClientSettings() << "Secret is present but destinations list is empty. It makes no sense");
        }
        if (NeedServiceTicketsFetching()) {
            Y_ENSURE_EX(SelfTvmId != 0,
                        TBrokenTvmClientSettings() << "SelfTvmId cannot be 0 if fetching of Service Tickets required");
            Y_ENSURE_EX((TStringBuf)Secret,
                        TBrokenTvmClientSettings() << "Secret is required for fetching of Service Tickets");
        }

        if (CheckServiceTickets) {
            Y_ENSURE_EX(SelfTvmId != 0,
                        TBrokenTvmClientSettings() << "SelfTvmId cannot be 0 if checking of Service Tickets required");
        }

        if (FetchRolesForIdmSystemSlug) {
            Y_ENSURE_EX(DiskCacheDir,
                        TBrokenTvmClientSettings() << "Disk cache must be enabled to use roles: "
                                                      "they can be heavy");
        }

        bool needSmth = NeedServiceTicketsFetching() ||
                        CheckServiceTickets ||
                        CheckUserTicketsWithBbEnv;
        Y_ENSURE_EX(needSmth, TBrokenTvmClientSettings() << "Invalid settings: nothing to do");

        // Useless now: keep it here to avoid forgetting check from TDst. TODO: PASSP-35377
        for (const auto& dst : FetchServiceTicketsForDsts) {
            Y_ENSURE_EX(dst.Id != 0, TBrokenTvmClientSettings() << "TvmId cannot be 0");
        }
        // TODO: check only FetchServiceTicketsForDsts_
        // Python binding checks settings before normalization
        for (const auto& [alias, dst] : FetchServiceTicketsForDstsWithAliases) {
            Y_ENSURE_EX(dst.Id != 0, TBrokenTvmClientSettings() << "TvmId cannot be 0");
        }
        Y_ENSURE_EX(TiroleTvmId != 0, TBrokenTvmClientSettings() << "TiroleTvmId cannot be 0");
    }

    TClientSettings TClientSettings::CloneNormalized() const {
        TClientSettings res = *this;

        std::set<TTvmId> allDsts;
        for (const auto& tvmid : res.FetchServiceTicketsForDsts) {
            allDsts.insert(tvmid.Id);
        }
        for (const auto& [alias, tvmid] : res.FetchServiceTicketsForDstsWithAliases) {
            allDsts.insert(tvmid.Id);
        }
        if (FetchRolesForIdmSystemSlug) {
            allDsts.insert(res.TiroleTvmId);
        }

        res.FetchServiceTicketsForDsts = {allDsts.begin(), allDsts.end()};

        res.CheckValid();

        return res;
    }
}
