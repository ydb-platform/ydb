#include "s3.h"

namespace NKikimr::NBlobDepot {

    using TS3Manager = TBlobDepot::TS3Manager;

    TS3Manager::TS3Manager(TBlobDepot *self)
        : Self(self)
    {}

    TS3Manager::~TS3Manager() = default;

    void TS3Manager::Init(const NKikimrBlobDepot::TS3BackendSettings *settings) {
        if (settings) {
            Y_ABORT("S3 is not supported on Windows");
        } else {
            SyncMode = false;
            AsyncMode = false;
        }
    }

    void TS3Manager::TerminateAllActors() {}

    void TS3Manager::Handle(TAutoPtr<IEventHandle> /*ev*/) {
        Y_ABORT("S3 is not supported on Windows");
    }

    void TS3Manager::AddTrashToCollect(TS3Locator /*locator*/) {
        Y_ABORT("S3 is not supported on Windows");
    }

    void TS3Manager::HandleDeleter(TAutoPtr<IEventHandle> /*ev*/) {
        Y_ABORT("S3 is not supported on Windows");
    }

    void TS3Manager::HandleScanner(TAutoPtr<IEventHandle> /*ev*/) {
        Y_ABORT("S3 is not supported on Windows");
    }

    void TS3Manager::OnKeyWritten(const TData::TKey& /*key*/, const TValueChain& /*valueChain*/) {}

    void TBlobDepot::Handle(TEvBlobDepot::TEvPrepareWriteS3::TPtr /*ev*/) {
        Y_ABORT("S3 is not supported on Windows");
    }

    void TBlobDepot::InitS3Manager() {
        S3Manager->Init(Config.HasS3BackendSettings() ? &Config.GetS3BackendSettings() : nullptr);
    }

} // NKikimr::NBlobDepot
