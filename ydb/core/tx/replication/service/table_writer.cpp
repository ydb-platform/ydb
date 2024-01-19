#include "table_writer.h"
#include "worker.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NReplication::NService {

class TLocalTableWriter: public TActor<TLocalTableWriter> {
    void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
        Worker = ev->Sender;
        Send(Worker, new TEvWorker::TEvHandshake());
    }

    void Handle(TEvWorker::TEvData::TPtr& ev) {
        Worker = ev->Sender;
        // TODO
    }

public:
    explicit TLocalTableWriter(const TString& path)
        : TActor(&TThis::StateWork)
        , Path(path)
    {
        Y_UNUSED(Path);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWorker::TEvHandshake, Handle);
            hFunc(TEvWorker::TEvData, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TString Path;

    TActorId Worker;

}; // TLocalTableWriter

IActor* CreateLocalTableWriter(const TString& path) {
    return new TLocalTableWriter(path);
}

}
