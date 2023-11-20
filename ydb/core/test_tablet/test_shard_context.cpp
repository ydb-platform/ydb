#include "test_shard_context.h"

namespace NKikimr::NTestShard {

    TTestShardContext::TTestShardContext()
        : Data(std::make_unique<TData>())
    {}

    TTestShardContext::~TTestShardContext()
    {}

    TTestShardContext::TPtr TTestShardContext::Create() {
        return MakeIntrusive<TTestShardContext>();
    }

    void TTestShardContext::TData::Action(TActorIdentity self, TEvStateServerRequest::TPtr ev) {
        const auto& record = ev->Get()->Record;
        switch (record.GetCommandCase()) {
            case ::NTestShard::TStateServer::TRequest::kWrite: {
                auto r = std::make_unique<TEvStateServerWriteResult>();
                r->Record = Processor.Execute(record.GetWrite());
                self.Send(ev->Sender, r.release(), 0, ev->Cookie);
                break;
            }

            case ::NTestShard::TStateServer::TRequest::kRead: {
                auto r = std::make_unique<TEvStateServerReadResult>();
                r->Record = Processor.Execute(record.GetRead());
                self.Send(ev->Sender, r.release(), 0, ev->Cookie);
                break;
            }

            case ::NTestShard::TStateServer::TRequest::kTabletInfo:
            case ::NTestShard::TStateServer::TRequest::COMMAND_NOT_SET:
                Y_ABORT("incorrect request received");
        }
        
    }

} // NKikimr::NTestShard
