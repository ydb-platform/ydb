#include <ydb/core/base/events.h>

#include <ydb/core/protos/testload.pb.h>

namespace NKikimr {
struct TEvLoad {
    enum EEv {
        EvTestLoadRequest = EventSpaceBegin(TKikimrEvents::ES_TEST_LOAD),
        EvTestLoadResponse,
    };

    struct TEvTestLoadRequest : public TEventPB<TEvTestLoadRequest,
        NKikimr::TEvTestLoadRequest, EvTestLoadRequest>
    {};

    struct TEvTestLoadResponse : public TEventPB<TEvTestLoadResponse,
        NKikimr::TEvTestLoadResponse, EvTestLoadResponse>
    {};
};

}
