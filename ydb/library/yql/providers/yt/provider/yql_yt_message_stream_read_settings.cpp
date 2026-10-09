#include "yql_yt_message_stream_impl.h"
#include <util/generic/yexception.h>
namespace NYql {
TYtMessageStreamReadSettings ParseYtMessageStreamReadSettings(const TExprNode& read) {
    Y_ENSURE(read.ChildrenSize() == 5, "Expected five read arguments");
    Y_ENSURE(read.Child(4)->IsList(), "Expected read settings list");
    const TExprNode* key = read.Child(2);
    if (key->IsCallable("MrTableConcat")) {
        Y_ENSURE(key->ChildrenSize() == 1, "Expected one YT queue path");
        key = key->Child(0);
    }
    TString path;
    if (key->IsCallable("MrObject")) {
        Y_ENSURE(key->ChildrenSize() >= 3 && key->Head().IsAtom(), "Expected a literal YT queue path");
        Y_ENSURE(key->Child(1)->IsAtom("raw") && key->Child(2)->Content().empty(),
            "YT MessageStream supports uncompressed FORMAT=raw");
        path = key->Head().Content();
    } else {
        Y_ENSURE(key->IsCallable("Key") && key->ChildrenSize() == 1, "Expected one YT queue path");
        const auto& entry = key->Head();
        Y_ENSURE(entry.IsList() && entry.ChildrenSize() == 2 && entry.Head().IsAtom("table")
            && entry.Child(1)->IsCallable("String") && entry.Child(1)->ChildrenSize() == 1 && entry.Child(1)->Head().IsAtom(), "Expected a literal YT queue path");
        path = entry.Child(1)->Head().Content();
    }
    TString consumer;
    for (const auto& setting : read.Child(4)->Children()) {
        Y_ENSURE(setting->IsList() && setting->ChildrenSize() == 2 && setting->Head().IsAtom(), "Invalid YT read setting");
        const auto name = setting->Head().Content();
        if (name == "consumer") {
            Y_ENSURE(consumer.empty() && setting->ChildrenSize() == 2 && setting->Child(1)->IsAtom(), "Expected one CONSUMER");
            consumer = setting->Child(1)->Content();
        } else if (name == "format") {
            Y_ENSURE(setting->ChildrenSize() == 2 && setting->Child(1)->IsAtom("raw"), "YT MessageStream supports FORMAT=raw");
        } else {
            ythrow yexception() << "Unsupported YT MessageStream read setting: " << name;
        }
    }
    Y_ENSURE(!consumer.empty(), "YT MessageStream requires CONSUMER");
    return {path, consumer};
}
}
