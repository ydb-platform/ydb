#include "etcd_shared.h"

namespace NEtcd {

namespace {
const TSharedStuff::TPtr Shared = std::make_shared<TSharedStuff>();
}

TSharedStuff::TPtr TSharedStuff::Get()  { return Shared; }

}
