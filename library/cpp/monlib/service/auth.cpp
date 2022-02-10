#include "auth.h"

#include <util/generic/hash_set.h>


namespace NMonitoring {
namespace {
    class TFakeAuthProvider final: public IAuthProvider {
    public:
        TAuthResult Check(const IHttpRequest&) override {
            return TAuthResult::Ok();
        }
    };

} // namespace

THolder<IAuthProvider> CreateFakeAuth() {
    return MakeHolder<TFakeAuthProvider>();
}


} // namespace NMonitoring
