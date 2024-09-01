#include "abstract.h"

namespace NKikimr::NOlap::NDataLocks {

// helper type for the visitor
template<class... Ts>
struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

bool TLockFilter::operator()(const TString& name, const ELockCategory c) const {
    auto present = std::visit(overloaded{
        [&c](TLockCategories categories) {
            return categories[c];
        },
        [&name](const THashSet<TString>& names) {
            return names.contains(name);
        }
    }, Filter);

    switch (Type) {
        case EType::Including:
            return present;
        case EType::Excluding:
            return !present;
    }
}

} //namespace NKikimr::NOlap::NDataLocks