#include <Dictionaries/HashedDictionary.h>

namespace DB_CHDB
{

template class HashedDictionary<DictionaryKeyType::Simple, /* sparse= */ false, /* sharded= */ false >;
template class HashedDictionary<DictionaryKeyType::Simple, /* sparse= */ false, /* sharded= */ true  >;

template class HashedDictionary<DictionaryKeyType::Complex, /* sparse= */ false, /* sharded= */ false >;
template class HashedDictionary<DictionaryKeyType::Complex, /* sparse= */ false, /* sharded= */ true  >;

}
