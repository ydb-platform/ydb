#include <Dictionaries/HashedDictionary.h>

namespace DB_CHDB
{

template class HashedDictionary<DictionaryKeyType::Simple, /* sparse= */ true, /* sharded= */ false >;
template class HashedDictionary<DictionaryKeyType::Simple, /* sparse= */ true, /* sharded= */ true  >;

template class HashedDictionary<DictionaryKeyType::Complex, /* sparse= */ true, /* sharded= */ false >;
template class HashedDictionary<DictionaryKeyType::Complex, /* sparse= */ true, /* sharded= */ true  >;

}
