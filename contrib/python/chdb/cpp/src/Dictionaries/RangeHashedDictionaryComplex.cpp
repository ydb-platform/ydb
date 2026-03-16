#include <Dictionaries/RangeHashedDictionary.h>

/// RangeHashedDictionary is instantiated from two files
/// RangeHashedDictionarySimple.cpp and RangeHashedDictionaryComplex.cpp
/// to better parallelize the build procedure and avoid MSan build failure
/// due to excessive resource consumption.

namespace DB_CHDB
{

template class RangeHashedDictionary<DictionaryKeyType::Complex>;

}
