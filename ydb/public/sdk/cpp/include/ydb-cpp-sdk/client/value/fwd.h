#pragma once

namespace NYdb::inline Dev {

class TType;
class TTypeParser;
class TTypeBuilder;
class TValue;
class TValueParser;
class TValueBuilder;
class TArenaAllocatedValueBuilder;

template<typename TDerived, typename ValueHolder>
class TValueBuilderBase;

}  // namespace NYdb
