#ifndef FORMAT_INL_H_
#error "Direct inclusion of this file is not allowed, include format.h"
// For the sake of sane code completion.
#include "format.h"
#endif

#include "string_builder_stream.h"
#include "writer.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <NYson::CYsonFormattable T>
void FormatValue(TStringBuilderBase* builder, const T& value, TStringBuf /*spec*/)
{
    TStringBuilderStream stream(builder);
    NYson::TYsonWriter writer(&stream, NYson::TYsonFormatTraits<T>::YsonFormat);
    Serialize(value, &writer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
