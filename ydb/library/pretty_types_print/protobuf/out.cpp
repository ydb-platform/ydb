#include <util/stream/output.h>
#include <util/string/join.h>

#include <google/protobuf/repeated_field.h>

Y_DECLARE_OUT_SPEC(, google::protobuf::RepeatedPtrField<TString>, stream, value)
{
    stream << JoinSeq(", ", value);
}
