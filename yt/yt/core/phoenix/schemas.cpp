#include "schemas.h"

namespace NYT::NPhoenix2 {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TFieldSchema::Register(TRegistrar registrar)
{
    registrar.Parameter("name", &TThis::Name);
    registrar.Parameter("tag", &TThis::Tag);
    registrar.Parameter("min_version", &TThis::MinVersion)
        .Default(std::numeric_limits<int>::min())
        .DontSerializeDefault();
    registrar.Parameter("max_version", &TThis::MaxVersion)
        .Default(std::numeric_limits<int>::max())
        .DontSerializeDefault();
    registrar.Parameter("deprecated", &TThis::Deprecated)
        .Default(false)
        .DontSerializeDefault();
}

////////////////////////////////////////////////////////////////////////////////

void TTypeSchema::Register(TRegistrar registrar)
{
    registrar.Parameter("name", &TThis::Name);
    registrar.Parameter("tag", &TThis::Tag);
    registrar.Parameter("fields", &TThis::Fields);
    registrar.Parameter("base_type_tags", &TThis::BaseTypeTags)
        .Default();
    registrar.Parameter("template", &TThis::Template)
        .Default(false)
        .DontSerializeDefault();
}

////////////////////////////////////////////////////////////////////////////////

void TUniverseSchema::Register(TRegistrar registrar)
{
    registrar.Parameter("types", &TThis::Types);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2

