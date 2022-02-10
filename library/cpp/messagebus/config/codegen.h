#pragma once

#define COMMA ,

#define STRUCT_FIELD_GEN(name, type, ...) type name;

#define STRUCT_FIELD_INIT(name, type, defa) name(defa)
#define STRUCT_FIELD_INIT_DEFAULT(name, type, ...) name()

#define STRUCT_FIELD_PRINT(name, ...) ss << #name << "=" << name << "\n";
