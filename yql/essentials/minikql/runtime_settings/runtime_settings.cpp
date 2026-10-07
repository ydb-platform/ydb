#include "runtime_settings.h"

namespace NYql {

TRuntimeSettings::TRuntimeSettings() = default;

TRuntimeSettings::~TRuntimeSettings() = default;

} // namespace NYql

template <>
void Out<NYql::EDatumValidationMode>(IOutputStream& out, NYql::EDatumValidationMode value) {
    switch (value) {
        case NYql::EDatumValidationMode::None:
            out << "None";
            break;
        case NYql::EDatumValidationMode::Cheap:
            out << "Cheap";
            break;
        case NYql::EDatumValidationMode::Expensive:
            out << "Expensive";
            break;
    }
}
