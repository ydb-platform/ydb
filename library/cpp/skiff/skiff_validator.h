#pragma once

#include "public.h"

#include "skiff_schema.h"

#include <util/string/cast.h>

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

class TValidatorNodeStack;

////////////////////////////////////////////////////////////////////////////////

class TSkiffValidator
{
public:
    explicit TSkiffValidator(std::shared_ptr<TSkiffSchema> skiffSchema);
    ~TSkiffValidator();

    void BeforeVariant8Tag();
    void OnVariant8Tag(ui8 tag);

    void BeforeVariant16Tag();
    void OnVariant16Tag(ui16 tag);

    void OnSimpleType(EWireType value);

    void ValidateFinished();

private:
    const std::unique_ptr<TValidatorNodeStack> Context_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
