#include "config.h"

namespace NYT::NCrypto {

////////////////////////////////////////////////////////////////////////////////

void TPemBlobConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("file_name", &TThis::FileName)
        .Optional();
    registrar.Parameter("value", &TThis::Value)
        .Optional();

    registrar.Postprocessor([] (TThis* config) {
        if (config->FileName && config->Value) {
            THROW_ERROR_EXCEPTION("Cannot specify both \"file_name\" and \"value\"");
        }
        if (!config->FileName && !config->Value) {
            THROW_ERROR_EXCEPTION("Must specify either \"file_name\" or \"value\"");
        }
    });
}

TString TPemBlobConfig::LoadBlob() const
{
    if (FileName) {
        return TFileInput(*FileName).ReadAll();
    } else if (Value) {
        return *Value;
    } else {
        THROW_ERROR_EXCEPTION("Neither \"file_name\" nor \"value\" is given");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto

