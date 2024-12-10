#include "import.h"

Y_DECLARE_OUT_SPEC(, NYdb::NImport::TImportDataResult, o, x) {
    return x.Out(o);
}
