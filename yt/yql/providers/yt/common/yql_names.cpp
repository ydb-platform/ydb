#include "yql_names.h"

namespace NYql {

const TVector<TStringBuf> YAMR_FIELDS = {
    TStringBuf("key"),
    TStringBuf("subkey"),
    TStringBuf("value"),
};

const TVector<TStringBuf> ORDERED_TABLE_READ_ONLY_FIELDS = {
    TStringBuf("$timestamp"),
    TStringBuf("$cumulative_data_weight"),
};

}
