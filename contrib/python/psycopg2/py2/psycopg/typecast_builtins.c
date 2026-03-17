static long int typecast_NUMBER_types[] = {20, 23, 21, 701, 700, 1700, 0};
static long int typecast_LONGINTEGER_types[] = {20, 0};
static long int typecast_INTEGER_types[] = {23, 21, 0};
static long int typecast_FLOAT_types[] = {701, 700, 0};
static long int typecast_DECIMAL_types[] = {1700, 0};
static long int typecast_STRING_types[] = {19, 18, 25, 1042, 1043, 0};
static long int typecast_BOOLEAN_types[] = {16, 0};
static long int typecast_DATETIME_types[] = {1114, 0};
static long int typecast_DATETIMETZ_types[] = {1184, 0};
static long int typecast_TIME_types[] = {1083, 1266, 0};
static long int typecast_DATE_types[] = {1082, 0};
static long int typecast_INTERVAL_types[] = {704, 1186, 0};
static long int typecast_BINARY_types[] = {17, 0};
static long int typecast_ROWID_types[] = {26, 0};
static long int typecast_LONGINTEGERARRAY_types[] = {1016, 0};
static long int typecast_INTEGERARRAY_types[] = {1005, 1006, 1007, 0};
static long int typecast_FLOATARRAY_types[] = {1021, 1022, 0};
static long int typecast_DECIMALARRAY_types[] = {1231, 0};
static long int typecast_STRINGARRAY_types[] = {1002, 1003, 1009, 1014, 1015, 0};
static long int typecast_BOOLEANARRAY_types[] = {1000, 0};
static long int typecast_DATETIMEARRAY_types[] = {1115, 0};
static long int typecast_DATETIMETZARRAY_types[] = {1185, 0};
static long int typecast_TIMEARRAY_types[] = {1183, 1270, 0};
static long int typecast_DATEARRAY_types[] = {1182, 0};
static long int typecast_INTERVALARRAY_types[] = {1187, 0};
static long int typecast_BINARYARRAY_types[] = {1001, 0};
static long int typecast_ROWIDARRAY_types[] = {1028, 1013, 0};
static long int typecast_INETARRAY_types[] = {1041, 0};
static long int typecast_CIDRARRAY_types[] = {651, 0};
static long int typecast_MACADDRARRAY_types[] = {1040, 0};
static long int typecast_UNKNOWN_types[] = {705, 0};


static typecastObject_initlist typecast_builtins[] = {
  {"NUMBER", typecast_NUMBER_types, typecast_NUMBER_cast, NULL},
  {"LONGINTEGER", typecast_LONGINTEGER_types, typecast_LONGINTEGER_cast, NULL},
  {"INTEGER", typecast_INTEGER_types, typecast_INTEGER_cast, NULL},
  {"FLOAT", typecast_FLOAT_types, typecast_FLOAT_cast, NULL},
  {"DECIMAL", typecast_DECIMAL_types, typecast_DECIMAL_cast, NULL},
  {"UNICODE", typecast_STRING_types, typecast_UNICODE_cast, NULL},
  {"BYTES", typecast_STRING_types, typecast_BYTES_cast, NULL},
  {"STRING", typecast_STRING_types, typecast_STRING_cast, NULL},
  {"BOOLEAN", typecast_BOOLEAN_types, typecast_BOOLEAN_cast, NULL},
  {"DATETIME", typecast_DATETIME_types, typecast_DATETIME_cast, NULL},
  {"DATETIMETZ", typecast_DATETIMETZ_types, typecast_DATETIMETZ_cast, NULL},
  {"TIME", typecast_TIME_types, typecast_TIME_cast, NULL},
  {"DATE", typecast_DATE_types, typecast_DATE_cast, NULL},
  {"INTERVAL", typecast_INTERVAL_types, typecast_INTERVAL_cast, NULL},
  {"BINARY", typecast_BINARY_types, typecast_BINARY_cast, NULL},
  {"ROWID", typecast_ROWID_types, typecast_ROWID_cast, NULL},
  {"LONGINTEGERARRAY", typecast_LONGINTEGERARRAY_types, typecast_LONGINTEGERARRAY_cast, "LONGINTEGER"},
  {"INTEGERARRAY", typecast_INTEGERARRAY_types, typecast_INTEGERARRAY_cast, "INTEGER"},
  {"FLOATARRAY", typecast_FLOATARRAY_types, typecast_FLOATARRAY_cast, "FLOAT"},
  {"DECIMALARRAY", typecast_DECIMALARRAY_types, typecast_DECIMALARRAY_cast, "DECIMAL"},
  {"UNICODEARRAY", typecast_STRINGARRAY_types, typecast_UNICODEARRAY_cast, "UNICODE"},
  {"BYTESARRAY", typecast_STRINGARRAY_types, typecast_BYTESARRAY_cast, "BYTES"},
  {"STRINGARRAY", typecast_STRINGARRAY_types, typecast_STRINGARRAY_cast, "STRING"},
  {"BOOLEANARRAY", typecast_BOOLEANARRAY_types, typecast_BOOLEANARRAY_cast, "BOOLEAN"},
  {"DATETIMEARRAY", typecast_DATETIMEARRAY_types, typecast_DATETIMEARRAY_cast, "DATETIME"},
  {"DATETIMETZARRAY", typecast_DATETIMETZARRAY_types, typecast_DATETIMETZARRAY_cast, "DATETIMETZ"},
  {"TIMEARRAY", typecast_TIMEARRAY_types, typecast_TIMEARRAY_cast, "TIME"},
  {"DATEARRAY", typecast_DATEARRAY_types, typecast_DATEARRAY_cast, "DATE"},
  {"INTERVALARRAY", typecast_INTERVALARRAY_types, typecast_INTERVALARRAY_cast, "INTERVAL"},
  {"BINARYARRAY", typecast_BINARYARRAY_types, typecast_BINARYARRAY_cast, "BINARY"},
  {"ROWIDARRAY", typecast_ROWIDARRAY_types, typecast_ROWIDARRAY_cast, "ROWID"},
  {"UNKNOWN", typecast_UNKNOWN_types, typecast_UNKNOWN_cast, NULL},
  {"INETARRAY", typecast_INETARRAY_types, typecast_STRINGARRAY_cast, "STRING"},
  {"CIDRARRAY", typecast_CIDRARRAY_types, typecast_STRINGARRAY_cast, "STRING"},
  {"MACADDRARRAY", typecast_MACADDRARRAY_types, typecast_STRINGARRAY_cast, "STRING"},
    {NULL, NULL, NULL, NULL}
};
