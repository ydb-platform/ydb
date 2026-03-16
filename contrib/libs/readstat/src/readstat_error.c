
#include "readstat.h"

const char *readstat_error_message(readstat_error_t error_code) {
    if (error_code == READSTAT_OK)
        return NULL;

    if (error_code == READSTAT_ERROR_OPEN)
        return "Unable to open file";

    if (error_code == READSTAT_ERROR_READ)
        return "Unable to read from file";

    if (error_code == READSTAT_ERROR_MALLOC)
        return "Unable to allocate memory";

    if (error_code == READSTAT_ERROR_USER_ABORT)
        return "The parsing was aborted (callback returned non-zero value)";

    if (error_code == READSTAT_ERROR_PARSE)
        return "Invalid file, or file has unsupported features";

    if (error_code == READSTAT_ERROR_UNSUPPORTED_COMPRESSION)
        return "File has unsupported compression scheme";

    if (error_code == READSTAT_ERROR_UNSUPPORTED_CHARSET)
        return "File has an unsupported character set";

    if (error_code == READSTAT_ERROR_COLUMN_COUNT_MISMATCH)
        return "File did not contain the expected number of columns";

    if (error_code == READSTAT_ERROR_ROW_COUNT_MISMATCH)
        return "File did not contain the expected number of rows";

    if (error_code == READSTAT_ERROR_ROW_WIDTH_MISMATCH)
        return "A row in the file was not the expected length";

    if (error_code == READSTAT_ERROR_BAD_FORMAT_STRING)
        return "A provided format string could not be understood";

    if (error_code == READSTAT_ERROR_VALUE_TYPE_MISMATCH)
        return "A provided value was incompatible with the variable's declared type";

    if (error_code == READSTAT_ERROR_WRITE)
        return "Unable to write data";
    
    if (error_code == READSTAT_ERROR_WRITER_NOT_INITIALIZED)
        return "The writer object was not properly initialized (call and check return value of readstat_begin_writing_XXX)";

    if (error_code == READSTAT_ERROR_SEEK)
        return "Unable to seek within file";

    if (error_code == READSTAT_ERROR_CONVERT)
        return "Unable to convert string to the requested encoding";

    if (error_code == READSTAT_ERROR_CONVERT_BAD_STRING)
        return "Unable to convert string to the requested encoding (invalid byte sequence)";

    if (error_code == READSTAT_ERROR_CONVERT_SHORT_STRING)
        return "Unable to convert string to the requested encoding (incomplete byte sequence)";

    if (error_code == READSTAT_ERROR_CONVERT_LONG_STRING)
        return "Unable to convert string to the requested encoding (output buffer too small)";

    if (error_code == READSTAT_ERROR_NUMERIC_VALUE_IS_OUT_OF_RANGE)
        return "A provided numeric value was outside the range of representable values in the specified file format";

    if (error_code == READSTAT_ERROR_TAGGED_VALUE_IS_OUT_OF_RANGE)
        return "A provided tag value was outside the range of allowed values in the specified file format";

    if (error_code == READSTAT_ERROR_STRING_VALUE_IS_TOO_LONG)
        return "A provided string value was longer than the available storage size of the specified column";

    if (error_code == READSTAT_ERROR_TAGGED_VALUES_NOT_SUPPORTED)
        return "The file format does not supported character tags for missing values";

    if (error_code == READSTAT_ERROR_UNSUPPORTED_FILE_FORMAT_VERSION)
        return "This version of the file format is not supported";

    if (error_code == READSTAT_ERROR_NAME_BEGINS_WITH_ILLEGAL_CHARACTER)
        return "A provided name begins with an illegal character";

    if (error_code == READSTAT_ERROR_NAME_CONTAINS_ILLEGAL_CHARACTER)
        return "A provided name contains an illegal character";

    if (error_code == READSTAT_ERROR_NAME_IS_RESERVED_WORD)
        return "A provided name is a reserved word";

    if (error_code == READSTAT_ERROR_NAME_IS_TOO_LONG)
        return "A provided name is too long for the file format";

    if (error_code == READSTAT_ERROR_NAME_IS_ZERO_LENGTH)
        return "A provided name is blank or empty";

    if (error_code == READSTAT_ERROR_BAD_TIMESTAMP_STRING)
        return "The file's timestamp string is invalid";

    if (error_code == READSTAT_ERROR_BAD_FREQUENCY_WEIGHT)
        return "The provided variable can't be used as a frequency weight";

    if (error_code == READSTAT_ERROR_TOO_MANY_MISSING_VALUE_DEFINITIONS)
        return "The number of defined missing values exceeds the format limit";

    if (error_code == READSTAT_ERROR_NOTE_IS_TOO_LONG)
        return "The provided note is too long for the file format";

    if (error_code == READSTAT_ERROR_STRING_REFS_NOT_SUPPORTED)
        return "This version of the file format does not support string references";

    if (error_code == READSTAT_ERROR_STRING_REF_IS_REQUIRED)
        return "The provided value was not a valid string reference";

    if (error_code == READSTAT_ERROR_ROW_IS_TOO_WIDE_FOR_PAGE)
        return "A row of data will not fit into the file format";

    if (error_code == READSTAT_ERROR_TOO_FEW_COLUMNS)
        return "One or more columns must be provided";

    if (error_code == READSTAT_ERROR_TOO_MANY_COLUMNS)
        return "Too many columns for this file format version";

    if (error_code == READSTAT_ERROR_BAD_TIMESTAMP_VALUE)
        return "The provided file timestamp is invalid";

    return "Unknown error";
}
