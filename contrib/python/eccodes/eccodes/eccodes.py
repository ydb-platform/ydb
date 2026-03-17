#
# (C) Copyright 2017- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#
#
from gribapi import (
    CODES_FEATURES_ALL,
    CODES_FEATURES_DISABLED,
    CODES_FEATURES_ENABLED,
    CODES_PRODUCT_ANY,
    CODES_PRODUCT_BUFR,
    CODES_PRODUCT_GRIB,
    CODES_PRODUCT_GTS,
    CODES_PRODUCT_METAR,
)
from gribapi import GRIB_CHECK as CODES_CHECK
from gribapi import GRIB_MISSING_DOUBLE as CODES_MISSING_DOUBLE
from gribapi import GRIB_MISSING_LONG as CODES_MISSING_LONG
from gribapi import GRIB_NEAREST_SAME_DATA as CODES_GRIB_NEAREST_SAME_DATA
from gribapi import GRIB_NEAREST_SAME_GRID as CODES_GRIB_NEAREST_SAME_GRID
from gribapi import GRIB_NEAREST_SAME_POINT as CODES_GRIB_NEAREST_SAME_POINT
from gribapi import (
    __version__,
)
from gribapi import any_new_from_file as codes_any_new_from_file
from gribapi import (
    bindings_version,
)
from gribapi import bufr_new_from_file as codes_bufr_new_from_file
from gribapi import (
    codes_any_new_from_samples,
    codes_bufr_copy_data,
    codes_bufr_extract_headers,
    codes_bufr_key_is_coordinate,
    codes_bufr_key_is_header,
    codes_bufr_keys_iterator_delete,
    codes_bufr_keys_iterator_get_name,
    codes_bufr_keys_iterator_new,
    codes_bufr_keys_iterator_next,
    codes_bufr_keys_iterator_rewind,
    codes_bufr_multi_element_constant_arrays_off,
    codes_bufr_multi_element_constant_arrays_on,
    codes_bufr_new_from_samples,
    codes_definition_path,
    codes_dump,
    codes_extract_offsets,
    codes_extract_offsets_sizes,
    codes_get_features,
    codes_get_gaussian_latitudes,
    codes_get_library_path,
    codes_get_version_info,
    codes_new_from_file,
    codes_new_from_samples,
    codes_samples_path,
)
from gribapi import grib_clone as codes_clone
from gribapi import grib_context_delete as codes_context_delete
from gribapi import grib_copy_namespace as codes_copy_namespace
from gribapi import grib_count_in_file as codes_count_in_file
from gribapi import grib_find_nearest as codes_grib_find_nearest
from gribapi import grib_find_nearest_multiple as codes_grib_find_nearest_multiple
from gribapi import grib_get as codes_get
from gribapi import grib_get_api_version as codes_get_api_version
from gribapi import grib_get_array as codes_get_array
from gribapi import grib_get_data as codes_grib_get_data
from gribapi import grib_get_double as codes_get_double
from gribapi import grib_get_double_array as codes_get_double_array
from gribapi import grib_get_double_element as codes_get_double_element
from gribapi import grib_get_double_elements as codes_get_double_elements
from gribapi import grib_get_elements as codes_get_elements
from gribapi import grib_get_float_array as codes_get_float_array
from gribapi import grib_get_long as codes_get_long
from gribapi import grib_get_long_array as codes_get_long_array
from gribapi import grib_get_message as codes_get_message
from gribapi import grib_get_message_offset as codes_get_message_offset
from gribapi import grib_get_message_size as codes_get_message_size
from gribapi import grib_get_native_type as codes_get_native_type
from gribapi import grib_get_offset as codes_get_offset
from gribapi import grib_get_size as codes_get_size
from gribapi import grib_get_string as codes_get_string
from gribapi import grib_get_string_array as codes_get_string_array
from gribapi import grib_get_string_length as codes_get_string_length
from gribapi import grib_get_values as codes_get_values
from gribapi import grib_gribex_mode_off as codes_gribex_mode_off
from gribapi import grib_gribex_mode_on as codes_gribex_mode_on
from gribapi import grib_gts_header as codes_gts_header
from gribapi import grib_index_add_file as codes_index_add_file
from gribapi import grib_index_get as codes_index_get
from gribapi import grib_index_get_double as codes_index_get_double
from gribapi import grib_index_get_long as codes_index_get_long
from gribapi import grib_index_get_size as codes_index_get_size
from gribapi import grib_index_get_string as codes_index_get_string
from gribapi import grib_index_new_from_file as codes_index_new_from_file
from gribapi import grib_index_read as codes_index_read
from gribapi import grib_index_release as codes_index_release
from gribapi import grib_index_select as codes_index_select
from gribapi import grib_index_select_double as codes_index_select_double
from gribapi import grib_index_select_long as codes_index_select_long
from gribapi import grib_index_select_string as codes_index_select_string
from gribapi import grib_index_write as codes_index_write
from gribapi import grib_is_defined as codes_is_defined
from gribapi import grib_is_missing as codes_is_missing
from gribapi import grib_iterator_delete as codes_grib_iterator_delete
from gribapi import grib_iterator_new as codes_grib_iterator_new
from gribapi import grib_iterator_next as codes_grib_iterator_next
from gribapi import grib_keys_iterator_delete as codes_keys_iterator_delete
from gribapi import grib_keys_iterator_get_name as codes_keys_iterator_get_name
from gribapi import grib_keys_iterator_new as codes_keys_iterator_new
from gribapi import grib_keys_iterator_next as codes_keys_iterator_next
from gribapi import grib_keys_iterator_rewind as codes_keys_iterator_rewind
from gribapi import grib_multi_append as codes_grib_multi_append
from gribapi import grib_multi_new as codes_grib_multi_new
from gribapi import grib_multi_release as codes_grib_multi_release
from gribapi import grib_multi_support_off as codes_grib_multi_support_off
from gribapi import grib_multi_support_on as codes_grib_multi_support_on
from gribapi import grib_multi_support_reset_file as codes_grib_multi_support_reset_file
from gribapi import grib_multi_write as codes_grib_multi_write
from gribapi import grib_nearest_delete as codes_grib_nearest_delete
from gribapi import grib_nearest_find as codes_grib_nearest_find
from gribapi import grib_nearest_new as codes_grib_nearest_new
from gribapi import grib_new_from_file as codes_grib_new_from_file
from gribapi import grib_new_from_index as codes_new_from_index
from gribapi import grib_new_from_message as codes_new_from_message
from gribapi import grib_new_from_samples as codes_grib_new_from_samples
from gribapi import grib_no_fail_on_wrong_length as codes_no_fail_on_wrong_length
from gribapi import grib_release as codes_release
from gribapi import grib_set as codes_set
from gribapi import grib_set_array as codes_set_array
from gribapi import grib_set_data_quality_checks as codes_set_data_quality_checks
from gribapi import grib_set_debug as codes_set_debug
from gribapi import grib_set_definitions_path as codes_set_definitions_path
from gribapi import grib_set_double as codes_set_double
from gribapi import grib_set_double_array as codes_set_double_array
from gribapi import grib_set_key_vals as codes_set_key_vals
from gribapi import grib_set_long as codes_set_long
from gribapi import grib_set_long_array as codes_set_long_array
from gribapi import grib_set_missing as codes_set_missing
from gribapi import grib_set_samples_path as codes_set_samples_path
from gribapi import grib_set_string as codes_set_string
from gribapi import grib_set_string_array as codes_set_string_array
from gribapi import grib_set_values as codes_set_values
from gribapi import grib_skip_coded as codes_skip_coded
from gribapi import grib_skip_computed as codes_skip_computed
from gribapi import grib_skip_duplicates as codes_skip_duplicates
from gribapi import grib_skip_edition_specific as codes_skip_edition_specific
from gribapi import grib_skip_function as codes_skip_function
from gribapi import grib_skip_read_only as codes_skip_read_only
from gribapi import grib_write as codes_write
from gribapi import gts_new_from_file as codes_gts_new_from_file
from gribapi import metar_new_from_file as codes_metar_new_from_file
from gribapi.errors import (
    ArrayTooSmallError,
    AttributeClashError,
    AttributeNotFoundError,
    BufferTooSmallError,
    CodeNotFoundInTableError,
    ConceptNoMatchError,
    ConstantFieldError,
    CorruptedIndexError,
    DecodingError,
    DifferentEditionError,
    EncodingError,
    EndError,
    EndOfFileError,
    EndOfIndexError,
    FileNotFoundError,
    FunctionalityNotEnabledError,
    FunctionNotImplementedError,
    GeocalculusError,
)
from gribapi.errors import GribInternalError
from gribapi.errors import GribInternalError as CodesInternalError
from gribapi.errors import (
    HashArrayNoMatchError,
    InternalArrayTooSmallError,
    InternalError,
    InvalidArgumentError,
    InvalidBitsPerValueError,
    InvalidFileError,
    InvalidGribError,
    InvalidIndexError,
    InvalidIteratorError,
    InvalidKeysIteratorError,
    InvalidKeyValueError,
    InvalidNearestError,
    InvalidOrderByError,
    InvalidSectionNumberError,
    InvalidTypeError,
    IOProblemError,
    KeyValueNotFoundError,
    MemoryAllocationError,
    MessageEndNotFoundError,
    MessageInvalidError,
    MessageMalformedError,
    MessageTooLargeError,
    MissingBufrEntryError,
    MissingKeyError,
    NoDefinitionsError,
    NoMoreInSetError,
    NoValuesError,
    NullHandleError,
    NullIndexError,
    NullPointerError,
    OutOfAreaError,
    OutOfRangeError,
    PrematureEndOfFileError,
    ReadOnlyError,
    StringTooSmallError,
    SwitchNoMatchError,
    TooManyAttributesError,
    UnderflowError,
    UnsupportedEditionError,
    ValueCannotBeMissingError,
    ValueDifferentError,
    WrongArraySizeError,
    WrongBitmapSizeError,
    WrongConversionError,
    WrongGridError,
    WrongLengthError,
    WrongStepError,
    WrongStepUnitError,
    WrongTypeError,
)

__all__ = [
    "__version__",
    "ArrayTooSmallError",
    "AttributeClashError",
    "AttributeNotFoundError",
    "bindings_version",
    "BufferTooSmallError",
    "CodeNotFoundInTableError",
    "codes_any_new_from_file",
    "codes_bufr_copy_data",
    "codes_bufr_extract_headers",
    "codes_bufr_key_is_header",
    "codes_bufr_key_is_coordinate",
    "codes_bufr_keys_iterator_delete",
    "codes_bufr_keys_iterator_get_name",
    "codes_bufr_keys_iterator_new",
    "codes_bufr_keys_iterator_next",
    "codes_bufr_keys_iterator_rewind",
    "codes_bufr_multi_element_constant_arrays_off",
    "codes_bufr_multi_element_constant_arrays_on",
    "codes_bufr_new_from_file",
    "codes_bufr_new_from_samples",
    "codes_any_new_from_samples",
    "CODES_CHECK",
    "codes_clone",
    "codes_copy_namespace",
    "codes_count_in_file",
    "codes_definition_path",
    "codes_extract_offsets",
    "codes_extract_offsets_sizes",
    "CODES_FEATURES_ALL",
    "CODES_FEATURES_ENABLED",
    "CODES_FEATURES_DISABLED",
    "codes_get_api_version",
    "codes_get_array",
    "codes_get_double_array",
    "codes_get_double_element",
    "codes_get_double_elements",
    "codes_get_double",
    "codes_get_elements",
    "codes_get_float_array",
    "codes_get_gaussian_latitudes",
    "codes_get_library_path",
    "codes_get_long_array",
    "codes_get_long",
    "codes_get_message_offset",
    "codes_get_message_size",
    "codes_get_message",
    "codes_get_native_type",
    "codes_get_offset",
    "codes_get_size",
    "codes_get_string_array",
    "codes_get_string_length",
    "codes_get_string",
    "codes_get_values",
    "codes_get_version_info",
    "codes_get",
    "codes_get_features",
    "codes_grib_find_nearest_multiple",
    "codes_grib_find_nearest",
    "codes_grib_get_data",
    "codes_grib_iterator_delete",
    "codes_grib_iterator_new",
    "codes_grib_iterator_next",
    "codes_grib_multi_append",
    "codes_grib_multi_new",
    "codes_grib_multi_release",
    "codes_grib_multi_support_off",
    "codes_grib_multi_support_on",
    "codes_grib_multi_support_reset_file",
    "codes_grib_multi_write",
    "codes_grib_nearest_delete",
    "codes_grib_nearest_find",
    "codes_grib_nearest_new",
    "CODES_GRIB_NEAREST_SAME_DATA",
    "CODES_GRIB_NEAREST_SAME_GRID",
    "CODES_GRIB_NEAREST_SAME_POINT",
    "codes_grib_new_from_file",
    "codes_grib_new_from_samples",
    "codes_gribex_mode_off",
    "codes_gribex_mode_on",
    "codes_gts_header",
    "codes_gts_new_from_file",
    "codes_index_add_file",
    "codes_index_get_double",
    "codes_index_get_long",
    "codes_index_get_size",
    "codes_index_get_string",
    "codes_index_get",
    "codes_index_new_from_file",
    "codes_index_read",
    "codes_index_release",
    "codes_index_select_double",
    "codes_index_select_long",
    "codes_index_select_string",
    "codes_index_select",
    "codes_index_write",
    "codes_is_defined",
    "codes_is_missing",
    "codes_keys_iterator_delete",
    "codes_keys_iterator_get_name",
    "codes_keys_iterator_new",
    "codes_keys_iterator_next",
    "codes_keys_iterator_rewind",
    "codes_metar_new_from_file",
    "CODES_MISSING_DOUBLE",
    "CODES_MISSING_LONG",
    "codes_new_from_file",
    "codes_new_from_index",
    "codes_new_from_message",
    "codes_new_from_samples",
    "codes_no_fail_on_wrong_length",
    "CODES_PRODUCT_ANY",
    "CODES_PRODUCT_BUFR",
    "CODES_PRODUCT_GRIB",
    "CODES_PRODUCT_GTS",
    "CODES_PRODUCT_METAR",
    "codes_release",
    "codes_samples_path",
    "codes_dump",
    "codes_set_array",
    "codes_set_data_quality_checks",
    "codes_set_debug",
    "codes_set_definitions_path",
    "codes_set_double_array",
    "codes_set_double",
    "codes_set_key_vals",
    "codes_set_long_array",
    "codes_set_long",
    "codes_set_missing",
    "codes_set_samples_path",
    "codes_set_string_array",
    "codes_set_string",
    "codes_set_values",
    "codes_set",
    "codes_skip_coded",
    "codes_skip_computed",
    "codes_skip_duplicates",
    "codes_skip_edition_specific",
    "codes_skip_function",
    "codes_skip_read_only",
    "codes_write",
    "codes_context_delete",
    "CodesInternalError",
    "ConceptNoMatchError",
    "ConstantFieldError",
    "CorruptedIndexError",
    "DecodingError",
    "DifferentEditionError",
    "EncodingError",
    "EndError",
    "EndOfFileError",
    "EndOfIndexError",
    "FileNotFoundError",
    "FunctionalityNotEnabledError",
    "FunctionNotImplementedError",
    "GeocalculusError",
    "GribInternalError",
    "HashArrayNoMatchError",
    "InternalArrayTooSmallError",
    "InternalError",
    "InvalidArgumentError",
    "InvalidBitsPerValueError",
    "InvalidFileError",
    "InvalidGribError",
    "InvalidIndexError",
    "InvalidIteratorError",
    "InvalidKeysIteratorError",
    "InvalidKeyValueError",
    "InvalidNearestError",
    "InvalidOrderByError",
    "InvalidSectionNumberError",
    "InvalidTypeError",
    "IOProblemError",
    "KeyValueNotFoundError",
    "MemoryAllocationError",
    "MessageEndNotFoundError",
    "MessageInvalidError",
    "MessageMalformedError",
    "MessageTooLargeError",
    "MissingBufrEntryError",
    "MissingKeyError",
    "NoDefinitionsError",
    "NoMoreInSetError",
    "NoValuesError",
    "NullHandleError",
    "NullIndexError",
    "NullPointerError",
    "OutOfAreaError",
    "OutOfRangeError",
    "PrematureEndOfFileError",
    "ReadOnlyError",
    "StringTooSmallError",
    "SwitchNoMatchError",
    "TooManyAttributesError",
    "UnderflowError",
    "UnsupportedEditionError",
    "ValueCannotBeMissingError",
    "ValueDifferentError",
    "WrongArraySizeError",
    "WrongBitmapSizeError",
    "WrongConversionError",
    "WrongGridError",
    "WrongLengthError",
    "WrongStepError",
    "WrongStepUnitError",
    "WrongTypeError",
]
