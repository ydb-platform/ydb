/* ANSI-C code produced by gperf version 3.1 */
/* Command-line: gperf -C -W classes -t -G -H grib_accessor_classes_get_id -N grib_accessor_hash -m 1 -j 1 accessor_class_list.gperf  */
/* Computed positions: -k'1-2,5,7,11' */

#if !((' ' == 32) && ('!' == 33) && ('"' == 34) && ('#' == 35) \
      && ('%' == 37) && ('&' == 38) && ('\'' == 39) && ('(' == 40) \
      && (')' == 41) && ('*' == 42) && ('+' == 43) && (',' == 44) \
      && ('-' == 45) && ('.' == 46) && ('/' == 47) && ('0' == 48) \
      && ('1' == 49) && ('2' == 50) && ('3' == 51) && ('4' == 52) \
      && ('5' == 53) && ('6' == 54) && ('7' == 55) && ('8' == 56) \
      && ('9' == 57) && (':' == 58) && (';' == 59) && ('<' == 60) \
      && ('=' == 61) && ('>' == 62) && ('?' == 63) && ('A' == 65) \
      && ('B' == 66) && ('C' == 67) && ('D' == 68) && ('E' == 69) \
      && ('F' == 70) && ('G' == 71) && ('H' == 72) && ('I' == 73) \
      && ('J' == 74) && ('K' == 75) && ('L' == 76) && ('M' == 77) \
      && ('N' == 78) && ('O' == 79) && ('P' == 80) && ('Q' == 81) \
      && ('R' == 82) && ('S' == 83) && ('T' == 84) && ('U' == 85) \
      && ('V' == 86) && ('W' == 87) && ('X' == 88) && ('Y' == 89) \
      && ('Z' == 90) && ('[' == 91) && ('\\' == 92) && (']' == 93) \
      && ('^' == 94) && ('_' == 95) && ('a' == 97) && ('b' == 98) \
      && ('c' == 99) && ('d' == 100) && ('e' == 101) && ('f' == 102) \
      && ('g' == 103) && ('h' == 104) && ('i' == 105) && ('j' == 106) \
      && ('k' == 107) && ('l' == 108) && ('m' == 109) && ('n' == 110) \
      && ('o' == 111) && ('p' == 112) && ('q' == 113) && ('r' == 114) \
      && ('s' == 115) && ('t' == 116) && ('u' == 117) && ('v' == 118) \
      && ('w' == 119) && ('x' == 120) && ('y' == 121) && ('z' == 122) \
      && ('{' == 123) && ('|' == 124) && ('}' == 125) && ('~' == 126))
/* The character set is not based on ISO-646.  */
#error "gperf generated tables don't work with this execution character set. Please report a bug to <bug-gperf@gnu.org>."
#endif

#line 1 "accessor_class_list.gperf"

#include "grib_api_internal.h"
#include "grib_accessor_class.h"

#line 6 "accessor_class_list.gperf"
struct accessor_class_hash { const char *name; grib_accessor **cclass;};

#define TOTAL_KEYWORDS 210
#define MIN_WORD_LENGTH 1
#define MAX_WORD_LENGTH 44
#define MIN_HASH_VALUE 1
#define MAX_HASH_VALUE 684
/* maximum key range = 684, duplicates = 0 */

#ifdef __GNUC__

#else
#ifdef __cplusplus

#endif
#endif
static unsigned int
grib_accessor_classes_get_id (const char *str, size_t len)
{
  static const unsigned short asso_values[] =
    {
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685,   0, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685,   1, 207,
       21,  13,   0, 685,   4, 685,   0, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685,   0, 685,   0,  94, 117,
        1,   3,   4,   1, 181,  47,   2,   0, 162, 253,
       63,   1,   6, 685,  91,  30,   0,  85, 169,   1,
       16,   2, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685, 685, 685, 685, 685,
      685, 685, 685, 685, 685, 685
    };
  unsigned int hval = len;

  switch (hval)
    {
      default:
        hval += asso_values[(unsigned char)str[10]];
      /*FALLTHROUGH*/
      case 10:
      case 9:
      case 8:
      case 7:
        hval += asso_values[(unsigned char)str[6]];
      /*FALLTHROUGH*/
      case 6:
      case 5:
        hval += asso_values[(unsigned char)str[4]];
      /*FALLTHROUGH*/
      case 4:
      case 3:
      case 2:
        hval += asso_values[(unsigned char)str[1]];
      /*FALLTHROUGH*/
      case 1:
        hval += asso_values[(unsigned char)str[0]];
        break;
    }
  return hval;
}

static const struct accessor_class_hash classes[] =
  {
    {""},
#line 9 "accessor_class_list.gperf"
    {" "},
    {""}, {""}, {""}, {""}, {""},
#line 113 "accessor_class_list.gperf"
    {"gen", &grib_accessor_gen},
    {""},
#line 160 "accessor_class_list.gperf"
    {"pad", &grib_accessor_pad},
    {""}, {""},
#line 162 "accessor_class_list.gperf"
    {"padto", &grib_accessor_padto},
    {""}, {""}, {""},
#line 165 "accessor_class_list.gperf"
    {"position", &grib_accessor_position},
#line 66 "accessor_class_list.gperf"
    {"data_raw_packing", &grib_accessor_data_raw_packing},
    {""}, {""}, {""}, {""},
#line 155 "accessor_class_list.gperf"
    {"offset_file", &grib_accessor_offset_file},
    {""},
#line 43 "accessor_class_list.gperf"
    {"data_apply_bitmap", &grib_accessor_data_apply_bitmap},
#line 47 "accessor_class_list.gperf"
    {"data_complex_packing", &grib_accessor_data_complex_packing},
#line 68 "accessor_class_list.gperf"
    {"data_secondary_bitmap", &grib_accessor_data_secondary_bitmap},
    {""},
#line 104 "accessor_class_list.gperf"
    {"g2date", &grib_accessor_g2date},
#line 64 "accessor_class_list.gperf"
    {"data_jpeg2000_packing", &grib_accessor_data_jpeg2000_packing},
#line 102 "accessor_class_list.gperf"
    {"g2bitmap", &grib_accessor_g2bitmap},
#line 107 "accessor_class_list.gperf"
    {"g2latlon", &grib_accessor_g2latlon},
    {""},
#line 44 "accessor_class_list.gperf"
    {"data_apply_boustrophedonic", &grib_accessor_data_apply_boustrophedonic},
#line 99 "accessor_class_list.gperf"
    {"g2_eps", &grib_accessor_g2_eps},
    {""},
#line 96 "accessor_class_list.gperf"
    {"g2_aerosol", &grib_accessor_g2_aerosol},
    {""},
#line 110 "accessor_class_list.gperf"
    {"g2step_range", &grib_accessor_g2step_range},
#line 186 "accessor_class_list.gperf"
    {"spd", &grib_accessor_spd},
#line 45 "accessor_class_list.gperf"
    {"data_apply_boustrophedonic_bitmap", &grib_accessor_data_apply_boustrophedonic_bitmap},
    {""}, {""}, {""},
#line 57 "accessor_class_list.gperf"
    {"data_g22order_packing", &grib_accessor_data_g22order_packing},
    {""},
#line 60 "accessor_class_list.gperf"
    {"data_g2secondary_bitmap", &grib_accessor_data_g2secondary_bitmap},
#line 58 "accessor_class_list.gperf"
    {"data_g2bifourier_packing", &grib_accessor_data_g2bifourier_packing},
#line 101 "accessor_class_list.gperf"
    {"g2_param_concept_filename", &grib_accessor_g2_param_concept_filename},
#line 62 "accessor_class_list.gperf"
    {"data_g2simple_packing", &grib_accessor_data_g2simple_packing},
#line 59 "accessor_class_list.gperf"
    {"data_g2complex_packing", &grib_accessor_data_g2complex_packing},
#line 195 "accessor_class_list.gperf"
    {"time", &grib_accessor_time},
    {""}, {""}, {""},
#line 75 "accessor_class_list.gperf"
    {"dirty", &grib_accessor_dirty},
#line 124 "accessor_class_list.gperf"
    {"iterator", &grib_accessor_iterator},
#line 198 "accessor_class_list.gperf"
    {"to_string", &grib_accessor_to_string},
    {""}, {""}, {""},
#line 161 "accessor_class_list.gperf"
    {"padding", &grib_accessor_padding},
    {""},
#line 105 "accessor_class_list.gperf"
    {"g2end_step", &grib_accessor_g2end_step},
#line 122 "accessor_class_list.gperf"
    {"ieeefloat", &grib_accessor_ieeefloat},
    {""}, {""}, {""},
#line 63 "accessor_class_list.gperf"
    {"data_g2simple_packing_with_preprocessing", &grib_accessor_data_g2simple_packing_with_preprocessing},
    {""},
#line 72 "accessor_class_list.gperf"
    {"data_simple_packing", &grib_accessor_data_simple_packing},
    {""},
#line 159 "accessor_class_list.gperf"
    {"packing_type", &grib_accessor_packing_type},
#line 114 "accessor_class_list.gperf"
    {"getenv", &grib_accessor_getenv},
    {""},
#line 106 "accessor_class_list.gperf"
    {"g2grid", &grib_accessor_g2grid},
#line 146 "accessor_class_list.gperf"
    {"nearest", &grib_accessor_nearest},
#line 197 "accessor_class_list.gperf"
    {"to_integer", &grib_accessor_to_integer},
    {""},
#line 147 "accessor_class_list.gperf"
    {"non_alpha", &grib_accessor_non_alpha},
#line 65 "accessor_class_list.gperf"
    {"data_png_packing", &grib_accessor_data_png_packing},
#line 183 "accessor_class_list.gperf"
    {"size", &grib_accessor_size},
#line 12 "accessor_class_list.gperf"
    {"ascii", &grib_accessor_ascii},
    {""}, {""}, {""},
#line 180 "accessor_class_list.gperf"
    {"signed", &grib_accessor_signed},
#line 189 "accessor_class_list.gperf"
    {"statistics", &grib_accessor_statistics},
    {""}, {""},
#line 109 "accessor_class_list.gperf"
    {"g2lon", &grib_accessor_g2lon},
    {""},
#line 61 "accessor_class_list.gperf"
    {"data_g2shsimple_packing", &grib_accessor_data_g2shsimple_packing},
#line 112 "accessor_class_list.gperf"
    {"gds_is_present", &grib_accessor_gds_is_present},
#line 167 "accessor_class_list.gperf"
    {"raw", &grib_accessor_raw},
#line 201 "accessor_class_list.gperf"
    {"trim", &grib_accessor_trim},
#line 190 "accessor_class_list.gperf"
    {"statistics_spectral", &grib_accessor_statistics_spectral},
#line 126 "accessor_class_list.gperf"
    {"julian_day", &grib_accessor_julian_day},
#line 170 "accessor_class_list.gperf"
    {"round", &grib_accessor_round},
    {""}, {""},
#line 125 "accessor_class_list.gperf"
    {"julian_date", &grib_accessor_julian_date},
#line 48 "accessor_class_list.gperf"
    {"data_dummy_field", &grib_accessor_data_dummy_field},
#line 117 "accessor_class_list.gperf"
    {"group", &grib_accessor_group},
    {""},
#line 196 "accessor_class_list.gperf"
    {"to_double", &grib_accessor_to_double},
#line 156 "accessor_class_list.gperf"
    {"offset_values", &grib_accessor_offset_values},
#line 116 "accessor_class_list.gperf"
    {"grid_spec", &grib_accessor_grid_spec},
#line 80 "accessor_class_list.gperf"
    {"expanded_descriptors", &grib_accessor_expanded_descriptors},
#line 166 "accessor_class_list.gperf"
    {"proj_string", &grib_accessor_proj_string},
#line 188 "accessor_class_list.gperf"
    {"sprintf", &grib_accessor_sprintf},
#line 164 "accessor_class_list.gperf"
    {"padtomultiple", &grib_accessor_padtomultiple},
#line 67 "accessor_class_list.gperf"
    {"data_run_length_packing", &grib_accessor_data_run_length_packing},
    {""}, {""}, {""}, {""}, {""},
#line 193 "accessor_class_list.gperf"
    {"sum", &grib_accessor_sum},
    {""}, {""},
#line 181 "accessor_class_list.gperf"
    {"signed_bits", &grib_accessor_signed_bits},
    {""}, {""}, {""}, {""},
#line 173 "accessor_class_list.gperf"
    {"second_order_bits_per_value", &grib_accessor_second_order_bits_per_value},
    {""},
#line 38 "accessor_class_list.gperf"
    {"concept", &grib_accessor_concept},
#line 103 "accessor_class_list.gperf"
    {"g2bitmap_present", &grib_accessor_g2bitmap_present},
#line 34 "accessor_class_list.gperf"
    {"codeflag", &grib_accessor_codeflag},
#line 29 "accessor_class_list.gperf"
    {"bytes", &grib_accessor_bytes},
#line 40 "accessor_class_list.gperf"
    {"count_file", &grib_accessor_count_file},
#line 199 "accessor_class_list.gperf"
    {"transient", &grib_accessor_transient},
#line 191 "accessor_class_list.gperf"
    {"step_human_readable", &grib_accessor_step_human_readable},
#line 218 "accessor_class_list.gperf"
    {"cf_var_name", &grib_accessor_cf_var_name},
#line 46 "accessor_class_list.gperf"
    {"data_ccsds_packing", &grib_accessor_data_ccsds_packing},
#line 207 "accessor_class_list.gperf"
    {"uint8", &grib_accessor_uint8},
    {""}, {""},
#line 111 "accessor_class_list.gperf"
    {"gaussian_grid_name", &grib_accessor_gaussian_grid_name},
#line 200 "accessor_class_list.gperf"
    {"transient_darray", &grib_accessor_transient_darray},
#line 205 "accessor_class_list.gperf"
    {"uint64", &grib_accessor_uint64},
    {""},
#line 13 "accessor_class_list.gperf"
    {"bit", &grib_accessor_bit},
#line 15 "accessor_class_list.gperf"
    {"bits", &grib_accessor_bits},
#line 187 "accessor_class_list.gperf"
    {"spectral_truncation", &grib_accessor_spectral_truncation},
#line 14 "accessor_class_list.gperf"
    {"bitmap", &grib_accessor_bitmap},
    {""}, {""},
#line 174 "accessor_class_list.gperf"
    {"section", &grib_accessor_section},
#line 203 "accessor_class_list.gperf"
    {"uint32", &grib_accessor_uint32},
#line 76 "accessor_class_list.gperf"
    {"divdouble", &grib_accessor_divdouble},
#line 192 "accessor_class_list.gperf"
    {"step_in_units", &grib_accessor_step_in_units},
#line 98 "accessor_class_list.gperf"
    {"g2_concept_dir", &grib_accessor_g2_concept_dir},
#line 171 "accessor_class_list.gperf"
    {"scale", &grib_accessor_scale},
#line 206 "accessor_class_list.gperf"
    {"uint64_little_endian", &grib_accessor_uint64_little_endian},
#line 123 "accessor_class_list.gperf"
    {"ifs_param", &grib_accessor_ifs_param},
#line 16 "accessor_class_list.gperf"
    {"bits_per_value", &grib_accessor_bits_per_value},
#line 176 "accessor_class_list.gperf"
    {"section_padding", &grib_accessor_section_padding},
#line 210 "accessor_class_list.gperf"
    {"unsigned", &grib_accessor_unsigned},
    {""},
#line 100 "accessor_class_list.gperf"
    {"g2_mars_labeling", &grib_accessor_g2_mars_labeling},
#line 168 "accessor_class_list.gperf"
    {"rdbtime_guess_date", &grib_accessor_rdbtime_guess_date},
    {""},
#line 204 "accessor_class_list.gperf"
    {"uint32_little_endian", &grib_accessor_uint32_little_endian},
    {""},
#line 134 "accessor_class_list.gperf"
    {"long", &grib_accessor_long},
#line 74 "accessor_class_list.gperf"
    {"dictionary", &grib_accessor_dictionary},
    {""},
#line 77 "accessor_class_list.gperf"
    {"double", &grib_accessor_double},
    {""},
#line 129 "accessor_class_list.gperf"
    {"latitudes", &grib_accessor_latitudes},
#line 149 "accessor_class_list.gperf"
    {"number_of_points", &grib_accessor_number_of_points},
    {""},
#line 78 "accessor_class_list.gperf"
    {"element", &grib_accessor_element},
#line 178 "accessor_class_list.gperf"
    {"select_step_template", &grib_accessor_select_step_template},
    {""},
#line 214 "accessor_class_list.gperf"
    {"values", &grib_accessor_values},
#line 216 "accessor_class_list.gperf"
    {"vector", &grib_accessor_vector},
    {""}, {""},
#line 150 "accessor_class_list.gperf"
    {"number_of_points_gaussian", &grib_accessor_number_of_points_gaussian},
#line 212 "accessor_class_list.gperf"
    {"validity_date", &grib_accessor_validity_date},
    {""},
#line 163 "accessor_class_list.gperf"
    {"padtoeven", &grib_accessor_padtoeven},
#line 217 "accessor_class_list.gperf"
    {"when", &grib_accessor_when},
    {""},
#line 18 "accessor_class_list.gperf"
    {"budgdate", &grib_accessor_budgdate},
#line 39 "accessor_class_list.gperf"
    {"constant", &grib_accessor_constant},
    {""}, {""},
#line 118 "accessor_class_list.gperf"
    {"gts_header", &grib_accessor_gts_header},
    {""},
#line 19 "accessor_class_list.gperf"
    {"bufr_data_array", &grib_accessor_bufr_data_array},
    {""},
#line 154 "accessor_class_list.gperf"
    {"octet_number", &grib_accessor_octet_number},
    {""},
#line 27 "accessor_class_list.gperf"
    {"bufr_string_values", &grib_accessor_bufr_string_values},
#line 20 "accessor_class_list.gperf"
    {"bufr_data_element", &grib_accessor_bufr_data_element},
    {""}, {""}, {""}, {""},
#line 70 "accessor_class_list.gperf"
    {"data_sh_unpacked", &grib_accessor_data_sh_unpacked},
#line 177 "accessor_class_list.gperf"
    {"section_pointer", &grib_accessor_section_pointer},
    {""},
#line 28 "accessor_class_list.gperf"
    {"bufrdc_expanded_descriptors", &grib_accessor_bufrdc_expanded_descriptors},
#line 115 "accessor_class_list.gperf"
    {"global_gaussian", &grib_accessor_global_gaussian},
#line 71 "accessor_class_list.gperf"
    {"data_shsimple_packing", &grib_accessor_data_shsimple_packing},
    {""}, {""},
#line 211 "accessor_class_list.gperf"
    {"unsigned_bits", &grib_accessor_unsigned_bits},
    {""},
#line 86 "accessor_class_list.gperf"
    {"g1date", &grib_accessor_g1date},
    {""},
#line 85 "accessor_class_list.gperf"
    {"g1bitmap", &grib_accessor_g1bitmap},
    {""}, {""}, {""},
#line 175 "accessor_class_list.gperf"
    {"section_length", &grib_accessor_section_length},
#line 35 "accessor_class_list.gperf"
    {"codetable", &grib_accessor_codetable},
    {""}, {""},
#line 94 "accessor_class_list.gperf"
    {"g1step_range", &grib_accessor_g1step_range},
    {""}, {""},
#line 36 "accessor_class_list.gperf"
    {"codetable_title", &grib_accessor_codetable_title},
    {""},
#line 84 "accessor_class_list.gperf"
    {"g1_section4_length", &grib_accessor_g1_section4_length},
#line 213 "accessor_class_list.gperf"
    {"validity_time", &grib_accessor_validity_time},
    {""},
#line 54 "accessor_class_list.gperf"
    {"data_g1secondary_bitmap", &grib_accessor_data_g1secondary_bitmap},
#line 82 "accessor_class_list.gperf"
    {"g1_half_byte_codeflag", &grib_accessor_g1_half_byte_codeflag},
    {""},
#line 56 "accessor_class_list.gperf"
    {"data_g1simple_packing", &grib_accessor_data_g1simple_packing},
#line 49 "accessor_class_list.gperf"
    {"data_g1complex_packing", &grib_accessor_data_g1complex_packing},
    {""}, {""},
#line 208 "accessor_class_list.gperf"
    {"unexpanded_descriptors", &grib_accessor_unexpanded_descriptors},
    {""}, {""},
#line 81 "accessor_class_list.gperf"
    {"from_scale_factor_scaled_value", &grib_accessor_from_scale_factor_scaled_value},
    {""},
#line 52 "accessor_class_list.gperf"
    {"data_g1second_order_general_packing", &grib_accessor_data_g1second_order_general_packing},
#line 179 "accessor_class_list.gperf"
    {"sexagesimal2decimal", &grib_accessor_sexagesimal2decimal},
#line 194 "accessor_class_list.gperf"
    {"suppressed", &grib_accessor_suppressed},
#line 53 "accessor_class_list.gperf"
    {"data_g1second_order_row_by_row_packing", &grib_accessor_data_g1second_order_row_by_row_packing},
    {""},
#line 26 "accessor_class_list.gperf"
    {"bufr_simple_thinning", &grib_accessor_bufr_simple_thinning},
    {""},
#line 50 "accessor_class_list.gperf"
    {"data_g1second_order_constant_width_packing", &grib_accessor_data_g1second_order_constant_width_packing},
    {""},
#line 51 "accessor_class_list.gperf"
    {"data_g1second_order_general_extended_packing", &grib_accessor_data_g1second_order_general_extended_packing},
#line 137 "accessor_class_list.gperf"
    {"lookup", &grib_accessor_lookup},
    {""}, {""},
#line 140 "accessor_class_list.gperf"
    {"md5", &grib_accessor_md5},
#line 83 "accessor_class_list.gperf"
    {"g1_message_length", &grib_accessor_g1_message_length},
#line 182 "accessor_class_list.gperf"
    {"simple_packing_error", &grib_accessor_simple_packing_error},
#line 17 "accessor_class_list.gperf"
    {"blob", &grib_accessor_blob},
    {""},
#line 139 "accessor_class_list.gperf"
    {"mars_step", &grib_accessor_mars_step},
#line 138 "accessor_class_list.gperf"
    {"mars_param", &grib_accessor_mars_param},
    {""},
#line 79 "accessor_class_list.gperf"
    {"evaluate", &grib_accessor_evaluate},
#line 141 "accessor_class_list.gperf"
    {"message", &grib_accessor_message},
    {""},
#line 135 "accessor_class_list.gperf"
    {"long_vector", &grib_accessor_long_vector},
    {""},
#line 130 "accessor_class_list.gperf"
    {"latlon_increment", &grib_accessor_latlon_increment},
    {""}, {""}, {""}, {""},
#line 143 "accessor_class_list.gperf"
    {"message_is_valid", &grib_accessor_message_is_valid},
#line 158 "accessor_class_list.gperf"
    {"pack_bufr_values", &grib_accessor_pack_bufr_values},
#line 142 "accessor_class_list.gperf"
    {"message_copy", &grib_accessor_message_copy},
#line 55 "accessor_class_list.gperf"
    {"data_g1shsimple_packing", &grib_accessor_data_g1shsimple_packing},
    {""},
#line 25 "accessor_class_list.gperf"
    {"bufr_group", &grib_accessor_bufr_group},
    {""},
#line 119 "accessor_class_list.gperf"
    {"hash_array", &grib_accessor_hash_array},
    {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 148 "accessor_class_list.gperf"
    {"number_of_coded_values", &grib_accessor_number_of_coded_values},
#line 42 "accessor_class_list.gperf"
    {"count_total", &grib_accessor_count_total},
    {""}, {""},
#line 33 "accessor_class_list.gperf"
    {"closest_date", &grib_accessor_closest_date},
    {""}, {""},
#line 184 "accessor_class_list.gperf"
    {"smart_table", &grib_accessor_smart_table},
#line 88 "accessor_class_list.gperf"
    {"g1end_of_interval_monthly", &grib_accessor_g1end_of_interval_monthly},
    {""}, {""}, {""}, {""}, {""},
#line 185 "accessor_class_list.gperf"
    {"smart_table_column", &grib_accessor_smart_table_column},
#line 136 "accessor_class_list.gperf"
    {"longitudes", &grib_accessor_longitudes},
    {""}, {""}, {""}, {""}, {""},
#line 121 "accessor_class_list.gperf"
    {"ibmfloat", &grib_accessor_ibmfloat},
#line 37 "accessor_class_list.gperf"
    {"codetable_units", &grib_accessor_codetable_units},
#line 69 "accessor_class_list.gperf"
    {"data_sh_packed", &grib_accessor_data_sh_packed},
    {""},
#line 89 "accessor_class_list.gperf"
    {"g1fcperiod", &grib_accessor_g1fcperiod},
    {""},
#line 132 "accessor_class_list.gperf"
    {"library_version", &grib_accessor_library_version},
    {""},
#line 153 "accessor_class_list.gperf"
    {"octahedral_gaussian", &grib_accessor_octahedral_gaussian},
#line 11 "accessor_class_list.gperf"
    {"abstract_vector", &grib_accessor_abstract_vector},
#line 95 "accessor_class_list.gperf"
    {"g1verificationdate", &grib_accessor_g1verificationdate},
    {""},
#line 10 "accessor_class_list.gperf"
    {"abstract_long_vector", &grib_accessor_abstract_long_vector},
    {""}, {""}, {""}, {""}, {""},
#line 128 "accessor_class_list.gperf"
    {"label", &grib_accessor_label},
    {""}, {""},
#line 24 "accessor_class_list.gperf"
    {"bufr_extract_subsets", &grib_accessor_bufr_extract_subsets},
#line 30 "accessor_class_list.gperf"
    {"change_alternative_row_scanning", &grib_accessor_change_alternative_row_scanning},
#line 172 "accessor_class_list.gperf"
    {"scale_values", &grib_accessor_scale_values},
    {""},
#line 151 "accessor_class_list.gperf"
    {"number_of_values", &grib_accessor_number_of_values},
#line 22 "accessor_class_list.gperf"
    {"bufr_extract_area_subsets", &grib_accessor_bufr_extract_area_subsets},
    {""},
#line 215 "accessor_class_list.gperf"
    {"variable", &grib_accessor_variable},
    {""},
#line 23 "accessor_class_list.gperf"
    {"bufr_extract_datetime_subsets", &grib_accessor_bufr_extract_datetime_subsets},
    {""}, {""}, {""},
#line 202 "accessor_class_list.gperf"
    {"uint16", &grib_accessor_uint16},
    {""},
#line 131 "accessor_class_list.gperf"
    {"latlonvalues", &grib_accessor_latlonvalues},
    {""}, {""}, {""}, {""}, {""},
#line 152 "accessor_class_list.gperf"
    {"number_of_values_data_raw_packing", &grib_accessor_number_of_values_data_raw_packing},
    {""},
#line 127 "accessor_class_list.gperf"
    {"ksec1expver", &grib_accessor_ksec1expver},
    {""}, {""}, {""}, {""},
#line 108 "accessor_class_list.gperf"
    {"g2level", &grib_accessor_g2level},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""},
#line 209 "accessor_class_list.gperf"
    {"unpack_bufr_values", &grib_accessor_unpack_bufr_values},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""},
#line 31 "accessor_class_list.gperf"
    {"change_scanning_direction", &grib_accessor_change_scanning_direction},
    {""}, {""}, {""},
#line 120 "accessor_class_list.gperf"
    {"headers_only", &grib_accessor_headers_only},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""},
#line 133 "accessor_class_list.gperf"
    {"local_definition", &grib_accessor_local_definition},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 87 "accessor_class_list.gperf"
    {"g1day_of_the_year_date", &grib_accessor_g1day_of_the_year_date},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 21 "accessor_class_list.gperf"
    {"bufr_elements_table", &grib_accessor_bufr_elements_table},
    {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 41 "accessor_class_list.gperf"
    {"count_missing", &grib_accessor_count_missing},
    {""}, {""},
#line 145 "accessor_class_list.gperf"
    {"multdouble", &grib_accessor_multdouble},
    {""}, {""}, {""},
#line 169 "accessor_class_list.gperf"
    {"reference_value_error", &grib_accessor_reference_value_error},
#line 73 "accessor_class_list.gperf"
    {"decimal_precision", &grib_accessor_decimal_precision},
    {""}, {""}, {""},
#line 157 "accessor_class_list.gperf"
    {"optimal_step_units", &grib_accessor_optimal_step_units},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""},
#line 144 "accessor_class_list.gperf"
    {"mtg2_switch_default", &grib_accessor_mtg2_switch_default},
    {""}, {""}, {""},
#line 32 "accessor_class_list.gperf"
    {"check_internal_version", &grib_accessor_check_internal_version},
    {""}, {""}, {""}, {""}, {""}, {""},
#line 91 "accessor_class_list.gperf"
    {"g1monthlydate", &grib_accessor_g1monthlydate},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 93 "accessor_class_list.gperf"
    {"g1number_of_coded_values_sh_simple", &grib_accessor_g1number_of_coded_values_sh_simple},
#line 92 "accessor_class_list.gperf"
    {"g1number_of_coded_values_sh_complex", &grib_accessor_g1number_of_coded_values_sh_complex},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 97 "accessor_class_list.gperf"
    {"g2_chemical", &grib_accessor_g2_chemical},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
    {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 90 "accessor_class_list.gperf"
    {"g1forecastmonth", &grib_accessor_g1forecastmonth}
  };

static const struct accessor_class_hash* grib_accessor_hash(const char* str, size_t len)
{
    unsigned int key = grib_accessor_classes_get_id(str, len);

#ifdef DEBUG
    {
        const char* s;
        ECCODES_ASSERT(len <= MAX_WORD_LENGTH && len >= MIN_WORD_LENGTH);
        ECCODES_ASSERT(key <= MAX_HASH_VALUE);
        s = classes[key].name;
        ECCODES_ASSERT(*str == *s && strcmp(str + 1, s + 1) == 0);
    }
#endif

    return &classes[key];
}
