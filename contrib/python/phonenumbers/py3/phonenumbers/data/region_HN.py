"""Auto-generated file, do not edit by hand. HN metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_HN = PhoneMetadata(id='HN', country_code=504, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='8\\d{10}|[237-9]\\d{7}', possible_length=(8, 11)),
    fixed_line=PhoneNumberDesc(national_number_pattern='2(?:2(?:0[0-59]|1[1-9]|[23]\\d|4[02-7]|5[57]|6[245]|7[0135689]|8[01346-9]|9[0-2])|4(?:0[578]|2[3-59]|3[13-9]|4[0-68]|5[1-3589])|5(?:0[2357-9]|1[1-356]|4[03-5]|5\\d|6[014-69]|7[04]|80)|6(?:[056]\\d|17|2[067]|3[047]|4[0-378]|[78][0-8]|9[01])|7(?:0[5-79]|6[46-9]|7[02-9]|8[034]|91)|8(?:79|8[0-357-9]|9[1-57-9]))\\d{4}', example_number='22123456', possible_length=(8,)),
    mobile=PhoneNumberDesc(national_number_pattern='[37-9]\\d{7}', example_number='91234567', possible_length=(8,)),
    toll_free=PhoneNumberDesc(national_number_pattern='8002\\d{7}', example_number='80021234567', possible_length=(11,)),
    no_international_dialling=PhoneNumberDesc(national_number_pattern='8002\\d{7}', possible_length=(11,)),
    number_format=[NumberFormat(pattern='(\\d{4})(\\d{4})', format='\\1-\\2', leading_digits_pattern=['[237-9]']),
        NumberFormat(pattern='(\\d{3})(\\d{4})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['8'])],
    intl_number_format=[NumberFormat(pattern='(\\d{4})(\\d{4})', format='\\1-\\2', leading_digits_pattern=['[237-9]'])])
