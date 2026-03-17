"""Auto-generated file, do not edit by hand. TJ metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_TJ = PhoneMetadata(id='TJ', country_code=992, international_prefix='810',
    general_desc=PhoneNumberDesc(national_number_pattern='[0-57-9]\\d{8}', possible_length=(9,), possible_length_local_only=(3, 5, 6, 7)),
    fixed_line=PhoneNumberDesc(national_number_pattern='(?:3(?:1[3-5]|2[245]|3[12]|4[24-7]|5[25]|72)|4(?:46|74|87))\\d{6}', example_number='372123456', possible_length=(9,), possible_length_local_only=(3, 5, 6, 7)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:33[03-9]|4(?:1[18]|4[02-479])|81[1-9])\\d{6}|(?:[09]\\d|1[0-27-9]|2[0-27]|[34]0|5[05]|7[01578]|8[078])\\d{7}', example_number='917123456', possible_length=(9,)),
    preferred_international_prefix='8~10',
    number_format=[NumberFormat(pattern='(\\d{6})(\\d)(\\d{2})', format='\\1 \\2 \\3', leading_digits_pattern=['331', '3317']),
        NumberFormat(pattern='(\\d{3})(\\d{2})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['44[02-479]|[34]7']),
        NumberFormat(pattern='(\\d{4})(\\d)(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['3(?:[1245]|3[12])']),
        NumberFormat(pattern='(\\d{2})(\\d{3})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['[0-57-9]'])])
