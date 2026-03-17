"""Auto-generated file, do not edit by hand. NP metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_NP = PhoneMetadata(id='NP', country_code=977, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='(?:1\\d|9)\\d{9}|[1-9]\\d{7}', possible_length=(8, 10, 11), possible_length_local_only=(6, 7)),
    fixed_line=PhoneNumberDesc(national_number_pattern='(?:1[0-6]\\d|99[02-6])\\d{5}|(?:2[13-79]|3[135-8]|4[146-9]|5[135-7]|6[13-9]|7[15-9]|8[1-46-9]|9[1-7])[2-6]\\d{5}', example_number='14567890', possible_length=(8,), possible_length_local_only=(6, 7)),
    mobile=PhoneNumberDesc(national_number_pattern='9(?:00|6[0-3]|7[024-6]|8[0-24-68])\\d{7}', example_number='9841234567', possible_length=(10,)),
    toll_free=PhoneNumberDesc(national_number_pattern='1(?:66001|800\\d\\d)\\d{5}', example_number='16600101234', possible_length=(11,)),
    national_prefix='0',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d)(\\d{7})', format='\\1-\\2', leading_digits_pattern=['1[2-6]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{6})', format='\\1-\\2', leading_digits_pattern=['1[01]|[2-8]|9(?:[1-59]|[67][2-6])'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{3})(\\d{7})', format='\\1-\\2', leading_digits_pattern=['9']),
        NumberFormat(pattern='(\\d{4})(\\d{2})(\\d{5})', format='\\1-\\2-\\3', leading_digits_pattern=['1'])],
    intl_number_format=[NumberFormat(pattern='(\\d)(\\d{7})', format='\\1-\\2', leading_digits_pattern=['1[2-6]']),
        NumberFormat(pattern='(\\d{2})(\\d{6})', format='\\1-\\2', leading_digits_pattern=['1[01]|[2-8]|9(?:[1-59]|[67][2-6])']),
        NumberFormat(pattern='(\\d{3})(\\d{7})', format='\\1-\\2', leading_digits_pattern=['9'])])
