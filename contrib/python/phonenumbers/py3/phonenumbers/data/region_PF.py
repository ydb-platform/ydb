"""Auto-generated file, do not edit by hand. PF metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_PF = PhoneMetadata(id='PF', country_code=689, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='4\\d{5}(?:\\d{2})?|8\\d{7,8}', possible_length=(6, 8, 9)),
    fixed_line=PhoneNumberDesc(national_number_pattern='4(?:0[4-689]|9[4-68])\\d{5}', example_number='40412345', possible_length=(8,)),
    mobile=PhoneNumberDesc(national_number_pattern='8[7-9]\\d{6}', example_number='87123456', possible_length=(8,)),
    toll_free=PhoneNumberDesc(national_number_pattern='80[0-5]\\d{6}', example_number='800012345', possible_length=(9,)),
    voip=PhoneNumberDesc(national_number_pattern='499\\d{5}', example_number='49901234', possible_length=(8,)),
    uan=PhoneNumberDesc(national_number_pattern='44\\d{4}', example_number='440123', possible_length=(6,)),
    no_international_dialling=PhoneNumberDesc(national_number_pattern='44\\d{4}', possible_length=(6,)),
    number_format=[NumberFormat(pattern='(\\d{2})(\\d{2})(\\d{2})', format='\\1 \\2 \\3', leading_digits_pattern=['44']),
        NumberFormat(pattern='(\\d{2})(\\d{2})(\\d{2})(\\d{2})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['4|8[7-9]']),
        NumberFormat(pattern='(\\d{3})(\\d{2})(\\d{2})(\\d{2})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['8'])])
