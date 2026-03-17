"""Auto-generated file, do not edit by hand. QA metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_QA = PhoneMetadata(id='QA', country_code=974, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='800\\d{4}|(?:2|800)\\d{6}|(?:0080|[3-7])\\d{7}', possible_length=(7, 8, 9, 11)),
    fixed_line=PhoneNumberDesc(national_number_pattern='4(?:1111|2022)\\d{3}|4(?:[04]\\d\\d|14[0-6]|999)\\d{4}', example_number='44123456', possible_length=(8,)),
    mobile=PhoneNumberDesc(national_number_pattern='[35-7]\\d{7}', example_number='33123456', possible_length=(8,)),
    toll_free=PhoneNumberDesc(national_number_pattern='800\\d{4}|(?:0080[01]|800)\\d{6}', example_number='8001234', possible_length=(7, 9, 11)),
    pager=PhoneNumberDesc(national_number_pattern='2[136]\\d{5}', example_number='2123456', possible_length=(7,)),
    number_format=[NumberFormat(pattern='(\\d{3})(\\d{4})', format='\\1 \\2', leading_digits_pattern=['2[136]|8']),
        NumberFormat(pattern='(\\d{4})(\\d{4})', format='\\1 \\2', leading_digits_pattern=['[3-7]'])],
    mobile_number_portable_region=True)
