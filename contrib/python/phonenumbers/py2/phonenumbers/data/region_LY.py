"""Auto-generated file, do not edit by hand. LY metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_LY = PhoneMetadata(id='LY', country_code=218, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='[2-9]\\d{8}', possible_length=(9,), possible_length_local_only=(7,)),
    fixed_line=PhoneNumberDesc(national_number_pattern='(?:2(?:0[56]|[1-6]\\d|7[124579]|8[124])|3(?:1\\d|2[2356])|4(?:[17]\\d|2[1-357]|5[2-4]|8[124])|5(?:[1347]\\d|2[1-469]|5[13-5]|8[1-4])|6(?:[1-479]\\d|5[2-57]|8[1-5])|7(?:[13]\\d|2[13-79])|8(?:[124]\\d|5[124]|84))\\d{6}', example_number='212345678', possible_length=(9,), possible_length_local_only=(7,)),
    mobile=PhoneNumberDesc(national_number_pattern='9[1-6]\\d{7}', example_number='912345678', possible_length=(9,)),
    national_prefix='0',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d{2})(\\d{7})', format='\\1-\\2', leading_digits_pattern=['[2-9]'], national_prefix_formatting_rule='0\\1')])
