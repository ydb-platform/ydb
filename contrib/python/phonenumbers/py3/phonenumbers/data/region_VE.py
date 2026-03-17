"""Auto-generated file, do not edit by hand. VE metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_VE = PhoneMetadata(id='VE', country_code=58, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='[68]00\\d{7}|(?:[24]\\d|[59]0)\\d{8}', possible_length=(10,), possible_length_local_only=(7,)),
    fixed_line=PhoneNumberDesc(national_number_pattern='(?:2(?:12|3[457-9]|[467]\\d|[58][1-9]|9[1-6])|[4-6]00)\\d{7}', example_number='2121234567', possible_length=(10,), possible_length_local_only=(7,)),
    mobile=PhoneNumberDesc(national_number_pattern='4(?:1[24-8]|2[246])\\d{7}', example_number='4121234567', possible_length=(10,)),
    toll_free=PhoneNumberDesc(national_number_pattern='800\\d{7}', example_number='8001234567', possible_length=(10,)),
    premium_rate=PhoneNumberDesc(national_number_pattern='90[01]\\d{7}', example_number='9001234567', possible_length=(10,)),
    uan=PhoneNumberDesc(national_number_pattern='501\\d{7}', example_number='5010123456', possible_length=(10,), possible_length_local_only=(7,)),
    national_prefix='0',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d{3})(\\d{7})', format='\\1-\\2', leading_digits_pattern=['[24-689]'], national_prefix_formatting_rule='0\\1', domestic_carrier_code_formatting_rule='$CC \\1')])
