"""Auto-generated file, do not edit by hand. GF metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_GF = PhoneMetadata(id='GF', country_code=594, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='(?:[56]94\\d|7093)\\d{5}|(?:80|9\\d)\\d{7}', possible_length=(9,)),
    fixed_line=PhoneNumberDesc(national_number_pattern='594(?:[02-49]\\d|1[0-5]|5[6-9]|6[0-3]|80)\\d{4}', example_number='594101234', possible_length=(9,)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:694(?:[0-249]\\d|3[0-8])|7093[0-3])\\d{4}', example_number='694201234', possible_length=(9,)),
    toll_free=PhoneNumberDesc(national_number_pattern='80[0-5]\\d{6}', example_number='800012345', possible_length=(9,)),
    voip=PhoneNumberDesc(national_number_pattern='9(?:(?:396|76\\d)\\d|476[0-6])\\d{4}', example_number='976012345', possible_length=(9,)),
    national_prefix='0',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d{3})(\\d{2})(\\d{2})(\\d{2})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['[5-7]|9[47]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{3})(\\d{2})(\\d{2})(\\d{2})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['[89]'], national_prefix_formatting_rule='0\\1')],
    mobile_number_portable_region=True)
