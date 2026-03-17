"""Auto-generated file, do not edit by hand. LA metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_LA = PhoneMetadata(id='LA', country_code=856, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='[23]\\d{9}|3\\d{8}|(?:[235-8]\\d|41)\\d{6}', possible_length=(8, 9, 10), possible_length_local_only=(6,)),
    fixed_line=PhoneNumberDesc(national_number_pattern='(?:2[13]|[35-7][14]|41|8[1468])\\d{6}', example_number='21212862', possible_length=(8,), possible_length_local_only=(6,)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:20(?:[23579]\\d|8[78])|30[24]\\d)\\d{6}|30\\d{7}', example_number='2023123456', possible_length=(9, 10)),
    national_prefix='0',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d{2})(\\d{3})(\\d{3})', format='\\1 \\2 \\3', leading_digits_pattern=['2[13]|3[14]|[4-8]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{2})(\\d{2})(\\d{3})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['3'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{2})(\\d{3})(\\d{3})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['[23]'], national_prefix_formatting_rule='0\\1')])
