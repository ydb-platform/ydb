"""Auto-generated file, do not edit by hand. GA metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_GA = PhoneMetadata(id='GA', country_code=241, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='(?:[067]\\d|11)\\d{6}|[2-7]\\d{6}', possible_length=(7, 8)),
    fixed_line=PhoneNumberDesc(national_number_pattern='[01]1\\d{6}', example_number='01441234', possible_length=(8,)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:(?:0[2-7]|7[467])\\d|6(?:0[0-4]|10|[256]\\d))\\d{5}|[2-7]\\d{6}', example_number='06031234', possible_length=(7, 8)),
    national_prefix_for_parsing='0(11\\d{6}|60\\d{6}|61\\d{6}|6[256]\\d{6}|7[467]\\d{6})',
    national_prefix_transform_rule='\\1',
    number_format=[NumberFormat(pattern='(\\d)(\\d{2})(\\d{2})(\\d{2})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['[2-7]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{2})(\\d{2})(\\d{2})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['0']),
        NumberFormat(pattern='(\\d{2})(\\d{2})(\\d{2})(\\d{2})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['11|[67]'], national_prefix_formatting_rule='0\\1')])
