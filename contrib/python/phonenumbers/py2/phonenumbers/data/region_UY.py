"""Auto-generated file, do not edit by hand. UY metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_UY = PhoneMetadata(id='UY', country_code=598, international_prefix='0(?:0|1[3-9]\\d)',
    general_desc=PhoneNumberDesc(national_number_pattern='0004\\d{2,9}|[1249]\\d{7}|(?:[49]\\d|80)\\d{5}', possible_length=(6, 7, 8, 9, 10, 11, 12, 13)),
    fixed_line=PhoneNumberDesc(national_number_pattern='(?:1(?:770|9(?:20|[89]7))|(?:2\\d|4[2-7])\\d\\d)\\d{4}', example_number='21231234', possible_length=(8,), possible_length_local_only=(7,)),
    mobile=PhoneNumberDesc(national_number_pattern='9[1-9]\\d{6}', example_number='94231234', possible_length=(8,)),
    toll_free=PhoneNumberDesc(national_number_pattern='0004\\d{2,9}|(?:405|80[05])\\d{4}', example_number='8001234', possible_length=(6, 7, 8, 9, 10, 11, 12, 13)),
    premium_rate=PhoneNumberDesc(national_number_pattern='90[0-8]\\d{4}', example_number='9001234', possible_length=(7,)),
    preferred_international_prefix='00',
    national_prefix='0',
    preferred_extn_prefix=' int. ',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d{3})(\\d{3,4})', format='\\1 \\2', leading_digits_pattern=['0']),
        NumberFormat(pattern='(\\d{3})(\\d{4})', format='\\1 \\2', leading_digits_pattern=['[49]0|8'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{3})(\\d{3})', format='\\1 \\2 \\3', leading_digits_pattern=['9'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{4})(\\d{4})', format='\\1 \\2', leading_digits_pattern=['[124]']),
        NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{2,4})', format='\\1 \\2 \\3', leading_digits_pattern=['0']),
        NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{3})(\\d{2,4})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['0'])])
