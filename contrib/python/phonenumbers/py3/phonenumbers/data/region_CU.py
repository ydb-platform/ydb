"""Auto-generated file, do not edit by hand. CU metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_CU = PhoneMetadata(id='CU', country_code=53, international_prefix='119',
    general_desc=PhoneNumberDesc(national_number_pattern='(?:[2-7]|8\\d\\d)\\d{7}|[2-47]\\d{6}|[34]\\d{5}', possible_length=(6, 7, 8, 10), possible_length_local_only=(4, 5)),
    fixed_line=PhoneNumberDesc(national_number_pattern='(?:3[23]|4[89])\\d{4,6}|(?:31|4[36]|8(?:0[25]|78)\\d)\\d{6}|(?:2[1-4]|4[1257]|7\\d)\\d{5,6}', example_number='71234567', possible_length=(6, 7, 8, 10), possible_length_local_only=(4, 5)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:5\\d|6[2-4])\\d{6}', example_number='51234567', possible_length=(8,)),
    toll_free=PhoneNumberDesc(national_number_pattern='800\\d{7}', example_number='8001234567', possible_length=(10,)),
    shared_cost=PhoneNumberDesc(national_number_pattern='807\\d{7}', example_number='8071234567', possible_length=(10,)),
    national_prefix='0',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d{2})(\\d{4,6})', format='\\1 \\2', leading_digits_pattern=['2[1-4]|[34]'], national_prefix_formatting_rule='(0\\1)'),
        NumberFormat(pattern='(\\d)(\\d{6,7})', format='\\1 \\2', leading_digits_pattern=['7'], national_prefix_formatting_rule='(0\\1)'),
        NumberFormat(pattern='(\\d)(\\d{7})', format='\\1 \\2', leading_digits_pattern=['[56]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{3})(\\d{7})', format='\\1 \\2', leading_digits_pattern=['8'], national_prefix_formatting_rule='0\\1')])
