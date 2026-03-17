"""Auto-generated file, do not edit by hand. PE metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_PE = PhoneMetadata(id='PE', country_code=51, international_prefix='00|19(?:1[124]|77|90)00',
    general_desc=PhoneNumberDesc(national_number_pattern='(?:[14-8]|9\\d)\\d{7}', possible_length=(8, 9), possible_length_local_only=(6, 7)),
    fixed_line=PhoneNumberDesc(national_number_pattern='(?:(?:(?:4[34]|5[14])[0-8]|687)\\d|7(?:173|(?:3[0-8]|55)\\d)|8(?:10[05689]|6(?:0[06-9]|1[6-9]|29)|7(?:0[0569]|[56]0)))\\d{4}|(?:1[0-8]|4[12]|5[236]|6[1-7]|7[246]|8[2-4])\\d{6}', example_number='11234567', possible_length=(8,), possible_length_local_only=(6, 7)),
    mobile=PhoneNumberDesc(national_number_pattern='9\\d{8}', example_number='912345678', possible_length=(9,)),
    toll_free=PhoneNumberDesc(national_number_pattern='800\\d{5}', example_number='80012345', possible_length=(8,)),
    premium_rate=PhoneNumberDesc(national_number_pattern='805\\d{5}', example_number='80512345', possible_length=(8,)),
    shared_cost=PhoneNumberDesc(national_number_pattern='801\\d{5}', example_number='80112345', possible_length=(8,)),
    personal_number=PhoneNumberDesc(national_number_pattern='80[24]\\d{5}', example_number='80212345', possible_length=(8,)),
    preferred_international_prefix='00',
    national_prefix='0',
    preferred_extn_prefix=' Anexo ',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d{3})(\\d{5})', format='\\1 \\2', leading_digits_pattern=['80'], national_prefix_formatting_rule='(0\\1)'),
        NumberFormat(pattern='(\\d)(\\d{7})', format='\\1 \\2', leading_digits_pattern=['1'], national_prefix_formatting_rule='(0\\1)'),
        NumberFormat(pattern='(\\d{2})(\\d{6})', format='\\1 \\2', leading_digits_pattern=['[4-8]'], national_prefix_formatting_rule='(0\\1)'),
        NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{3})', format='\\1 \\2 \\3', leading_digits_pattern=['9'])],
    mobile_number_portable_region=True)
