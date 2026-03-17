"""Auto-generated file, do not edit by hand. MQ metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_MQ = PhoneMetadata(id='MQ', country_code=596, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='(?:596\\d|7091)\\d{5}|(?:69|[89]\\d)\\d{7}', possible_length=(9,)),
    fixed_line=PhoneNumberDesc(national_number_pattern='(?:596(?:[03-7]\\d|1[05]|2[7-9]|8[0-39]|9[04-9])|80[6-9]\\d\\d|9(?:477[6-9]|767[4589]))\\d{4}', example_number='596301234', possible_length=(9,)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:69[67]\\d\\d|7091[0-3])\\d{4}', example_number='696201234', possible_length=(9,)),
    toll_free=PhoneNumberDesc(national_number_pattern='80[0-5]\\d{6}', example_number='800012345', possible_length=(9,)),
    premium_rate=PhoneNumberDesc(national_number_pattern='8[129]\\d{7}', example_number='810123456', possible_length=(9,)),
    voip=PhoneNumberDesc(national_number_pattern='9(?:397[0-3]|477[0-5]|76(?:6\\d|7[0-367]))\\d{4}', example_number='976612345', possible_length=(9,)),
    national_prefix='0',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d{3})(\\d{2})(\\d{2})(\\d{2})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['[5-79]|8(?:0[6-9]|[36])'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{3})(\\d{2})(\\d{2})(\\d{2})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['8'], national_prefix_formatting_rule='0\\1')],
    mobile_number_portable_region=True)
