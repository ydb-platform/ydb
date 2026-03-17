"""Auto-generated file, do not edit by hand. RE metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_RE = PhoneMetadata(id='RE', country_code=262, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='709\\d{6}|(?:26|[689]\\d)\\d{7}', possible_length=(9,)),
    fixed_line=PhoneNumberDesc(national_number_pattern='26(?:2\\d\\d|3(?:0\\d|1[0-6]))\\d{4}', example_number='262161234', possible_length=(9,)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:69(?:2\\d\\d|3(?:[06][0-6]|1[0-3]|2[0-2]|3[0-39]|4\\d|5[0-5]|7[0-37]|8[0-8]|9[0-479]))|7092[0-3])\\d{4}', example_number='692123456', possible_length=(9,)),
    toll_free=PhoneNumberDesc(national_number_pattern='80\\d{7}', example_number='801234567', possible_length=(9,)),
    premium_rate=PhoneNumberDesc(national_number_pattern='89[1-37-9]\\d{6}', example_number='891123456', possible_length=(9,)),
    shared_cost=PhoneNumberDesc(national_number_pattern='8(?:1[019]|2[0156]|84|90)\\d{6}', example_number='810123456', possible_length=(9,)),
    voip=PhoneNumberDesc(national_number_pattern='9(?:399[0-3]|479[0-6]|76(?:2[278]|3[0-37]))\\d{4}', example_number='939901234', possible_length=(9,)),
    national_prefix='0',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d{3})(\\d{2})(\\d{2})(\\d{2})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['[26-9]'], national_prefix_formatting_rule='0\\1')],
    main_country_for_code=True)
