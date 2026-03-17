"""Auto-generated file, do not edit by hand. TW metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_TW = PhoneMetadata(id='TW', country_code=886, international_prefix='0(?:0[25-79]|19)',
    general_desc=PhoneNumberDesc(national_number_pattern='[2-689]\\d{8}|7\\d{9,10}|[2-8]\\d{7}|2\\d{6}', possible_length=(7, 8, 9, 10, 11)),
    fixed_line=PhoneNumberDesc(national_number_pattern='(?:2[2-8]\\d|370|55[01]|7[1-9])\\d{6}|4(?:(?:0(?:0[1-9]|[2-48]\\d)|1[023]\\d)\\d{4,5}|(?:[239]\\d\\d|4(?:0[56]|12|49))\\d{5})|6(?:[01]\\d{7}|4(?:0[56]|12|24|4[09])\\d{4,5})|8(?:(?:2(?:3\\d|4[0-269]|[578]0|66)|36[24-9]|90\\d\\d)\\d{4}|4(?:0[56]|12|24|4[09])\\d{4,5})|(?:2(?:2(?:0\\d\\d|4(?:0[68]|[249]0|3[0-467]|5[0-25-9]|6[0235689]))|(?:3(?:[09]\\d|1[0-4])|(?:4\\d|5[0-49]|6[0-29]|7[0-5])\\d)\\d)|(?:(?:3[2-9]|5[2-8]|6[0-35-79]|8[7-9])\\d\\d|4(?:2(?:[089]\\d|7[1-9])|(?:3[0-4]|[78]\\d|9[01])\\d))\\d)\\d{3}', example_number='221234567', possible_length=(8, 9)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:40001[0-2]|9[0-8]\\d{4})\\d{3}', example_number='912345678', possible_length=(9,)),
    toll_free=PhoneNumberDesc(national_number_pattern='80[0-79]\\d{6}|800\\d{5}', example_number='800123456', possible_length=(8, 9)),
    premium_rate=PhoneNumberDesc(national_number_pattern='20(?:[013-9]\\d\\d|2)\\d{4}', example_number='203123456', possible_length=(7, 9)),
    personal_number=PhoneNumberDesc(national_number_pattern='99\\d{7}', example_number='990123456', possible_length=(9,)),
    voip=PhoneNumberDesc(national_number_pattern='7010(?:[0-2679]\\d|3[0-7]|8[0-5])\\d{5}|70\\d{8}', example_number='7012345678', possible_length=(10, 11)),
    uan=PhoneNumberDesc(national_number_pattern='50[0-46-9]\\d{6}', example_number='500123456', possible_length=(9,)),
    national_prefix='0',
    preferred_extn_prefix='#',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d{2})(\\d)(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['202'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{3})(\\d{3,4})', format='\\1 \\2 \\3', leading_digits_pattern=['[258]0'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d)(\\d{3,4})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['[23568]|4(?:0[02-48]|[1-47-9])|7[1-9]', '[23568]|4(?:0[2-48]|[1-47-9])|(?:400|7)[1-9]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{3})', format='\\1 \\2 \\3', leading_digits_pattern=['[49]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{4})(\\d{4,5})', format='\\1 \\2 \\3', leading_digits_pattern=['7'], national_prefix_formatting_rule='0\\1')],
    mobile_number_portable_region=True)
