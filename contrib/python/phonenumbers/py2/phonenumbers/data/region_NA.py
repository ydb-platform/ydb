"""Auto-generated file, do not edit by hand. NA metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_NA = PhoneMetadata(id='NA', country_code=264, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='[68]\\d{7,8}', possible_length=(8, 9)),
    fixed_line=PhoneNumberDesc(national_number_pattern='64426\\d{3}|6(?:1(?:2[2-7]|3[01378]|4[0-4])|254|32[0237]|4(?:27|41|5[25])|52[236-8]|626|7(?:2[2-4]|30))\\d{4,5}|6(?:1(?:(?:0\\d|2[0189]|3[24-69]|4[5-9])\\d|17|69|7[014])|2(?:17|5[0-36-8]|69|70)|3(?:17|2[14-689]|34|6[289]|7[01]|81)|4(?:17|2[0-2]|4[06]|5[0137]|69|7[01])|5(?:17|2[0459]|69|7[01])|6(?:17|25|38|42|69|7[01])|7(?:17|2[569]|3[13]|6[89]|7[01]))\\d{4}', example_number='61221234', possible_length=(8, 9)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:60|8[1245])\\d{7}', example_number='811234567', possible_length=(9,)),
    toll_free=PhoneNumberDesc(national_number_pattern='80\\d{7}', example_number='800123456', possible_length=(9,)),
    premium_rate=PhoneNumberDesc(national_number_pattern='8701\\d{5}', example_number='870123456', possible_length=(9,)),
    voip=PhoneNumberDesc(national_number_pattern='8(?:3\\d\\d|86)\\d{5}', example_number='88612345', possible_length=(8, 9)),
    national_prefix='0',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d{2})(\\d{3})(\\d{3})', format='\\1 \\2 \\3', leading_digits_pattern=['88'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{3})(\\d{3,4})', format='\\1 \\2 \\3', leading_digits_pattern=['6'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{3})', format='\\1 \\2 \\3', leading_digits_pattern=['87'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{3})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['8'], national_prefix_formatting_rule='0\\1')])
