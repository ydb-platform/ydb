"""Auto-generated file, do not edit by hand. PH metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_PH = PhoneMetadata(id='PH', country_code=63, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='(?:[2-7]|9\\d)\\d{8}|2\\d{5}|(?:1800|8)\\d{7,9}', possible_length=(6, 8, 9, 10, 11, 12, 13), possible_length_local_only=(4, 5, 7)),
    fixed_line=PhoneNumberDesc(national_number_pattern='(?:(?:2[3-8]|3[2-68]|4[2-9]|5[2-6]|6[2-58]|7[24578])\\d{3}|88(?:22\\d\\d|42))\\d{4}|(?:2|8[2-8]\\d\\d)\\d{5}', example_number='232345678', possible_length=(6, 8, 9, 10), possible_length_local_only=(4, 5, 7)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:8(?:1[37]|9[5-8])|9(?:0[5-9]|1[0-24-9]|[235-7]\\d|4[2-9]|8[135-9]|9[1-9]))\\d{7}', example_number='9051234567', possible_length=(10,)),
    toll_free=PhoneNumberDesc(national_number_pattern='1800\\d{7,9}', example_number='180012345678', possible_length=(11, 12, 13)),
    national_prefix='0',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d)(\\d{5})', format='\\1 \\2', leading_digits_pattern=['2'], national_prefix_formatting_rule='(0\\1)'),
        NumberFormat(pattern='(\\d{4})(\\d{4,6})', format='\\1 \\2', leading_digits_pattern=['3(?:23|39|46)|4(?:2[3-6]|[35]9|4[26]|76)|544|88[245]|(?:52|64|86)2', '3(?:230|397|461)|4(?:2(?:35|[46]4|51)|396|4(?:22|63)|59[347]|76[15])|5(?:221|446)|642[23]|8(?:622|8(?:[24]2|5[13]))'], national_prefix_formatting_rule='(0\\1)'),
        NumberFormat(pattern='(\\d{5})(\\d{4})', format='\\1 \\2', leading_digits_pattern=['346|4(?:27|9[35])|883', '3469|4(?:279|9(?:30|56))|8834'], national_prefix_formatting_rule='(0\\1)'),
        NumberFormat(pattern='(\\d)(\\d{4})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['2'], national_prefix_formatting_rule='(0\\1)'),
        NumberFormat(pattern='(\\d{2})(\\d{3})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['[3-7]|8[2-8]'], national_prefix_formatting_rule='(0\\1)'),
        NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['[89]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{4})(\\d{3})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['1']),
        NumberFormat(pattern='(\\d{4})(\\d{1,2})(\\d{3})(\\d{4})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['1'])])
