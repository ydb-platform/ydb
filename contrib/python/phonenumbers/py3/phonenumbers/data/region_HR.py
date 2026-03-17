"""Auto-generated file, do not edit by hand. HR metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_HR = PhoneMetadata(id='HR', country_code=385, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='[2-69]\\d{8}|80\\d{5,7}|[1-79]\\d{7}|6\\d{6}', possible_length=(7, 8, 9), possible_length_local_only=(6,)),
    fixed_line=PhoneNumberDesc(national_number_pattern='1\\d{7}|(?:2[0-3]|3[1-5]|4[02-47-9]|5[1-3])\\d{6,7}', example_number='12345678', possible_length=(8, 9), possible_length_local_only=(6, 7)),
    mobile=PhoneNumberDesc(national_number_pattern='9(?:(?:0[1-9]|[12589]\\d)\\d\\d|7(?:[0679]\\d\\d|5(?:[01]\\d|44|55|77|9[5-79])))\\d{4}|98\\d{6}', example_number='921234567', possible_length=(8, 9)),
    toll_free=PhoneNumberDesc(national_number_pattern='80\\d{5,7}', example_number='800123456', possible_length=(7, 8, 9)),
    premium_rate=PhoneNumberDesc(national_number_pattern='6[01459]\\d{6}|6[01]\\d{5}', example_number='6001234', possible_length=(7, 8)),
    personal_number=PhoneNumberDesc(national_number_pattern='7[45]\\d{6}', example_number='74123456', possible_length=(8,)),
    uan=PhoneNumberDesc(national_number_pattern='62\\d{6,7}|72\\d{6}', example_number='62123456', possible_length=(8, 9)),
    national_prefix='0',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d{2})(\\d{2})(\\d{3})', format='\\1 \\2 \\3', leading_digits_pattern=['6[01]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{3})(\\d{2})(\\d{2,3})', format='\\1 \\2 \\3', leading_digits_pattern=['8'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d)(\\d{4})(\\d{3})', format='\\1 \\2 \\3', leading_digits_pattern=['1'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{3})(\\d{3,4})', format='\\1 \\2 \\3', leading_digits_pattern=['6|7[245]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{3})(\\d{3,4})', format='\\1 \\2 \\3', leading_digits_pattern=['9'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{3})(\\d{3,4})', format='\\1 \\2 \\3', leading_digits_pattern=['[2-57]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{3})', format='\\1 \\2 \\3', leading_digits_pattern=['8'], national_prefix_formatting_rule='0\\1')],
    mobile_number_portable_region=True)
