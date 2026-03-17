"""Auto-generated file, do not edit by hand. NZ metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_NZ = PhoneMetadata(id='NZ', country_code=64, international_prefix='0(?:0|161)',
    general_desc=PhoneNumberDesc(national_number_pattern='[1289]\\d{9}|50\\d{5}(?:\\d{2,3})?|[27-9]\\d{7,8}|(?:[34]\\d|6[0-35-9])\\d{6}|8\\d{4,6}', possible_length=(5, 6, 7, 8, 9, 10)),
    fixed_line=PhoneNumberDesc(national_number_pattern='240\\d{5}|(?:3[2-79]|[49][2-9]|6[235-9]|7[2-57-9])\\d{6}', example_number='32345678', possible_length=(8,), possible_length_local_only=(7,)),
    mobile=PhoneNumberDesc(national_number_pattern='2(?:[0-27-9]\\d|6)\\d{6,7}|2(?:1\\d|75)\\d{5}', example_number='211234567', possible_length=(8, 9, 10)),
    toll_free=PhoneNumberDesc(national_number_pattern='508\\d{6,7}|80\\d{6,8}', example_number='800123456', possible_length=(8, 9, 10)),
    premium_rate=PhoneNumberDesc(national_number_pattern='(?:1[13-57-9]\\d{5}|50(?:0[08]|30|66|77|88))\\d{3}|90\\d{6,8}', example_number='900123456', possible_length=(7, 8, 9, 10)),
    personal_number=PhoneNumberDesc(national_number_pattern='70\\d{7}', example_number='701234567', possible_length=(9,)),
    uan=PhoneNumberDesc(national_number_pattern='8(?:1[16-9]|22|3\\d|4[045]|5[459]|6[235-9]|7[0-3579]|90)\\d{2,7}', example_number='83012378', possible_length=(5, 6, 7, 8, 9, 10)),
    preferred_international_prefix='00',
    national_prefix='0',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d{2})(\\d{3,8})', format='\\1 \\2', leading_digits_pattern=['8[1-79]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{3})(\\d{2})(\\d{2,3})', format='\\1 \\2 \\3', leading_digits_pattern=['50[036-8]|8|90', '50(?:[0367]|88)|8|90'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d)(\\d{3})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['24|[346]|7[2-57-9]|9[2-9]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{3,4})', format='\\1 \\2 \\3', leading_digits_pattern=['2(?:10|74)|[589]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{3,4})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['1|2[028]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{3})(\\d{3,5})', format='\\1 \\2 \\3', leading_digits_pattern=['2(?:[169]|7[0-35-9])|7'], national_prefix_formatting_rule='0\\1')],
    mobile_number_portable_region=True)
