"""Auto-generated file, do not edit by hand. IR metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_IR = PhoneMetadata(id='IR', country_code=98, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='[1-9]\\d{9}|(?:[1-8]\\d\\d|9)\\d{3,4}', possible_length=(4, 5, 6, 7, 10), possible_length_local_only=(8,)),
    fixed_line=PhoneNumberDesc(national_number_pattern='(?:1[137]|2[13-68]|3[1458]|4[145]|5[1468]|6[16]|7[1467]|8[13467])(?:[03-57]\\d{7}|[16]\\d{3}(?:\\d{4})?|[289]\\d{3}(?:\\d(?:\\d{3})?)?)|94(?:000[09]|(?:12\\d|30[0-2])\\d|2(?:121|[2689]0\\d)|4(?:111|40\\d))\\d{4}', example_number='2123456789', possible_length=(6, 7, 10), possible_length_local_only=(4, 5, 8)),
    mobile=PhoneNumberDesc(national_number_pattern='9(?:(?:0(?:[0-35]\\d|4[4-6])|(?:[13]\\d|2[0-3])\\d)\\d|9(?:[0-46]\\d\\d|5[15]0|8(?:[12]\\d|88)|9(?:0[0-3]|[19]\\d|21|69|77|8[7-9])))\\d{5}', example_number='9123456789', possible_length=(10,)),
    uan=PhoneNumberDesc(national_number_pattern='96(?:0[12]|2[16-8]|3(?:08|[14]5|[23]|66)|4(?:0|80)|5[01]|6[89]|86|9[19])', example_number='9601', possible_length=(4, 5)),
    no_international_dialling=PhoneNumberDesc(national_number_pattern='9(?:4440\\d{5}|6(?:0[12]|2[16-8]|3(?:08|[14]5|[23]|66)|4(?:0|80)|5[01]|6[89]|86|9[19]))', possible_length=(4, 5, 10)),
    national_prefix='0',
    national_prefix_for_parsing='0',
    number_format=[NumberFormat(pattern='(\\d{4,5})', format='\\1', leading_digits_pattern=['96'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{4,5})', format='\\1 \\2', leading_digits_pattern=['(?:1[137]|2[13-68]|3[1458]|4[145]|5[1468]|6[16]|7[1467]|8[13467])[12689]'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{3,4})', format='\\1 \\2 \\3', leading_digits_pattern=['9'], national_prefix_formatting_rule='0\\1'),
        NumberFormat(pattern='(\\d{2})(\\d{4})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['[1-8]'], national_prefix_formatting_rule='0\\1')],
    mobile_number_portable_region=True)
