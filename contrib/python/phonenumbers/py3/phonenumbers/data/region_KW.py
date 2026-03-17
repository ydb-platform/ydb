"""Auto-generated file, do not edit by hand. KW metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_KW = PhoneMetadata(id='KW', country_code=965, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='18\\d{5}|(?:[2569]\\d|41)\\d{6}', possible_length=(7, 8)),
    fixed_line=PhoneNumberDesc(national_number_pattern='2(?:[23]\\d\\d|4(?:[1-35-9]\\d|44)|5(?:0[034]|[2-46]\\d|5[1-3]|7[1-7]))\\d{4}', example_number='22345678', possible_length=(8,)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:41\\d\\d|5(?:(?:[05]\\d|1[0-7]|6[56])\\d|2(?:22|5[25])|7(?:55|77)|88[58])|6(?:(?:0[034679]|5[015-9]|6\\d)\\d|1(?:00|11|6[16])|2[26]2|3[36]3|4[46]4|7(?:0[013-9]|[67]\\d)|8[68]8|9(?:[069]\\d|3[039]))|9(?:(?:[04679]\\d|8[057-9])\\d|1(?:1[01]|99)|2(?:00|2\\d)|3(?:00|3[03])|5(?:00|5\\d)))\\d{4}', example_number='50012345', possible_length=(8,)),
    toll_free=PhoneNumberDesc(national_number_pattern='18\\d{5}', example_number='1801234', possible_length=(7,)),
    number_format=[NumberFormat(pattern='(\\d{4})(\\d{3,4})', format='\\1 \\2', leading_digits_pattern=['[169]|2(?:[235]|4[1-35-9])|52']),
        NumberFormat(pattern='(\\d{3})(\\d{5})', format='\\1 \\2', leading_digits_pattern=['[245]'])],
    mobile_number_portable_region=True)
