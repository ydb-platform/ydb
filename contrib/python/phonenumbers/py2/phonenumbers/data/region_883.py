"""Auto-generated file, do not edit by hand. 883 metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_883 = PhoneMetadata(id='001', country_code=883, international_prefix=None,
    general_desc=PhoneNumberDesc(national_number_pattern='(?:[1-4]\\d|51)\\d{6,10}', possible_length=(8, 9, 10, 11, 12)),
    voip=PhoneNumberDesc(national_number_pattern='(?:2(?:00\\d\\d|10)|(?:370[1-9]|51\\d0)\\d)\\d{7}|51(?:00\\d{5}|[24-9]0\\d{4,7})|(?:1[0-79]|2[24-689]|3[02-689]|4[0-4])0\\d{5,9}', example_number='510012345', possible_length=(8, 9, 10, 11, 12)),
    number_format=[NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{2,8})', format='\\1 \\2 \\3', leading_digits_pattern=['[14]|2[24-689]|3[02-689]|51[24-9]']),
        NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{3})', format='\\1 \\2 \\3', leading_digits_pattern=['510']),
        NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['21']),
        NumberFormat(pattern='(\\d{4})(\\d{4})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['51[13]']),
        NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{3})(\\d{3})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['[235]'])])
