"""Auto-generated file, do not edit by hand. 882 metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_882 = PhoneMetadata(id='001', country_code=882, international_prefix=None,
    general_desc=PhoneNumberDesc(national_number_pattern='[13]\\d{6}(?:\\d{2,5})?|[19]\\d{7}|(?:[25]\\d\\d|4)\\d{7}(?:\\d{2})?', possible_length=(7, 8, 9, 10, 11, 12)),
    mobile=PhoneNumberDesc(national_number_pattern='342\\d{4}|(?:337|49)\\d{6}|(?:3(?:2|47|7\\d{3})|50\\d{3})\\d{7}', example_number='3421234', possible_length=(7, 8, 9, 10, 12)),
    voip=PhoneNumberDesc(national_number_pattern='1(?:3(?:0[0347]|[13][0139]|2[035]|4[013568]|6[0459]|7[06]|8[15-8]|9[0689])\\d{4}|6\\d{5,10})|(?:345\\d|9[89])\\d{6}|(?:10|2(?:3|85\\d)|3(?:[15]|[69]\\d\\d)|4[15-8]|51)\\d{8}', example_number='390123456789', possible_length=(7, 8, 9, 10, 11, 12)),
    voicemail=PhoneNumberDesc(national_number_pattern='348[57]\\d{7}', example_number='34851234567', possible_length=(11,)),
    number_format=[NumberFormat(pattern='(\\d{2})(\\d{5})', format='\\1 \\2', leading_digits_pattern=['16|342']),
        NumberFormat(pattern='(\\d{2})(\\d{6})', format='\\1 \\2', leading_digits_pattern=['49']),
        NumberFormat(pattern='(\\d{2})(\\d{2})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['1[36]|9']),
        NumberFormat(pattern='(\\d{2})(\\d{4})(\\d{3})', format='\\1 \\2 \\3', leading_digits_pattern=['3[23]']),
        NumberFormat(pattern='(\\d{2})(\\d{3,4})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['16']),
        NumberFormat(pattern='(\\d{2})(\\d{4})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['10|23|3(?:[15]|4[57])|4|51']),
        NumberFormat(pattern='(\\d{3})(\\d{4})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['34']),
        NumberFormat(pattern='(\\d{2})(\\d{4,5})(\\d{5})', format='\\1 \\2 \\3', leading_digits_pattern=['[1-35]'])])
