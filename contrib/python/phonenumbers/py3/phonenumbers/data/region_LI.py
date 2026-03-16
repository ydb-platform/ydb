"""Auto-generated file, do not edit by hand. LI metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_LI = PhoneMetadata(id='LI', country_code=423, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='[68]\\d{8}|(?:[2378]\\d|90)\\d{5}', possible_length=(7, 9)),
    fixed_line=PhoneNumberDesc(national_number_pattern='(?:2(?:01|1[27]|2[024]|3\\d|6[02-578]|96)|3(?:[24]0|33|7[0135-7]|8[048]|9[0269]))\\d{4}', example_number='2345678', possible_length=(7,)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:6(?:(?:4[5-9]|5[0-46-9])\\d|6(?:[024-6]\\d|[17]0|3[7-9]))\\d|7(?:[37-9]\\d|42|56))\\d{4}', example_number='660234567', possible_length=(7, 9)),
    toll_free=PhoneNumberDesc(national_number_pattern='8002[28]\\d\\d|80(?:05\\d|9)\\d{4}', example_number='8002222', possible_length=(7, 9)),
    premium_rate=PhoneNumberDesc(national_number_pattern='90(?:02[258]|1(?:23|3[14])|66[136])\\d\\d', example_number='9002222', possible_length=(7,)),
    uan=PhoneNumberDesc(national_number_pattern='870(?:28|87)\\d\\d', example_number='8702812', possible_length=(7,)),
    voicemail=PhoneNumberDesc(national_number_pattern='697(?:42|56|[78]\\d)\\d{4}', example_number='697861234', possible_length=(9,)),
    national_prefix='0',
    national_prefix_for_parsing='(1001)|0',
    number_format=[NumberFormat(pattern='(\\d{3})(\\d{2})(\\d{2})', format='\\1 \\2 \\3', leading_digits_pattern=['[2379]|8(?:0[09]|7)', '[2379]|8(?:0(?:02|9)|7)'], domestic_carrier_code_formatting_rule='$CC \\1'),
        NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{3})', format='\\1 \\2 \\3', leading_digits_pattern=['8']),
        NumberFormat(pattern='(\\d{2})(\\d{3})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['69'], domestic_carrier_code_formatting_rule='$CC \\1'),
        NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{3})', format='\\1 \\2 \\3', leading_digits_pattern=['6'], domestic_carrier_code_formatting_rule='$CC \\1')])
