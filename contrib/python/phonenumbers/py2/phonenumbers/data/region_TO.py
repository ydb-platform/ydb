"""Auto-generated file, do not edit by hand. TO metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_TO = PhoneMetadata(id='TO', country_code=676, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='(?:0800|(?:[5-8]\\d\\d|999)\\d)\\d{3}|[2-8]\\d{4}', possible_length=(5, 7)),
    fixed_line=PhoneNumberDesc(national_number_pattern='(?:2\\d|3[0-8]|4[0-4]|50|6[09]|7[0-24-69]|8[05])\\d{3}', example_number='20123', possible_length=(5,)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:5(?:4[0-5]|5[4-6])|6(?:[09]\\d|3[02]|8[15-9])|(?:7\\d|8[46-9])\\d|999)\\d{4}', example_number='7715123', possible_length=(7,)),
    toll_free=PhoneNumberDesc(national_number_pattern='0800\\d{3}', example_number='0800222', possible_length=(7,)),
    voip=PhoneNumberDesc(national_number_pattern='55[0-37-9]\\d{4}', example_number='5510123', possible_length=(7,)),
    number_format=[NumberFormat(pattern='(\\d{2})(\\d{3})', format='\\1-\\2', leading_digits_pattern=['[2-4]|50|6[09]|7[0-24-69]|8[05]']),
        NumberFormat(pattern='(\\d{4})(\\d{3})', format='\\1 \\2', leading_digits_pattern=['0']),
        NumberFormat(pattern='(\\d{3})(\\d{4})', format='\\1 \\2', leading_digits_pattern=['[5-9]'])])
