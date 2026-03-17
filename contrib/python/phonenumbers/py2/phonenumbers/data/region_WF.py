"""Auto-generated file, do not edit by hand. WF metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_WF = PhoneMetadata(id='WF', country_code=681, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='(?:40|72|8\\d{4})\\d{4}|[89]\\d{5}', possible_length=(6, 9)),
    fixed_line=PhoneNumberDesc(national_number_pattern='72\\d{4}', example_number='721234', possible_length=(6,)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:72|8[23])\\d{4}', example_number='821234', possible_length=(6,)),
    toll_free=PhoneNumberDesc(national_number_pattern='80[0-5]\\d{6}', example_number='800012345', possible_length=(9,)),
    voip=PhoneNumberDesc(national_number_pattern='9[23]\\d{4}', example_number='921234', possible_length=(6,)),
    voicemail=PhoneNumberDesc(national_number_pattern='[48]0\\d{4}', example_number='401234', possible_length=(6,)),
    number_format=[NumberFormat(pattern='(\\d{2})(\\d{2})(\\d{2})', format='\\1 \\2 \\3', leading_digits_pattern=['[47-9]']),
        NumberFormat(pattern='(\\d{3})(\\d{2})(\\d{2})(\\d{2})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['8'])])
