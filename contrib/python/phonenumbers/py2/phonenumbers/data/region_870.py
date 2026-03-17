"""Auto-generated file, do not edit by hand. 870 metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_870 = PhoneMetadata(id='001', country_code=870, international_prefix=None,
    general_desc=PhoneNumberDesc(national_number_pattern='7\\d{11}|[235-7]\\d{8}', possible_length=(9, 12)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:[356]|774[45])\\d{8}|7[6-8]\\d{7}', example_number='301234567', possible_length=(9, 12)),
    voip=PhoneNumberDesc(national_number_pattern='2\\d{8}', example_number='201234567', possible_length=(9,)),
    number_format=[NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{3})', format='\\1 \\2 \\3', leading_digits_pattern=['[235-7]'])])
