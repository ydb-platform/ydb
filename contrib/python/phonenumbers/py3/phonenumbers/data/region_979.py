"""Auto-generated file, do not edit by hand. 979 metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_979 = PhoneMetadata(id='001', country_code=979, international_prefix=None,
    general_desc=PhoneNumberDesc(national_number_pattern='[1359]\\d{8}', possible_length=(9,), possible_length_local_only=(8,)),
    premium_rate=PhoneNumberDesc(national_number_pattern='[1359]\\d{8}', example_number='123456789', possible_length=(9,), possible_length_local_only=(8,)),
    number_format=[NumberFormat(pattern='(\\d)(\\d{4})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['[1359]'])])
