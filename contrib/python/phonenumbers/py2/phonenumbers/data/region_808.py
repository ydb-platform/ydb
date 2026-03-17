"""Auto-generated file, do not edit by hand. 808 metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_808 = PhoneMetadata(id='001', country_code=808, international_prefix=None,
    general_desc=PhoneNumberDesc(national_number_pattern='[1-9]\\d{7}', possible_length=(8,)),
    shared_cost=PhoneNumberDesc(national_number_pattern='[1-9]\\d{7}', example_number='12345678', possible_length=(8,)),
    number_format=[NumberFormat(pattern='(\\d{4})(\\d{4})', format='\\1 \\2', leading_digits_pattern=['[1-9]'])])
