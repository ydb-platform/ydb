"""Auto-generated file, do not edit by hand. 881 metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_881 = PhoneMetadata(id='001', country_code=881, international_prefix=None,
    general_desc=PhoneNumberDesc(national_number_pattern='6\\d{9}|[0-36-9]\\d{8}', possible_length=(9, 10)),
    mobile=PhoneNumberDesc(national_number_pattern='6\\d{9}|[0-36-9]\\d{8}', example_number='612345678', possible_length=(9, 10)),
    number_format=[NumberFormat(pattern='(\\d)(\\d{3})(\\d{5})', format='\\1 \\2 \\3', leading_digits_pattern=['[0-37-9]']),
        NumberFormat(pattern='(\\d)(\\d{3})(\\d{5,6})', format='\\1 \\2 \\3', leading_digits_pattern=['6'])])
