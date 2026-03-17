"""Auto-generated file, do not edit by hand. CM metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_CM = PhoneMetadata(id='CM', country_code=237, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='[26]\\d{8}|88\\d{6,7}', possible_length=(8, 9)),
    fixed_line=PhoneNumberDesc(national_number_pattern='2(?:22|33)\\d{6}', example_number='222123456', possible_length=(9,)),
    mobile=PhoneNumberDesc(national_number_pattern='(?:24[23]|6(?:[25-9]\\d|40))\\d{6}', example_number='671234567', possible_length=(9,)),
    toll_free=PhoneNumberDesc(national_number_pattern='88\\d{6,7}', example_number='88012345', possible_length=(8, 9)),
    number_format=[NumberFormat(pattern='(\\d{2})(\\d{2})(\\d{2})(\\d{2})', format='\\1 \\2 \\3 \\4', leading_digits_pattern=['88']),
        NumberFormat(pattern='(\\d)(\\d{2})(\\d{2})(\\d{2})(\\d{2})', format='\\1 \\2 \\3 \\4 \\5', leading_digits_pattern=['[26]|88'])],
    mobile_number_portable_region=True)
