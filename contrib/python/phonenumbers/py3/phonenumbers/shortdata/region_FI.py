"""Auto-generated file, do not edit by hand. FI metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_FI = PhoneMetadata(id='FI', country_code=None, international_prefix=None,
    general_desc=PhoneNumberDesc(national_number_pattern='[17]\\d\\d(?:\\d{2,3})?', possible_length=(3, 5, 6)),
    toll_free=PhoneNumberDesc(national_number_pattern='11(?:2|6\\d{3})', example_number='112', possible_length=(3, 6)),
    emergency=PhoneNumberDesc(national_number_pattern='112', example_number='112', possible_length=(3,)),
    short_code=PhoneNumberDesc(national_number_pattern='11(?:2|6(?:00[06]|1(?:1[17]|23)))|(?:1[2-8]\\d|75[12])\\d\\d', example_number='112', possible_length=(3, 5, 6)),
    standard_rate=PhoneNumberDesc(national_number_pattern='1[2-8]\\d{3}', example_number='12000', possible_length=(5,)),
    short_data=True)
