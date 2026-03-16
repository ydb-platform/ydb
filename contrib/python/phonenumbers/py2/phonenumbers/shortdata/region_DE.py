"""Auto-generated file, do not edit by hand. DE metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_DE = PhoneMetadata(id='DE', country_code=None, international_prefix=None,
    general_desc=PhoneNumberDesc(national_number_pattern='[13]\\d{2,5}', possible_length=(3, 4, 5, 6)),
    toll_free=PhoneNumberDesc(national_number_pattern='11(?:[02]|6\\d{3})', example_number='110', possible_length=(3, 6)),
    emergency=PhoneNumberDesc(national_number_pattern='11[02]', example_number='110', possible_length=(3,)),
    short_code=PhoneNumberDesc(national_number_pattern='11(?:[025]|6(?:00[06]|1(?:1[167]|23))|800\\d)|3311|118\\d\\d', example_number='110', possible_length=(3, 4, 5, 6)),
    carrier_specific=PhoneNumberDesc(national_number_pattern='331\\d', example_number='3310', possible_length=(4,)),
    short_data=True)
