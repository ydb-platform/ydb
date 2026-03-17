"""Auto-generated file, do not edit by hand. MQ metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_MQ = PhoneMetadata(id='MQ', country_code=None, international_prefix=None,
    general_desc=PhoneNumberDesc(national_number_pattern='[13]\\d(?:\\d(?:\\d(?:\\d{2})?)?)?', possible_length=(2, 3, 4, 6)),
    toll_free=PhoneNumberDesc(national_number_pattern='1(?:12|[578])|3[01]\\d\\d', example_number='15', possible_length=(2, 3, 4)),
    premium_rate=PhoneNumberDesc(national_number_pattern='3[2469]\\d\\d', example_number='3200', possible_length=(4,)),
    emergency=PhoneNumberDesc(national_number_pattern='1(?:12|[578])', example_number='15', possible_length=(2, 3)),
    short_code=PhoneNumberDesc(national_number_pattern='1(?:12|[578])|(?:118[02-9]|3[0-2469])\\d\\d', example_number='15', possible_length=(2, 3, 4, 6)),
    standard_rate=PhoneNumberDesc(national_number_pattern='118\\d{3}', example_number='118000', possible_length=(6,)),
    short_data=True)
