"""Auto-generated file, do not edit by hand. PM metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_PM = PhoneMetadata(id='PM', country_code=None, international_prefix=None,
    general_desc=PhoneNumberDesc(national_number_pattern='[13]\\d(?:\\d\\d(?:\\d{2})?)?', possible_length=(2, 4, 6)),
    toll_free=PhoneNumberDesc(national_number_pattern='1[578]|3(?:0\\d|1[689])\\d', example_number='15', possible_length=(2, 4)),
    premium_rate=PhoneNumberDesc(national_number_pattern='3[2469]\\d\\d', example_number='3200', possible_length=(4,)),
    emergency=PhoneNumberDesc(national_number_pattern='1[578]', example_number='15', possible_length=(2,)),
    short_code=PhoneNumberDesc(national_number_pattern='1[578]|31(?:03|[689]\\d)|(?:118[02-9]|3[02469])\\d\\d', example_number='15', possible_length=(2, 4, 6)),
    standard_rate=PhoneNumberDesc(national_number_pattern='118\\d{3}', example_number='118000', possible_length=(6,)),
    carrier_specific=PhoneNumberDesc(national_number_pattern='310\\d', example_number='3100', possible_length=(4,)),
    short_data=True)
