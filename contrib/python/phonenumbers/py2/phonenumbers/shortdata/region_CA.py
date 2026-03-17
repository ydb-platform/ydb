"""Auto-generated file, do not edit by hand. CA metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_CA = PhoneMetadata(id='CA', country_code=None, international_prefix=None,
    general_desc=PhoneNumberDesc(national_number_pattern='[1-9]\\d\\d(?:\\d{2,3})?', possible_length=(3, 5, 6)),
    toll_free=PhoneNumberDesc(national_number_pattern='112|988|[29]11', example_number='112', possible_length=(3,)),
    emergency=PhoneNumberDesc(national_number_pattern='112|911', example_number='112', possible_length=(3,)),
    short_code=PhoneNumberDesc(national_number_pattern='[1-35-9]\\d{4,5}|112|[2-8]11|9(?:11|88)', example_number='112', possible_length=(3, 5, 6)),
    carrier_specific=PhoneNumberDesc(national_number_pattern='[235-7]11', example_number='211', possible_length=(3,)),
    sms_services=PhoneNumberDesc(national_number_pattern='[1-35-9]\\d{4,5}', example_number='10000', possible_length=(5, 6)),
    short_data=True)
