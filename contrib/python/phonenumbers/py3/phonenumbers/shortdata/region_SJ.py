"""Auto-generated file, do not edit by hand. SJ metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_SJ = PhoneMetadata(id='SJ', country_code=None, international_prefix=None,
    general_desc=PhoneNumberDesc(national_number_pattern='[01]\\d\\d(?:\\d{2})?', possible_length=(3, 5)),
    toll_free=PhoneNumberDesc(national_number_pattern='11[023]', example_number='110', possible_length=(3,)),
    emergency=PhoneNumberDesc(national_number_pattern='11[023]', example_number='110', possible_length=(3,)),
    short_code=PhoneNumberDesc(national_number_pattern='04\\d{3}|11[023]', example_number='110', possible_length=(3, 5)),
    sms_services=PhoneNumberDesc(national_number_pattern='04\\d{3}', example_number='04000', possible_length=(5,)),
    short_data=True)
