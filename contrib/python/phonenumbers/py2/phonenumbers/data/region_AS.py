"""Auto-generated file, do not edit by hand. AS metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_AS = PhoneMetadata(id='AS', country_code=1, international_prefix='011',
    general_desc=PhoneNumberDesc(national_number_pattern='(?:[58]\\d\\d|684|900)\\d{7}', possible_length=(10,), possible_length_local_only=(7,)),
    fixed_line=PhoneNumberDesc(national_number_pattern='6846(?:22|33|44|55|77|88|9[19])\\d{4}', example_number='6846221234', possible_length=(10,), possible_length_local_only=(7,)),
    mobile=PhoneNumberDesc(national_number_pattern='684(?:2(?:48|5[2468]|7[26])|7(?:3[13]|70|82))\\d{4}', example_number='6847331234', possible_length=(10,), possible_length_local_only=(7,)),
    toll_free=PhoneNumberDesc(national_number_pattern='8(?:00|33|44|55|66|77|88)[2-9]\\d{6}', example_number='8002123456', possible_length=(10,)),
    premium_rate=PhoneNumberDesc(national_number_pattern='900[2-9]\\d{6}', example_number='9002123456', possible_length=(10,)),
    personal_number=PhoneNumberDesc(national_number_pattern='52(?:3(?:[2-46-9][02-9]\\d|5(?:[02-46-9]\\d|5[0-46-9]))|4(?:[2-478][02-9]\\d|5(?:[034]\\d|2[024-9]|5[0-46-9])|6(?:0[1-9]|[2-9]\\d)|9(?:[05-9]\\d|2[0-5]|49)))\\d{4}|52[34][2-9]1[02-9]\\d{4}|5(?:00|2[125-9]|33|44|66|77|88)[2-9]\\d{6}', example_number='5002345678', possible_length=(10,)),
    national_prefix='1',
    national_prefix_for_parsing='([267]\\d{6})$|1',
    national_prefix_transform_rule='684\\1',
    leading_digits='684')
