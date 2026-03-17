"""Auto-generated file, do not edit by hand. GR metadata"""
from ..phonemetadata import NumberFormat, PhoneNumberDesc, PhoneMetadata

PHONE_METADATA_GR = PhoneMetadata(id='GR', country_code=30, international_prefix='00',
    general_desc=PhoneNumberDesc(national_number_pattern='5005000\\d{3}|8\\d{9,11}|(?:[269]\\d|70)\\d{8}', possible_length=(10, 11, 12)),
    fixed_line=PhoneNumberDesc(national_number_pattern='2(?:1\\d\\d|2(?:2[1-46-9]|[36][1-8]|4[1-7]|5[1-4]|7[1-5]|[89][1-9])|3(?:1\\d|2[1-57]|[35][1-3]|4[13]|7[1-7]|8[124-6]|9[1-79])|4(?:1\\d|2[1-8]|3[1-4]|4[13-5]|6[1-578]|9[1-5])|5(?:1\\d|[29][1-4]|3[1-5]|4[124]|5[1-6])|6(?:1\\d|[269][1-6]|3[1245]|4[1-7]|5[13-9]|7[14]|8[1-5])|7(?:1\\d|2[1-5]|3[1-6]|4[1-7]|5[1-57]|6[135]|9[125-7])|8(?:1\\d|2[1-5]|[34][1-4]|9[1-57]))\\d{6}', example_number='2123456789', possible_length=(10,)),
    mobile=PhoneNumberDesc(national_number_pattern='68[57-9]\\d{7}|(?:69|94)\\d{8}', example_number='6912345678', possible_length=(10,)),
    toll_free=PhoneNumberDesc(national_number_pattern='800\\d{7,9}', example_number='8001234567', possible_length=(10, 11, 12)),
    premium_rate=PhoneNumberDesc(national_number_pattern='90[19]\\d{7}', example_number='9091234567', possible_length=(10,)),
    shared_cost=PhoneNumberDesc(national_number_pattern='8(?:0[16]|12|[27]5|50)\\d{7}', example_number='8011234567', possible_length=(10,)),
    personal_number=PhoneNumberDesc(national_number_pattern='70\\d{8}', example_number='7012345678', possible_length=(10,)),
    uan=PhoneNumberDesc(national_number_pattern='5005000\\d{3}', example_number='5005000123', possible_length=(10,)),
    number_format=[NumberFormat(pattern='(\\d{2})(\\d{4})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['21|7']),
        NumberFormat(pattern='(\\d{4})(\\d{6})', format='\\1 \\2', leading_digits_pattern=['2(?:2|3[2-57-9]|4[2-469]|5[2-59]|6[2-9]|7[2-69]|8[2-49])|5']),
        NumberFormat(pattern='(\\d{3})(\\d{3})(\\d{4})', format='\\1 \\2 \\3', leading_digits_pattern=['[2689]']),
        NumberFormat(pattern='(\\d{3})(\\d{3,4})(\\d{5})', format='\\1 \\2 \\3', leading_digits_pattern=['8'])],
    mobile_number_portable_region=True)
