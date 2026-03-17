# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class Targeting(
    AbstractObject,
):

    def __init__(self, api=None):
        super(Targeting, self).__init__()
        self._isTargeting = True
        self._api = api

    class Field(AbstractObject.Field):
        adgroup_id = 'adgroup_id'
        age_max = 'age_max'
        age_min = 'age_min'
        age_range = 'age_range'
        alternate_auto_targeting_option = 'alternate_auto_targeting_option'
        app_install_state = 'app_install_state'
        audience_network_positions = 'audience_network_positions'
        behaviors = 'behaviors'
        brand_safety_content_filter_levels = 'brand_safety_content_filter_levels'
        catalog_based_targeting = 'catalog_based_targeting'
        cities = 'cities'
        college_years = 'college_years'
        connections = 'connections'
        contextual_targeting_categories = 'contextual_targeting_categories'
        countries = 'countries'
        country = 'country'
        country_groups = 'country_groups'
        custom_audiences = 'custom_audiences'
        device_platforms = 'device_platforms'
        direct_install_devices = 'direct_install_devices'
        dynamic_audience_ids = 'dynamic_audience_ids'
        education_majors = 'education_majors'
        education_schools = 'education_schools'
        education_statuses = 'education_statuses'
        effective_audience_network_positions = 'effective_audience_network_positions'
        effective_device_platforms = 'effective_device_platforms'
        effective_facebook_positions = 'effective_facebook_positions'
        effective_instagram_positions = 'effective_instagram_positions'
        effective_messenger_positions = 'effective_messenger_positions'
        effective_publisher_platforms = 'effective_publisher_platforms'
        effective_threads_positions = 'effective_threads_positions'
        engagement_specs = 'engagement_specs'
        ethnic_affinity = 'ethnic_affinity'
        exclude_reached_since = 'exclude_reached_since'
        excluded_brand_safety_content_types = 'excluded_brand_safety_content_types'
        excluded_connections = 'excluded_connections'
        excluded_custom_audiences = 'excluded_custom_audiences'
        excluded_dynamic_audience_ids = 'excluded_dynamic_audience_ids'
        excluded_engagement_specs = 'excluded_engagement_specs'
        excluded_geo_locations = 'excluded_geo_locations'
        excluded_mobile_device_model = 'excluded_mobile_device_model'
        excluded_product_audience_specs = 'excluded_product_audience_specs'
        excluded_publisher_categories = 'excluded_publisher_categories'
        excluded_publisher_list_ids = 'excluded_publisher_list_ids'
        excluded_user_device = 'excluded_user_device'
        exclusions = 'exclusions'
        facebook_positions = 'facebook_positions'
        family_statuses = 'family_statuses'
        fb_deal_id = 'fb_deal_id'
        flexible_spec = 'flexible_spec'
        friends_of_connections = 'friends_of_connections'
        genders = 'genders'
        generation = 'generation'
        geo_locations = 'geo_locations'
        home_ownership = 'home_ownership'
        home_type = 'home_type'
        home_value = 'home_value'
        household_composition = 'household_composition'
        income = 'income'
        industries = 'industries'
        instagram_positions = 'instagram_positions'
        instream_video_skippable_excluded = 'instream_video_skippable_excluded'
        interested_in = 'interested_in'
        interests = 'interests'
        is_whatsapp_destination_ad = 'is_whatsapp_destination_ad'
        keywords = 'keywords'
        life_events = 'life_events'
        locales = 'locales'
        messenger_positions = 'messenger_positions'
        moms = 'moms'
        net_worth = 'net_worth'
        office_type = 'office_type'
        place_page_set_ids = 'place_page_set_ids'
        political_views = 'political_views'
        politics = 'politics'
        product_audience_specs = 'product_audience_specs'
        prospecting_audience = 'prospecting_audience'
        publisher_platforms = 'publisher_platforms'
        radius = 'radius'
        regions = 'regions'
        relationship_statuses = 'relationship_statuses'
        site_category = 'site_category'
        targeting_automation = 'targeting_automation'
        targeting_optimization = 'targeting_optimization'
        targeting_relaxation_types = 'targeting_relaxation_types'
        threads_positions = 'threads_positions'
        user_adclusters = 'user_adclusters'
        user_device = 'user_device'
        user_event = 'user_event'
        user_os = 'user_os'
        wireless_carrier = 'wireless_carrier'
        work_employers = 'work_employers'
        work_positions = 'work_positions'
        zips = 'zips'

    class DevicePlatforms:
        desktop = 'desktop'
        mobile = 'mobile'

    class EffectiveDevicePlatforms:
        desktop = 'desktop'
        mobile = 'mobile'

    _field_types = {
        'adgroup_id': 'string',
        'age_max': 'unsigned int',
        'age_min': 'unsigned int',
        'age_range': 'list<unsigned int>',
        'alternate_auto_targeting_option': 'string',
        'app_install_state': 'string',
        'audience_network_positions': 'list<string>',
        'behaviors': 'list<IDName>',
        'brand_safety_content_filter_levels': 'list<string>',
        'catalog_based_targeting': 'CatalogBasedTargeting',
        'cities': 'list<IDName>',
        'college_years': 'list<unsigned int>',
        'connections': 'list<ConnectionsTargeting>',
        'contextual_targeting_categories': 'list<IDName>',
        'countries': 'list<string>',
        'country': 'list<string>',
        'country_groups': 'list<string>',
        'custom_audiences': 'list<RawCustomAudience>',
        'device_platforms': 'list<DevicePlatforms>',
        'direct_install_devices': 'bool',
        'dynamic_audience_ids': 'list<string>',
        'education_majors': 'list<IDName>',
        'education_schools': 'list<IDName>',
        'education_statuses': 'list<unsigned int>',
        'effective_audience_network_positions': 'list<string>',
        'effective_device_platforms': 'list<EffectiveDevicePlatforms>',
        'effective_facebook_positions': 'list<string>',
        'effective_instagram_positions': 'list<string>',
        'effective_messenger_positions': 'list<string>',
        'effective_publisher_platforms': 'list<string>',
        'effective_threads_positions': 'list<string>',
        'engagement_specs': 'list<TargetingDynamicRule>',
        'ethnic_affinity': 'list<IDName>',
        'exclude_reached_since': 'list<string>',
        'excluded_brand_safety_content_types': 'list<string>',
        'excluded_connections': 'list<ConnectionsTargeting>',
        'excluded_custom_audiences': 'list<RawCustomAudience>',
        'excluded_dynamic_audience_ids': 'list<string>',
        'excluded_engagement_specs': 'list<TargetingDynamicRule>',
        'excluded_geo_locations': 'TargetingGeoLocation',
        'excluded_mobile_device_model': 'list<string>',
        'excluded_product_audience_specs': 'list<TargetingProductAudienceSpec>',
        'excluded_publisher_categories': 'list<string>',
        'excluded_publisher_list_ids': 'list<string>',
        'excluded_user_device': 'list<string>',
        'exclusions': 'FlexibleTargeting',
        'facebook_positions': 'list<string>',
        'family_statuses': 'list<IDName>',
        'fb_deal_id': 'string',
        'flexible_spec': 'list<FlexibleTargeting>',
        'friends_of_connections': 'list<ConnectionsTargeting>',
        'genders': 'list<unsigned int>',
        'generation': 'list<IDName>',
        'geo_locations': 'TargetingGeoLocation',
        'home_ownership': 'list<IDName>',
        'home_type': 'list<IDName>',
        'home_value': 'list<IDName>',
        'household_composition': 'list<IDName>',
        'income': 'list<IDName>',
        'industries': 'list<IDName>',
        'instagram_positions': 'list<string>',
        'instream_video_skippable_excluded': 'bool',
        'interested_in': 'list<unsigned int>',
        'interests': 'list<IDName>',
        'is_whatsapp_destination_ad': 'bool',
        'keywords': 'list<string>',
        'life_events': 'list<IDName>',
        'locales': 'list<unsigned int>',
        'messenger_positions': 'list<string>',
        'moms': 'list<IDName>',
        'net_worth': 'list<IDName>',
        'office_type': 'list<IDName>',
        'place_page_set_ids': 'list<string>',
        'political_views': 'list<unsigned int>',
        'politics': 'list<IDName>',
        'product_audience_specs': 'list<TargetingProductAudienceSpec>',
        'prospecting_audience': 'TargetingProspectingAudience',
        'publisher_platforms': 'list<string>',
        'radius': 'string',
        'regions': 'list<IDName>',
        'relationship_statuses': 'list<unsigned int>',
        'site_category': 'list<string>',
        'targeting_automation': 'TargetingAutomation',
        'targeting_optimization': 'string',
        'targeting_relaxation_types': 'TargetingRelaxation',
        'threads_positions': 'list<string>',
        'user_adclusters': 'list<IDName>',
        'user_device': 'list<string>',
        'user_event': 'list<unsigned int>',
        'user_os': 'list<string>',
        'wireless_carrier': 'list<string>',
        'work_employers': 'list<IDName>',
        'work_positions': 'list<IDName>',
        'zips': 'list<string>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['DevicePlatforms'] = Targeting.DevicePlatforms.__dict__.values()
        field_enum_info['EffectiveDevicePlatforms'] = Targeting.EffectiveDevicePlatforms.__dict__.values()
        return field_enum_info


