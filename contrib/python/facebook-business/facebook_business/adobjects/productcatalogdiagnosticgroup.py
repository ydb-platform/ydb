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

class ProductCatalogDiagnosticGroup(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ProductCatalogDiagnosticGroup, self).__init__()
        self._isProductCatalogDiagnosticGroup = True
        self._api = api

    class Field(AbstractObject.Field):
        affected_channels = 'affected_channels'
        affected_entity = 'affected_entity'
        affected_features = 'affected_features'
        diagnostics = 'diagnostics'
        error_code = 'error_code'
        number_of_affected_entities = 'number_of_affected_entities'
        number_of_affected_items = 'number_of_affected_items'
        severity = 'severity'
        subtitle = 'subtitle'
        title = 'title'
        type = 'type'

    class AffectedEntity:
        product_catalog = 'product_catalog'
        product_event = 'product_event'
        product_item = 'product_item'
        product_set = 'product_set'

    class AffectedFeatures:
        augmented_reality = 'augmented_reality'
        checkout = 'checkout'

    class Severity:
        must_fix = 'MUST_FIX'
        opportunity = 'OPPORTUNITY'

    class Type:
        ar_visibility_issues = 'AR_VISIBILITY_ISSUES'
        attributes_invalid = 'ATTRIBUTES_INVALID'
        attributes_missing = 'ATTRIBUTES_MISSING'
        category = 'CATEGORY'
        checkout = 'CHECKOUT'
        da_visibility_issues = 'DA_VISIBILITY_ISSUES'
        event_source_issues = 'EVENT_SOURCE_ISSUES'
        image_quality = 'IMAGE_QUALITY'
        low_quality_title_and_description = 'LOW_QUALITY_TITLE_AND_DESCRIPTION'
        policy_violation = 'POLICY_VIOLATION'
        shops_visibility_issues = 'SHOPS_VISIBILITY_ISSUES'

    class AffectedChannels:
        b2c_marketplace = 'b2c_marketplace'
        c2c_marketplace = 'c2c_marketplace'
        da = 'da'
        daily_deals = 'daily_deals'
        daily_deals_legacy = 'daily_deals_legacy'
        ig_product_tagging = 'ig_product_tagging'
        marketplace = 'marketplace'
        marketplace_ads_deprecated = 'marketplace_ads_deprecated'
        marketplace_shops = 'marketplace_shops'
        mini_shops = 'mini_shops'
        offline_conversions = 'offline_conversions'
        shops = 'shops'
        universal_checkout = 'universal_checkout'
        whatsapp = 'whatsapp'

    class AffectedEntities:
        product_catalog = 'product_catalog'
        product_event = 'product_event'
        product_item = 'product_item'
        product_set = 'product_set'

    class Severities:
        must_fix = 'MUST_FIX'
        opportunity = 'OPPORTUNITY'

    class Types:
        ar_visibility_issues = 'AR_VISIBILITY_ISSUES'
        attributes_invalid = 'ATTRIBUTES_INVALID'
        attributes_missing = 'ATTRIBUTES_MISSING'
        category = 'CATEGORY'
        checkout = 'CHECKOUT'
        da_visibility_issues = 'DA_VISIBILITY_ISSUES'
        event_source_issues = 'EVENT_SOURCE_ISSUES'
        image_quality = 'IMAGE_QUALITY'
        low_quality_title_and_description = 'LOW_QUALITY_TITLE_AND_DESCRIPTION'
        policy_violation = 'POLICY_VIOLATION'
        shops_visibility_issues = 'SHOPS_VISIBILITY_ISSUES'

    _field_types = {
        'affected_channels': 'list<string>',
        'affected_entity': 'AffectedEntity',
        'affected_features': 'list<AffectedFeatures>',
        'diagnostics': 'list<Object>',
        'error_code': 'int',
        'number_of_affected_entities': 'int',
        'number_of_affected_items': 'int',
        'severity': 'Severity',
        'subtitle': 'string',
        'title': 'string',
        'type': 'Type',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['AffectedEntity'] = ProductCatalogDiagnosticGroup.AffectedEntity.__dict__.values()
        field_enum_info['AffectedFeatures'] = ProductCatalogDiagnosticGroup.AffectedFeatures.__dict__.values()
        field_enum_info['Severity'] = ProductCatalogDiagnosticGroup.Severity.__dict__.values()
        field_enum_info['Type'] = ProductCatalogDiagnosticGroup.Type.__dict__.values()
        field_enum_info['AffectedChannels'] = ProductCatalogDiagnosticGroup.AffectedChannels.__dict__.values()
        field_enum_info['AffectedEntities'] = ProductCatalogDiagnosticGroup.AffectedEntities.__dict__.values()
        field_enum_info['Severities'] = ProductCatalogDiagnosticGroup.Severities.__dict__.values()
        field_enum_info['Types'] = ProductCatalogDiagnosticGroup.Types.__dict__.values()
        return field_enum_info


