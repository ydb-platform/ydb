# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker
from facebook_business.adobjects.helpers.businessmixin import BusinessMixin

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class Business(
    BusinessMixin,
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isBusiness = True
        super(Business, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        block_offline_analytics = 'block_offline_analytics'
        collaborative_ads_managed_partner_business_info = 'collaborative_ads_managed_partner_business_info'
        collaborative_ads_managed_partner_eligibility = 'collaborative_ads_managed_partner_eligibility'
        collaborative_ads_partner_premium_options = 'collaborative_ads_partner_premium_options'
        created_by = 'created_by'
        created_time = 'created_time'
        extended_updated_time = 'extended_updated_time'
        id = 'id'
        is_hidden = 'is_hidden'
        link = 'link'
        marketing_messages_onboarding_status = 'marketing_messages_onboarding_status'
        name = 'name'
        payment_account_id = 'payment_account_id'
        primary_page = 'primary_page'
        profile_picture_uri = 'profile_picture_uri'
        timezone_id = 'timezone_id'
        two_factor_type = 'two_factor_type'
        updated_by = 'updated_by'
        updated_time = 'updated_time'
        user_access_expire_time = 'user_access_expire_time'
        verification_status = 'verification_status'
        vertical = 'vertical'
        vertical_id = 'vertical_id'
        whatsapp_business_manager_messaging_limit = 'whatsapp_business_manager_messaging_limit'

    class VerificationStatus:
        expired = 'expired'
        failed = 'failed'
        ineligible = 'ineligible'
        not_verified = 'not_verified'
        pending = 'pending'
        pending_need_more_info = 'pending_need_more_info'
        pending_submission = 'pending_submission'
        rejected = 'rejected'
        revoked = 'revoked'
        verified = 'verified'

    class WhatsappBusinessManagerMessagingLimit:
        tier_100k = 'TIER_100K'
        tier_10k = 'TIER_10K'
        tier_250 = 'TIER_250'
        tier_2k = 'TIER_2K'
        tier_unlimited = 'TIER_UNLIMITED'
        untiered = 'UNTIERED'

    class TwoFactorType:
        admin_required = 'admin_required'
        all_required = 'all_required'
        none = 'none'

    class Vertical:
        advertising = 'ADVERTISING'
        automotive = 'AUTOMOTIVE'
        consumer_packaged_goods = 'CONSUMER_PACKAGED_GOODS'
        ecommerce = 'ECOMMERCE'
        education = 'EDUCATION'
        energy_and_utilities = 'ENERGY_AND_UTILITIES'
        entertainment_and_media = 'ENTERTAINMENT_AND_MEDIA'
        financial_services = 'FINANCIAL_SERVICES'
        gaming = 'GAMING'
        government_and_politics = 'GOVERNMENT_AND_POLITICS'
        health = 'HEALTH'
        luxury = 'LUXURY'
        marketing = 'MARKETING'
        non_profit = 'NON_PROFIT'
        not_set = 'NOT_SET'
        organizations_and_associations = 'ORGANIZATIONS_AND_ASSOCIATIONS'
        other = 'OTHER'
        professional_services = 'PROFESSIONAL_SERVICES'
        restaurant = 'RESTAURANT'
        retail = 'RETAIL'
        technology = 'TECHNOLOGY'
        telecom = 'TELECOM'
        travel = 'TRAVEL'

    class PermittedTasks:
        advertise = 'ADVERTISE'
        analyze = 'ANALYZE'
        cashier_role = 'CASHIER_ROLE'
        create_content = 'CREATE_CONTENT'
        global_structure_management = 'GLOBAL_STRUCTURE_MANAGEMENT'
        manage = 'MANAGE'
        manage_jobs = 'MANAGE_JOBS'
        manage_leads = 'MANAGE_LEADS'
        messaging = 'MESSAGING'
        moderate = 'MODERATE'
        moderate_community = 'MODERATE_COMMUNITY'
        pages_messaging = 'PAGES_MESSAGING'
        pages_messaging_subscriptions = 'PAGES_MESSAGING_SUBSCRIPTIONS'
        profile_plus_advertise = 'PROFILE_PLUS_ADVERTISE'
        profile_plus_analyze = 'PROFILE_PLUS_ANALYZE'
        profile_plus_create_content = 'PROFILE_PLUS_CREATE_CONTENT'
        profile_plus_facebook_access = 'PROFILE_PLUS_FACEBOOK_ACCESS'
        profile_plus_full_control = 'PROFILE_PLUS_FULL_CONTROL'
        profile_plus_global_structure_management = 'PROFILE_PLUS_GLOBAL_STRUCTURE_MANAGEMENT'
        profile_plus_manage = 'PROFILE_PLUS_MANAGE'
        profile_plus_manage_leads = 'PROFILE_PLUS_MANAGE_LEADS'
        profile_plus_messaging = 'PROFILE_PLUS_MESSAGING'
        profile_plus_moderate = 'PROFILE_PLUS_MODERATE'
        profile_plus_moderate_delegate_community = 'PROFILE_PLUS_MODERATE_DELEGATE_COMMUNITY'
        profile_plus_revenue = 'PROFILE_PLUS_REVENUE'
        read_page_mailboxes = 'READ_PAGE_MAILBOXES'
        view_monetization_insights = 'VIEW_MONETIZATION_INSIGHTS'

    class SurveyBusinessType:
        advertiser = 'ADVERTISER'
        agency = 'AGENCY'
        app_developer = 'APP_DEVELOPER'
        publisher = 'PUBLISHER'

    class TimezoneId:
        value_0 = '0'
        value_1 = '1'
        value_2 = '2'
        value_3 = '3'
        value_4 = '4'
        value_5 = '5'
        value_6 = '6'
        value_7 = '7'
        value_8 = '8'
        value_9 = '9'
        value_10 = '10'
        value_11 = '11'
        value_12 = '12'
        value_13 = '13'
        value_14 = '14'
        value_15 = '15'
        value_16 = '16'
        value_17 = '17'
        value_18 = '18'
        value_19 = '19'
        value_20 = '20'
        value_21 = '21'
        value_22 = '22'
        value_23 = '23'
        value_24 = '24'
        value_25 = '25'
        value_26 = '26'
        value_27 = '27'
        value_28 = '28'
        value_29 = '29'
        value_30 = '30'
        value_31 = '31'
        value_32 = '32'
        value_33 = '33'
        value_34 = '34'
        value_35 = '35'
        value_36 = '36'
        value_37 = '37'
        value_38 = '38'
        value_39 = '39'
        value_40 = '40'
        value_41 = '41'
        value_42 = '42'
        value_43 = '43'
        value_44 = '44'
        value_45 = '45'
        value_46 = '46'
        value_47 = '47'
        value_48 = '48'
        value_49 = '49'
        value_50 = '50'
        value_51 = '51'
        value_52 = '52'
        value_53 = '53'
        value_54 = '54'
        value_55 = '55'
        value_56 = '56'
        value_57 = '57'
        value_58 = '58'
        value_59 = '59'
        value_60 = '60'
        value_61 = '61'
        value_62 = '62'
        value_63 = '63'
        value_64 = '64'
        value_65 = '65'
        value_66 = '66'
        value_67 = '67'
        value_68 = '68'
        value_69 = '69'
        value_70 = '70'
        value_71 = '71'
        value_72 = '72'
        value_73 = '73'
        value_74 = '74'
        value_75 = '75'
        value_76 = '76'
        value_77 = '77'
        value_78 = '78'
        value_79 = '79'
        value_80 = '80'
        value_81 = '81'
        value_82 = '82'
        value_83 = '83'
        value_84 = '84'
        value_85 = '85'
        value_86 = '86'
        value_87 = '87'
        value_88 = '88'
        value_89 = '89'
        value_90 = '90'
        value_91 = '91'
        value_92 = '92'
        value_93 = '93'
        value_94 = '94'
        value_95 = '95'
        value_96 = '96'
        value_97 = '97'
        value_98 = '98'
        value_99 = '99'
        value_100 = '100'
        value_101 = '101'
        value_102 = '102'
        value_103 = '103'
        value_104 = '104'
        value_105 = '105'
        value_106 = '106'
        value_107 = '107'
        value_108 = '108'
        value_109 = '109'
        value_110 = '110'
        value_111 = '111'
        value_112 = '112'
        value_113 = '113'
        value_114 = '114'
        value_115 = '115'
        value_116 = '116'
        value_117 = '117'
        value_118 = '118'
        value_119 = '119'
        value_120 = '120'
        value_121 = '121'
        value_122 = '122'
        value_123 = '123'
        value_124 = '124'
        value_125 = '125'
        value_126 = '126'
        value_127 = '127'
        value_128 = '128'
        value_129 = '129'
        value_130 = '130'
        value_131 = '131'
        value_132 = '132'
        value_133 = '133'
        value_134 = '134'
        value_135 = '135'
        value_136 = '136'
        value_137 = '137'
        value_138 = '138'
        value_139 = '139'
        value_140 = '140'
        value_141 = '141'
        value_142 = '142'
        value_143 = '143'
        value_144 = '144'
        value_145 = '145'
        value_146 = '146'
        value_147 = '147'
        value_148 = '148'
        value_149 = '149'
        value_150 = '150'
        value_151 = '151'
        value_152 = '152'
        value_153 = '153'
        value_154 = '154'
        value_155 = '155'
        value_156 = '156'
        value_157 = '157'
        value_158 = '158'
        value_159 = '159'
        value_160 = '160'
        value_161 = '161'
        value_162 = '162'
        value_163 = '163'
        value_164 = '164'
        value_165 = '165'
        value_166 = '166'
        value_167 = '167'
        value_168 = '168'
        value_169 = '169'
        value_170 = '170'
        value_171 = '171'
        value_172 = '172'
        value_173 = '173'
        value_174 = '174'
        value_175 = '175'
        value_176 = '176'
        value_177 = '177'
        value_178 = '178'
        value_179 = '179'
        value_180 = '180'
        value_181 = '181'
        value_182 = '182'
        value_183 = '183'
        value_184 = '184'
        value_185 = '185'
        value_186 = '186'
        value_187 = '187'
        value_188 = '188'
        value_189 = '189'
        value_190 = '190'
        value_191 = '191'
        value_192 = '192'
        value_193 = '193'
        value_194 = '194'
        value_195 = '195'
        value_196 = '196'
        value_197 = '197'
        value_198 = '198'
        value_199 = '199'
        value_200 = '200'
        value_201 = '201'
        value_202 = '202'
        value_203 = '203'
        value_204 = '204'
        value_205 = '205'
        value_206 = '206'
        value_207 = '207'
        value_208 = '208'
        value_209 = '209'
        value_210 = '210'
        value_211 = '211'
        value_212 = '212'
        value_213 = '213'
        value_214 = '214'
        value_215 = '215'
        value_216 = '216'
        value_217 = '217'
        value_218 = '218'
        value_219 = '219'
        value_220 = '220'
        value_221 = '221'
        value_222 = '222'
        value_223 = '223'
        value_224 = '224'
        value_225 = '225'
        value_226 = '226'
        value_227 = '227'
        value_228 = '228'
        value_229 = '229'
        value_230 = '230'
        value_231 = '231'
        value_232 = '232'
        value_233 = '233'
        value_234 = '234'
        value_235 = '235'
        value_236 = '236'
        value_237 = '237'
        value_238 = '238'
        value_239 = '239'
        value_240 = '240'
        value_241 = '241'
        value_242 = '242'
        value_243 = '243'
        value_244 = '244'
        value_245 = '245'
        value_246 = '246'
        value_247 = '247'
        value_248 = '248'
        value_249 = '249'
        value_250 = '250'
        value_251 = '251'
        value_252 = '252'
        value_253 = '253'
        value_254 = '254'
        value_255 = '255'
        value_256 = '256'
        value_257 = '257'
        value_258 = '258'
        value_259 = '259'
        value_260 = '260'
        value_261 = '261'
        value_262 = '262'
        value_263 = '263'
        value_264 = '264'
        value_265 = '265'
        value_266 = '266'
        value_267 = '267'
        value_268 = '268'
        value_269 = '269'
        value_270 = '270'
        value_271 = '271'
        value_272 = '272'
        value_273 = '273'
        value_274 = '274'
        value_275 = '275'
        value_276 = '276'
        value_277 = '277'
        value_278 = '278'
        value_279 = '279'
        value_280 = '280'
        value_281 = '281'
        value_282 = '282'
        value_283 = '283'
        value_284 = '284'
        value_285 = '285'
        value_286 = '286'
        value_287 = '287'
        value_288 = '288'
        value_289 = '289'
        value_290 = '290'
        value_291 = '291'
        value_292 = '292'
        value_293 = '293'
        value_294 = '294'
        value_295 = '295'
        value_296 = '296'
        value_297 = '297'
        value_298 = '298'
        value_299 = '299'
        value_300 = '300'
        value_301 = '301'
        value_302 = '302'
        value_303 = '303'
        value_304 = '304'
        value_305 = '305'
        value_306 = '306'
        value_307 = '307'
        value_308 = '308'
        value_309 = '309'
        value_310 = '310'
        value_311 = '311'
        value_312 = '312'
        value_313 = '313'
        value_314 = '314'
        value_315 = '315'
        value_316 = '316'
        value_317 = '317'
        value_318 = '318'
        value_319 = '319'
        value_320 = '320'
        value_321 = '321'
        value_322 = '322'
        value_323 = '323'
        value_324 = '324'
        value_325 = '325'
        value_326 = '326'
        value_327 = '327'
        value_328 = '328'
        value_329 = '329'
        value_330 = '330'
        value_331 = '331'
        value_332 = '332'
        value_333 = '333'
        value_334 = '334'
        value_335 = '335'
        value_336 = '336'
        value_337 = '337'
        value_338 = '338'
        value_339 = '339'
        value_340 = '340'
        value_341 = '341'
        value_342 = '342'
        value_343 = '343'
        value_344 = '344'
        value_345 = '345'
        value_346 = '346'
        value_347 = '347'
        value_348 = '348'
        value_349 = '349'
        value_350 = '350'
        value_351 = '351'
        value_352 = '352'
        value_353 = '353'
        value_354 = '354'
        value_355 = '355'
        value_356 = '356'
        value_357 = '357'
        value_358 = '358'
        value_359 = '359'
        value_360 = '360'
        value_361 = '361'
        value_362 = '362'
        value_363 = '363'
        value_364 = '364'
        value_365 = '365'
        value_366 = '366'
        value_367 = '367'
        value_368 = '368'
        value_369 = '369'
        value_370 = '370'
        value_371 = '371'
        value_372 = '372'
        value_373 = '373'
        value_374 = '374'
        value_375 = '375'
        value_376 = '376'
        value_377 = '377'
        value_378 = '378'
        value_379 = '379'
        value_380 = '380'
        value_381 = '381'
        value_382 = '382'
        value_383 = '383'
        value_384 = '384'
        value_385 = '385'
        value_386 = '386'
        value_387 = '387'
        value_388 = '388'
        value_389 = '389'
        value_390 = '390'
        value_391 = '391'
        value_392 = '392'
        value_393 = '393'
        value_394 = '394'
        value_395 = '395'
        value_396 = '396'
        value_397 = '397'
        value_398 = '398'
        value_399 = '399'
        value_400 = '400'
        value_401 = '401'
        value_402 = '402'
        value_403 = '403'
        value_404 = '404'
        value_405 = '405'
        value_406 = '406'
        value_407 = '407'
        value_408 = '408'
        value_409 = '409'
        value_410 = '410'
        value_411 = '411'
        value_412 = '412'
        value_413 = '413'
        value_414 = '414'
        value_415 = '415'
        value_416 = '416'
        value_417 = '417'
        value_418 = '418'
        value_419 = '419'
        value_420 = '420'
        value_421 = '421'
        value_422 = '422'
        value_423 = '423'
        value_424 = '424'
        value_425 = '425'
        value_426 = '426'
        value_427 = '427'
        value_428 = '428'
        value_429 = '429'
        value_430 = '430'
        value_431 = '431'
        value_432 = '432'
        value_433 = '433'
        value_434 = '434'
        value_435 = '435'
        value_436 = '436'
        value_437 = '437'
        value_438 = '438'
        value_439 = '439'
        value_440 = '440'
        value_441 = '441'
        value_442 = '442'
        value_443 = '443'
        value_444 = '444'
        value_445 = '445'
        value_446 = '446'
        value_447 = '447'
        value_448 = '448'
        value_449 = '449'
        value_450 = '450'
        value_451 = '451'
        value_452 = '452'
        value_453 = '453'
        value_454 = '454'
        value_455 = '455'
        value_456 = '456'
        value_457 = '457'
        value_458 = '458'
        value_459 = '459'
        value_460 = '460'
        value_461 = '461'
        value_462 = '462'
        value_463 = '463'
        value_464 = '464'
        value_465 = '465'
        value_466 = '466'
        value_467 = '467'
        value_468 = '468'
        value_469 = '469'
        value_470 = '470'
        value_471 = '471'
        value_472 = '472'
        value_473 = '473'
        value_474 = '474'
        value_475 = '475'
        value_476 = '476'
        value_477 = '477'
        value_478 = '478'
        value_479 = '479'
        value_480 = '480'

    class PagePermittedTasks:
        advertise = 'ADVERTISE'
        analyze = 'ANALYZE'
        cashier_role = 'CASHIER_ROLE'
        create_content = 'CREATE_CONTENT'
        global_structure_management = 'GLOBAL_STRUCTURE_MANAGEMENT'
        manage = 'MANAGE'
        manage_jobs = 'MANAGE_JOBS'
        manage_leads = 'MANAGE_LEADS'
        messaging = 'MESSAGING'
        moderate = 'MODERATE'
        moderate_community = 'MODERATE_COMMUNITY'
        pages_messaging = 'PAGES_MESSAGING'
        pages_messaging_subscriptions = 'PAGES_MESSAGING_SUBSCRIPTIONS'
        profile_plus_advertise = 'PROFILE_PLUS_ADVERTISE'
        profile_plus_analyze = 'PROFILE_PLUS_ANALYZE'
        profile_plus_create_content = 'PROFILE_PLUS_CREATE_CONTENT'
        profile_plus_facebook_access = 'PROFILE_PLUS_FACEBOOK_ACCESS'
        profile_plus_full_control = 'PROFILE_PLUS_FULL_CONTROL'
        profile_plus_global_structure_management = 'PROFILE_PLUS_GLOBAL_STRUCTURE_MANAGEMENT'
        profile_plus_manage = 'PROFILE_PLUS_MANAGE'
        profile_plus_manage_leads = 'PROFILE_PLUS_MANAGE_LEADS'
        profile_plus_messaging = 'PROFILE_PLUS_MESSAGING'
        profile_plus_moderate = 'PROFILE_PLUS_MODERATE'
        profile_plus_moderate_delegate_community = 'PROFILE_PLUS_MODERATE_DELEGATE_COMMUNITY'
        profile_plus_revenue = 'PROFILE_PLUS_REVENUE'
        read_page_mailboxes = 'READ_PAGE_MAILBOXES'
        view_monetization_insights = 'VIEW_MONETIZATION_INSIGHTS'

    class BusinessVertical:
        adult_products_and_services = 'ADULT_PRODUCTS_AND_SERVICES'
        alcohol_and_tobacco = 'ALCOHOL_AND_TOBACCO'
        automotive_dealers = 'AUTOMOTIVE_DEALERS'
        body_parts_fluids = 'BODY_PARTS_FLUIDS'
        business_and_utility = 'BUSINESS_AND_UTILITY'
        content_and_apps = 'CONTENT_AND_APPS'
        creators_and_celebrities = 'CREATORS_AND_CELEBRITIES'
        dating = 'DATING'
        drugs = 'DRUGS'
        endangered_species = 'ENDANGERED_SPECIES'
        firearms = 'FIREARMS'
        fraudulent_misleading_offensive = 'FRAUDULENT_MISLEADING_OFFENSIVE'
        gambling = 'GAMBLING'
        grocery_and_convenience_store = 'GROCERY_AND_CONVENIENCE_STORE'
        hazardous_goods_and_materials = 'HAZARDOUS_GOODS_AND_MATERIALS'
        home = 'HOME'
        home_and_auto_manufacturing = 'HOME_AND_AUTO_MANUFACTURING'
        lifestyle = 'LIFESTYLE'
        live_non_endangered_species = 'LIVE_NON_ENDANGERED_SPECIES'
        loans_debt_collection_bail_bonds = 'LOANS_DEBT_COLLECTION_BAIL_BONDS'
        local_events = 'LOCAL_EVENTS'
        medical_healthcare = 'MEDICAL_HEALTHCARE'
        multilevel_marketing = 'MULTILEVEL_MARKETING'
        non_profit_and_religious_orgs = 'NON_PROFIT_AND_RELIGIOUS_ORGS'
        professional = 'PROFESSIONAL'
        real_virtual_fake_currency = 'REAL_VIRTUAL_FAKE_CURRENCY'
        restaurants = 'RESTAURANTS'
        retail = 'RETAIL'
        transportation_and_accommodation = 'TRANSPORTATION_AND_ACCOMMODATION'

    class SubverticalV2:
        accounting_and_tax = 'ACCOUNTING_AND_TAX'
        activities_and_leisure = 'ACTIVITIES_AND_LEISURE'
        air = 'AIR'
        apparel_and_accessories = 'APPAREL_AND_ACCESSORIES'
        arts_and_heritage_and_education = 'ARTS_AND_HERITAGE_AND_EDUCATION'
        ar_or_vr_gaming = 'AR_OR_VR_GAMING'
        audio_streaming = 'AUDIO_STREAMING'
        auto = 'AUTO'
        auto_insurance = 'AUTO_INSURANCE'
        auto_rental = 'AUTO_RENTAL'
        baby = 'BABY'
        ballot_initiative_or_referendum = 'BALLOT_INITIATIVE_OR_REFERENDUM'
        beauty = 'BEAUTY'
        beauty_and_fashion = 'BEAUTY_AND_FASHION'
        beer_and_wine_and_liquor_and_malt_beverages = 'BEER_AND_WINE_AND_LIQUOR_AND_MALT_BEVERAGES'
        bookstores = 'BOOKSTORES'
        broadcast_television = 'BROADCAST_TELEVISION'
        business_consultants = 'BUSINESS_CONSULTANTS'
        buying_agency = 'BUYING_AGENCY'
        cable_and_satellite = 'CABLE_AND_SATELLITE'
        cable_television = 'CABLE_TELEVISION'
        call_center_and_messaging_services = 'CALL_CENTER_AND_MESSAGING_SERVICES'
        candidate_or_politician = 'CANDIDATE_OR_POLITICIAN'
        career = 'CAREER'
        career_and_tech = 'CAREER_AND_TECH'
        casual_dining = 'CASUAL_DINING'
        chronic_conditions_and_medical_causes = 'CHRONIC_CONDITIONS_AND_MEDICAL_CAUSES'
        civic_influencers = 'CIVIC_INFLUENCERS'
        clinical_trials = 'CLINICAL_TRIALS'
        coffee = 'COFFEE'
        computer_and_software_and_hardware = 'COMPUTER_AND_SOFTWARE_AND_HARDWARE'
        console_and_cross_platform_gaming = 'CONSOLE_AND_CROSS_PLATFORM_GAMING'
        consulting = 'CONSULTING'
        consumer_electronics = 'CONSUMER_ELECTRONICS'
        counseling_and_psychotherapy = 'COUNSELING_AND_PSYCHOTHERAPY'
        creative_agency = 'CREATIVE_AGENCY'
        credit_and_financing_and_mortages = 'CREDIT_AND_FINANCING_AND_MORTAGES'
        cruises_and_marine = 'CRUISES_AND_MARINE'
        culture_and_lifestyle = 'CULTURE_AND_LIFESTYLE'
        data_analytics_and_data_management = 'DATA_ANALYTICS_AND_DATA_MANAGEMENT'
        dating_and_technology_apps = 'DATING_AND_TECHNOLOGY_APPS'
        department_store = 'DEPARTMENT_STORE'
        desktop_software = 'DESKTOP_SOFTWARE'
        dieting_and_fitness_programs = 'DIETING_AND_FITNESS_PROGRAMS'
        digital_native_education_or_training = 'DIGITAL_NATIVE_EDUCATION_OR_TRAINING'
        drinking_places = 'DRINKING_PLACES'
        education_resources = 'EDUCATION_RESOURCES'
        ed_tech = 'ED_TECH'
        elearning_and_massive_online_open_courses = 'ELEARNING_AND_MASSIVE_ONLINE_OPEN_COURSES'
        election_commission = 'ELECTION_COMMISSION'
        electronics_and_appliances = 'ELECTRONICS_AND_APPLIANCES'
        engineering_and_design = 'ENGINEERING_AND_DESIGN'
        environment_and_animal_welfare = 'ENVIRONMENT_AND_ANIMAL_WELFARE'
        esports = 'ESPORTS'
        events = 'EVENTS'
        farming_and_ranching = 'FARMING_AND_RANCHING'
        file_storage_and_cloud_and_data_services = 'FILE_STORAGE_AND_CLOUD_AND_DATA_SERVICES'
        finance = 'FINANCE'
        fin_tech = 'FIN_TECH'
        fishing_and_hunting_and_forestry_and_logging = 'FISHING_AND_HUNTING_AND_FORESTRY_AND_LOGGING'
        fitness = 'FITNESS'
        food = 'FOOD'
        footwear = 'FOOTWEAR'
        for_profit_colleges_and_universities = 'FOR_PROFIT_COLLEGES_AND_UNIVERSITIES'
        full_service_agency = 'FULL_SERVICE_AGENCY'
        government_controlled_entity = 'GOVERNMENT_CONTROLLED_ENTITY'
        government_department_or_agency = 'GOVERNMENT_DEPARTMENT_OR_AGENCY'
        government_official = 'GOVERNMENT_OFFICIAL'
        government_owned_media = 'GOVERNMENT_OWNED_MEDIA'
        grocery_and_drug_and_convenience = 'GROCERY_AND_DRUG_AND_CONVENIENCE'
        head_of_state = 'HEAD_OF_STATE'
        health_insurance = 'HEALTH_INSURANCE'
        health_systems_and_practitioners = 'HEALTH_SYSTEMS_AND_PRACTITIONERS'
        health_tech = 'HEALTH_TECH'
        home_and_furniture_and_office = 'HOME_AND_FURNITURE_AND_OFFICE'
        home_improvement = 'HOME_IMPROVEMENT'
        home_insurance = 'HOME_INSURANCE'
        home_tech = 'HOME_TECH'
        hotel_and_accomodation = 'HOTEL_AND_ACCOMODATION'
        household_goods_durable = 'HOUSEHOLD_GOODS_DURABLE'
        household_goods_non_durable = 'HOUSEHOLD_GOODS_NON_DURABLE'
        hr_and_financial_management = 'HR_AND_FINANCIAL_MANAGEMENT'
        humanitarian_or_disaster_relief = 'HUMANITARIAN_OR_DISASTER_RELIEF'
        independent_expenditure_group = 'INDEPENDENT_EXPENDITURE_GROUP'
        insurance_tech = 'INSURANCE_TECH'
        international_organizaton = 'INTERNATIONAL_ORGANIZATON'
        investment_bank_and_brokerage = 'INVESTMENT_BANK_AND_BROKERAGE'
        issue_advocacy = 'ISSUE_ADVOCACY'
        legal = 'LEGAL'
        life_insurance = 'LIFE_INSURANCE'
        logistics_and_transportation_and_fleet_management = 'LOGISTICS_AND_TRANSPORTATION_AND_FLEET_MANAGEMENT'
        manufacturing = 'MANUFACTURING'
        medical_devices_and_supplies_and_equipment = 'MEDICAL_DEVICES_AND_SUPPLIES_AND_EQUIPMENT'
        medspa_and_elective_surgeries_and_alternative_medicine = 'MEDSPA_AND_ELECTIVE_SURGERIES_AND_ALTERNATIVE_MEDICINE'
        mining_and_quarrying = 'MINING_AND_QUARRYING'
        mobile_gaming = 'MOBILE_GAMING'
        movies = 'MOVIES'
        museums_and_parks_and_libraries = 'MUSEUMS_AND_PARKS_AND_LIBRARIES'
        music = 'MUSIC'
        network_security_products = 'NETWORK_SECURITY_PRODUCTS'
        news_and_current_events = 'NEWS_AND_CURRENT_EVENTS'
        non_prescription = 'NON_PRESCRIPTION'
        not_for_profit_colleges_and_universities = 'NOT_FOR_PROFIT_COLLEGES_AND_UNIVERSITIES'
        office = 'OFFICE'
        office_or_business_supplies = 'OFFICE_OR_BUSINESS_SUPPLIES'
        oil_and_gas_and_consumable_fuel = 'OIL_AND_GAS_AND_CONSUMABLE_FUEL'
        online_only_publications = 'ONLINE_ONLY_PUBLICATIONS'
        package_or_freight_delivery = 'PACKAGE_OR_FREIGHT_DELIVERY'
        party_independent_expenditure_group_us = 'PARTY_INDEPENDENT_EXPENDITURE_GROUP_US'
        payment_processing_and_gateway_solutions = 'PAYMENT_PROCESSING_AND_GATEWAY_SOLUTIONS'
        pc_gaming = 'PC_GAMING'
        people = 'PEOPLE'
        personal_care = 'PERSONAL_CARE'
        pet = 'PET'
        photography_and_filming_services = 'PHOTOGRAPHY_AND_FILMING_SERVICES'
        pizza = 'PIZZA'
        planning_agency = 'PLANNING_AGENCY'
        political_party_or_committee = 'POLITICAL_PARTY_OR_COMMITTEE'
        prescription = 'PRESCRIPTION'
        professional_associations = 'PROFESSIONAL_ASSOCIATIONS'
        property_and_casualty = 'PROPERTY_AND_CASUALTY'
        quick_service = 'QUICK_SERVICE'
        radio = 'RADIO'
        railroads = 'RAILROADS'
        real_estate = 'REAL_ESTATE'
        real_money_gaming = 'REAL_MONEY_GAMING'
        recreational = 'RECREATIONAL'
        religious = 'RELIGIOUS'
        reseller = 'RESELLER'
        residential_and_long_term_care_facilities_and_outpatient_care_centers = 'RESIDENTIAL_AND_LONG_TERM_CARE_FACILITIES_AND_OUTPATIENT_CARE_CENTERS'
        retail_and_credit_union_and_commercial_bank = 'RETAIL_AND_CREDIT_UNION_AND_COMMERCIAL_BANK'
        ride_sharing_or_taxi_services = 'RIDE_SHARING_OR_TAXI_SERVICES'
        safety_services = 'SAFETY_SERVICES'
        scholarly = 'SCHOLARLY'
        school_and_early_children_edcation = 'SCHOOL_AND_EARLY_CHILDREN_EDCATION'
        social_media = 'SOCIAL_MEDIA'
        software_as_a_service = 'SOFTWARE_AS_A_SERVICE'
        sporting = 'SPORTING'
        sporting_and_outdoor = 'SPORTING_AND_OUTDOOR'
        sports = 'SPORTS'
        superstores = 'SUPERSTORES'
        t1_automotive_manufacturer = 'T1_AUTOMOTIVE_MANUFACTURER'
        t1_motorcycle = 'T1_MOTORCYCLE'
        t2_dealer_associations = 'T2_DEALER_ASSOCIATIONS'
        t3_auto_agency = 'T3_AUTO_AGENCY'
        t3_auto_resellers = 'T3_AUTO_RESELLERS'
        t3_dealer_groups = 'T3_DEALER_GROUPS'
        t3_franchise_dealer = 'T3_FRANCHISE_DEALER'
        t3_independent_dealer = 'T3_INDEPENDENT_DEALER'
        t3_parts_and_services = 'T3_PARTS_AND_SERVICES'
        t3_portals = 'T3_PORTALS'
        telecommunications_equipment_and_accessories = 'TELECOMMUNICATIONS_EQUIPMENT_AND_ACCESSORIES'
        telephone_service_providers_and_carriers = 'TELEPHONE_SERVICE_PROVIDERS_AND_CARRIERS'
        ticketing = 'TICKETING'
        tobacco = 'TOBACCO'
        tourism_and_travel_services = 'TOURISM_AND_TRAVEL_SERVICES'
        tourism_board = 'TOURISM_BOARD'
        toy_and_hobby = 'TOY_AND_HOBBY'
        trade_school = 'TRADE_SCHOOL'
        travel_agencies_and_guides_and_otas = 'TRAVEL_AGENCIES_AND_GUIDES_AND_OTAS'
        utilities_and_energy_equipment_and_services = 'UTILITIES_AND_ENERGY_EQUIPMENT_AND_SERVICES'
        veterinary_clinics_and_services = 'VETERINARY_CLINICS_AND_SERVICES'
        video_streaming = 'VIDEO_STREAMING'
        virtual_services = 'VIRTUAL_SERVICES'
        vitamins_or_wellness = 'VITAMINS_OR_WELLNESS'
        warehousing_and_storage = 'WAREHOUSING_AND_STORAGE'
        water_and_soft_drink_and_baverage = 'WATER_AND_SOFT_DRINK_AND_BAVERAGE'
        website_designers_or_graphic_designers = 'WEBSITE_DESIGNERS_OR_GRAPHIC_DESIGNERS'
        wholesale = 'WHOLESALE'
        wireless_services = 'WIRELESS_SERVICES'

    class VerticalV2:
        advertising_and_marketing = 'ADVERTISING_AND_MARKETING'
        agriculture = 'AGRICULTURE'
        automotive = 'AUTOMOTIVE'
        banking_and_credit_cards = 'BANKING_AND_CREDIT_CARDS'
        business_to_business = 'BUSINESS_TO_BUSINESS'
        consumer_packaged_goods = 'CONSUMER_PACKAGED_GOODS'
        ecommerce = 'ECOMMERCE'
        education = 'EDUCATION'
        energy_and_natural_resources_and_utilities = 'ENERGY_AND_NATURAL_RESOURCES_AND_UTILITIES'
        entertainment_and_media = 'ENTERTAINMENT_AND_MEDIA'
        gaming = 'GAMING'
        government = 'GOVERNMENT'
        healthcare_and_pharmaceuticals_and_biotech = 'HEALTHCARE_AND_PHARMACEUTICALS_AND_BIOTECH'
        insurance = 'INSURANCE'
        non_profit = 'NON_PROFIT'
        organizations_and_associations = 'ORGANIZATIONS_AND_ASSOCIATIONS'
        politics = 'POLITICS'
        professional_services = 'PROFESSIONAL_SERVICES'
        publishing = 'PUBLISHING'
        restaurants = 'RESTAURANTS'
        retail = 'RETAIL'
        technology = 'TECHNOLOGY'
        telecom = 'TELECOM'
        travel = 'TRAVEL'

    class ActionSource:
        physical_store = 'PHYSICAL_STORE'
        website = 'WEBSITE'

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='NODE',
            response_parser=ObjectParser(reuse_object=self),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def api_update(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'entry_point': 'string',
            'name': 'string',
            'primary_page': 'string',
            'timezone_id': 'unsigned int',
            'two_factor_type': 'two_factor_type_enum',
            'vertical': 'vertical_enum',
        }
        enums = {
            'two_factor_type_enum': Business.TwoFactorType.__dict__.values(),
            'vertical_enum': Business.Vertical.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='NODE',
            response_parser=ObjectParser(reuse_object=self),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_access_token(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'app_id': 'string',
            'fbe_external_business_id': 'string',
            'scope': 'list<Permission>',
            'system_user_name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/access_token',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_ad_account_infos(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.almadaccountinfo import ALMAdAccountInfo
        param_types = {
            'ad_account_id': 'string',
            'parent_advertiser_id': 'string',
            'user_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ad_account_infos',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ALMAdAccountInfo,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ALMAdAccountInfo, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def delete_ad_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'adaccount_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/ad_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_ad_custom_derived_metrics(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adcustomderivedmetrics import AdCustomDerivedMetrics
        param_types = {
            'adhoc_custom_metrics': 'list<string>',
            'scope': 'scope_enum',
        }
        enums = {
            'scope_enum': AdCustomDerivedMetrics.Scope.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ad_custom_derived_metrics',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdCustomDerivedMetrics,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdCustomDerivedMetrics, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_ad_review_request(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'ad_account_ids': 'list<string>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/ad_review_requests',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_ad_studies(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adstudy import AdStudy
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ad_studies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdStudy,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdStudy, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_ad_study(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adstudy import AdStudy
        param_types = {
            'cells': 'list<Object>',
            'client_business': 'string',
            'confidence_level': 'float',
            'cooldown_start_time': 'int',
            'description': 'string',
            'end_time': 'int',
            'name': 'string',
            'objectives': 'list<Object>',
            'observation_end_time': 'int',
            'start_time': 'int',
            'type': 'type_enum',
            'viewers': 'list<int>',
        }
        enums = {
            'type_enum': AdStudy.Type.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/ad_studies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdStudy,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdStudy, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_ad_account(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccount import AdAccount
        param_types = {
            'ad_account_created_from_bm_flag': 'bool',
            'currency': 'string',
            'end_advertiser': 'Object',
            'funding_id': 'string',
            'invoice': 'bool',
            'invoice_group_id': 'string',
            'invoicing_emails': 'list<string>',
            'io': 'bool',
            'media_agency': 'string',
            'name': 'string',
            'partner': 'string',
            'po_number': 'string',
            'timezone_id': 'unsigned int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/adaccount',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccount, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_add_phone_number(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'phone_number': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/add_phone_numbers',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_ad_network_application(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.application import Application
        param_types = {
            'name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/adnetwork_applications',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Application,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Application, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_ad_network_analytics(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adnetworkanalyticssyncqueryresult import AdNetworkAnalyticsSyncQueryResult
        param_types = {
            'aggregation_period': 'aggregation_period_enum',
            'breakdowns': 'list<breakdowns_enum>',
            'filters': 'list<map>',
            'limit': 'unsigned int',
            'metrics': 'list<metrics_enum>',
            'ordering_column': 'ordering_column_enum',
            'ordering_type': 'ordering_type_enum',
            'should_include_until': 'bool',
            'since': 'datetime',
            'until': 'datetime',
        }
        enums = {
            'aggregation_period_enum': AdNetworkAnalyticsSyncQueryResult.AggregationPeriod.__dict__.values(),
            'breakdowns_enum': AdNetworkAnalyticsSyncQueryResult.Breakdowns.__dict__.values(),
            'metrics_enum': AdNetworkAnalyticsSyncQueryResult.Metrics.__dict__.values(),
            'ordering_column_enum': AdNetworkAnalyticsSyncQueryResult.OrderingColumn.__dict__.values(),
            'ordering_type_enum': AdNetworkAnalyticsSyncQueryResult.OrderingType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adnetworkanalytics',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdNetworkAnalyticsSyncQueryResult,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdNetworkAnalyticsSyncQueryResult, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_ad_network_analytic(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adnetworkanalyticssyncqueryresult import AdNetworkAnalyticsSyncQueryResult
        param_types = {
            'aggregation_period': 'aggregation_period_enum',
            'breakdowns': 'list<breakdowns_enum>',
            'filters': 'list<Object>',
            'limit': 'int',
            'metrics': 'list<metrics_enum>',
            'ordering_column': 'ordering_column_enum',
            'ordering_type': 'ordering_type_enum',
            'since': 'datetime',
            'until': 'datetime',
        }
        enums = {
            'aggregation_period_enum': AdNetworkAnalyticsSyncQueryResult.AggregationPeriod.__dict__.values(),
            'breakdowns_enum': AdNetworkAnalyticsSyncQueryResult.Breakdowns.__dict__.values(),
            'metrics_enum': AdNetworkAnalyticsSyncQueryResult.Metrics.__dict__.values(),
            'ordering_column_enum': AdNetworkAnalyticsSyncQueryResult.OrderingColumn.__dict__.values(),
            'ordering_type_enum': AdNetworkAnalyticsSyncQueryResult.OrderingType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/adnetworkanalytics',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_ad_network_analytics_results(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adnetworkanalyticsasyncqueryresult import AdNetworkAnalyticsAsyncQueryResult
        param_types = {
            'query_ids': 'list<string>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adnetworkanalytics_results',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdNetworkAnalyticsAsyncQueryResult,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdNetworkAnalyticsAsyncQueryResult, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_ads_dataset(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adsdataset import AdsDataset
        param_types = {
            'id_filter': 'string',
            'name_filter': 'string',
            'sort_by': 'sort_by_enum',
        }
        enums = {
            'sort_by_enum': AdsDataset.SortBy.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ads_dataset',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsDataset,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsDataset, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_ads_data_set(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'ad_account_id': 'string',
            'app_id': 'string',
            'is_crm': 'bool',
            'name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/ads_dataset',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_ads_reporting_mmm_reports(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adsreportbuildermmmreport import AdsReportBuilderMMMReport
        param_types = {
            'filtering': 'list<map>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ads_reporting_mmm_reports',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsReportBuilderMMMReport,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsReportBuilderMMMReport, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_ads_reporting_mmm_schedulers(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adsreportbuildermmmreportscheduler import AdsReportBuilderMMMReportScheduler
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ads_reporting_mmm_schedulers',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsReportBuilderMMMReportScheduler,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsReportBuilderMMMReportScheduler, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_ads_pixels(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adspixel import AdsPixel
        param_types = {
            'id_filter': 'string',
            'name_filter': 'string',
            'sort_by': 'sort_by_enum',
        }
        enums = {
            'sort_by_enum': AdsPixel.SortBy.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/adspixels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsPixel,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsPixel, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_ads_pixel(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adspixel import AdsPixel
        param_types = {
            'is_crm': 'bool',
            'name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/adspixels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsPixel,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsPixel, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def delete_agencies(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'business': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/agencies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_agencies(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/agencies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_an_placements(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adplacement import AdPlacement
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/an_placements',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdPlacement,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdPlacement, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_block_list_draft(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'publisher_urls_file': 'file',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/block_list_drafts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_bm_review_request(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'business_manager_ids': 'list<string>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/bm_review_requests',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_business_asset_groups(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businessassetgroup import BusinessAssetGroup
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/business_asset_groups',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessAssetGroup,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessAssetGroup, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_business_invoices(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.omegacustomertrx import OmegaCustomerTrx
        param_types = {
            'end_date': 'string',
            'invoice_id': 'string',
            'issue_end_date': 'string',
            'issue_start_date': 'string',
            'root_id': 'unsigned int',
            'start_date': 'string',
            'type': 'type_enum',
        }
        enums = {
            'type_enum': OmegaCustomerTrx.Type.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/business_invoices',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=OmegaCustomerTrx,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=OmegaCustomerTrx, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_business_users(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businessuser import BusinessUser
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/business_users',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessUser, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_business_user(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businessuser import BusinessUser
        param_types = {
            'email': 'string',
            'invited_user_type': 'list<invited_user_type_enum>',
            'role': 'role_enum',
            'tasks': 'list<tasks_enum>',
        }
        enums = {
            'invited_user_type_enum': BusinessUser.InvitedUserType.__dict__.values(),
            'role_enum': BusinessUser.Role.__dict__.values(),
            'tasks_enum': BusinessUser.Tasks.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/business_users',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessUser, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_business_projects(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businessproject import BusinessProject
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/businessprojects',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessProject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessProject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_claim_custom_conversion(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.customconversion import CustomConversion
        param_types = {
            'custom_conversion_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/claim_custom_conversions',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomConversion,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CustomConversion, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_client_ad_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccount import AdAccount
        param_types = {
            'search_query': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/client_ad_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccount, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_client_apps(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.application import Application
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/client_apps',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Application,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Application, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_client_app(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'app_id': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/client_apps',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_client_instagram_assets(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.instagrambusinessasset import InstagramBusinessAsset
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/client_instagram_assets',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=InstagramBusinessAsset,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=InstagramBusinessAsset, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_client_offsite_signal_container_business_objects(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.offsitesignalcontainerbusinessobject import OffsiteSignalContainerBusinessObject
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/client_offsite_signal_container_business_objects',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=OffsiteSignalContainerBusinessObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=OffsiteSignalContainerBusinessObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_client_pages(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.page import Page
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/client_pages',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Page,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Page, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_client_page(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'page_id': 'int',
            'permitted_tasks': 'list<permitted_tasks_enum>',
        }
        enums = {
            'permitted_tasks_enum': Business.PermittedTasks.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/client_pages',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_client_pixels(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adspixel import AdsPixel
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/client_pixels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsPixel,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsPixel, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_client_product_catalogs(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productcatalog import ProductCatalog
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/client_product_catalogs',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_client_whats_app_business_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.whatsappbusinessaccount import WhatsAppBusinessAccount
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/client_whatsapp_business_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=WhatsAppBusinessAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=WhatsAppBusinessAccount, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def delete_clients(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'business': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/clients',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_clients(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/clients',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_collaborative_ads_collaboration_requests(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.cpascollaborationrequest import CPASCollaborationRequest
        param_types = {
            'status': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/collaborative_ads_collaboration_requests',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CPASCollaborationRequest,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CPASCollaborationRequest, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_collaborative_ads_collaboration_request(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.cpascollaborationrequest import CPASCollaborationRequest
        param_types = {
            'brands': 'list<string>',
            'contact_email': 'string',
            'contact_first_name': 'string',
            'contact_last_name': 'string',
            'phone_number': 'string',
            'receiver_business': 'string',
            'requester_agency_or_brand': 'requester_agency_or_brand_enum',
            'sender_client_business': 'string',
        }
        enums = {
            'requester_agency_or_brand_enum': CPASCollaborationRequest.RequesterAgencyOrBrand.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/collaborative_ads_collaboration_requests',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CPASCollaborationRequest,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CPASCollaborationRequest, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_collaborative_ads_suggested_partners(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.cpasadvertiserpartnershiprecommendation import CPASAdvertiserPartnershipRecommendation
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/collaborative_ads_suggested_partners',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CPASAdvertiserPartnershipRecommendation,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CPASAdvertiserPartnershipRecommendation, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_commerce_merchant_settings(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.commercemerchantsettings import CommerceMerchantSettings
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/commerce_merchant_settings',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CommerceMerchantSettings,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CommerceMerchantSettings, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_cpas_business_setup_config(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.cpasbusinesssetupconfig import CPASBusinessSetupConfig
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/cpas_business_setup_config',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CPASBusinessSetupConfig,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CPASBusinessSetupConfig, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_cpas_business_setup_config(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.cpasbusinesssetupconfig import CPASBusinessSetupConfig
        param_types = {
            'accepted_collab_ads_tos': 'bool',
            'ad_accounts': 'list<string>',
            'business_capabilities_status': 'map',
            'capabilities_compliance_status': 'map',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/cpas_business_setup_config',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CPASBusinessSetupConfig,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CPASBusinessSetupConfig, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_cpas_merchant_config(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.cpasmerchantconfig import CPASMerchantConfig
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/cpas_merchant_config',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CPASMerchantConfig,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CPASMerchantConfig, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_creative_folder(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businesscreativefolder import BusinessCreativeFolder
        param_types = {
            'description': 'string',
            'name': 'string',
            'parent_folder_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/creative_folders',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessCreativeFolder,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessCreativeFolder, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_credit_cards(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.creditcard import CreditCard
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/creditcards',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CreditCard,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CreditCard, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_custom_conversion(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.customconversion import CustomConversion
        param_types = {
            'action_source_type': 'action_source_type_enum',
            'advanced_rule': 'string',
            'custom_event_type': 'custom_event_type_enum',
            'default_conversion_value': 'float',
            'description': 'string',
            'event_source_id': 'string',
            'name': 'string',
            'rule': 'string',
        }
        enums = {
            'action_source_type_enum': CustomConversion.ActionSourceType.__dict__.values(),
            'custom_event_type_enum': CustomConversion.CustomEventType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/customconversions',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CustomConversion,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CustomConversion, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_event_source_groups(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.eventsourcegroup import EventSourceGroup
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/event_source_groups',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=EventSourceGroup,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=EventSourceGroup, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_event_source_group(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.eventsourcegroup import EventSourceGroup
        param_types = {
            'event_sources': 'list<string>',
            'name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/event_source_groups',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=EventSourceGroup,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=EventSourceGroup, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_extended_credit_applications(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.extendedcreditapplication import ExtendedCreditApplication
        param_types = {
            'only_show_pending': 'bool',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/extendedcreditapplications',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ExtendedCreditApplication,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ExtendedCreditApplication, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_extended_credits(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.extendedcredit import ExtendedCredit
        param_types = {
            'order_by_is_owned_credential': 'bool',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/extendedcredits',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ExtendedCredit,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ExtendedCredit, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_image(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businessimage import BusinessImage
        param_types = {
            'ad_placements_validation_only': 'bool',
            'bytes': 'string',
            'creative_folder_id': 'string',
            'name': 'string',
            'validation_ad_placements': 'list<validation_ad_placements_enum>',
        }
        enums = {
            'validation_ad_placements_enum': BusinessImage.ValidationAdPlacements.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/images',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessImage,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessImage, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_initiated_audience_sharing_requests(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businessassetsharingagreement import BusinessAssetSharingAgreement
        param_types = {
            'recipient_id': 'string',
            'request_status': 'request_status_enum',
        }
        enums = {
            'request_status_enum': BusinessAssetSharingAgreement.RequestStatus.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/initiated_audience_sharing_requests',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessAssetSharingAgreement,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessAssetSharingAgreement, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def delete_instagram_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'instagram_account': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/instagram_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_instagram_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.iguser import IGUser
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/instagram_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGUser, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_instagram_business_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.iguser import IGUser
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/instagram_business_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGUser, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def delete_managed_businesses(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'existing_client_business_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/managed_businesses',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_managed_business(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'child_business_external_id': 'string',
            'existing_client_business_id': 'string',
            'name': 'string',
            'sales_rep_email': 'string',
            'survey_business_type': 'survey_business_type_enum',
            'survey_num_assets': 'unsigned int',
            'survey_num_people': 'unsigned int',
            'timezone_id': 'timezone_id_enum',
            'vertical': 'vertical_enum',
        }
        enums = {
            'survey_business_type_enum': Business.SurveyBusinessType.__dict__.values(),
            'timezone_id_enum': Business.TimezoneId.__dict__.values(),
            'vertical_enum': Business.Vertical.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/managed_businesses',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_managed_partner_ads_funding_source_details(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.fundingsourcedetailscoupon import FundingSourceDetailsCoupon
        param_types = {
            'year_quarter': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/managed_partner_ads_funding_source_details',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=FundingSourceDetailsCoupon,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=FundingSourceDetailsCoupon, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_managed_partner_business_setup(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'active_ad_account_id': 'string',
            'active_page_id': 'int',
            'partner_facebook_page_url': 'string',
            'partner_registration_countries': 'list<string>',
            'seller_email_address': 'string',
            'seller_external_website_url': 'string',
            'template': 'list<map>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/managed_partner_business_setup',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def delete_managed_partner_businesses(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'child_business_external_id': 'string',
            'child_business_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/managed_partner_businesses',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_managed_partner_business(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.managedpartnerbusiness import ManagedPartnerBusiness
        param_types = {
            'ad_account_currency': 'string',
            'catalog_id': 'string',
            'child_business_external_id': 'string',
            'credit_limit': 'unsigned int',
            'line_of_credit_id': 'string',
            'name': 'string',
            'no_ad_account': 'bool',
            'page_name': 'string',
            'page_profile_image_url': 'string',
            'partition_type': 'partition_type_enum',
            'partner_facebook_page_url': 'string',
            'partner_registration_countries': 'list<string>',
            'sales_rep_email': 'string',
            'seller_external_website_url': 'string',
            'seller_targeting_countries': 'list<string>',
            'skip_partner_page_creation': 'bool',
            'survey_business_type': 'survey_business_type_enum',
            'survey_num_assets': 'unsigned int',
            'survey_num_people': 'unsigned int',
            'timezone_id': 'timezone_id_enum',
            'vertical': 'vertical_enum',
        }
        enums = {
            'partition_type_enum': ManagedPartnerBusiness.PartitionType.__dict__.values(),
            'survey_business_type_enum': ManagedPartnerBusiness.SurveyBusinessType.__dict__.values(),
            'timezone_id_enum': ManagedPartnerBusiness.TimezoneId.__dict__.values(),
            'vertical_enum': ManagedPartnerBusiness.Vertical.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/managed_partner_businesses',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ManagedPartnerBusiness,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ManagedPartnerBusiness, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_onboard_partners_to_mm_lite(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'solution_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/onboard_partners_to_mm_lite',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_open_bridge_configurations(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.openbridgeconfiguration import OpenBridgeConfiguration
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/openbridge_configurations',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=OpenBridgeConfiguration,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=OpenBridgeConfiguration, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_open_bridge_configuration(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.openbridgeconfiguration import OpenBridgeConfiguration
        param_types = {
            'active': 'bool',
            'blocked_event_types': 'list<string>',
            'blocked_websites': 'list<string>',
            'cloud_provider': 'string',
            'cloud_region': 'string',
            'destination_id': 'string',
            'endpoint': 'string',
            'event_enrichment_state': 'event_enrichment_state_enum',
            'fallback_domain': 'string',
            'first_party_domain': 'string',
            'host_business_id': 'unsigned int',
            'instance_id': 'string',
            'instance_version': 'string',
            'is_sgw_instance': 'bool',
            'is_sgw_pixel_from_meta_pixel': 'bool',
            'partner_name': 'string',
            'pixel_id': 'unsigned int',
            'sgw_account_id': 'string',
            'sgw_instance_url': 'string',
            'sgw_pixel_id': 'unsigned int',
        }
        enums = {
            'event_enrichment_state_enum': OpenBridgeConfiguration.EventEnrichmentState.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/openbridge_configurations',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=OpenBridgeConfiguration,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=OpenBridgeConfiguration, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_owned_ad_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccount import AdAccount
        param_types = {
            'search_query': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/owned_ad_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccount, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_owned_ad_account(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'adaccount_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/owned_ad_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_owned_apps(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.application import Application
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/owned_apps',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Application,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Application, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_owned_app(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'app_id': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/owned_apps',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def delete_owned_businesses(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'client_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/owned_businesses',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_owned_businesses(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'child_business_external_id': 'string',
            'client_user_id': 'int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/owned_businesses',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_owned_business(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'child_business_external_id': 'string',
            'name': 'string',
            'page_permitted_tasks': 'list<page_permitted_tasks_enum>',
            'sales_rep_email': 'string',
            'shared_page_id': 'string',
            'should_generate_name': 'bool',
            'survey_business_type': 'survey_business_type_enum',
            'survey_num_assets': 'unsigned int',
            'survey_num_people': 'unsigned int',
            'timezone_id': 'timezone_id_enum',
            'vertical': 'vertical_enum',
        }
        enums = {
            'page_permitted_tasks_enum': Business.PagePermittedTasks.__dict__.values(),
            'survey_business_type_enum': Business.SurveyBusinessType.__dict__.values(),
            'timezone_id_enum': Business.TimezoneId.__dict__.values(),
            'vertical_enum': Business.Vertical.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/owned_businesses',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_owned_instagram_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.iguser import IGUser
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/owned_instagram_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGUser, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_owned_instagram_assets(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.instagrambusinessasset import InstagramBusinessAsset
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/owned_instagram_assets',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=InstagramBusinessAsset,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=InstagramBusinessAsset, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_owned_offsite_signal_container_business_objects(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.offsitesignalcontainerbusinessobject import OffsiteSignalContainerBusinessObject
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/owned_offsite_signal_container_business_objects',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=OffsiteSignalContainerBusinessObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=OffsiteSignalContainerBusinessObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_owned_pages(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.page import Page
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/owned_pages',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Page,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Page, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_owned_page(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'code': 'string',
            'entry_point': 'string',
            'page_id': 'int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/owned_pages',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_owned_pixels(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adspixel import AdsPixel
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/owned_pixels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdsPixel,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdsPixel, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_owned_product_catalogs(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productcatalog import ProductCatalog
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/owned_product_catalogs',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_owned_product_catalog(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productcatalog import ProductCatalog
        param_types = {
            'additional_vertical_option': 'additional_vertical_option_enum',
            'business_metadata': 'map',
            'catalog_segment_filter': 'Object',
            'catalog_segment_product_set_id': 'string',
            'da_display_settings': 'Object',
            'destination_catalog_settings': 'map',
            'flight_catalog_settings': 'map',
            'name': 'string',
            'parent_catalog_id': 'string',
            'partner_integration': 'map',
            'store_catalog_settings': 'map',
            'vertical': 'vertical_enum',
        }
        enums = {
            'additional_vertical_option_enum': ProductCatalog.AdditionalVerticalOption.__dict__.values(),
            'vertical_enum': ProductCatalog.Vertical.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/owned_product_catalogs',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_owned_whats_app_business_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.whatsappbusinessaccount import WhatsAppBusinessAccount
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/owned_whatsapp_business_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=WhatsAppBusinessAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=WhatsAppBusinessAccount, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def delete_pages(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'page_id': 'int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/pages',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_partner_account_linking(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.partneraccountlinking import PartnerAccountLinking
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/partner_account_linking',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=PartnerAccountLinking,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=PartnerAccountLinking, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_partner_premium_opt_i_on(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'catalog_segment_id': 'string',
            'enable_basket_insight': 'bool',
            'enable_extended_audience_retargeting': 'bool',
            'partner_business_id': 'string',
            'retailer_custom_audience_config': 'map',
            'vendor_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/partner_premium_options',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_passback_attribution_metadata_configs(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/passback_attribution_metadata_configs',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_pending_client_ad_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businessadaccountrequest import BusinessAdAccountRequest
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/pending_client_ad_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessAdAccountRequest,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessAdAccountRequest, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_pending_client_apps(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businessapplicationrequest import BusinessApplicationRequest
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/pending_client_apps',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessApplicationRequest,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessApplicationRequest, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_pending_client_pages(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businesspagerequest import BusinessPageRequest
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/pending_client_pages',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessPageRequest,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessPageRequest, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_pending_owned_ad_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businessadaccountrequest import BusinessAdAccountRequest
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/pending_owned_ad_accounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessAdAccountRequest,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessAdAccountRequest, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_pending_owned_pages(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businesspagerequest import BusinessPageRequest
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/pending_owned_pages',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessPageRequest,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessPageRequest, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_pending_shared_offsite_signal_container_business_objects(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.offsitesignalcontainerbusinessobject import OffsiteSignalContainerBusinessObject
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/pending_shared_offsite_signal_container_business_objects',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=OffsiteSignalContainerBusinessObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=OffsiteSignalContainerBusinessObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_pending_users(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businessrolerequest import BusinessRoleRequest
        param_types = {
            'email': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/pending_users',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessRoleRequest,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessRoleRequest, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_picture(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.profilepicturesource import ProfilePictureSource
        param_types = {
            'height': 'int',
            'redirect': 'bool',
            'type': 'type_enum',
            'width': 'int',
        }
        enums = {
            'type_enum': ProfilePictureSource.Type.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/picture',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProfilePictureSource,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProfilePictureSource, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_pixel_to(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/pixel_tos',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_pre_verified_numbers(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.whatsappbusinesspreverifiedphonenumber import WhatsAppBusinessPreVerifiedPhoneNumber
        param_types = {
            'code_verification_status': 'code_verification_status_enum',
            'phone_number': 'string',
        }
        enums = {
            'code_verification_status_enum': WhatsAppBusinessPreVerifiedPhoneNumber.CodeVerificationStatus.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/preverified_numbers',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=WhatsAppBusinessPreVerifiedPhoneNumber,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=WhatsAppBusinessPreVerifiedPhoneNumber, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_received_audience_sharing_requests(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.businessassetsharingagreement import BusinessAssetSharingAgreement
        param_types = {
            'initiator_id': 'string',
            'request_status': 'request_status_enum',
        }
        enums = {
            'request_status_enum': BusinessAssetSharingAgreement.RequestStatus.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/received_audience_sharing_requests',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BusinessAssetSharingAgreement,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BusinessAssetSharingAgreement, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_reseller_guidances(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.resellerguidance import ResellerGuidance
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/reseller_guidances',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ResellerGuidance,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ResellerGuidance, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_self_certified_whats_app_business_submissions(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.whatsappbusinesspartnerclientverificationsubmission import WhatsAppBusinessPartnerClientVerificationSubmission
        param_types = {
            'end_business_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/self_certified_whatsapp_business_submissions',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=WhatsAppBusinessPartnerClientVerificationSubmission,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=WhatsAppBusinessPartnerClientVerificationSubmission, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_self_certify_whats_app_business(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'average_monthly_revenue_spend_with_partner': 'map',
            'business_documents': 'list<file>',
            'business_vertical': 'business_vertical_enum',
            'end_business_address': 'map',
            'end_business_id': 'string',
            'end_business_legal_name': 'string',
            'end_business_trade_names': 'list<string>',
            'end_business_website': 'string',
            'num_billing_cycles_with_partner': 'unsigned int',
        }
        enums = {
            'business_vertical_enum': Business.BusinessVertical.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/self_certify_whatsapp_business',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_setup_managed_partner_ad_account(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'credit_line_id': 'string',
            'marketplace_business_id': 'string',
            'subvertical_v2': 'subvertical_v2_enum',
            'vendor_id': 'string',
            'vertical_v2': 'vertical_v2_enum',
        }
        enums = {
            'subvertical_v2_enum': Business.SubverticalV2.__dict__.values(),
            'vertical_v2_enum': Business.VerticalV2.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/setup_managed_partner_adaccounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def delete_share_pre_verified_numbers(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'partner_business_id': 'string',
            'preverified_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/share_preverified_numbers',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_share_pre_verified_number(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'partner_business_id': 'string',
            'preverified_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/share_preverified_numbers',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_system_user_access_token(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'asset': 'list<unsigned int>',
            'fetch_only': 'bool',
            'scope': 'list<Permission>',
            'set_token_expires_in_60_days': 'bool',
            'system_user_id': 'unsigned int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/system_user_access_tokens',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_system_users(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.systemuser import SystemUser
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/system_users',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=SystemUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=SystemUser, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_system_user(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.systemuser import SystemUser
        param_types = {
            'name': 'string',
            'role': 'role_enum',
            'system_user_id': 'int',
        }
        enums = {
            'role_enum': SystemUser.Role.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/system_users',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=SystemUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=SystemUser, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def get_third_party_measurement_report_dataset(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.thirdpartymeasurementreportdataset import ThirdPartyMeasurementReportDataset
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/third_party_measurement_report_dataset',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ThirdPartyMeasurementReportDataset,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ThirdPartyMeasurementReportDataset, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    def create_video(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.advideo import AdVideo
        param_types = {
            'ad_placements_validation_only': 'bool',
            'application_id': 'string',
            'asked_fun_fact_prompt_id': 'unsigned int',
            'audio_story_wave_animation_handle': 'string',
            'chunk_session_id': 'string',
            'composer_entry_picker': 'string',
            'composer_entry_point': 'string',
            'composer_entry_time': 'unsigned int',
            'composer_session_events_log': 'string',
            'composer_session_id': 'string',
            'composer_source_surface': 'string',
            'composer_type': 'string',
            'container_type': 'container_type_enum',
            'content_category': 'content_category_enum',
            'creative_folder_id': 'string',
            'creative_tools': 'string',
            'description': 'string',
            'embeddable': 'bool',
            'end_offset': 'unsigned int',
            'fbuploader_video_file_chunk': 'string',
            'file_size': 'unsigned int',
            'file_url': 'string',
            'fisheye_video_cropped': 'bool',
            'formatting': 'formatting_enum',
            'fov': 'unsigned int',
            'front_z_rotation': 'float',
            'fun_fact_prompt_id': 'string',
            'fun_fact_toastee_id': 'unsigned int',
            'guide': 'list<list<unsigned int>>',
            'guide_enabled': 'bool',
            'initial_heading': 'unsigned int',
            'initial_pitch': 'unsigned int',
            'instant_game_entry_point_data': 'string',
            'is_boost_intended': 'bool',
            'is_group_linking_post': 'bool',
            'is_partnership_ad': 'bool',
            'is_voice_clip': 'bool',
            'location_source_id': 'string',
            'og_action_type_id': 'string',
            'og_icon_id': 'string',
            'og_object_id': 'string',
            'og_phrase': 'string',
            'og_suggestion_mechanism': 'string',
            'original_fov': 'unsigned int',
            'original_projection_type': 'original_projection_type_enum',
            'partnership_ad_ad_code': 'string',
            'publish_event_id': 'unsigned int',
            'referenced_sticker_id': 'string',
            'replace_video_id': 'string',
            'slideshow_spec': 'map',
            'source': 'string',
            'source_instagram_media_id': 'string',
            'spherical': 'bool',
            'start_offset': 'unsigned int',
            'swap_mode': 'swap_mode_enum',
            'text_format_metadata': 'string',
            'thumb': 'file',
            'time_since_original_post': 'unsigned int',
            'title': 'string',
            'transcode_setting_properties': 'string',
            'unpublished_content_type': 'unpublished_content_type_enum',
            'upload_phase': 'upload_phase_enum',
            'upload_session_id': 'string',
            'upload_setting_properties': 'string',
            'validation_ad_placements': 'list<validation_ad_placements_enum>',
            'video_file_chunk': 'string',
            'video_id_original': 'string',
            'video_start_time_ms': 'unsigned int',
            'waterfall_id': 'string',
        }
        enums = {
            'container_type_enum': AdVideo.ContainerType.__dict__.values(),
            'content_category_enum': AdVideo.ContentCategory.__dict__.values(),
            'formatting_enum': AdVideo.Formatting.__dict__.values(),
            'original_projection_type_enum': AdVideo.OriginalProjectionType.__dict__.values(),
            'swap_mode_enum': AdVideo.SwapMode.__dict__.values(),
            'unpublished_content_type_enum': AdVideo.UnpublishedContentType.__dict__.values(),
            'upload_phase_enum': AdVideo.UploadPhase.__dict__.values(),
            'validation_ad_placements_enum': AdVideo.ValidationAdPlacements.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/videos',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdVideo,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdVideo, api=self._api),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    _field_types = {
        'block_offline_analytics': 'bool',
        'collaborative_ads_managed_partner_business_info': 'ManagedPartnerBusiness',
        'collaborative_ads_managed_partner_eligibility': 'BusinessManagedPartnerEligibility',
        'collaborative_ads_partner_premium_options': 'BusinessPartnerPremiumOptions',
        'created_by': 'Object',
        'created_time': 'datetime',
        'extended_updated_time': 'datetime',
        'id': 'string',
        'is_hidden': 'bool',
        'link': 'string',
        'marketing_messages_onboarding_status': 'MarketingMessagesOnboardingStatus',
        'name': 'string',
        'payment_account_id': 'string',
        'primary_page': 'Page',
        'profile_picture_uri': 'string',
        'timezone_id': 'unsigned int',
        'two_factor_type': 'string',
        'updated_by': 'Object',
        'updated_time': 'datetime',
        'user_access_expire_time': 'datetime',
        'verification_status': 'VerificationStatus',
        'vertical': 'string',
        'vertical_id': 'unsigned int',
        'whatsapp_business_manager_messaging_limit': 'WhatsappBusinessManagerMessagingLimit',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['VerificationStatus'] = Business.VerificationStatus.__dict__.values()
        field_enum_info['WhatsappBusinessManagerMessagingLimit'] = Business.WhatsappBusinessManagerMessagingLimit.__dict__.values()
        field_enum_info['TwoFactorType'] = Business.TwoFactorType.__dict__.values()
        field_enum_info['Vertical'] = Business.Vertical.__dict__.values()
        field_enum_info['PermittedTasks'] = Business.PermittedTasks.__dict__.values()
        field_enum_info['SurveyBusinessType'] = Business.SurveyBusinessType.__dict__.values()
        field_enum_info['TimezoneId'] = Business.TimezoneId.__dict__.values()
        field_enum_info['PagePermittedTasks'] = Business.PagePermittedTasks.__dict__.values()
        field_enum_info['BusinessVertical'] = Business.BusinessVertical.__dict__.values()
        field_enum_info['SubverticalV2'] = Business.SubverticalV2.__dict__.values()
        field_enum_info['VerticalV2'] = Business.VerticalV2.__dict__.values()
        field_enum_info['ActionSource'] = Business.ActionSource.__dict__.values()
        return field_enum_info


