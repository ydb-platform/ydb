# PYTHON-AMAZON-AD-API

![CodeQL](https://img.shields.io/badge/coverage-95%25-green)
![CodeQL](https://img.shields.io/badge/Docs-sphinx-green)
![CodeQL](https://img.shields.io/github/v/release/denisneuf/python-amazon-ad-api)
[![Documentation Status](https://readthedocs.org/projects/python-amazon-ad-api/badge/?version=latest)](https://python-amazon-ad-api.readthedocs.io/en/latest/?badge=latest)


## Amazon's Advertising API

A python 3 wrapper to access Amazon's Advertising API with an easy-to-use interface.

### Install

[![Badge](https://img.shields.io/pypi/v/python-amazon-ad-api?style=for-the-badge)](https://pypi.org/project/python-amazon-ad-api/)

```
pip install python-amazon-ad-api
```

### Donate

If you find this project is useful consider donating or [sponsor](https://github.com/sponsors/denisneuf) it to keep on going on it, thank you.

[![paypal](https://www.paypalobjects.com/en_US/i/btn/btn_donate_LG.gif)](https://www.paypal.com/donate?hosted_button_id=G3KB6M2G9YV9C)

![alt text](https://github.com/denisneuf/python-amazon-ad-api/blob/main/test/codigo-QR.png?raw=true)


### Overview

You need obtain your own credentials with Amazon that may include an amazon developer account and access as seller or vendor. Please view the checklist of [Amazon Ads API onboarding overview](https://advertising.amazon.com/API/docs/en-us/setting-up/overview) 


### Code Credentials
You can use your credentials as follows passing it to the client as a dict. Please review the full [documentation](https://github.com/sponsors/denisneuf) to see all posibilities to include your credentials.

```javascript
from ad_api.api import sponsored_products


my_credentials = dict(
    refresh_token='your-refresh_token',
    client_id='your-client_id',
    client_secret='your-client_secret',
    profile_id='your-profile_id',
)

result=sponsored_products.Campaigns(credentials=my_credentials).list_campaigns()

```

### YAML Credentials
Use a credentials.yml file with your credentials for more convenience and manage diferent accounts or profiles. Amazon requires one profile per marketplace so it is helpful to keep all in one file and switch directly from the code, using the account.

Create a file credentials.yml

```javascript
version: '1.0'

default:
  refresh_token: 'your-refresh-token'
  client_id: 'your-client-id'
  client_secret: 'your-client-secret'
  profile_id: 'your-profile-id'

germany:
  refresh_token: 'other-refresh-token'
  client_id: 'other-client-id'
  client_secret: 'other-client-secret'
  profile_id: 'other-profile-id'

```

Python code

```python
from ad_api.api import sponsored_products

# Leave empty will use the 'default' account
result=sponsored_products.Campaigns().list_campaigns()
# will use germany account data
result=sponsored_products.Campaigns(account="germany").list_campaigns()
```



### Search path for credentials.yml

* macOS and Other Unix: `~/.config/python-ad-api`
* Windows: `%APPDATA%\python-ad-api` where the <cite>APPDATA</cite> environment variable falls
back to `%HOME%\AppData\Roaming` if undefined


[Confuse Help](https://confuse.readthedocs.io/en/latest/usage.html#search-paths)


### Marketplaces

Marketplaces are used to define basically the [API endpoints](https://advertising.amazon.com/API/docs/en-us/info/api-overview#api-endpoints) Amazon need to use depending on the regions, by default it will use EU so if you are using one of the marketplaces that are under the Europe (EU). Covers UK, FR, IT, ES, DE, NL, AE, SE, PL, and TR marketplaces you can skip. If you are using either North America (NA) or Far East (FE), you will need import from base and pass the marketplace as follows:

```python
from ad_api.api import sponsored_products
from ad_api.base import Marketplaces

# You can pass NA or US, CA, MX or BR for North America and JP, AU or SG for Far East
result=sponsored_products.Campaigns(marketplace=Marketplaces.NA).list_campaigns()

```

### Exceptions

You can use a [try](https://docs.python.org/3.10/reference/compound_stmts.html#try) except statement when you call the API and catch exceptions if some problem ocurred:

```python
from ad_api.api import sponsored_products
from ad_api.base import AdvertisingApiException

try:

    result = sponsored_products.Campaigns().get_campaign_extended(
        campaignId=campaign_id
    )

    logging.info(result)

except AdvertisingApiException as error:
    logging.info(error)
```

### Debug

Use debug=True if you want see some logs like the header you submit to the api endpoint, the method and path used among the params and the data submitted if any, to trace some possible errors.

```python
from ad_api.api import sponsored_products
from ad_api.base import AdvertisingApiException

try:

    result = sponsored_products.Campaigns(debug=True).get_campaign_extended(
        campaignId=campaign_id
    )

    logging.info(result)

except AdvertisingApiException as error:
    logging.info(error)
```


```python
import logging
from ad_api.api import Profiles
from ad_api.base import AdvertisingApiException

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s"
)


def register_assistant(value: str):

    logging.info("-------------------------------------")
    logging.info("Profiles > register_assistant(%s)" % value)
    logging.info("-------------------------------------")

    try:

        result = Profiles(debug=True).register_assistant(
            country_code=value
        )
        logging.info(result)

    except AdvertisingApiException as error:
        logging.info(error)


if __name__ == '__main__':

    amz_country_code = "ES"
    register_assistant(amz_country_code)
```
Or you could do with a curl command, note the ***{"countryCode":"ES"}*** that refers to the marketplace you will operate.

```curl
curl \
    -X PUT \
    -H "Content-Type:application/json" \
    -H "Authorization: Bearer Your-Token \
    -H "Amazon-Advertising-API-ClientId: your-client-id" \
    --data '{"countryCode":"ES"}' \
     https://advertising-api-test.amazon.com/v2/profiles/register

```

### [Modules Available Common Resources](https://python-amazon-ad-api.readthedocs.io/en/latest/api.html)

* [Profiles](https://python-amazon-ad-api.readthedocs.io/en/latest/api/profiles.html)
* [Manager Accounts](https://python-amazon-ad-api.readthedocs.io/en/latest/api/manager_accounts.html)
* [Portfolios](https://python-amazon-ad-api.readthedocs.io/en/latest/api/portfolios.html)
* [Invoices](https://python-amazon-ad-api.readthedocs.io/en/latest/api/invoices.html)
* [Billing](https://python-amazon-ad-api.readthedocs.io/en/latest/api/billing.html)
* [Audiences](https://python-amazon-ad-api.readthedocs.io/en/latest/api/audiences.html)
* [Change History open Beta](https://python-amazon-ad-api.readthedocs.io/en/latest/api/history.html)
* [Creative Assets open Beta](https://python-amazon-ad-api.readthedocs.io/en/latest/api//creative_assets.html)
* [Elegibility](https://python-amazon-ad-api.readthedocs.io/en/latest/api/eligibility.html)
* [Insights](https://python-amazon-ad-api.readthedocs.io/en/latest/api/insights.html)
* [Localization](https://python-amazon-ad-api.readthedocs.io/en/latest/api/localization.html)
* [Product Selector](https://python-amazon-ad-api.readthedocs.io/en/latest/api/metadata.html)
* [Validation Configurations](https://python-amazon-ad-api.readthedocs.io/en/latest/api/validation_configurations.html)
* [Tactical recommendations beta](https://python-amazon-ad-api.readthedocs.io/en/latest/api/recommendations.html)


### [Amazon Attribution open beta](https://python-amazon-ad-api.readthedocs.io/en/latest/api/attribution.html)
* [Advertisers](https://python-amazon-ad-api.readthedocs.io/en/latest/api/attribution.html#ad_api.api.Attribution.Attribution.get_advertisers)
* [Publishers](https://python-amazon-ad-api.readthedocs.io/en/latest/api/attribution.html#ad_api.api.Attribution.Attribution.get_publishers)
* [Macro tags](https://python-amazon-ad-api.readthedocs.io/en/latest/api/attribution.html#ad_api.api.Attribution.Attribution.get_macro_tag)
* [Non Macro tags](https://python-amazon-ad-api.readthedocs.io/en/latest/api/attribution.html#ad_api.api.Attribution.Attribution.get_non_macro_template_tag)
* [Reports](https://python-amazon-ad-api.readthedocs.io/en/latest/api/attribution.html#ad_api.api.Attribution.Attribution.post_report)

### [Brand Metrics open beta](https://python-amazon-ad-api.readthedocs.io/en/latest/api/brand_metrics.html)
* [Post Report](https://python-amazon-ad-api.readthedocs.io/en/latest/api/brand_metrics.html#ad_api.api.BrandMetrics.BrandMetrics.post_report)
* [Get Report](https://python-amazon-ad-api.readthedocs.io/en/latest/api/brand_metrics.html#ad_api.api.BrandMetrics.BrandMetrics.get_report)
* [Download Report](https://python-amazon-ad-api.readthedocs.io/en/latest/api/brand_metrics.html#ad_api.api.BrandMetrics.BrandMetrics.download_report)

### [Advertising Test Account](https://python-amazon-ad-api.readthedocs.io/en/latest/api/advertising_test_account.html)
* [Create test account](https://python-amazon-ad-api.readthedocs.io/en/latest/api/advertising_test_account.html#ad_api.api.AdvertisingTestAccount)
* [Get test account information](https://python-amazon-ad-api.readthedocs.io/en/latest/api/advertising_test_account.html#ad_api.api.AdvertisingTestAccount.AdvertisingTestAccount.get_test_account)


### [Modules Available Sponsored Products 2.0](https://python-amazon-ad-api.readthedocs.io/en/latest/sp_v2.html)

Warning: [PLANNED DEPRECATION 6/30/2023]
There is a new version 3 of Sponsored Product API, please check the [migration guide](https://advertising.amazon.com/API/docs/en-us/sponsored-products/v3-migration-guide).


### [Modules Available Sponsored Products 3.0](https://python-amazon-ad-api.readthedocs.io/en/latest/sp_v3.html)


* [ThemeBased Bid Recommendation](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/bid_recommendations_v3.html)
* [Keyword Recommendations](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/ranked_keywords_recommendations.html)
* [Keywords](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/keywords_v3.html)
* [Negative Keywords](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/negative_keywords_v3.html)
* [Product Targeting](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/product_targeting.html)
* [Campaign Optimization](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/campaign_optimization_rules.html)
* [Budget Rules](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/budget_rules.html)
* [Product Ads](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/product_ads_v3.html)
* [Negative Targeting Clauses](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/negative_product_targeting_v3.html)
* [Campaign Negative Targeting Clauses](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/campaign_negative_targets.html)
* [Budget recommendations and missed opportunities](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/product_recommendations.html)
* [Budget Rules Recommendation](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/budget_rules_recommendations.html)
* [Campaigns](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/campaignsv3.html)
* [Ad Groups](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/ad_groups_v3.html)
* [Consolidated Recommendations](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/campaigns_consolidated_recommendations.html)
* [Campaign Negative Keywords](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/campaign_negative_keywords_v3.html)
* [Product Recommendations](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/budget_recommendations.html)
* [Budget Usage](https://python-amazon-ad-api.readthedocs.io/en/latest/sp/campaign_budget_usage.html)
* [Reports](https://python-amazon-ad-api.readthedocs.io/en/latest/api/reports.html)


### [Modules Available Sponsored Brands 3.0](https://python-amazon-ad-api.readthedocs.io/en/latest/sb_v3.html)

* [Campaigns](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/campaigns.html)
* [Ad Groups](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/ad_groups.html)
* [Keywords](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/keywords.html)
* [Negative Keywords](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/negative_keywords.html)
* [Product Targeting](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/product_targeting.html)
* [Negative Product Targeting](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/negative_product_targeting.html)
* [Targeting Recommendations](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/targeting_recommendations.html)
* [Bid Recommendations](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/bid_recommendations.html)
* [Stores](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/stores.html)
* [Landing Page Asins](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/landing_page_asins.html)
* [Media](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/media.html)
* [Brands](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/brands.html)
* [Moderation](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/moderation.html)
* [Reports](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/reports.html)
* [Snapshots](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/snapshots.html)

### [Modules Available Sponsored Brands 4.0](https://python-amazon-ad-api.readthedocs.io/en/latest/sb_v4.html)

* [Campaigns](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/campaigns_v4.html)
* [Ad Groups](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/ad_groups_v4.html)
* [Ads Groups](https://python-amazon-ad-api.readthedocs.io/en/latest/sb/ads_v4.html)

### [Modules Available Sponsored Display](https://python-amazon-ad-api.readthedocs.io/en/latest/sd.html)

* [Campaigns](https://python-amazon-ad-api.readthedocs.io/en/latest/sd/campaigns.html)
* [Ad Groups](https://python-amazon-ad-api.readthedocs.io/en/latest/sd/ad_groups.html)
* [Reports](https://python-amazon-ad-api.readthedocs.io/en/latest/sd/reports.html)
* [Product Ads](https://python-amazon-ad-api.readthedocs.io/en/latest/sd/product_ads.html)
* [Targets](https://python-amazon-ad-api.readthedocs.io/en/latest/sd/product_targeting.html)
* [Negative Targets](https://python-amazon-ad-api.readthedocs.io/en/latest/sd/negative_product_targeting.html)
* [Targets Recommendations](https://python-amazon-ad-api.readthedocs.io/en/latest/sd/targeting_recommendations.html)
* [Bid Recommendations](https://python-amazon-ad-api.readthedocs.io/en/latest/sd/bid_recommendations.html)
* [Creatives](https://python-amazon-ad-api.readthedocs.io/en/latest/sd/creatives.html)
* [Brand Safety List](https://python-amazon-ad-api.readthedocs.io/en/latest/sd/brand_safety.html)
* [Budget Rules](https://python-amazon-ad-api.readthedocs.io/en/latest/sd/budget_rules.html)
* [Campaigns Budget Usage](https://python-amazon-ad-api.readthedocs.io/en/latest/sd/campaign_budget_usage.html)
* [Forecasts](https://python-amazon-ad-api.readthedocs.io/en/latest/sd/forecast.html)
* [Recommendations](https://python-amazon-ad-api.readthedocs.io/en/latest/sd/recommendations.html)

### [Modules Available DSP](https://python-amazon-ad-api.readthedocs.io/en/latest/dsp.html)

* [Reports](https://python-amazon-ad-api.readthedocs.io/en/latest/dsp/reports.html)

### Simple Example Usage Campaigns with Credentials

```python
import logging
from ad_api.base import AdvertisingApiException
from ad_api.api.sp import Campaigns

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s"
)


credentials = dict(
    refresh_token='your-refresh_token',
    client_id='your-client_id',
    client_secret='your-client_secret',
    profile_id='your-profile_id',
)

try:

    states = 'enabled'

    res = Campaigns(credentials=credentials, debug=True).list_campaigns_extended(
        stateFilter=states
    )

    campaigns = res.payload
    for campaign in campaigns:
        logging.info(campaign)

    logging.info(len(campaigns))


except AdvertisingApiException as error:
    logging.info(error)

```

### API NOTICE

This API is based on the [API Client](https://github.com/saleweaver/rapid_rest_client) created by [@saleweaver](https://github.com/saleweaver) but adapted to amazon advertising authentication requeriments

### DISCLAIMER

We are not affiliated with Amazon but they used our api :)

### LICENSE

![License](https://img.shields.io/badge/license-MIT-green)
