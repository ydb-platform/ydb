Lenta Uplift Modeling Dataset
================================

Data description
################

An uplift modeling dataset containing data about Lenta's customers grociery shopping and related marketing campaigns.

Source: **BigTarget Hackathon** hosted by Lenta and Microsoft in summer 2020.

Fields
################

Major features:

    * ``group`` (str): treatment/control group flag
    * ``response_att`` (binary): target
    * ``gender`` (str): customer gender
    * ``age`` (float): customer age
    * ``main_format`` (int): store type (1 - grociery store, 0 - superstore)


.. list-table::
    :align: center
    :header-rows: 1
    :widths: 5 5

    * - Feature
      - Description
    * - CardHolder
      - customer id
    * - customer
      - age
    * - children
      - number of children
    * - cheque_count_[3,6,12]m_g*
      - number of customer receipts collected within last 3, 6, 12 months
        before campaign. g* is a product group
    * - crazy_purchases_cheque_count_[1,3,6,12]m
      - number of customer receipts with items purchased on "crazy"
        marketing campaign collected within last 1, 3, 6, 12 months before campaign
    * - crazy_purchases_goods_count_[6,12]m
      - items amount purchased on "crazy" marketing campaign collected
        within last 6, 12 months before campaign
    * - disc_sum_6m_g34
      - discount sum for past 6 month on a 34 product group
    * - food_share_[15d,1m]
      - food share in customer purchases for 15 days, 1 month
    * - gender
      - customer gender
    * - group
      - treatment/control group flag
    * - k_var_cheque_[15d,3m]
      - average check coefficient of variation for 15 days, 3 months
    * - k_var_cheque_category_width_15d
      - coefficient of variation of the average number of purchased
        categories (2nd level of the hierarchy) in one receipt for 15 days
    * - k_var_cheque_group_width_15d
      - coefficient of variation of the average number of purchased
        groups (1st level of the hierarchy) in one receipt for 15 days
    * - k_var_count_per_cheque_[15d,1m,3m,6m]_g*
      - unique product id (SKU) coefficient of variation for 15 days, 1, 3 ,6 months
        for g* product group
    * - k_var_days_between_visits_[15d,1m,3m]
      - coefficient of variation of the average period between visits
        for 15 days, 1 month, 3 months
    * - k_var_disc_per_cheque_15d
      - discount sum coefficient of variation for 15 days
    * - k_var_disc_share_[15d,1m,3m,6m,12m]_g*
      - discount amount coefficient of variation for 15 days, 1 month, 3 months, 6 months, 12 months
        for g* product group
    * - k_var_discount_depth_[15d,1m]
      - discount amount coefficient of variation for 15 days, 1 month
    * - k_var_sku_per_cheque_15d
      - number of unique product ids (SKU) coefficient of variation
        for 15 days
    * - k_var_sku_price_12m_g*
      - price coefficient of variation for 15 days, 3, 6, 12 months
        for g* product group
    * - main_format
      - store type (1 - grociery store, 0 - superstore)
    * - mean_discount_depth_15d
      - mean discount depth for 15 days
    * - months_from_register
      - number of months from a moment of register
    * - perdelta_days_between_visits_15_30d
      - timdelta in percent between visits during the first half
        of the month and visits during second half of the month
    * - promo_share_15d
      - promo goods share in the customer bucket
    * - response_att
      - binary target variable = store visit
    * - response_sms
      - share of customer responses to previous SMS.
        Response = store visit
    * - response_viber
      - share of responses to previous Viber messages.
        Response = store visit
    * - sale_count_[3,6,12]m_g*
      - number of purchased items from the group * for 3, 6, 12 months
    * - sale_sum_[3,6,12]m_g*
      - sum of sales from the group * for 3, 6, 12 months
    * - stdev_days_between_visits_15d
      - coefficient of variation of the days between visits for 15 days
    * - stdev_discount_depth_[15d,1m]
      - discount sum coefficient of variation for 15 days, 1 month

Key figures
################

* Format: CSV
* Size: 153M (compressed) 567M (uncompressed)
* Rows: 687,029
* Response Ratio: .1
* Treatment Ratio: .75

