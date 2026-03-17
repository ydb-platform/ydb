from enum import Enum


class NotificationType(str, Enum):
    # BRANDED_ITEM_CONTENT_CHANGE = "Sent whenever there is a change to the title, description, bullet points, or images, for any ASIN that the selling partner has a brand relationship with."
    BRANDED_ITEM_CONTENT_CHANGE = "BRANDED_ITEM_CONTENT_CHANGE"

    # ITEM_PRODUCT_TYPE_CHANGE = "Sent whenever there is a change to the product type name of any ASIN that the selling partner has a brand relationship with."
    ITEM_PRODUCT_TYPE_CHANGE = "ITEM_PRODUCT_TYPE_CHANGE"

    # LISTINGS_ITEM_STATUS_CHANGE = "Sent whenever there is a listing status change including buyable transition, listing create, or listing delete for any SKU that the selling partner has."
    LISTINGS_ITEM_STATUS_CHANGE = "LISTINGS_ITEM_STATUS_CHANGE"

    # LISTINGS_ITEM_ISSUES_CHANGE = "Sent whenever there is an issues change for any SKU that the selling partner has."
    LISTINGS_ITEM_ISSUES_CHANGE = "LISTINGS_ITEM_ISSUES_CHANGE"

    # LISTINGS_ITEM_MFN_QUANTITY_CHANGE = "Sent whenever there is a change in the available quantity of a listings item."
    LISTINGS_ITEM_MFN_QUANTITY_CHANGE = "LISTINGS_ITEM_MFN_QUANTITY_CHANGE"

    # PRODUCT_TYPE_DEFINITIONS_CHANGE = "Sent whenever there is a new Product Type or Product Type Version."
    PRODUCT_TYPE_DEFINITIONS_CHANGE = "PRODUCT_TYPE_DEFINITIONS_CHANGE"

    # ACCOUNT_STATUS_CHANGED = "Sent whenever the account status changes for the developers subscribed selling partner/marketplace pairs. A notification is published whenever the selling partner's account status changes between NORMAL, AT_RISK, and DEACTIVATED."
    ACCOUNT_STATUS_CHANGED = "ACCOUNT_STATUS_CHANGED"

    # ANY_OFFER_CHANGED = "Sent whenever there is a change to any of the top 20 offers, by condition (new or used), or if the external price (the price from other retailers) changes for an item listed by the seller."
    ANY_OFFER_CHANGED = "ANY_OFFER_CHANGED"

    # B2B_ANY_OFFER_CHANGED = "Sent whenever there is a change in any of the top 20 B2B offers, in the form of any price change (either single unit or quantity discount tier prices) for an item listed by the seller."
    B2B_ANY_OFFER_CHANGED = "B2B_ANY_OFFER_CHANGED"

    # DETAIL_PAGE_TRAFFIC_EVENT = "Sent at the beginning of every hour. This notification shares traffic data at an ASIN level and includes data for the previous hour, as well as any delayed data from up to 24 hours earlier. Each notification may include multiple ASINs, and a selling partner can expect to receive multiple notifications every hour."
    DETAIL_PAGE_TRAFFIC_EVENT = "DETAIL_PAGE_TRAFFIC_EVENT"

    # FBA_INVENTORY_AVAILABILITY_CHANGES = "Sent whenever there is a change in the Fulfillment By Amazon (FBA) inventory quantities. This notification includes a snapshot of the FBA inventory in all eligible marketplaces in a particular region."
    FBA_INVENTORY_AVAILABILITY_CHANGES = "FBA_INVENTORY_AVAILABILITY_CHANGES"

    # FBA_OUTBOUND_SHIPMENT_STATUS = "Sent whenever we create or cancel a Fulfillment by Amazon shipment for a seller."
    FBA_OUTBOUND_SHIPMENT_STATUS = "FBA_OUTBOUND_SHIPMENT_STATUS"

    # FEE_PROMOTION = "Sent when a promotion becomes active."
    FEE_PROMOTION = "FEE_PROMOTION"

    # FEED_PROCESSING_FINISHED = "Sent whenever any feed submitted using the Selling Partner API for Feeds reaches a feed processing status of DONE, CANCELLED, or FATAL."
    FEED_PROCESSING_FINISHED = "FEED_PROCESSING_FINISHED"

    # FULFILLMENT_ORDER_STATUS = "Sent whenever there is a change in the status of a Multi-Channel Fulfillment order."
    FULFILLMENT_ORDER_STATUS = "FULFILLMENT_ORDER_STATUS"

    # ITEM_INVENTORY_EVENT_CHANGE = "Sent at the beginning of every hour. This notification shares inventory data at an ASIN level, and includes data for the previous hour, as well as any delayed data from up to 24 hours earlier. Each notification may include multiple ASINs, and a selling partner can expect to receive multiple notifications every hour."
    ITEM_INVENTORY_EVENT_CHANGE = "ITEM_INVENTORY_EVENT_CHANGE"

    # ITEM_SALES_EVENT_CHANGE = "Sent at the beginning of every hour. This notification shares sales data at an ASIN level and includes data for the previous hour, as well as any delayed data from up to 24 hours earlier. Each notification may include multiple ASINs, and a selling partner can expect to receive multiple notifications every hour."
    ITEM_SALES_EVENT_CHANGE = "ITEM_SALES_EVENT_CHANGE"

    # ORDER_CHANGE = "Sent whenever there is an important change in the order. Important changes include order status changes and buyer requested cancelations."
    ORDER_CHANGE = "ORDER_CHANGE"

    # ORDER_STATUS_CHANGE = "Sent whenever there is a change in the status of new or existing order availability."
    ORDER_STATUS_CHANGE = "ORDER_STATUS_CHANGE"

    # PRICING_HEALTH = "Sent whenever a seller offer is ineligible to be the Featured Offer because of an uncompetitive price."
    PRICING_HEALTH = "PRICING_HEALTH"

    # REPORT_PROCESSING_FINISHED = "Sent whenever any report that you have requested using the Selling Partner API for Reports reaches a report processing status of DONE, CANCELLED, or FATAL."
    REPORT_PROCESSING_FINISHED = "REPORT_PROCESSING_FINISHED"
