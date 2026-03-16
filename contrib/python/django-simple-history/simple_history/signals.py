import django.dispatch

# Arguments: "instance",  "history_instance", "history_date",
#            "history_user", "history_change_reason", "using"
pre_create_historical_record = django.dispatch.Signal()

# Arguments: "instance",  "history_instance", "history_date",
#            "history_user", "history_change_reason", "using"
post_create_historical_record = django.dispatch.Signal()

# Arguments: "sender",  "rows", "history_instance", "instance",
#             "field"
pre_create_historical_m2m_records = django.dispatch.Signal()

# Arguments: "sender",  "created_rows", "history_instance",
#            "instance", "field"
post_create_historical_m2m_records = django.dispatch.Signal()
