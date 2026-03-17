from django.dispatch import Signal

# Args: model
post_export = Signal()
# Args: model
post_import = Signal()
