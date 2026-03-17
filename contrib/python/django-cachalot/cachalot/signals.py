from django.dispatch import Signal

# sender: name of table invalidated
# db_alias: name of database that was effected
post_invalidation = Signal()
