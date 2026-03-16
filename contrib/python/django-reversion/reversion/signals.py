from django.dispatch.dispatcher import Signal

# Signal arguments: revision and versions
pre_revision_commit = Signal()
post_revision_commit = Signal()
