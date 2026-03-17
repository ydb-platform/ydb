from django.db import transaction


class atomic_if_using_transaction:
    """Context manager wraps `atomic` if `using_transactions`.

    Replaces code::

        if using_transactions:
            with transaction.atomic(using=using):
                return something()
        return something()
    """

    def __init__(self, using_transactions, using):
        self.using_transactions = using_transactions
        if using_transactions:
            self.context_manager = transaction.atomic(using=using)

    def __enter__(self):
        if self.using_transactions:
            self.context_manager.__enter__()

    def __exit__(self, *args):
        if self.using_transactions:
            self.context_manager.__exit__(*args)


def get_related_model(field):
    if hasattr(field, "related_model"):
        return field.related_model
