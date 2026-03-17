# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params.test_helpers.terminal._reader_present_payment_method_params import (
        ReaderPresentPaymentMethodParams as ReaderPresentPaymentMethodParams,
        ReaderPresentPaymentMethodParamsCard as ReaderPresentPaymentMethodParamsCard,
        ReaderPresentPaymentMethodParamsCardPresent as ReaderPresentPaymentMethodParamsCardPresent,
        ReaderPresentPaymentMethodParamsInteracPresent as ReaderPresentPaymentMethodParamsInteracPresent,
    )
    from stripe.params.test_helpers.terminal._reader_succeed_input_collection_params import (
        ReaderSucceedInputCollectionParams as ReaderSucceedInputCollectionParams,
    )
    from stripe.params.test_helpers.terminal._reader_timeout_input_collection_params import (
        ReaderTimeoutInputCollectionParams as ReaderTimeoutInputCollectionParams,
    )

# name -> (import_target, is_submodule)
_import_map = {
    "ReaderPresentPaymentMethodParams": (
        "stripe.params.test_helpers.terminal._reader_present_payment_method_params",
        False,
    ),
    "ReaderPresentPaymentMethodParamsCard": (
        "stripe.params.test_helpers.terminal._reader_present_payment_method_params",
        False,
    ),
    "ReaderPresentPaymentMethodParamsCardPresent": (
        "stripe.params.test_helpers.terminal._reader_present_payment_method_params",
        False,
    ),
    "ReaderPresentPaymentMethodParamsInteracPresent": (
        "stripe.params.test_helpers.terminal._reader_present_payment_method_params",
        False,
    ),
    "ReaderSucceedInputCollectionParams": (
        "stripe.params.test_helpers.terminal._reader_succeed_input_collection_params",
        False,
    ),
    "ReaderTimeoutInputCollectionParams": (
        "stripe.params.test_helpers.terminal._reader_timeout_input_collection_params",
        False,
    ),
}
if not TYPE_CHECKING:

    def __getattr__(name):
        try:
            target, is_submodule = _import_map[name]
            module = import_module(target)
            if is_submodule:
                return module

            return getattr(
                module,
                name,
            )
        except KeyError:
            raise AttributeError()
