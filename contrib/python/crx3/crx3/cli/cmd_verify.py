# -*- coding: utf-8 -*-
import sys

from crx3 import verifier
from crx3.verifier import VerifierResult


def verify(crx_file, verbose=False):
    result, header_info = verifier.verify(crx_file)
    sys.stdout.write('{}\n'.format(result.value))
    if verbose:
        if header_info is None:
            sys.stdout.write('header info is: None\n')
        else:
            sys.stdout.write('crx id is: {}\n'.format(header_info.crx_id))
            sys.stdout.write('public key is: {}\n'.format(header_info.public_key))
    if result == VerifierResult.OK_FULL:
        return 0
    elif result == VerifierResult.ERROR_FILE_NOT_READABLE:
        return 1
    elif result == VerifierResult.ERROR_HEADER_INVALID:
        return 2
    elif result == VerifierResult.ERROR_SIGNATURE_VERIFICATION_FAILED:
        return 6
    elif result == VerifierResult.ERROR_REQUIRED_PROOF_MISSING:
        return 7
    # unreachable
    return -1
