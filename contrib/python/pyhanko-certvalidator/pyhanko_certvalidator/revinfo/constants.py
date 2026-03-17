KNOWN_CRL_EXTENSIONS = {
    'issuer_alt_name',
    'crl_number',
    'delta_crl_indicator',
    'issuing_distribution_point',
    'authority_key_identifier',
    'freshest_crl',
    'authority_information_access',
}
VALID_REVOCATION_REASONS = {
    'key_compromise',
    'ca_compromise',
    'affiliation_changed',
    'superseded',
    'cessation_of_operation',
    'certificate_hold',
    'privilege_withdrawn',
    'aa_compromise',
}
KNOWN_CRL_ENTRY_EXTENSIONS = {
    'crl_reason',
    'hold_instruction_code',
    'invalidity_date',
    'certificate_issuer',
}
