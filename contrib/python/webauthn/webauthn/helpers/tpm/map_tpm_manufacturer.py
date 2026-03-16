from .structs import TPM_MANUFACTURERS, TPMManufacturerInfo


def map_tpm_manufacturer_id(id: str) -> TPMManufacturerInfo:
    """
    Map a TPM manufacturer's hex ID to a manufacturer's assigned name and ASCII identifier

    Args:
        - `id`: A TPM manufacturer ID string like `"id:FFFFFFFF"`
          (a.k.a. oid "2.23.133.2.1" in SubjectAlternativeName extension)

    Returns:
        An instance of `TPMManufacturerInfo`

    Raises:
        `KeyError` on unrecognized TPM manufacturer ID
    """
    return TPM_MANUFACTURERS[id]
