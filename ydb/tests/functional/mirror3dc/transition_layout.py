"""Check RFC 192's confirmed guarantee E using physical VDisk copies."""

from ydb.core.protos.blobstorage_base3_pb2 import TDataCenterState, TGroupState


_ENSURED_LAYOUTS = {
    TGroupState.NORMAL: ((1, 1, 1), (2, 2)),
    TGroupState.DC_FAILURE: ((1, 1, 1), (2, 2)),
    TGroupState.DC_FAILURE_INV1: ((1, 1),),
    TGroupState.DC_FAILURE_INV2_PENDING: ((1, 1),),
    TGroupState.DC_FAILURE_INV2: ((2, 2),),
    TGroupState.DC_RECOVERY_INV1: ((1, 1),),
    TGroupState.DC_SOLO_INV1: ((1,),),
    TGroupState.DC_SOLO_INV2_PENDING: ((1,),),
    TGroupState.DC_SOLO_INV2: ((2,),),
    TGroupState.DC_SOLO_RECOVERY_INV1_PENDING: ((1,),),
    TGroupState.DC_SOLO_RECOVERY_INV1: ((1,),),
    TGroupState.DC_SOLO_RECOVERY_INV2_PENDING: ((2,),),
    TGroupState.DC_SOLO_RECOVERY_INV2: ((2,),),
}


def check_ensured_layout(test, state, realm_states, blobs):
    """Check fixed, acknowledged blobs; live in-flight writes need not satisfy E.

    ``realm_states`` maps physical realm indices to roles; each blob maps to a
    set of physical ``(realm, domain)`` copies. Recovering and Excluded copies
    never count towards E. Leaving contributes only in NORMAL/DC_FAILURE.
    """
    test.assertIn(state, _ENSURED_LAYOUTS, f'Unknown mirror3dc state: {state}')
    test.assertEqual(set(realm_states), {0, 1, 2}, 'Expected three physical fail realms')
    alternatives = _ENSURED_LAYOUTS[state]
    allowed_roles = {TDataCenterState.INCLUDED}
    if state in (TGroupState.NORMAL, TGroupState.DC_FAILURE):
        allowed_roles.add(TDataCenterState.LEAVING)
    realms = {realm for realm, role in realm_states.items() if role in allowed_roles}
    state_name = TGroupState.EState.Name(state)
    test.assertEqual(len(realms), max(map(len, alternatives)),
                     f'{state_name}: invalid realms for E: {realm_states}')
    expected = ' OR '.join('+'.join(map(str, alternative)) for alternative in alternatives)
    for blob, layout in blobs.items():
        copies = {
            realm: len({domain for physical_realm, domain in layout if physical_realm == realm})
            for realm in sorted(realms)
        }
        counts = sorted(copies.values(), reverse=True)
        test.assertTrue(
            any(all(counts[index] >= minimum for index, minimum in enumerate(alternative))
                for alternative in alternatives),
            f'{state_name}: blob {blob} violates E={expected}; '
            f'copies={copies}; physical layout={sorted(layout)}',
        )
