__all__ = ['Routine']

from typing import Optional


class Routine:
    """
    Defines a list of constants that are routine identifiers defined by the UDS standard.
    This class provides no functionality apart from defining these constants
    """
    DeployLoopRoutineID = 0xE200
    EraseMemory = 0xFF00
    CheckProgrammingDependencies = 0xFF01
    EraseMirrorMemoryDTCs = 0xFF02

    @classmethod
    def name_from_id(cls, routine_id: int) -> Optional[str]:
        # Helper to print the type of requests (logging purpose) as defined by ISO-14229:2006, Annex F
        if not isinstance(routine_id, int) or routine_id < 0 or routine_id > 0xFFFF:
            raise ValueError('Routine ID must be a valid integer between 0 and 0xFFFF')

        if routine_id >= 0x0000 and routine_id <= 0x00FF:
            return 'ISOSAEReserved'
        if routine_id >= 0x0100 and routine_id <= 0x01FF:
            return 'TachographTestIds'
        if routine_id >= 0x0200 and routine_id <= 0xDFFF:
            return 'VehicleManufacturerSpecific'
        if routine_id >= 0xE000 and routine_id <= 0xE1FF:
            return 'OBDTestIds'
        if routine_id == 0xE200:
            return 'DeployLoopRoutineID'
        if routine_id >= 0xE201 and routine_id <= 0xE2FF:
            return 'SafetySystemRoutineIDs'
        if routine_id >= 0xE300 and routine_id <= 0xEFFF:
            return 'ISOSAEReserved'
        if routine_id >= 0xF000 and routine_id <= 0xFEFF:
            return 'SystemSupplierSpecific'
        if routine_id == 0xFF00:
            return 'EraseMemory'
        if routine_id == 0xFF01:
            return 'CheckProgrammingDependencies'
        if routine_id == 0xFF02:
            return 'EraseMirrorMemoryDTCs'
        if routine_id >= 0xFF03 and routine_id <= 0xFFFF:
            return 'ISOSAEReserved'
        return None
