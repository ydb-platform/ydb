from office365.runtime.client_value import ClientValue


class MachineLearningSampleMeta(ClientValue):
    @property
    def entity_type_name(self):
        return "SP.MachineLearningSampleMeta"
