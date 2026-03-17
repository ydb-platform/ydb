from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class TaxonomyFieldValue(ClientValue):
    def __init__(self, label=None, term_guid=None, wss_id=-1):
        """
        Represents a single value held in a TaxonomyField (section 3.1.5.27) object.

        :param str label: Specifies the label of the TaxonomyField (section 3.1.5.27) object.
        :parm str term_guid: Specifies a string representing Term (section 3.1.5.16) GUID.
        :parm int wss_id: Specifies the list item identifier of the list item containing the TaxonomyFieldValue
            that is encapsulated by the TaxonomyFieldValue (section 3.1.5.13) object.
        """
        super(TaxonomyFieldValue, self).__init__()
        self.Label = label
        self.TermGuid = term_guid
        self.WssId = wss_id

    def __str__(self):
        return "{0};#{1}|{2}".format(self.WssId, self.Label, self.TermGuid)

    @property
    def entity_type_name(self):
        return "SP.Taxonomy.TaxonomyFieldValue"


class TaxonomyFieldValueCollection(ClientValueCollection[TaxonomyFieldValue]):
    """Represents the multi-value object for the taxonomy column."""

    def __init__(self, initial_values):
        """
        :param list[TaxonomyFieldValue] initial_values:
        """
        super(TaxonomyFieldValueCollection, self).__init__(
            TaxonomyFieldValue, initial_values
        )

    def __str__(self):
        return ";#".join([str(item) for item in self._data])
