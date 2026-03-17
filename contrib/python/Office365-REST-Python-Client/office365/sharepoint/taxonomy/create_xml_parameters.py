import uuid

from office365.runtime.client_value import ClientValue


class TaxonomyFieldCreateXmlParameters(ClientValue):
    def __init__(
        self,
        name,
        term_set_id,
        term_store_id=None,
        anchor_id="00000000-0000-0000-0000-000000000000",
        allow_multiple_values=False,
    ):
        """
        :param str name:
        """
        self.Name = name
        self.SspId = term_store_id
        self.TermSetId = term_set_id
        self.AnchorId = anchor_id
        self.FieldId = str(uuid.uuid1())
        self.TextFieldId = None
        self.WebId = None
        self.ListId = None
        self.AllowMultipleValues = allow_multiple_values

    @property
    def type_name(self):
        return (
            "TaxonomyFieldTypeMulti"
            if self.AllowMultipleValues
            else "TaxonomyFieldType"
        )

    @property
    def schema_xml(self):
        list_attr = (
            'List="{{{list_id}}}"'.format(list_id=self.ListId)
            if self.ListId is not None
            else ""
        )

        return """
            <Field Type="{type_name}" DisplayName="{name}" {list_attr}
                   WebId="{web_id}" Required="FALSE" EnforceUniqueValues="FALSE"
                   ID="{{{field_id}}}" StaticName="{name}" Name="{name}" Mult="{allow_multiple_values}">
                <Default/>
                <Customization>
                    <ArrayOfProperty>
                        <Property>
                            <Name>SspId</Name>
                            <Value xmlns:q1="http://www.w3.org/2001/XMLSchema" p4:type="q1:string"
                                   xmlns:p4="http://www.w3.org/2001/XMLSchema-instance">{ssp_id}
                            </Value>
                        </Property>
                        <Property>
                            <Name>GroupId</Name>
                        </Property>
                        <Property>
                            <Name>TermSetId</Name>
                            <Value xmlns:q2="http://www.w3.org/2001/XMLSchema" p4:type="q2:string"
                                   xmlns:p4="http://www.w3.org/2001/XMLSchema-instance">{term_set_id}
                            </Value>
                        </Property>
                        <Property>
                            <Name>AnchorId</Name>
                            <Value xmlns:q3="http://www.w3.org/2001/XMLSchema" p4:type="q3:string"
                                   xmlns:p4="http://www.w3.org/2001/XMLSchema-instance">{anchor_id}
                            </Value>
                        </Property>
                        <Property>
                            <Name>UserCreated</Name>
                            <Value xmlns:q4="http://www.w3.org/2001/XMLSchema" p4:type="q4:boolean"
                                   xmlns:p4="http://www.w3.org/2001/XMLSchema-instance">false
                            </Value>
                        </Property>
                        <Property>
                            <Name>Open</Name>
                            <Value xmlns:q5="http://www.w3.org/2001/XMLSchema" p4:type="q5:boolean"
                                   xmlns:p4="http://www.w3.org/2001/XMLSchema-instance">true
                            </Value>
                        </Property>
                        <Property>
                            <Name>TextField</Name>
                            <Value xmlns:q6="http://www.w3.org/2001/XMLSchema" p4:type="q6:string"
                                   xmlns:p4="http://www.w3.org/2001/XMLSchema-instance">{{{text_field_id}}}
                            </Value>
                        </Property>
                        <Property>
                            <Name>IsPathRendered</Name>
                            <Value xmlns:q7="http://www.w3.org/2001/XMLSchema" p4:type="q7:boolean"
                                   xmlns:p4="http://www.w3.org/2001/XMLSchema-instance">false
                            </Value>
                        </Property>
                        <Property>
                            <Name>IsKeyword</Name>
                            <Value xmlns:q8="http://www.w3.org/2001/XMLSchema" p4:type="q8:boolean"
                                   xmlns:p4="http://www.w3.org/2001/XMLSchema-instance">false
                            </Value>
                        </Property>
                        <Property>
                            <Name>TargetTemplate</Name>
                        </Property>
                        <Property>
                            <Name>CreateValuesInEditForm</Name>
                            <Value xmlns:q9="http://www.w3.org/2001/XMLSchema" p4:type="q9:boolean"
                                   xmlns:p4="http://www.w3.org/2001/XMLSchema-instance">false
                            </Value>
                        </Property>
                        <Property>
                            <Name>FilterAssemblyStrongName</Name>
                            <Value xmlns:q10="http://www.w3.org/2001/XMLSchema" p4:type="q10:string"
                                   xmlns:p4="http://www.w3.org/2001/XMLSchema-instance">Microsoft.SharePoint.Taxonomy,
                                Version=16.0.0.0, Culture=neutral, PublicKeyToken=71e9bce111e9429c
                            </Value>
                        </Property>
                        <Property>
                            <Name>FilterClassName</Name>
                            <Value xmlns:q11="http://www.w3.org/2001/XMLSchema" p4:type="q11:string"
                                   xmlns:p4="http://www.w3.org/2001/XMLSchema-instance">Microsoft.SharePoint.Taxonomy.TaxonomyField
                            </Value>
                        </Property>
                        <Property>
                            <Name>FilterMethodName</Name>
                            <Value xmlns:q12="http://www.w3.org/2001/XMLSchema" p4:type="q12:string"
                                   xmlns:p4="http://www.w3.org/2001/XMLSchema-instance">GetFilteringHtml
                            </Value>
                        </Property>
                        <Property>
                            <Name>FilterJavascriptProperty</Name>
                            <Value xmlns:q13="http://www.w3.org/2001/XMLSchema" p4:type="q13:string"
                                   xmlns:p4="http://www.w3.org/2001/XMLSchema-instance">FilteringJavascript
                            </Value>
                        </Property>
                    </ArrayOfProperty>
                </Customization>
            </Field>
            """.format(
            name=self.Name,
            list_attr=list_attr,
            web_id=self.WebId,
            field_id=self.FieldId,
            ssp_id=self.SspId,
            term_set_id=self.TermSetId,
            anchor_id=self.AnchorId,
            text_field_id=self.TextFieldId,
            allow_multiple_values=str(self.AllowMultipleValues).upper(),
            type_name=self.type_name,
        )
