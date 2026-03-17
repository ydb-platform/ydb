from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.forms.form import Form


class FormCollection(EntityCollection[Form]):
    def __init__(self, context, resource_path=None):
        """Specifies a collection of list forms for a list."""
        super(FormCollection, self).__init__(context, Form, resource_path)

    def get_by_id(self, _id):
        """Gets the form with the specified ID.

        :param str _id: Specifies the identifier of the list form.
        """
        return Form(
            self.context, ServiceOperationPath("GetById", [_id], self.resource_path)
        )

    def get_by_page_type(self, form_type):
        """
        Returns the list form with the specified page type. If there is more than one list form with
        the specified page type, the protocol server MUST return one list form as determined by the protocol server.
        If there is no list form with the specified page type, the server MUST return NULL.

        :param str or office365.sharepoint.pages.page_type.PageType form_type: Specifies the page type of the list
            form to return. It MUST be DISPLAYFORM, EDITFORM or NEWFORM.
            Type: office365.sharepoint.pages.page_type.PageType
        """
        return Form(
            self.context,
            ServiceOperationPath("GetByPageType", [form_type], self.resource_path),
        )
