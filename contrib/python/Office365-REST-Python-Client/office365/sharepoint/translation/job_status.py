from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.translation.job_info import TranslationJobInfo


class TranslationJobStatus(Entity):
    """The TranslationJobStatus type is used to get information about previously submitted translation jobs and
    the translation items associated with them. The type provides methods to retrieve
    TranslationJobInfo (section 3.1.5.4) and TranslationItemInfo (section 3.1.5.2) objects.
    """

    @staticmethod
    def get_all_jobs(context, return_type=None):
        if return_type is None:
            return_type = ClientResult(
                context, ClientValueCollection(TranslationJobInfo)
            )
        qry = ServiceOperationQuery(
            TranslationJobStatus(context),
            "GetAllJobs",
            None,
            None,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.Translation.TranslationJobStatus"
