from typing import Optional

from office365.runtime.client_result import ClientResult
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.translation.item_info import TranslationItemInfo


class SyncTranslator(Entity):
    """
    The SyncTranslator type is used to submit immediate translation jobs to the protocol server.

    Status: The Machine Translations Service API will no longer be supported as of the end of July 2022.
    https://go.microsoft.com/fwlink/?linkid=2187153
    """

    def __init__(self, context, target_language):
        """
        :param str target_language:
        """
        super(SyncTranslator, self).__init__(
            context,
            ServiceOperationPath(
                "SP.Translation.SyncTranslator", {"targetLanguage": target_language}
            ),
        )

    def translate(self, input_file, output_file):
        """
        The protocol client calls this method to submit an immediate translation job to the protocol server.
        The method returns a TranslationItemInfo object (section 3.1.5.2) that contains the results of the translation
        item of the immediate translation job.

        :param str input_file: This value MUST be the full or relative path to the file that contains the document
            to be translated.
            The file MUST be translatable. A file is considered translatable if it conforms to the constraints
            enumerated in the description of the inputFile parameter of the AddFile method (section 3.1.5.3.2.1.1).
        :param str output_file: This value MUST be the full or relative path to the file to where the translated
            document will be stored
        """
        payload = {"inputFile": input_file, "outputFile": output_file}
        return_type = ClientResult(self.context, TranslationItemInfo())
        qry = ServiceOperationQuery(self, "Translate", None, payload)
        self.context.add_query(qry)
        return return_type

    @property
    def output_save_behavior(self):
        # type: () -> Optional[int]
        """
        The protocol client sets this property to determine the behavior of the protocol server in the case that
        the output file already exists when a translation occurs.

        If the protocol client does not set this property, the AppendIfPossible (section 3.1.5.2.1.1) behavior is used.
        """
        return self.properties.get("OutputSaveBehavior", None)

    @property
    def entity_type_name(self):
        return "SP.Translation.SyncTranslator"
