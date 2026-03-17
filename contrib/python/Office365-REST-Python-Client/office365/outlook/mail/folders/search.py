from office365.outlook.mail.folders.folder import MailFolder


class MailSearchFolder(MailFolder):
    """
    A mailSearchFolder is a virtual folder in the user's mailbox that contains all the email items
    matching specified search criteria. mailSearchFolder inherits from mailFolder.
    Search folders can be created in any folder in a user's Exchange Online mailbox. However, for a search folder
    to appear in Outlook, Outlook for the web, or Outlook Live, the folder must be created in the
    WellKnownFolderName.SearchFolders folder.
    """
