import logging
import gspread
import datetime

from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

_LOG = logging.getLogger(__name__)

class FileNotFound(Exception): """ The Google drive file was not found. """

def load_credentials_file(credentials_file, scope):
    return ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)

class GoogleDrive(object):
    def __init__(self, credentials):
        self.credentials = credentials
        self._service = None
        self._gc = None

    @property
    def service(self):
        """ Lazy loaded. This allows network to not yet be available when created. """
        if self._service is None:
            self._service = build('drive', 'v3', credentials=self.credentials)
        return self._service

    @property
    def gc(self):
        """ Lazy loaded. This allows network to not yet be available when created. """
        if self._gc is None:
            self._gc = gspread.authorize(self.credentials)
        return self._gc



    def print_files(self):
        results = self.service.files().list(
            pageSize=10, fields="nextPageToken, files(id, name, webViewLink)").execute()
        items = results.get('files', [])

        if not items:
            print('No files found.')
        else:
            print('Files:')
            for item in items:
                print(u'{0} ({1})'.format(item['name'], item['id']))

    def get_or_create_folder(self, name, share_with=[]):
        folder = None
        try:
            folder_id = self.find_folder(name)
        except FileNotFound:
            folder_id = self.create_folder(name)
        permissions = self.service.permissions().list(fileId=folder_id, fields="permissions(emailAddress)").execute()['permissions']
        current_email_addresses = [perm['emailAddress'] for perm in permissions]
        for email in share_with:
            if email not in current_email_addresses:
                self.share_folder(folder_id, email)
        return folder_id


    def find_folder(self, folder_name):
        response = self.service.files().list(q="mimeType='application/vnd.google-apps.folder' and name='%s'" % folder_name,
                                          spaces='drive',
                                          fields='files(id, name)').execute()
        for f in response.get('files', []):
            return f['id']
        else:
            raise FileNotFound(folder_name)

    def create_folder(self, folder_name, parent_id = None):
        # Create a folder on Drive, returns the newely created folders ID
        body = {
          'name': folder_name,
          'mimeType': "application/vnd.google-apps.folder"
        }
        if parent_id:
            body['parents'] = [parent_id]
        root_folder = self.service.files().create(body = body).execute()
        return root_folder['id']

    def move_file_to_folder(self, file_id, folder_id):
        # Retrieve the existing parents to remove
        file = self.service.files().get(fileId=file_id,
                                        fields='parents').execute()
        previous_parents = ",".join(file.get('parents'))
        # Move the file to the new folder
        file = self.service.files().update(fileId=file_id,
                                            addParents=folder_id,
                                            removeParents=previous_parents,
                                            fields='id, parents').execute()

    def find_filename_in_folder(self, filename, folder_id):
        q = "name='%s'" % filename
        if folder_id:
            q = ("'%s' in parents and " % folder_id) + q

        response = self.service.files().list(q=q,
                                          spaces='drive',
                                          fields='files(id, name)').execute()
        for f in response.get('files', []):
            return f['id']
        else:
            raise FileNotFound('%s in %s' % (filename, folder_id))


    def share_folder(self, folder_id, email):
        def callback(request_id, response, exception):
            if exception:
                # Handle error
                print(exception)
            else:
                print("Permission Id: %s" % response.get('id'))

        batch = self.service.new_batch_http_request(callback=callback)
        user_permission = {
            'type': 'user',
            'role': 'writer',
            'emailAddress': email
        }
        batch.add(self.service.permissions().create(
                fileId=folder_id,
                body=user_permission,
                fields='id',
        ))
        batch.execute()

    def get_or_create_spreadsheet(self, filename, folder_id=None):
        try:
            file_id = self.find_filename_in_folder(filename, folder_id)
            spreadsheet = self.gc.open_by_key(file_id)
        except FileNotFound:
            spreadsheet = self.gc.create(filename)
            if folder_id:
                self.move_file_to_folder(spreadsheet.id, folder_id)
        
        return spreadsheet

class GoogleFolder(object):
    def __init__(self, gdrive, folder_id):
        self.gdrive = gdrive
        self.folder_id = folder_id

    def get_or_create_spreadsheet(self, filename):
        return self.gdrive.get_or_create_spreadsheet(filename, folder_id=self.folder_id)
    

class Worksheet(object):
    def __init__(self, spreadsheet, title, headers=[], first=False):
        self.spreadsheet = spreadsheet
        self.sheet = None
        self.headers = headers

        self._open_worksheet(title, first=first)

    def _open_worksheet(self, title, first=False):
        if first:
            self.sheet = self.spreadsheet.get_worksheet(0)
            self.sheet.update_title(title)
        else:
            try:
                self.sheet = self.spreadsheet.worksheet(title)
            except gspread.WorksheetNotFound:
                self.sheet = self.spreadsheet.add_worksheet(title=title, rows="100", cols="100")

        self.headers = self.update_headers(self.headers)

    def update_headers(self, headers):
        current_headers = self.sheet.row_values(1)
        if not headers:
            headers = []

        if current_headers:
            headers = current_headers + [h for h in headers if h not in current_headers]
            self.sheet.delete_row(1)

        if headers:
            self.sheet.insert_row(headers)

        return headers

    def append_row(self, row_d, value_input_option='USER_ENTERED'):
        row = self._row_d_to_row(row_d)

        self.sheet.append_row(row, value_input_option=value_input_option)

    def _row_d_to_row(self, row_d):
        """ Transform a dict row to a list in order based on current headers (self.headers)
        headers = ['a', 'b', 'c']
        row_d = {'a': 1, 'c': 2}

        Returns => [1, "", 2]
        """
        return [row_d.get(header, "") for header in self.headers]

    def append_rows(self, rows, value_input_option='USER_ENTERED'):
        params = {
            'valueInputOption': value_input_option
        }

        body = {
            'values': [self._row_d_to_row(row_d) for row_d in rows]
        }

        return self.sheet.spreadsheet.values_append(self.sheet.title, params, body)

