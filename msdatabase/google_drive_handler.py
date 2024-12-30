import os
import io
from typing import Dict, List

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials

from msdatabase.logger_config import get_logger

class GoogleDriveHandler:
    def __init__(self) -> None:
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initialized.")

        self.SERVICE_ACCOUNT_FILE = r'C:\Users\USUARIO\Downloads\ms-database-442116-07c57bef4cb7.json'
        self.SCOPES = ['https://www.googleapis.com/auth/drive']

    def authenticate_service_account(self) -> Credentials:
        self.logger.info("Authenticating service account.")
        try:
            creds = Credentials.from_service_account_file(self.SERVICE_ACCOUNT_FILE, scopes=self.SCOPES)
            self.logger.info("Service account authenticated successfully.")
            return creds
        except Exception as e:
            self.logger.exception(f"Error authenticating service account: {type(e)}")

    def create_drive_service(self, creds: Credentials):
        self.logger.info("Creating Google Drive service.")
        try:
            service = build('drive', 'v3', credentials=creds)
            self.logger.info("Google Drive service created successfully.")
            return service
        except Exception as e:
            self.logger.exception(f"Error creating Google Drive service: {type(e)}")

    def upload_pdf(self, service, local_pdf_path: str, drive_file_name: str) -> str:
        self.logger.info(f"Uploading PDF {local_pdf_path} to Google Drive as {drive_file_name}.")
        try:
            file_metadata = {'name': drive_file_name}
            media = MediaFileUpload(local_pdf_path, mimetype='application/pdf')
            uploaded_file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            self.logger.info(f"File uploaded successfully with ID {uploaded_file.get('id')}.")
            return uploaded_file.get('id')
        except Exception as e:
            self.logger.exception(f"Error uploading PDF: {type(e)}")

    def download_pdf(self, service, file_id: str, local_destination_path: str) -> None:
        self.logger.info(f"Downloading file with ID {file_id} to {local_destination_path}.")
        try:
            request = service.files().get_media(fileId=file_id)
            with io.FileIO(local_destination_path, 'wb') as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    self.logger.info(f"Download {int(status.progress() * 100)}%.")
            self.logger.info(f"PDF downloaded as: {local_destination_path}")
        except Exception as e:
            self.logger.exception(f"Error downloading PDF: {type(e)}")
        
    def delete_file(self, service, file_id: str) -> bool:
        self.logger.info(f"Attempting to delete file with ID {file_id}.")
        try:
            service.files().delete(fileId=file_id).execute()
            self.logger.info(f"File with ID {file_id} deleted successfully.")
            return True
        except Exception as e:
            self.logger.exception(f"Failed to delete file with ID {file_id}: {type(e)}")
            return False
            
    def remove_duplicate_files(self, service) -> None:
        self.logger.info("Removing duplicate files from Google Drive.")
        try:
            files = []
            page_token = None
            while True:
                results = service.files().list(fields="files(id, name), nextPageToken", pageToken=page_token).execute()
                files.extend(results.get('files', []))
                page_token = results.get('nextPageToken')
                if not page_token:
                    break

            if not files:
                self.logger.info("No files found.")
                return

            file_names = {}
            duplicate_files_ids = []

            for file in files:
                file_name = file['name']
                file_id = file['id']

                if file_name in file_names:
                    duplicate_files_ids.append(file_id)
                else:
                    file_names[file_name] = file_id

            if duplicate_files_ids:
                self.logger.info(f"{len(duplicate_files_ids)} duplicate files found.")

                for file_id in duplicate_files_ids:
                    self.delete_file(service, file_id)

                self.logger.info("Duplicate file removal process completed.")
            else:
                self.logger.info("No duplicates found.")

        except Exception as e:
            self.logger.exception(f"An error occurred while removing duplicate files: {type(e)}")
        
    def list_files(self, service) -> List[Dict[str, str]]:
        self.logger.info("Listing files from Google Drive.")
        try:
            file_list = []
            page_token = None

            while True:
                results = service.files().list(
                    fields="files(id, name), nextPageToken", 
                    pageToken=page_token
                ).execute()
                
                files = results.get('files', [])
                
                if not files:
                    self.logger.info("No files found.")
                    break
                
                self.logger.info(f"Found {len(files)} files.")
                for file in files:
                    self.logger.info(f"Name: {file['name']}, ID: {file['id']}")
                    file_list.append({'name': file['name'], 'id': file['id']})
                
                page_token = results.get('nextPageToken')
                if not page_token:
                    break

            return file_list

        except Exception as e:
            self.logger.exception(f"An error occurred while listing files: {type(e)}")
            return []
        
    def get_drive_info(self, service) -> Dict[str, str]:
        self.logger.info("Retrieving Google Drive storage information.")
        try:
            drive_info = service.about().get(fields="storageQuota").execute()

            quota = drive_info.get('storageQuota', {})
            total_space = quota.get('limit', 'N/A')
            used_space = quota.get('usage', 'N/A')

            if total_space != 'N/A':
                total_space = self.convert_bytes(total_space)
            if used_space != 'N/A':
                used_space = self.convert_bytes(used_space)

            self.logger.info(f"Total Space: {total_space}")
            self.logger.info(f"Used Space: {used_space}")
            
            return {
                'total_space': total_space,
                'used_space': used_space
            }

        except HttpError as e:
            self.logger.error(f"Error accessing Google Drive: {e}")
            return {}

    def convert_bytes(self, bytes_size: str) -> str:
        if bytes_size == 'N/A':
            return 'N/A'
        try:
            bytes_size = int(bytes_size)
            if bytes_size < 1024:
                return f"{bytes_size} bytes"
            elif bytes_size < 1024 * 1024:
                return f"{bytes_size / 1024:.2f} KB"
            elif bytes_size < 1024 * 1024 * 1024:
                return f"{bytes_size / (1024 * 1024):.2f} MB"
            else:
                return f"{bytes_size / (1024 * 1024 * 1024):.2f} GB"
        except ValueError as e:
            self.logger.error(f'An error occurred during conversion: {str(e)}')
            return 'Invalid size'
    

if __name__ == '__main__':
    
    handler = GoogleDriveHandler()
    creds = handler.authenticate_service_account()
    drive_service = handler.create_drive_service(creds)

    files = handler.list_files(drive_service)
    files_info = handler.get_drive_info(drive_service)

    pdf_files_to_upload = [
        (r"D:\Projects\mslookup\data\registers_pdf\100380043_01-05-2029.pdf", "100380043.pdf"),
        (r"D:\Projects\mslookup\data\registers_pdf\100431114_01-03-2025.pdf", "100431114.pdf"),
        (r"D:\Projects\mslookup\data\registers_pdf\100431422_01-10-2030.pdf", "100431422.pdf"),
        (r"D:\Projects\mslookup\data\registers_pdf\100470270_01-10-2025.pdf", "100470270.pdf"),
        (r"D:\Projects\mslookup\data\registers_pdf\100470331_01-09-2026.pdf", "100470331.pdf"),
    ]
    
    # local_pdf_path = r'D:\Projects\mslookup\data\registers_pdf\100380043_01-05-2029.pdf'
    # drive_file_name = '100380043.pdf'
    # pdf_file_id = handler.upload_pdf(drive_service, local_pdf_path, drive_file_name)
    for local_pdf_path, drive_file_name in pdf_files_to_upload:
        pdf_file_id = handler.upload_pdf(drive_service, local_pdf_path, drive_file_name)
        print(pdf_file_id)
    
    # for i in range(5):
    #     for local_pdf_path, drive_file_name in pdf_files_to_upload:
    #         pdf_file_id = handler.upload_pdf(drive_service, local_pdf_path, drive_file_name)
    #         print(pdf_file_id)
        
    handler.remove_duplicate_files(drive_service)
    files = handler.list_files(drive_service)
    
    for file in files:
        local_destination_path = os.path.join(os.path.expanduser('~'), 'Downloads', file['name'])
        handler.download_pdf(drive_service, file['id'], local_destination_path)
        
    