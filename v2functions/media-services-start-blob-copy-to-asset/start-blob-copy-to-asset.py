import logging
import os
import json
from os import makedirs, path, environ
import datetime
import azure.functions as func
from urllib import parse
from datetime import datetime, timedelta
from __app__.MediaServicesSharedCode import AMSClient # pylint: disable=import-error
from azure.mgmt.media.models import BuiltInStandardEncoderPreset, TransformOutput, EncoderNamedPreset

from azure.storage.blob import BlockBlobService, BlobPermissions

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python Blob Trigger function processed: media-services-start-blob-copy-to-asset')
    resource_group = os.environ['MEDIA_SERVICES_RESOURCE_GROUP']
    storage_account_name = os.environ['STORAGE_ACCOUNT_NAME']
    storage_account_key = os.environ['STORAGE_ACCOUNT_KEY']

    client = AMSClient.get_ams_client(
        os.environ['MEDIA_SERVICES_CLIENT_ID'],
        os.environ['MEDIA_SERVICES_CLIENT_KEY'],
        os.environ['MEDIA_SERVICES_SUBSCRIPTION_ID'],
        os.environ['MEDIA_SERVICES_TENANT_ID']
    )

    asset_id, file_name, source_storage_account_name, source_storage_account_key, source_container = get_and_validate_params(req)

    dest_account_name, dest_container = get_asset_info(client, resource_group, storage_account_name, asset_id)
    
    # Create a new input Asset and upload the specified local video file into it.
    destination_container = start_blob_copy_async(asset_id, file_name, source_storage_account_name, source_storage_account_key, source_container,
        dest_account_name, dest_container, storage_account_key)

    logging.info(destination_container)
    return func.HttpResponse(
        json.dumps({
            'destination_container': destination_container
        })
    )

def get_asset_info(client, resource_group, account_name, asset_id):
    asset = client.assets.get(resource_group, account_name, asset_id)
    logging.info(asset)
    return asset.storage_account_name, asset.container

    # https://github.com/Azure/azure-sdk-for-python/blob/master/sdk/storage/azure-storage-blob/tests/test_block_blob_sync_copy_async.py
def start_blob_copy_async(asset_id, file_name, source_storage_account_name, source_storage_account_key, source_container, dest_account_name, dest_container, dest_storage_account_key):
    source_block_blob_service = BlockBlobService(account_name = source_storage_account_name, account_key = source_storage_account_key)
    logging.info(source_block_blob_service)
    sas_token = source_block_blob_service.generate_blob_shared_access_signature(source_container, file_name, 
        permission=BlobPermissions.READ, expiry=datetime.utcnow() + timedelta(hours=1))
    sas_url = f"https://{source_storage_account_name}.blob.core.windows.net/{source_container}/{file_name}?{sas_token}"
    logging.info(sas_token)
    logging.info(sas_url)
      
    dest_block_blob_service = BlockBlobService(account_name = dest_account_name, account_key = dest_storage_account_key)
    # TODO:  Hardcoded.  I know

    result = dest_block_blob_service.copy_blob(dest_container, file_name, sas_url, requires_sync=None)
    logging.info(result)
    return 'foo'

def get_and_validate_params(req):
    asset_id = req.params.get('asset_id')
    file_name = req.params.get('file_name')
    source_storage_account_name = req.params.get('source_storage_account_name')
    source_storage_account_key = req.params.get('source_storage_account_key')
    source_container = req.params.get('source_container')

    if not asset_id and not file_name and not source_storage_account_name and not source_storage_account_key and not source_container:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            asset_id = req_body.get('asset_id')
            asset_id = req_body.get('file_name')
            asset_id = req_body.get('source_storage_account_name')
            asset_id = req_body.get('source_storage_account_key')
            asset_id = req_body.get('source_container')

    if not asset_id and not file_name and not source_storage_account_name and not source_storage_account_key and not source_container:
        return func.HttpResponse(
             "Required parameters: asset_id, file_name, source_storage_account_name, source_storage_account_key, source_container",
             status_code=400
        )
    return asset_id, file_name, source_storage_account_name, source_storage_account_key, source_container
