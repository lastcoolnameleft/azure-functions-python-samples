import logging
import os
import json
from os import makedirs, path, environ
import datetime
import azure.functions as func
from urllib import parse
from __app__.MediaServicesSharedCode import AMSClient
from azure.mgmt.media import *
from azure.mgmt.media.models import *

from azure.storage.blob import BlockBlobService

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python Blob Trigger function processed: media-services-create-empty-asset')

    resource_group = os.environ['MEDIA_SERVICES_RESOURCE_GROUP']
    account_name = os.environ['MEDIA_SERVICES_ACCOUNT_NAME']

    asset_name = get_and_validate_params(req)
    client = AMSClient.get_ams_client()

    # Create a new input Asset and upload the specified local video file into it.
    asset_id, container_name = create_input_asset(client, resource_group, account_name, asset_name)

    return func.HttpResponse(
        json.dumps({
            'asset_id': asset_id,
            'container_name': container_name
        })
    )

def create_input_asset(client, resource_group_name, account_name, asset_name):
    """ Creates a new input Asset and uploads the specified local video file into it.

    :param resource_group_name: The name of the resource group within the Azure subscription.
    :param account_name: The Media Services account name.
    :param asset_name: The asset name.
    :return Asset
    :rtype: ~azure.mgmt.media.models.Asset
    """
    # In this example, we are assuming that the asset name is unique.
    #
    # If you already have an asset with the desired name, use the Assets.Get method
    # to get the existing asset. In Media Services v3, the Get method on entities returns null 
    # if the entity doesn't exist (a case-insensitive check on the name).

    # Call Media Services API to create an Asset.
    # This method creates a container in storage for the Asset.
    # The files (blobs) associated with the asset will be stored in this container.
    asset = client.assets.create_or_update(resource_group_name, account_name, asset_name, Asset())
    logging.info('Create/Update Asset response:')
    logging.info(asset)
    asset_id = asset.asset_id
    logging.info("asset_id=" + str(asset_id))

    # Use Media Services API to get back a response that contains
    # SAS URL for the Asset container into which to upload blobs.
    # That is where you would specify read-write permissions 
    # and the expiration time for the SAS URL.
    response = client.assets.list_container_sas(
        resource_group_name,
        account_name,
        asset_name,
        permissions = AssetContainerPermission.read_write,
        expiry_time= datetime.datetime.utcnow() + datetime.timedelta(hours = 4))
    logging.info('List Container SAS response:')
    logging.info(response)
    sasUri = response.asset_container_sas_urls[0]

    # Use Storage API to get a reference to the Asset container
    # that was created by calling Asset's CreateOrUpdate method.
    parsed_url = parse.urlparse(sasUri)

    # Remove the leading /
    container_name = parsed_url.path[1:]
    logging.info('container_name=' + container_name)

    return asset_id, container_name

def get_and_validate_params(req):
    assetName = req.params.get('assetName')
    if not assetName:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            assetName = req_body.get('assetName')

    if not assetName:
        return func.HttpResponse(
             "Required parameters:  assetName",
             status_code=400
        )
    return assetName
