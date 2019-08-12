import logging
import os
import json
from os import makedirs, path, environ
import datetime
import azure.functions as func
from urllib import parse
from __app__.MediaServicesSharedCode import AMSClient
from azure.mgmt.media.models import BuiltInStandardEncoderPreset, TransformOutput, EncoderNamedPreset

from azure.storage.blob import BlockBlobService

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python Blob Trigger function processed: media-services-create-transform')

    resource_group = os.environ['MEDIA_SERVICES_RESOURCE_GROUP']
    account_name = os.environ['MEDIA_SERVICES_ACCOUNT_NAME']

    transform_name = get_and_validate_params(req)
    client = AMSClient.get_ams_client(
        os.environ['MEDIA_SERVICES_CLIENT_ID'],
        os.environ['MEDIA_SERVICES_CLIENT_KEY'],
        os.environ['MEDIA_SERVICES_SUBSCRIPTION_ID'],
        os.environ['MEDIA_SERVICES_TENANT_ID']
    )

    # Create a new input Asset and upload the specified local video file into it.
    transform = get_or_create_transform(client, resource_group, account_name, transform_name)
    logging.info(transform)
    return func.HttpResponse(
        json.dumps({
            'name': transform.name,
            'id': transform.id
        })
    )

def get_or_create_transform(client, resource_group_name, account_name, transform_name):
    """If the specified transform exists, get that transform.
    If the it does not exist, creates a new transform with the specified output. 
    In this case, the output is set to encode a video using one of the built-in encoding presets.

    :param resource_group_name: The name of the resource group within the Azure subscription.
    :param account_name: The Media Services account name.
    :param transform_name: The transform name.
    :return Transform
    :rtype: ~azure.mgmt.media.models.Transform
    """        
    # Does a Transform already exist with the desired name? Assume that an existing Transform with the desired name
    # also uses the same recipe or Preset for processing content.
    transform = client.transforms.get(resource_group_name, account_name, transform_name)

    if not transform:            

        # The preset for the Transform is set to one of Media Services built-in sample presets.
        # You can  customize the encoding settings by changing this to use "StandardEncoderPreset" class.
        preset = BuiltInStandardEncoderPreset(preset_name = EncoderNamedPreset.adaptive_streaming)

        transformOutput = TransformOutput(preset = preset)

        # You need to specify what you want it to produce as an output
        output = [transformOutput]

        # Create the Transform with the output defined above
        transform = client.transforms.create_or_update(resource_group_name, account_name, transform_name, output)

    return transform

def get_and_validate_params(req):
    transform_name = req.params.get('transform_name')
    if not transform_name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            transform_name = req_body.get('transform_name')

    if not transform_name:
        return func.HttpResponse(
             "Required parameters:  transform_name",
             status_code=400
        )
    return transform_name
