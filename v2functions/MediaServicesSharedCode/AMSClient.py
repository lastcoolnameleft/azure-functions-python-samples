import os
import adal
import logging

from msrestazure.azure_active_directory import AdalAuthentication
from msrestazure.azure_cloud import AZURE_PUBLIC_CLOUD
from azure.mgmt.media import *
from azure.mgmt.media.models import *

def get_ams_client(client_id, client_key, subscription_id, tenant_id):

    login_endpoint = AZURE_PUBLIC_CLOUD.endpoints.active_directory
    resource = AZURE_PUBLIC_CLOUD.endpoints.active_directory_resource_id
    context = adal.AuthenticationContext(login_endpoint + '/' + tenant_id)
    credentials = AdalAuthentication(
        context.acquire_token_with_client_credentials,
        resource,
        client_id,
        client_key
    )
    client = AzureMediaServices(credentials, subscription_id)
    logging.info("Signed into AMS")
    return client