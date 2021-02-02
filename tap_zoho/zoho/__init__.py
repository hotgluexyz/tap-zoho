"""
Created on Dec 14, 2019

@author: Juned Jabbar
"""

import singer
import zcrmsdk
import singer.utils as singer_utils
import dateutil.parser as parser
import datetime
import backoff
import requests
from requests.exceptions import RequestException

from zcrmsdk import ZCRMRestClient, ZCRMModule, ZohoOAuthClient, ZCRMOrganization
from singer import metadata

from tap_zoho.zoho.exceptions import TapZohoException
from tap_zoho.zoho.rest import Rest
from tap_zoho.zoho.bulk import Bulk

LOGGER = singer.get_logger()

ZOHO_UNSUPPORTED_API_MODULES = {
    'Home',
    'Reports',
    'Dashboards',
    'SalesInbox',
    'Feeds',
    'Documents',
    'Forecasts',
    'Social',
    'Visits',
    'Actions_Performed',
    'Events'
}

ZOHO_STANDARD_MODULES = {
    'Activities',
    'Accounts',
    'Leads',
    'Contacts',
    'Deals',
    'Tasks',
    'Calls',
    'Analytics',
    'Products',
    'Quotes',
    'Sales_Orders',
    'Purchase_Orders',
    'Invoices',
    'Vendors',
    'Price_Books',
    'Cases',
    'Solutions'
}

ORDER_INVOIVE_TABLE = set([
    'Sales_Orders',
    'Invoices'
])

LOOKUP_TYPES = set([
    'ownerlookup',
    'lookup'
])

STRING_TYPES = set([
    'id',
    'text',
    'picklist',
    'phone',
    'lookup',
    'website',
    'textarea',
    'profileimage',
    'multiselectpicklist',
    'email',
    # I don't know what data returns for these, so defaulting them to string for now
    # Until I know what to do with them
    'RRULE',
    'event_reminder',
    'ALARM'
])

DATE_TYPES = set([
    'datetime',
    'date'
])

NUMBER_TYPES = set([
    'double',
    'currency',
    'percent',
    'bigint',
    'autonumber',
])

ORDER_ADDITIONAL_FIELDS = [
    {'api_name': 'Product_Code', 'data_type': 'text'},
    {'api_name': 'Product_Category', 'data_type': 'text'},
    {'api_name': 'Product_Id', 'data_type': 'text', 'zoho_key': 'id'},
    {'api_name': 'Product_Name', 'data_type': 'text', 'zoho_key': 'name'},
    {'api_name': 'Quantity', 'data_type': 'integer', 'zoho_key': 'quantity'},
    {'api_name': 'Discount', 'data_type': 'integer', 'zoho_key': 'quantity'},
    {'api_name': 'Net_Item_Cost', 'data_type': 'integer', 'zoho_key': 'net_total'},
    {'api_name': 'Unit_Price', 'data_type': 'integer', 'zoho_key': 'net_total'},
    {'api_name': 'Tax', 'data_type': 'integer'},
    {'api_name': 'List_Price', 'data_type': 'integer', 'zoho_key': 'list_price'},
    {'api_name': 'Unit_Price', 'data_type': 'integer', 'zoho_key': 'unit_price'},
    {'api_name': 'Quantity_In_Stock', 'data_type': 'integer', 'zoho_key': 'quantity_in_stock'},
    {'api_name': 'Total_Item_Cost', 'data_type': 'integer', 'zoho_key': 'total'},
    {'api_name': 'Total_Item_Cost_After_Discount', 'data_type': 'integer', 'zoho_key': 'total_after_discount'},
    {'api_name': 'Product_Description', 'data_type': 'integer', 'zoho_key': 'product_description'},
    {'api_name': 'Line_Tax', 'data_type': 'text', 'zoho_key': 'line_tax'}
]

BULK_API_TYPE = "BULK"
REST_API_TYPE = "REST"


def log_backoff_attempt(details):
    LOGGER.info("ConnectionError detected, triggering backoff: %d try", details.get("tries"))


def add_additional_fields(sobject, fields):
    if sobject in ORDER_INVOIVE_TABLE:
        fields.extend(list(ORDER_ADDITIONAL_FIELDS))


class Zoho():

    def __init__(self,
                 config=None,
                 default_start_date=None,
                 is_sandbox=None,
                 api_type=None):
        self.default_start_date = default_start_date
        self.config = config
        # Disabling BULK by default
        self.pk_chunking = False
        self.is_sandbox = is_sandbox is True or (isinstance(is_sandbox, str) and is_sandbox.lower() == 'true')
        self.select_fields_by_default = config['select_fields_by_default'] is True or (
                isinstance(config['select_fields_by_default'], str)
                and
                config['select_fields_by_default'].lower() == 'true')

        self.api_type = api_type.upper() if api_type else None
        self.ZohoOAuthClient = None
        self.session = requests.Session()

        # validate start_date
        singer_utils.strptime(default_start_date)

    def describe(self, sobject=None):
        """Describes all objects or a specific object"""
        if sobject is None:
            to_return = ZCRMRestClient.get_instance().get_all_modules()
            # resp = ZCRMOrganization.get_instance().get_all_users()
            return to_return.response_json
        else:
            to_return = ZCRMModule.get_instance(sobject).get_all_fields()
            return to_return.response_json

    def get_start_date(self, state, catalog_entry, add_1_sec=False):
        catalog_metadata = metadata.to_map(catalog_entry['metadata'])
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        start_date = (singer.get_bookmark(state,
                                          catalog_entry['tap_stream_id'],
                                          replication_key) or self.default_start_date)

        if add_1_sec:
            start_date = parser.parse(start_date) + datetime.timedelta(0, 1)
        else:
            start_date = parser.parse(start_date)
        return start_date.isoformat()

    def get_data(self, sobject=None, state=None, catalog=None):
        if self.api_type == REST_API_TYPE:
            return Rest(self).get_data(sobject, state, catalog)
        elif self.api_type == BULK_API_TYPE:
            return Bulk(self).get_data(sobject, state, catalog)
        else:
            raise TapZohoException(
                "api_type should be REST or BULK was: {}".format(
                    self.api_type))

    # pylint: disable=too-many-arguments
    @backoff.on_exception(backoff.expo,
                          requests.exceptions.ConnectionError,
                          max_tries=10,
                          factor=2,
                          on_backoff=log_backoff_attempt)
    def _make_request(self, http_method, url, headers=None, body=None, stream=False, params=None):
        if http_method == "GET":
            LOGGER.info("Making %s request to %s with params: %s", http_method, url, params)
            resp = self.session.get(url, headers=headers, stream=stream, params=params)
        elif http_method == "POST":
            LOGGER.info("Making %s request to %s with body %s", http_method, url, body)
            resp = self.session.post(url, headers=headers, data=body)
        else:
            raise TapZohoException("Unsupported HTTP method")

        try:
            resp.raise_for_status()
        except RequestException as ex:
            raise ex

        LOGGER.info('Check for headers')

        return resp

    # Login with refresh token, .pkl file is generated under {tmp_directory}/refresh_token/*.pkl
    # Zoho SDK uses this file to manage login state.
    # This file is removed upon start and completion of this tap.
    def login(self):
        """
            # Login with refresh token, .pkl file is generated under {tmp_directory}/refresh_token/*.pkl
            # Zoho SDK uses this file to manage login state.
            # This file is removed upon start and completion of this tap.
        """
        zcrmsdk.ZCRMRestClient.initialize(self.config)
        ins = ZohoOAuthClient.get_instance(self.config)
        ins.generate_access_token_from_refresh_token(refreshToken=self.config['refresh_token'], userEmail=None)
        self.ZohoOAuthClient = ins

    def append_product_record(self, rec, product_record):
        # Normalize Product_Record first and then append them to rec
        product = product_record['product']
        del product_record['product']
        for p in product:
            product_record[p] = product[p]

        for field in ORDER_ADDITIONAL_FIELDS:
            if 'zoho_key' in field:
                field_name = field['zoho_key']
            else:
                field_name = field['api_name']

            rec[field['api_name']] = product_record[field_name]
