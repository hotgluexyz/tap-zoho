"""
Created on Dec 14, 2019

@author: Juned Jabbar
"""
import singer
import singer.utils as singer_utils
import sys
import json
import os
import tempfile
import shutil
import tap_zoho.zoho as zoho
from singer import metadata, metrics

from tap_zoho.sync import get_stream_version, sync_records
from tap_zoho.zoho import Zoho
from tap_zoho.zoho.exceptions import TapZohoQuotaExceededException, TapZohoException, TapZohoDataNotFoundException

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ['client_id',
                        'client_secret',
                        'redirect_uri',
                        'refresh_token']

CONFIG = {
    'refresh_token': None,
    'sandbox': "False",
    'client_id': None,
    'client_secret': None,
    'redirect_uri': None,
    'accounts_url': 'https://accounts.zoho.com',
    'token_persistence_path': "",
    'currentUserEmail': 'dummy@dummy.com',
    'persistence_handler_class': 'ZohoAuthPersistence',
    'persistence_handler_path': 'tap_zoho.zohoAuthPersistence',
    "api_type": "BULK",
    "current_dir": os.getcwd()
}


def field_to_property_schema(field, mdata):  # pylint:disable=too-many-branches
    property_schema = {}

    field_name = field['api_name']
    zh_type = field['data_type']

    if CONFIG.get('api_type') == 'REST':
        if zh_type in zoho.LOOKUP_TYPES:
            # Lookup fields usually have __id and __name fields, defaulting
            # values for these as 'string'
            property_schema['type'] = ["null", "object"]
            property_schema['properties'] = {
                "name": {"type": ["null", "string"]},
                "id": {"type": ["null", "string"]}
            }
        elif zh_type == 'integer':
            property_schema['anyOf'] = [{"type": "null"}, {"type": "integer"}]
        elif zh_type in zoho.DATE_TYPES:
            date_type = {"type": "string", "format": "date-time"}
            string_type = {"type": ["string", "null"]}
            property_schema["anyOf"] = [string_type, date_type]
        elif zh_type in zoho.STRING_TYPES and field_name == 'Product_Details':
            property_schema['type'] = "string"
        elif zh_type in zoho.STRING_TYPES:
            property_schema['type'] = "string"
        elif zh_type in zoho.NUMBER_TYPES:
            property_schema['type'] = "number"
        elif zh_type == "boolean" or zh_type == 'bool':
            property_schema['type'] = "boolean"
        else:
            # Add whichever is the field name
            raise TapZohoException("Found unsupported type: {} for field {}".format(zh_type, field_name))

    elif CONFIG.get('api_type') == 'BULK':
        # BULK PROCESS
        if zh_type in zoho.DATE_TYPES:
            date_type = {"type": "string", "format": "date-time"}
            string_type = {"type": ["string", "null"]}
            property_schema["anyOf"] = [string_type, date_type]
        else:
            property_schema['type'] = "string"

    return property_schema, mdata


# pylint: disable=undefined-variable
def create_property_schema(field, mdata):
    field_name = field['api_name']

    if field_name == "id":
        mdata = metadata.write(
            mdata, ('properties', field_name), 'inclusion', 'automatic')
    else:
        mdata = metadata.write(
            mdata, ('properties', field_name), 'inclusion', 'available')

    property_schema, mdata = field_to_property_schema(field, mdata)

    return (property_schema, mdata)


def get_replication_key(fields):
    fields_list = [f['api_name'] for f in fields]

    if 'Modified_Time' in fields_list:
        return 'Modified_Time'
    return None


# pylint: disable=too-many-branches,too-many-statements
def do_discover(zh):
    """Describes a Zoho instance's objects and generates a JSON schema for each field."""
    global_description = zh.describe()

    objects_to_discover = {o['api_name'] for o in global_description['modules']}

    key_properties = ['id']

    for module in global_description['modules']:
        LOGGER.info('Found Zoho Module {}.'.format(module['api_name']))

    # For each ZH Object describe it, loop its fields and build a schema
    entries = []
    for sobject_name in objects_to_discover:
        if sobject_name not in zoho.ZOHO_UNSUPPORTED_API_MODULES:
            LOGGER.info('Fetching fields for {}.'.format(sobject_name))
            sobject_description = zh.describe(sobject_name)
            fields = sobject_description['fields']

            if zh.api_type == 'REST':
                zoho.add_additional_fields(sobject_name, fields)

            for field in fields:
                LOGGER.info(
                    'Field {} of Table {} has Data type {}'.format(field['api_name'], sobject_name, field['data_type']))

            replication_key = get_replication_key(fields)

            properties = {}
            mdata = metadata.new()

            # Adding 'id' in the schema as the first property because this doesn't come with fields by default
            property_schema, mdata = create_property_schema({'api_name': key_properties[0], 'data_type': 'bigint'},
                                                            mdata)
            mdata = metadata.write(
                mdata, ('properties', key_properties[0]), 'inclusion', 'automatic')
            properties[key_properties[0]] = property_schema

            # Loop over the object's fields
            for f in fields:
                field_name = f['api_name']
                LOGGER.info('Object {} Data Type {} of field {}'.format(sobject_name, f['data_type'], field_name))
                property_schema, mdata = create_property_schema(
                    f, mdata)

                inclusion = metadata.get(
                    mdata, ('properties', field_name), 'inclusion')

                if zh.select_fields_by_default and inclusion != 'unsupported':
                    mdata = metadata.write(
                        mdata, ('properties', field_name), 'selected-by-default', True)

                properties[field_name] = property_schema

            if replication_key:
                mdata = metadata.write(
                    mdata, ('properties', replication_key), 'inclusion', 'automatic')

            if replication_key:
                mdata = metadata.write(
                    mdata, (), 'valid-replication-keys', [replication_key])
                mdata = metadata.write(
                    mdata, (), 'replication-key', replication_key)

            mdata = metadata.write(
                mdata, (), 'replication-method', 'INCREMENTAL')

            mdata = metadata.write(mdata, (), 'table-key-properties', key_properties)

            schema = {
                'type': 'object',
                'properties': properties
            }

            entry = {
                'stream': sobject_name,
                'tap_stream_id': sobject_name,
                'schema': schema,
                'metadata': metadata.to_list(mdata)
            }

            entries.append(entry)

    result = {'streams': entries}
    json.dump(result, sys.stdout, indent=4)

    # Move this code to some Utility class for re-usability
    if os.environ.get('propertiesFile') is not None:
        file = '{}/{}'.format(os.environ['propertiesFile'], '/properties.json')
        with open(file, 'w+') as outfile:
            json.dump(result, outfile, indent=4)


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def build_state(raw_state, catalog):
    state = {}

    for catalog_entry in catalog['streams']:
        tap_stream_id = catalog_entry['tap_stream_id']
        catalog_metadata = metadata.to_map(catalog_entry['metadata'])
        replication_method = catalog_metadata.get((), {}).get('replication-method')

        version = singer.get_bookmark(raw_state,
                                      tap_stream_id,
                                      'version')

        # Preserve state that deals with resuming an incomplete bulk job
        if singer.get_bookmark(raw_state, tap_stream_id, 'JobID'):
            job_id = singer.get_bookmark(raw_state, tap_stream_id, 'JobID')
            batches = singer.get_bookmark(raw_state, tap_stream_id, 'BatchIDs')
            current_bookmark = singer.get_bookmark(raw_state, tap_stream_id, 'JobHighestBookmarkSeen')
            state = singer.write_bookmark(state, tap_stream_id, 'JobID', job_id)
            state = singer.write_bookmark(state, tap_stream_id, 'BatchIDs', batches)
            state = singer.write_bookmark(state, tap_stream_id, 'JobHighestBookmarkSeen', current_bookmark)

        if replication_method == 'INCREMENTAL':
            replication_key = catalog_metadata.get((), {}).get('replication-key')
            replication_key_value = singer.get_bookmark(raw_state,
                                                        tap_stream_id,
                                                        replication_key)

            if version is not None:
                state = singer.write_bookmark(
                    state, tap_stream_id, 'version', version)

            if replication_key_value is not None:
                state = singer.write_bookmark(
                    state, tap_stream_id, replication_key, replication_key_value)

    return state


def do_sync(zh, catalog, state):
    starting_stream = state.get("current_stream")

    if starting_stream:
        LOGGER.info("Resuming sync from %s", starting_stream)
    else:
        LOGGER.info("Starting sync")

    for catalog_entry in catalog["streams"]:
        stream_version = get_stream_version(catalog_entry, state)
        stream = catalog_entry['stream']
        stream_alias = catalog_entry.get('stream_alias')
        stream_name = catalog_entry["tap_stream_id"]
        activate_version_message = singer.ActivateVersionMessage(
            stream=(stream_alias or stream), version=stream_version)

        catalog_metadata = metadata.to_map(catalog_entry['metadata'])
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        mdata = metadata.to_map(catalog_entry['metadata'])

        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        if starting_stream:
            if starting_stream == stream_name:
                LOGGER.info("%s: Resuming", stream_name)
                starting_stream = None
            else:
                LOGGER.info("%s: Skipping - already synced", stream_name)
                continue
        else:
            LOGGER.info("%s: Starting", stream_name)

        state["current_stream"] = stream_name
        singer.write_state(state)
        key_properties = metadata.to_map(catalog_entry['metadata']).get((), {}).get('table-key-properties')
        singer.write_schema(
            stream,
            catalog_entry['schema'],
            key_properties,
            replication_key,
            stream_alias)

        job_id = singer.get_bookmark(state, catalog_entry['tap_stream_id'], 'JobID')

        if job_id:
            with metrics.record_counter(stream) as counter:
                LOGGER.info("Found JobID from previous Bulk Query. Resuming sync for job: %s", job_id)
        else:
            # Tables with a replication_key or an empty bookmark will emit an
            # activate_version at the beginning of their sync
            bookmark_is_empty = state.get('bookmarks', {}).get(catalog_entry['tap_stream_id']) is None
            if replication_key or bookmark_is_empty:
                singer.write_message(activate_version_message)
                state = singer.write_bookmark(state,
                                              catalog_entry['tap_stream_id'],
                                              'version',
                                              stream_version)
                counter = sync_stream(zh, catalog_entry, state)
                LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter.value)

    state["current_stream"] = None
    singer.write_state(state)
    LOGGER.info("Finished sync")


def sync_stream(zh, catalog_entry, state):
    stream = catalog_entry['stream']

    with metrics.record_counter(stream) as counter:
        try:
            sync_records(zh, catalog_entry, state, counter)
            singer.write_state(state)
        except Exception as ex:
            raise Exception("Error syncing {}: {}".format(
                stream, ex)) from ex

    return counter


def initialize():
    token_directory = os.path.join(
        tempfile.gettempdir(),
        CONFIG['refresh_token'])
    try:
        do_cleanup()
        os.mkdir(token_directory)
    except Exception as e:
        LOGGER.error(
            'An error occurred while initializing tap-zoho. Unable to create directory {}'.format(token_directory))
        raise e


def do_cleanup():
    token_directory = os.path.join(
        tempfile.gettempdir(),
        CONFIG['refresh_token'])

    if os.path.exists(token_directory):
        shutil.rmtree(token_directory)


def validate():
    if CONFIG.get('api_type') not in ['REST', 'BULK']:
        # Raising error un-supported API_TYPE
        raise TapZohoException(
            "Found unsupported api_type: {}. Supported api_type are {}".format(CONFIG.get('api_type'), '[REST, BULK]'))


def main_impl():
    args = singer_utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    validate()
    initialize()
    try:
        zh = Zoho(
            config=CONFIG,
            default_start_date=CONFIG.get('start_date'),
            is_sandbox=CONFIG.get('sandbox'),
            api_type=CONFIG.get('api_type'))
        zh.login()

        if args.discover:
            do_discover(zh)
        elif args.properties:
            catalog = args.properties
            state = build_state(args.state, catalog)
            do_sync(zh, catalog, state)
    finally:
        LOGGER.info('Finally printing')
        do_cleanup()


def main():
    try:
        main_impl()
    except TapZohoQuotaExceededException as e:
        LOGGER.critical(e)
        sys.exit(2)
    except TapZohoException as e:
        LOGGER.critical(e)
        sys.exit(1)
    except Exception as e:
        LOGGER.critical(e)
        raise e
