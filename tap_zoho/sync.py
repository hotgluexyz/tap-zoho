"""
Created on Dec 14, 2019

@author: Juned Jabbar
"""

import time
import singer
import singer.utils as singer_utils
import copy
from tap_zoho.zoho.exceptions import TapZohoDataNotFoundException, get_error_message
from singer import Transformer, metadata

LOGGER = singer.get_logger()


# pylint: disable=unused-argument
def transform_bulk_data_hook(data, typ, schema):
  result = data
  # Zoho can return the value '0.0' for integer typed fields. This
  # causes a schema violation. Convert it to '0' if schema['type'] has
  # integer or number.
  if data == '0.0' and 'integer' in schema.get('type', []):
    result = '0'

  # Zoho can return the value 'None' (undefined/empty) for integer typed fields. This
  # causes a schema violation. Convert it to '0' if schema['type'] has
  # integer or number.
  if data is None and ('integer' in schema.get('type', []) or 'number' in schema.get('type', [])):
    result = '0'

  if data == "" and "null" in schema['type']:
    result = None

  if data is None and ('string' in schema.get('type', []) or 'date-time' in schema.get('type', [])):
    result = ''

  return result


def fix_tuple_keys(rec):
  to_return = copy.copy(rec)
  if isinstance(rec, dict):
    for item in rec.items():
      if isinstance(item, tuple):
        key = None
        tuple_value = None
        for value in item:
          if not isinstance(value, dict):
            key = value
          elif key is not None:
            for _item_key in value.keys():
              if tuple_value is None:
                tuple_value = {}
              tuple_value[_item_key] = value[_item_key]

          if tuple_value is not None and key is not None:
            rec[key] = tuple_value
            to_return = rec
  return to_return


def get_stream_version(catalog_entry, state):
  tap_stream_id = catalog_entry['tap_stream_id']
  catalog_metadata = metadata.to_map(catalog_entry['metadata'])
  replication_key = catalog_metadata.get((), {}).get('replication-key')

  if singer.get_bookmark(state, tap_stream_id, 'version') is None:
    stream_version = int(time.time() * 1000)
  else:
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

  if replication_key:
    return stream_version
  return int(time.time() * 1000)


def sync_records(zh, catalog_entry, state, counter):
  stream = catalog_entry['stream']
  schema = catalog_entry['schema']
  stream_alias = catalog_entry.get('stream_alias')
  catalog_metadata = metadata.to_map(catalog_entry['metadata'])
  replication_key = catalog_metadata.get((), {}).get('replication-key')
  stream_version = get_stream_version(catalog_entry, state)
  activate_version_message = singer.ActivateVersionMessage(stream=(stream_alias or stream),
                                                           version=stream_version)

  start_time = singer_utils.now()

  LOGGER.info('Syncing Zoho data for stream {}'.format(stream))

  try:
    for rec in zh.get_data(sobject=stream, state=state, catalog=catalog_entry):
      counter.increment()
      if zh.api_type == 'REST':
        with Transformer(pre_hook=transform_bulk_data_hook) as transformer:
          rec = transformer.transform(rec, schema)
      singer.write_message(
        singer.RecordMessage(
          stream=(
            stream_alias or stream),
          record=rec,
          version=stream_version,
          time_extracted=start_time))

      replication_key_value = replication_key and singer_utils.strptime_with_tz(rec[replication_key])

      if replication_key_value and replication_key_value <= start_time:
        state = singer.write_bookmark(
          state,
          catalog_entry['tap_stream_id'],
          replication_key,
          rec[replication_key])
        singer.write_state(state)

        # Tables with no replication_key will send an
        # activate_version message for the next sync
    if not replication_key:
      singer.write_message(activate_version_message)
      state = singer.write_bookmark(
        state, catalog_entry['tap_stream_id'], 'version', None)

  except TapZohoDataNotFoundException as data_not_found_ex:
    LOGGER.warning('No data found for stream {} error msg {} '.format(stream, get_error_message(data_not_found_ex)))

  if not replication_key:
    singer.write_message(activate_version_message)
    state = singer.write_bookmark(
      state, catalog_entry['tap_stream_id'], 'version', None)
