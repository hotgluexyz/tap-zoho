# pylint: disable=protected-access
import csv
import operator
import singer
import json
import time
import tempfile
import zipfile
import os
import shutil
from singer import metrics

from tap_zoho.zoho.exceptions import TapZohoException

LOGGER = singer.get_logger()


def get_body(sobject=None, page=1, modified_time=None):
  if sobject == 'Events':
    return {
      "query": {
        "module": sobject,
        "page": page
      }
    }
  else:
    return {
      "query": {
        "module": sobject,
        "criteria": {
          "api_name": "Modified_Time",
          "comparator": "greater_than",
          "value": modified_time
        },
        "page": page
      }
    }


BATCH_STATUS_POLLING_SLEEP = 20
ITER_CHUNK_SIZE = 1024


class Bulk():
  bulk_url = 'https://www.zohoapis.com/crm/bulk/v2/read'
  job_status_uri = bulk_url + "/{job_id}"
  job_result_uri = bulk_url + "/{job_id}/result"

  message = "Job {job_id} {message} Job Creation Time {created_time}"

  def __init__(self, zh):
    self.zh = zh

  def _get_bulk_headers(self):
    return {"Authorization": "Zoho-oauthtoken " + self.zh.ZohoOAuthClient.get_access_token(
      userEmail=self.zh.config['currentUserEmail'])}

  def _create_job(self, sobject=None, state=None, catalog_entry=None):
    url = self.bulk_url
    modified_time = self.zh.get_start_date(state, catalog_entry)
    body = get_body(sobject, 1, modified_time)

    LOGGER.info('Starting job for stream {} with body {}'.format(sobject, body))

    headers = self._get_bulk_headers()
    headers['Content-Type'] = "application/json"

    with metrics.http_request_timer("create_job") as timer:
      timer.tags['sobject'] = sobject
      resp = self.zh._make_request(
        'POST',
        url,
        headers=headers,
        body=json.dumps(body))

    job = resp.json()
    resp = job['data'][0]

    job_id = resp['details']['id']
    created_time = resp['details']['created_time']
    message = resp['message']

    LOGGER.info(self.message.format(job_id=job_id, message=message, created_time=created_time))

    return job_id

  def _get_batch_status(self, job_id=None):
    url = self.job_status_uri.format(job_id=job_id)
    headers = self._get_bulk_headers()

    with metrics.http_request_timer("get_batch"):
      resp = self.zh._make_request('GET', url, headers=headers)

    batch = json.loads(resp.text)

    LOGGER.info('Batch status json {}'.format(json.dumps(batch)))

    return batch['data'][0]

  def _poll_on_job_id(self, job_id):
    batch_status = self._get_batch_status(job_id=job_id)
    LOGGER.info('Batch status is {state}'.format(state=batch_status['state']))
    while batch_status['state'] not in ['COMPLETED']:
      time.sleep(BATCH_STATUS_POLLING_SLEEP)
      LOGGER.info('Batch status is {state}'.format(state=batch_status['state']))
      batch_status = self._get_batch_status(job_id=job_id)

    return batch_status

  def get_batch_results(self, job_id, sobject):
    """Given a job_id, queries the job and saves csv file by extracting it from zip file."""
    headers = self._get_bulk_headers()
    url = self.job_result_uri.format(job_id=job_id)

    with tempfile.NamedTemporaryFile(mode="wb", delete=False) as zip_file:
      resp = self.zh._make_request('GET', url, headers=headers, stream=True)
      zip_file.write(resp.content)
      zip_file.close()

    with zipfile.ZipFile(zip_file.name, 'r') as zip_ref:
      tmp_dir = tempfile.gettempdir()
      path = os.path.join(tmp_dir, sobject)
      zip_ref.extractall(path)
      zip_file.close()
      json_message = {'message': 'CSV file is available at location {}'.format(path)}
      LOGGER.info(json.dumps(json_message, indent=4))
      csv_f = os.path.join(path, job_id + '.csv')

      with open(csv_f) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
        column_name_list = next(csv_reader)
        col_index = self.__get_index_by_column_name__(column_name_list, 'Modified_Time')
        csv_reader = sorted(csv_reader, key=operator.itemgetter(col_index), reverse=False)

        for line in csv_reader:
          rec = dict(zip(column_name_list, line))
          yield rec
        csv_file.close()

      try:
        if os.path.exists(path):
          shutil.rmtree(path)
      except Exception as ex:
        LOGGER.warning('Unable to cleanup temporary csv file {}'.format(path))

  @staticmethod
  def __get_index_by_column_name__(data_array=None, key=None):
    if data_array is None:
      data_array = []
    index = 0
    to_return = 0
    for _key in data_array:
      if _key == key:
        to_return = index
        break
      index += 1

    return to_return

  def get_data(self, sobject=None, state=None, catalog=None):
    job_id = self._create_job(sobject=sobject, state=state, catalog_entry=catalog) # '4212079000000395009'

    batch_status = self._poll_on_job_id(job_id)

    if batch_status['state'] == 'COMPLETED':
      for result in self.get_batch_results(job_id, sobject):
        yield result
    else:
      raise TapZohoException('An error occurred. Job Status {}'.format(json.dumps(batch_status)))

    # check if there are more records, fetch them as well and append them to the existing csv/results
