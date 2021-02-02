"""
Created on Dec 14, 2019

@author: Juned Jabbar
"""

import singer
import zcrmsdk

from zcrmsdk import ZCRMRecord, ZCRMModule
from tap_zoho.sync import fix_tuple_keys
from tap_zoho.zoho.exceptions import TapZohoException, TapZohoDataNotFoundException

LOGGER = singer.get_logger()


class Rest():
    def __init__(self, zh):
        self.zh = zh

    # def execute(self):
    #   module_name = 'Accounts'
    #   authtoken = '0467d631f5cc9102afbef19e1019a7bf'
    #   params = {'authtoken': authtoken, 'scope': 'crmapi'}
    #
    #   url = "https://www.zohoapis.com/crm/private/json/" + module_name + "/getRecords"
    #
    #   resp = self.zh._make_request('GET', url, params=params)
    #
    #   return resp

    def get_data(self, sobject=None, state=None, catalog=None):
        to_return = []
        page = 1
        c_headers = {'If-Modified-Since': self.zh.get_start_date(state, catalog, True)}
        LOGGER.info('Adding custom headers as {}'.format(c_headers))
        try:
            module_ins = ZCRMModule.get_instance(sobject)
            # if state contains any is-modified variable which is to fetch modified data since the date.
            # pass custom_headers = {} in get records
            # get_records only returns 200 records per call, this function needs to be called iteratively
            # in order to fetch all records.
            resp = module_ins.get_records(custom_headers=c_headers, page=page, per_page=200, sort_by='Modified_Time',
                                          sort_order='asc')
            info = resp.response_json['info']
            to_return.extend(resp.response_json['data'])

            while True:
                if info['more_records']:
                    page += 1
                    resp = module_ins.get_records(custom_headers=c_headers, page=page, per_page=200,
                                                  sort_by='Modified_Time',
                                                  sort_order='asc')
                    info = resp.response_json['info']
                    to_return.extend(resp.response_json['data'])
                else:
                    break

            LOGGER.info('Normalizing data')
            for rec in to_return:
                if sobject == 'Invoices' or sobject == 'Sales_Orders':
                    LOGGER.info('Normalizing Product_Details for steam {} '.format(sobject))
                    # Here Product_Details can contain multiple line items, which needs to be converted in to separate
                    # objects.
                    product_details = rec['Product_Details']
                    for product in product_details:
                        product_id = product['product']['id']
                        product_record = ZCRMRecord.get_instance('Products', product_id).get()
                        product_record = product_record.response_json['data'][0]
                        product['Product_Category'] = product_record['Product_Category']
                        del product['id']
                        self.zh.append_product_record(rec, product)

                        owner = {'Owner': rec['Owner']}
                        created_by = {'Created_By': rec['Created_By']}
                        modified_by = {'Modified_By': rec['Modified_By']}
                        account_name = {'Account_Name': rec['Account_Name']}

                        del rec['Owner']
                        del rec['Created_By']
                        del rec['Modified_By']
                        del rec['Account_Name']

                        owner = fix_tuple_keys(owner)
                        rec['Owner'] = owner['Owner']

                        created_by = fix_tuple_keys(created_by)
                        rec['Created_By'] = created_by['Created_By']

                        modified_by = fix_tuple_keys(modified_by)
                        rec['Modified_By'] = modified_by['Modified_By']

                        account_name = fix_tuple_keys(account_name)
                        rec['Account_Name'] = account_name['Account_Name']
                        yield rec
                else:
                    # This will have data written to the singer target (target-csv) in this case.
                    rec = fix_tuple_keys(rec)
                    yield rec

            return to_return

        except zcrmsdk.ZCRMException as ex:
            if ex.error_code == 'No Content':
                raise TapZohoDataNotFoundException(ex)
            elif ex.error_code == 'Not Modified':
                raise TapZohoDataNotFoundException(ex)
            else:
                raise TapZohoException(ex)
