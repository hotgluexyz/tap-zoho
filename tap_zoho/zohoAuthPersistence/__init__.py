"""
Created on Dec 14, 2019

@author: Juned Jabbar
"""

import singer
import os
import tempfile
import singer.utils as singer_utils
import pickle
from zcrmsdk.OAuthUtility import ZohoOAuthConstants
from zcrmsdk.OAuthClient import ZohoOAuthTokens

from tap_zoho import REQUIRED_CONFIG_KEYS

LOGGER = singer.get_logger()


class ZohoAuthPersistence(object):
  '''
  This class deals with persistance of oauth related tokens in File
  '''

  def __init__(self):
    self.expiryTime = None
    self.args = singer_utils.parse_args(REQUIRED_CONFIG_KEYS)
    self.token_directory = os.path.join(tempfile.gettempdir(), self.args.config['refresh_token'])
    self.file_path = os.path.join(self.token_directory,
                                  ZohoOAuthConstants.PERSISTENCE_FILE_NAME)

  def save_oauthtokens(self, oAuthTokens):
    try:
      self.delete_oauthtokens(oAuthTokens.userEmail)
      if os.path.isfile(self.file_path):
        with open(self.file_path, 'ab') as fp:
          pickle.dump(oAuthTokens, fp, pickle.HIGHEST_PROTOCOL)
      else:
        with open(self.file_path, 'wb') as fp:
          pickle.dump(oAuthTokens, fp, pickle.HIGHEST_PROTOCOL)

    except Exception as ex:
      LOGGER.error("Exception occurred while saving oauthtokens into File {}".format(self.file_path), ex)
      raise ex

  def get_oauthtokens(self, userEmail=None):
    try:
      responseObj = ZohoOAuthTokens(None, None, None, None)
      if not os.path.isfile(self.file_path):
        return responseObj
      with open(self.file_path, 'rb') as fp:
        while True:
          try:
            oAuthObj = pickle.load(fp)
            responseObj = oAuthObj
            break
          except EOFError:
            break
      return responseObj
    except Exception as ex:
      LOGGER.error("Exception occurred while fetching oauthtokens from File {}".format(self.file_path), ex)
      raise ex

  def delete_oauthtokens(self, userEmail):
    try:
      if not os.path.isfile(self.file_path):
        return
      objectsToPreserve = []
      with open(self.file_path, 'rb') as fp:
        while True:
          try:
            oAuthObj = pickle.load(fp)
            objectsToPreserve.append(oAuthObj)
          except EOFError:
            break
      with open(self.file_path, 'wb') as fp:
        for eachObj in objectsToPreserve:
          pickle.dump(eachObj, fp, pickle.HIGHEST_PROTOCOL)

    except Exception as ex:
      LOGGER.error("Exception occurred while deleting oauthtokens from File {}".format(self.file_path), ex)
      raise ex
