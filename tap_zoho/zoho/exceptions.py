"""
Created on Dec 14, 2019

@author: Juned Jabbar
"""


class TapZohoException(Exception):
  pass


class TapZohoNotInitializedException(TapZohoException):
  pass


class TapZohoDataNotFoundException(TapZohoException):
  pass


class TapZohoQuotaExceededException(TapZohoException):
  pass


def get_error_message(ex):
  message = 'Error occurred for {url}. Error Code: {code} Error Code Desc: {code_desc} Error: Response ' \
            'error_content: {error_content}. Error Details:: {error_details}'

  ex = ex.args[0]
  url = ex is not None and ex.url or ''
  status_code = ex is not None and ex.status_code or 'UNKNOWN'
  error_content = ex is not None and ex.error_content or 'UNKNOWN Exception occurred'
  error_code_desc = ex is not None and ex.error_code or 'UNKNOWN'
  error_details = ex is not None and ex.error_details or ''

  return message.format(url=url,
                        code=status_code,
                        code_desc=error_code_desc,
                        error_content=error_content,
                        error_details=error_details)
