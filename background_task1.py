"""
Some examples of background task
"""
import time
from test_watchdog_svc_flask import BaseBackgroundTask

# virustotal-python APIv2
from virustotal_python import Virustotal as vt_v2

# https://virustotal.github.io/vt-py/
import vt as virustotal  # APIv3


class CustomBackgroundTask(BaseBackgroundTask):
    
    def _execute(self):
        time.sleep(self._t)

class VirusTotalScanTask(BaseBackgroundTask):
    def __init__(self, argdict):
        self._api_token = argdict['api_token']
        self._api_v2 = None
        self._api_v3 = None
        super().__init__(argdict)

    def execute(self):
        self._pre_exec()
        try:
            self._api_v2 = vt_v2(self._api_token)
            self._api_v3 = virustotal.Client(self._api_token)
            # Lookup before scanning
            try:
                _file = self._api_v3.get_object("/files/<HASHID>")
                if _file.sha256 != "<HASHID>":
                    print("sha256 not equal!")
            except virustotal.APIError as e:
                print("Exception: ", e)
            resp_v2 = self._api_v2.file_report(["<HASHID>"])
            if (resp_v2['json_resp']['response_code'] == 0):
                raise Exception(resp_v2['json_resp']['verbose_msg'])
            #resp_v3 = self._api_v3.get_object("/files/<HASHID>")

            # Scanning
            with open('uniq_ids.txt', 'rb') as f:
                analysis = self._api_v3.scan_file(f, wait_for_completion=False)

            while True:
                analysis = self._api_v3.get_object("/analyses/{}", analysis.id)
                if (analysis.status == 'completed'):
                    break
                print('zzz...')
                time.sleep(5)
            pprint(analysis.to_dict())

        except Exception as e:
            print('Exception: ', e)
            self._on_error(e)
        else:
            self._on_success()
        finally:
            if self._api_v3:
                self._api_v3.close()
        msg = '[%d] done. (has_error = %s)' % (self._id, self._has_error)
        self._post_exec()
        return (not self._has_error, self._item)
