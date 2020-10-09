# connnect-to-oracle

import cx_Oracle
import pandas as pd
from collections import defaultdict
from datetime import timedelta
import operator
import numpy as np
import tqdm
CONN_INFO_wh = {
    'host': '',
    'port': ,
    'user': '',
    'psw': '',
    'service': 'cdcxasdadwhp_ro.gecwalmart.com',
}
CONN_INFO_prod = {
    'host': '',
    'port': ,
    'user': '',
    'psw': '',
    'service': '',
}
CONN_STR = '{user}/{psw}@{host}:{port}/{service}'.format(**CONN_INFO_wh)
