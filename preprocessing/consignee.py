import pandas as pd
import unicodedata
import re

df = pd.read_csv('LeviStrauss-20160501-20170430.csv', index_col=False)

def preprocessConsignee(consignee):
    consignee = unicodedata.normalize('NFKD', consignee).encode('ascii', 'ignore').decode().lower()
    regex = re.compile('[^a-z]')
    consignee = regex.sub('', consignee)
    return consignee

df['consignee'].apply(preprocessConsignee)
