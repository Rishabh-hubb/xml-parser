import xml.etree.ElementTree as Xet
import pandas as pd
from io import StringIO
import boto3
import pandas as pd
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('result.log')  # create file handler which logs even debug messages
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
fh.setFormatter(formatter)
logger.debug("Starting the execution")

cols = ["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp",
        "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"]  # columns to provide in csv files

rows = []  # list of dictionaries to append in csv files

try:
    xmlparse = Xet.parse("DLTINS_20210117_01of01.xml")
    root = xmlparse.getroot()
    FindInstrm = root[1][0][0]
except:
    logger.debug("Index Out of Range or Filename is not correct")


# function to all values from xml file
def fetch_text(rows):
    for i in FindInstrm.findall("{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FinInstrm"):
        x = i[0][0]
        Id = x[0].text
        FullNm = x[1].text
        ClssfctnTp = x[3].text
        CmmdtyDerivInd = x[4].text
        NtnlCcy = x[5].text
        Issr = i[0][1].text
        rows.append({
            'FinInstrmGnlAttrbts.Id': Id,
            "FinInstrmGnlAttrbts.FullNm": FullNm,
            "FinInstrmGnlAttrbts.ClssfctnTp": ClssfctnTp,
            "FinInstrmGnlAttrbts.CmmdtyDerivInd": CmmdtyDerivInd,
            "FinInstrmGnlAttrbts.NtnlCcy": NtnlCcy,
            "Issr": Issr,

        })
    return rows


logger.debug("Function create_ execution starts.")


def create_csv(rows):                                                                 # function to convert list of dictionary to csv files
    df = pd.DataFrame(rows, columns=cols)
    df.to_csv("result.csv")
    create_bucket()                                                                # function to create aws s3 bucket.
    logger.debug("Function Executed Successfully.")


def create_bucket():
    try:
        file = pd.read_csv("result.csv")
        s3 = boto3.client("s3", aws_access_key_id="YOUR ACCESS KEY",
                          aws_secret_access_key="YOUR AWS SECRET ACCESS KEY")
        csv_buf = StringIO()
        file.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)
        s3.put_object(Bucket="steel-eye-csv", Body=csv_buf.getvalue(), Key="result.csv")
    except:
        logging.debug("Access key or Secret Key may be wrong.")
        logging.debug("Unidentifed Key in s3.")
        logging.debug("Bucket Does not exist.")


logging.debug("Function Run Successfully")
fetch_text(rows)
logging.debug("Function execution completed")
create_csv(rows)
