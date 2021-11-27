from io import BytesIO
import pandas as pd
from zipfile import ZipFile
import requests
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:password@localhost:5432/shred")

hh_r = requests.get(
    "https://www2.census.gov/programs-surveys/acs/data/pums/2019/1-Year/csv_hor.zip"
)

p_r = requests.get(
    "https://www2.census.gov/programs-surveys/acs/data/pums/2019/1-Year/csv_por.zip"
)

hh_file = ZipFile(BytesIO(hh_r.content))
p_file = ZipFile(BytesIO(p_r.content))

hh_pums_csv = hh_file.open("psam_h41.csv")
p_pums_csv = p_file.open("psam_p41.csv")


or_household_pums_2019 = pd.read_csv(hh_pums_csv)
or_persons_pums_2019 = pd.read_csv(p_pums_csv)

or_household_pums_2019.to_sql("or_pums_hh_2019", engine)
or_persons_pums_2019.to_sql("or_pums_p_2019", engine)

print("done")
