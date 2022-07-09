"""
Uploads the Snowex 2017 manual depths to the database

1. Data must be downloaded via sh ../download/download_nsidc.sh
2A. python run.py # To run all together all at once
2B. python add_snow_depths.py # To run individually
"""

import glob
import time
from os.path import abspath, join

from snowexsql.db import get_db
from snowex_db.upload import *


def main():

    # Read in the Grand Mesa Snow Depths Data
    data_dir = abspath('../download/data/SNOWEX/SNEX17_SD.001/2017.02.06/')
    meta = [('Grand Mesa', 'GM', 26912), ('Senator Beck', 'SBB', 26913)]

    errors = 0

    db_name = 'localhost/snowex'
    engine, session = get_db(db_name, credentials='./credentials.json')

    refs = glob.glob(join(data_dir, '*reference*.csv'))
    comments = glob.glob(join(data_dir, '*comments*.csv'))

    for site_name, abbrev, epsg in meta:
        standard_probe = glob.glob(join(data_dir, f'*SD_{abbrev}*transect_2017*.csv'))
        standard_probe = list(set(standard_probe) - set(refs) - set(comments))

        magna_probes = glob.glob(join(data_dir, f'*SD_{abbrev}*transect_MP_*.csv'))
        magna_probes = list(set(magna_probes) - set(refs) - set(comments))

        # Submit the magnaprobes
        for files, instrument in [(magna_probes, 'magnaprobe'), (standard_probe, 'standard probe')]:
            for f in files:
                csv = PointDataCSV(
                    f,
                    depth_is_metadata=False,
                    units='cm',
                    site_name=site_name,
                    in_timezone='US/Mountain',
                    epsg=epsg,
                    doi="https://doi.org/10.5067/WKC6VFMT7JTF",
                    instrument=instrument
                )
                csv.submit(session)
                errors += len(csv.errors)
    session.close()
    return errors


if __name__ == '__main__':
    main()
