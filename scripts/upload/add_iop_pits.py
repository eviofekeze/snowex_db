"""
Read in the SnowEx 2020 profiles from pits.

1. Data must be downloaded via sh ../download/download_nsidc.sh
2A. python run.py # To run all together all at once
2B. python add_profiles.py # To run individually
"""

import glob
from os import listdir
from os.path import abspath, basename, join, relpath

from snowex_db.batch import UploadProfileBatch, UploadSiteDetailsBatch


def main():
    debug = True
    doi = "https://doi.org/10.5067/DUD2VZEVBJ7S"

    # Obtain a list of Grand mesa pits
    data_dir = abspath('../download/data/SNOWEX/SNEX20_GM_SP.001')

    # Grab all the site details files
    sites = glob.glob(join(data_dir, '*/*site*.csv'))
    summaries = glob.glob(join(data_dir, '*/*Summary*.csv'))

    # Match up the keywords with instruments
    meta = [('temperature', 'digital thermometer'), ('density', 'snowmetrics density sampler'),
            ('stratigraphy', 'manual'),
            ('LWC', 'A2 photonics LWC sensor')]
    for kw, instrument in meta:

        # Remove the site details from the profiles of interest
        profiles = glob.glob(join(data_dir, f'*/*{kw}*.csv'))

        b = UploadProfileBatch(
            filenames=profiles,
            debug=debug,
            doi=doi,
            instrument=instrument,
            in_timezone='MST'
        )
        b.push()

    # # Upload all the site data
    # s = UploadSiteDetailsBatch(
    #     sites,
    #     debug=debug,
    #     doi=doi,
    #     in_timezone='MST'
    # )
    # s.push()

    return len(b.errors)  # + len(s.errors)


if __name__ == '__main__':
    main()
