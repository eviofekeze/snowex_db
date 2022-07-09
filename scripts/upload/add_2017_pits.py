"""
Read in the SnowEx 2017 profiles from pits.

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
    doi = "https://doi.org/10.5067/Q0310G1XULZS"

    # Obtain a list of Grand mesa pits
    data_dir = abspath('../download/data/SNOWEX/SNEX17_SnowPits.001/')
    summaries = glob.glob(join(data_dir, '*/*environment*.csv'))
    swes = glob.glob(join(data_dir, '*/*swe*.csv'))
    general = glob.glob(join(data_dir, '*/*GM_2017*.csv'))
    ignore = set(summaries + swes + general)

    # Upload Grand Mesa and Senator Beck
    for site_name, abbrev in [('Grand Mesa', 'GM'), ('Senator Beck', 'SBB')]:
        # Grab all the csvs in the pits folder
        filenames = glob.glob(join(data_dir, f'*/*{abbrev}*.csv'))

        # Grab all the site details files
        sites = glob.glob(join(data_dir, f'*/*{abbrev}*header*.csv'))

        # Remove the site details from the total file list to get only the
        profiles = list(set(filenames) - set(sites) - ignore)

        # Submit all profiles
        b = UploadProfileBatch(
            filenames=profiles,
            debug=debug,
            doi=doi,
            in_timezone='MST',
            site_name=site_name
        )
        b.push()

        # Upload all the site data
        s = UploadSiteDetailsBatch(
            sites,
            debug=debug,
            doi=doi,
            in_timezone='MST',
            site_name=site_name
        )
        s.push()

    return len(b.errors) + len(s.errors)


if __name__ == '__main__':
    main()
