"""
Script is used to confirm uploads to the db were successful
"""

from snowexsql.db import get_db
from snowexsql.data import SiteData, PointData, LayerData, ImageData
import pytest
from sqlalchemy.sql import func


@pytest.fixture(scope='session')
def session():
    db = 'snow:hackweek@db.snowexdata.org/snowex'
    engine, session = get_db(db)
    yield session
    session.close()


def build_query(session, base, filters, use_distinct):
    """
    Function to return a query.
    Args:
        base: Args to the query function e.g. ImageData.raster
        filters: List of boolean actions to chain filter a query
        use_distinct: Bool whether to use distinct at the end of a query
    Returns:
        qry: Sqlaclchemy qery object
    """
    qry = session.query(base)

    for f in filters:
        qry = qry.filter(f)
    if use_distinct:
        qry = qry.distinct().order_by(base)
    return qry


@pytest.mark.parametrize("base, filters, use_distinct, execute, expected", [
    (ImageData.raster, [ImageData.type == 'swe'], False, 'count', 4),
    (ImageData.raster, [ImageData.type == 'depth'], False, 'count', 840),
    (ImageData.type, [], True, 'all', [('depth',), ('swe',)])
])
def test_add_aso(session, base, filters, use_distinct, execute, expected):
    """
    Test some attributes of the ASO uploader script
    """
    # Always use ASO
    filters.insert(0, ImageData.observers == 'ASO Inc.')
    qry = build_query(session, base, filters, use_distinct)
    results = getattr(qry, execute)()

    assert results == expected


@pytest.mark.parametrize("base, filters, use_distinct, execute, expected", [
    (PointData.value, [PointData.type == 'two_way_travel'], False, 'count', 1264905),
    (PointData.type, [], True, 'all', [('depth',), ('swe',), ('two_way_travel',)]),
])
def test_add_bsu_gpr(session, base, filters, use_distinct, execute, expected):
    """
    Test some results of the BSU GPR uploader script
    """
    # Always use ASO
    filters.insert(0, PointData.observers == 'Tate Meehan')
    qry = build_query(session, base, filters, use_distinct)
    results = getattr(qry, execute)()

    assert results == expected


@pytest.mark.parametrize("base, filters, use_distinct, execute, expected", [
    (PointData.value, [PointData.type == 'depth'], False, 'count', 155440),
    (PointData.type, [], True, 'all', [('depth',), ('swe',), ('two_way_travel',)]),
])
def test_add_csu_gpr(session, base, filters, use_distinct, execute, expected):
    """
    Test some results of the CSU GPR uploader script
    """
    # Always use ASO
    filters.insert(0, PointData.observers == 'Randall Bonnell')
    qry = build_query(session, base, filters, use_distinct)
    results = getattr(qry, execute)()

    assert results == expected


@pytest.mark.parametrize("base, filters, use_distinct, execute, expected", [
    (ImageData.raster, [ImageData.type == 'dem'], False, 'count', 1),
])
def test_add_gm_snow_off(session, base, filters, use_distinct, execute, expected):
    """
    Test some attributes of the ASO uploader script
    """
    # Always use ASO
    filters.insert(0, ImageData.observers == 'USGS')
    qry = build_query(session, base, filters, use_distinct)
    results = getattr(qry, execute)()

    assert results == expected


@pytest.mark.parametrize("base, filters, use_distinct, execute, expected", [
    # Confirm this pit id has only 1 set of density profiles (9 layers)
    (LayerData.value, [LayerData.type == 'density', LayerData.pit_id == 'COGMTLSFL2A_20200210'], False, 'count', 18), # There is double density profiles 1 for density csv and the other for LWC
])
def test_add_iop_pits(session, base, filters, use_distinct, execute, expected):
    """
    Test to validate the upload of the IOP Pits
    """
    filters.insert(0, LayerData.site_name == 'Grand Mesa')
    qry = build_query(session, base, filters, use_distinct)
    results = getattr(qry, execute)()

    assert results == expected


@pytest.mark.parametrize("base, filters, use_distinct, execute, expected", [
    # Confirm this pit id has only 1 set of density profiles (9 layers)
    (PointData.utm_zone, [], True, 'all', [(12,), (13,)]),
])
def test_add_snow_poles(session, base, filters, use_distinct, execute, expected):
    """
    Test to validate the upload of the the camera timeseries
    """
    filters.insert(0, PointData.site_name == 'Grand Mesa')
    filters.insert(1, PointData.instrument == 'camera')
    qry = build_query(session, base, filters, use_distinct)
    results = getattr(qry, execute)()

    assert results == expected
