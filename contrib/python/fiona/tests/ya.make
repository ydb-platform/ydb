PY3TEST()

PEERDIR(
    contrib/python/fiona
    contrib/python/Shapely
)

NO_LINT()

TEST_SRCS(
    __init__.py
    conftest.py
    test__env.py
    test_bigint.py
    test_binary_field.py
#    test_bounds.py
#    test_bytescollection.py
#    test_collection.py
#    test_collection_crs.py
#    test_collection_legacy.py
#    test_compound_crs.py
    test_crs.py
#    test_cursor_interruptions.py
#    test_curve_geometries.py
#    test_data_paths.py
#    test_datetime.py
    test_driver_options.py
#    test_drivers.py
#    test_drvsupport.py
#    test_encoding.py
#    test_env.py
    test_feature.py
#    test_fio_bounds.py
#    test_fio_calc.py
#    test_fio_cat.py
#    test_fio_collect.py
#    test_fio_distrib.py
#    test_fio_dump.py
#    test_fio_filter.py
#    test_fio_info.py
#    test_fio_load.py
#    test_fio_ls.py
    test_fio_rm.py
#    test_geojson.py
#    test_geometry.py
#    test_geopackage.py
#    test_http_session.py
#    test_integration.py
#    test_layer.py
#    test_listing.py
#    test_logutils.py
#    test_memoryfile.py
#    test_meta.py
    test_model.py
#    test_multiconxn.py
#    test_non_counting_layer.py
#    test_open.py
#    test_profile.py
    test_props.py
#    test_read_drivers.py
    test_remove.py
#    test_revolvingdoor.py
#    test_rfc64_tin.py
    test_rfc3339.py
    test_schema.py
    test_schema_geom.py
#    test_session.py
#    test_slice.py
    test_subtypes.py
#    test_topojson.py
    test_transactions.py
    test_transform.py
#    test_unicode.py
    test_version.py
#    test_vfs.py
    test_write.py
)

END()
