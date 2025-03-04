from data_utils import dtl

def test_build_devicce_conn_df():
    df = dtl.build_devicce_conn_df("../../../notebooks/public/device_conn.csv")
    assert df is not None
