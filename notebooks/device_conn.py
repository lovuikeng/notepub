# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
#     "data-utils==0.1.0",
#     "altair==5.5.0",
# ]
#
# [tool.uv.sources]
# data-utils = { path = "./data_utils", editable = true }
# ///

import marimo

__generated_with = "0.11.10"
app = marimo.App(width="full")


@app.cell
def _(f_df, mo):
    import altair as alt
    chart = mo.ui.altair_chart(alt.Chart(f_df.value).mark_point().encode(
        y = 'count()',
        x = 'timestamp',
        color = 'conn_mode',
        shape = 'nats',
    ))
    chart
    return alt, chart


@app.cell
def _(chart, mo):
    mo.ui.table(chart.value, page_size=20)
    return


@app.cell
def _(df, mo):
    f_df = mo.ui.dataframe(df, page_size=20)
    f_df
    return (f_df,)


@app.cell
def _(df, mo):
    mo.ui.data_explorer(df)
    return


@app.cell
def _(csv_path, dtl):
    csv_source = csv_path
    if "github" in csv_path:
        import requests
        from io import StringIO

        response = requests.get(csv_path)
        data = response.text
        csv_source = StringIO(data)

    df = dtl.build_devicce_conn_df(csv_source)
    return (df,)


@app.cell
async def _(mo):
    csv_path = str(mo.notebook_location() / "public" / "device_conn.csv")
    if "http" in csv_path:
        print("wasm")
        whl = str(mo.notebook_location() / "public" / "data_utils-0.1.0-py3-none-any.whl")
        import micropip
        await micropip.install(whl)
    from data_utils import ( dtl )
    return csv_path, dtl, micropip, whl


@app.cell
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()
